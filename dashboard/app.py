"""
Streamlit dashboard for real-time BTC/USDT market pipeline monitoring.
Auto-refreshes every 2 seconds by sleeping then calling st.rerun().

Layout:
  Row 1 — four KPI tiles: ingestion rate, p99 latency, error rate, DLQ count
  Row 2 — price + VWAP line chart (left), volume bar chart (right)
  Row 3 — anomaly table (collapsed unless anomalies present)
  Row 4 — DLQ detail expander
"""
import os
import time
from datetime import datetime

import numpy as np
import pandas as pd
import psycopg2
import requests
import streamlit as st

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/marketdata",
)
API_URL = os.getenv("API_URL", "http://api:8000")
REFRESH_INTERVAL_S = 2
CHART_WINDOW_MINUTES = 5
SLA_P99_MS = 200.0
SLA_ERROR_RATE = 0.01

st.set_page_config(
    page_title="BTC Market Pipeline",
    layout="wide",
    initial_sidebar_state="collapsed",
)


# ── Data fetchers ──────────────────────────────────────────────────────────────

@st.cache_resource(ttl=None)
def _db_engine() -> psycopg2.extensions.connection:
    """Single persistent connection, reused across reruns (cache_resource)."""
    return psycopg2.connect(DATABASE_URL)


def _get_stats() -> dict | None:
    try:
        r = requests.get(f"{API_URL}/stats", timeout=2)
        r.raise_for_status()
        return r.json()
    except Exception:
        return None


def _get_trades() -> pd.DataFrame:
    try:
        conn = _db_engine()
        df = pd.read_sql(
            f"""
            SELECT time, price, quantity, vwap_1m, is_anomaly
            FROM trades
            WHERE time > NOW() - INTERVAL '{CHART_WINDOW_MINUTES} minutes'
            ORDER BY time ASC
            """,
            conn,
        )
        return df
    except Exception:
        try:
            _db_engine.clear()
        except Exception:
            pass
        return pd.DataFrame()


def _get_recent_dlq() -> pd.DataFrame:
    try:
        conn = _db_engine()
        df = pd.read_sql(
            """
            SELECT received_at, reason, raw_data
            FROM dead_letter_trades
            ORDER BY received_at DESC
            LIMIT 20
            """,
            conn,
        )
        return df
    except Exception:
        return pd.DataFrame()


# ── Page render ────────────────────────────────────────────────────────────────

st.title("BTC/USDT Real-Time Market Pipeline")
st.caption(f"Updated {datetime.now().strftime('%H:%M:%S')} — auto-refresh every {REFRESH_INTERVAL_S}s")

stats = _get_stats()
trades_df = _get_trades()

# ── KPI row ────────────────────────────────────────────────────────────────────

c1, c2, c3, c4 = st.columns(4)

with c1:
    if stats:
        st.metric(
            "Ingestion Rate",
            f"{stats['throughput_msgs_per_sec']:.2f} msg/s",
        )
    else:
        st.metric("Ingestion Rate", "—")

with c2:
    if stats:
        p99 = stats["p99_latency_ms"]
        delta_str = "⚠ above SLA" if p99 > SLA_P99_MS else None
        delta_color = "inverse" if p99 > SLA_P99_MS else "normal"
        st.metric(
            "p99 Latency",
            f"{p99:.1f} ms",
            delta=delta_str,
            delta_color=delta_color,
        )
    else:
        st.metric("p99 Latency", "—")

with c3:
    if stats:
        er = stats["error_rate"]
        delta_str = "⚠ above SLA" if er > SLA_ERROR_RATE else None
        delta_color = "inverse" if er > SLA_ERROR_RATE else "normal"
        st.metric(
            "Error Rate",
            f"{er * 100:.3f}%",
            delta=delta_str,
            delta_color=delta_color,
        )
    else:
        st.metric("Error Rate", "—")

with c4:
    if stats:
        st.metric("DLQ (total)", f"{stats['dlq_count_total']:,}")
    else:
        st.metric("DLQ (total)", "—")

st.divider()

# ── Charts ─────────────────────────────────────────────────────────────────────

if not trades_df.empty:
    trades_df["time"] = pd.to_datetime(trades_df["time"], utc=True)

    col_price, col_vol = st.columns([3, 1])

    with col_price:
        st.subheader(f"Price & 1-min VWAP — last {CHART_WINDOW_MINUTES} min")
        chart_df = (
            trades_df[["time", "price", "vwap_1m"]]
            .rename(columns={"price": "Price", "vwap_1m": "VWAP 1m"})
            .set_index("time")
        )
        st.line_chart(chart_df, color=["#00C6FF", "#FF6B35"])

    with col_vol:
        st.subheader("Volume")
        vol_df = trades_df[["time", "quantity"]].rename(
            columns={"quantity": "Volume"}
        ).set_index("time")
        st.bar_chart(vol_df, color=["#9B59B6"])

    # ── Anomaly table ──────────────────────────────────────────────────────────
    anomalies = trades_df[trades_df["is_anomaly"] == True].copy()
    if not anomalies.empty:
        st.warning(
            f"**{len(anomalies)} price anomaly event(s)** detected in last "
            f"{CHART_WINDOW_MINUTES} minutes (>2% single-tick move)"
        )
        st.dataframe(
            anomalies[["time", "price", "vwap_1m"]]
            .rename(columns={"price": "Price", "vwap_1m": "VWAP", "time": "Time"})
            .tail(10)
            .reset_index(drop=True),
            use_container_width=True,
        )

else:
    st.info("Waiting for trade data to arrive…")

# ── DLQ detail ─────────────────────────────────────────────────────────────────

if stats:
    dlq_1m = stats.get("dlq_last_60s", 0)
    trades_1m = stats.get("trades_last_60s", 0)
    with st.expander(f"Dead Letter Queue — {dlq_1m} bad ticks in last 60s ({trades_1m} valid)"):
        dlq_df = _get_recent_dlq()
        if dlq_df.empty:
            st.write("No DLQ records yet.")
        else:
            st.dataframe(
                dlq_df[["received_at", "reason"]],
                use_container_width=True,
            )

# ── System status footer ───────────────────────────────────────────────────────

st.divider()
if stats is None:
    st.error("API unreachable — pipeline may be starting up")

# ── Auto-refresh ───────────────────────────────────────────────────────────────

time.sleep(REFRESH_INTERVAL_S)
st.rerun()
