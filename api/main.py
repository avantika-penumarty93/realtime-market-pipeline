"""
FastAPI observability service for the real-time market pipeline.

Endpoints:
  GET /health  — liveness + DB connectivity check
  GET /stats   — throughput, p99 latency, error rate, DLQ count (all from DB)

Stats are computed over a rolling 60-second window so numbers stay current
without any in-memory state that could drift on restart.
"""
import asyncio
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional

import asyncpg
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("api")

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/marketdata",
)
DB_CONNECT_RETRIES = 30
DB_CONNECT_BACKOFF_S = 2

_pool: Optional[asyncpg.Pool] = None


# ── Startup / shutdown ─────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _pool
    for attempt in range(1, DB_CONNECT_RETRIES + 1):
        try:
            _pool = await asyncpg.create_pool(
                DATABASE_URL,
                min_size=2,
                max_size=10,
                command_timeout=10,
            )
            logger.info("DB pool ready")
            break
        except Exception as exc:
            logger.warning("DB pool attempt %d/%d: %s", attempt, DB_CONNECT_RETRIES, exc)
            if attempt < DB_CONNECT_RETRIES:
                await asyncio.sleep(DB_CONNECT_BACKOFF_S)
    else:
        raise RuntimeError("Could not create DB pool")

    yield

    if _pool:
        await _pool.close()
        logger.info("DB pool closed")


app = FastAPI(
    title="Market Pipeline API",
    version="1.0.0",
    description="Observability endpoints for the real-time BTC/USDT data pipeline",
    lifespan=lifespan,
)


# ── Response models ────────────────────────────────────────────────────────────

class HealthResponse(BaseModel):
    status: str          # "ok" | "degraded"
    timestamp: str
    db: str              # "ok" | "error"


class StatsResponse(BaseModel):
    throughput_msgs_per_sec: float   # valid trades processed in last 60s / 60
    error_rate: float                # dlq_1m / (trades_1m + dlq_1m)
    p99_latency_ms: float            # 99th-pct of (received_at - trade_time) ms
    dlq_count_total: int             # all-time DLQ row count
    trades_last_60s: int
    dlq_last_60s: int


# ── Endpoints ──────────────────────────────────────────────────────────────────

@app.get("/health", response_model=HealthResponse, tags=["ops"])
async def health() -> HealthResponse:
    db_status = "error"
    try:
        async with _pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        db_status = "ok"
    except Exception as exc:
        logger.warning("Health DB check failed: %s", exc)

    return HealthResponse(
        status="ok" if db_status == "ok" else "degraded",
        timestamp=datetime.now(timezone.utc).isoformat(),
        db=db_status,
    )


@app.get("/stats", response_model=StatsResponse, tags=["ops"])
async def stats() -> StatsResponse:
    try:
        async with _pool.acquire() as conn:
            trades_row = await conn.fetchrow(
                """
                SELECT
                    COUNT(*)::BIGINT AS cnt,
                    PERCENTILE_CONT(0.99) WITHIN GROUP (
                        ORDER BY
                            EXTRACT(EPOCH FROM (received_at - time)) * 1000.0
                    ) AS p99_ms
                FROM trades
                WHERE time > NOW() - INTERVAL '60 seconds'
                """
            )
            dlq_row = await conn.fetchrow(
                """
                SELECT COUNT(*)::BIGINT AS cnt
                FROM dead_letter_trades
                WHERE received_at > NOW() - INTERVAL '60 seconds'
                """
            )
            total_dlq = await conn.fetchval(
                "SELECT COUNT(*)::BIGINT FROM dead_letter_trades"
            )
    except asyncpg.PostgresError as exc:
        logger.error("Stats query failed: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database unavailable",
        )

    trades_1m = int(trades_row["cnt"] or 0)
    dlq_1m = int(dlq_row["cnt"] or 0)
    p99_ms = float(trades_row["p99_ms"] or 0.0)

    total_msgs = trades_1m + dlq_1m
    error_rate = dlq_1m / total_msgs if total_msgs > 0 else 0.0
    throughput = trades_1m / 60.0

    return StatsResponse(
        throughput_msgs_per_sec=round(throughput, 4),
        error_rate=round(error_rate, 6),
        p99_latency_ms=round(p99_ms, 2),
        dlq_count_total=int(total_dlq or 0),
        trades_last_60s=trades_1m,
        dlq_last_60s=dlq_1m,
    )
