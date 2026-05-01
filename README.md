# Real-Time Market Data Pipeline

Production-grade pipeline that ingests the Binance BTC/USDT live trade stream,
validates and deduplicates every tick, computes rolling VWAP, detects price
anomalies, and serves clean data through a REST API and a live Streamlit
dashboard — with full observability at every stage.

---

## System Architecture

```
                           ┌─────────────────────────────────────────────────┐
                           │              docker-compose network             │
                           │                                                 │
  ┌──────────────────┐     │  ┌───────────────────────────────────────────┐  │
  │  Binance WSS     │     │  │              ingester.py                  │  │
  │  BTCUSDT @trade  │─────┼──►  • schema validation (Pydantic)           │  │
  │  ~5-10 msg/s     │     │  │  • dedup  (10k-entry bounded ring-buffer) │  │
  └──────────────────┘     │  │  • out-of-order detection                 │  │
                           │  │  • 5s auto-reconnect on disconnect        │  │
                           │  └──────────────┬───────────────┬────────────┘  │
                           │                 │ valid         │ bad tick      │
                           │          ┌──────▼──────┐  ┌─────▼─────────┐     │
                           │          │   trades    │  │  dead-letter  │     │
                           │          │  (Kafka)    │  │   (Kafka)     │     │
                           │          └──────┬──────┘  └────┬──────────┘     │
                           │                 │               │               │
                           │          ┌──────▼───────────────▼────────────┐  │
                           │          │           processor.py            │  │
                           │          │  • rolling 1-min VWAP             │  │
                           │          │  • anomaly detection (>2% move)   │  │
                           │          │  • idempotent DB writes           │  │
                           │          └──────────────┬─────────────────── ┘  │
                           │                         │                       │
                           │          ┌──────────────▼───────────────────┐   │
                           │          │         TimescaleDB              │   │
                           │          │  trades hypertable (by time)     │   │
                           │          │  dead_letter_trades table        │   │
                           │          └──────────────┬───────────────────┘   │
                           │                         │                       │
                           │          ┌──────────────▼───────────────────┐   │
                           │          │          api/main.py             │   │
                           │          │  GET /health  GET /stats         │   │
                           │          │  (p99 latency via PERCENTILE_CONT│   │
                           │          │   computed from DB in real-time) │   │
                           │          └──────────────┬───────────────────┘   │
                           │                         │                       │
                           │          ┌──────────────▼───────────────────┐   │
                           │          │       dashboard/app.py           │   │
                           │          │  Streamlit — auto-refresh 2s     │   │
                           │          │  price chart · VWAP · DLQ count  │   │
                           │          └──────────────────────────────────┘   │
                           └─────────────────────────────────────────────────┘
```

---

## Problem Statement

Market data arrives as a high-throughput WebSocket stream. Without explicit
validation:

- **Bad ticks** (price spikes, exchange glitches) corrupt VWAP and downstream
  analytics silently.
- **Duplicate trade IDs** (exchange retransmissions) double-count volume.
- **Out-of-order events** (network reordering) break time-series assumptions.

This pipeline makes bad data *visible and traceable*: every rejected tick lands
in a tagged Dead Letter Queue with a machine-readable reason, never silently
discarded.

---

## Success Criteria

| Criterion | Target | How verified |
|---|---|---|
| Ingestion latency p99 | < 200 ms | `GET /stats` → `p99_latency_ms` field |
| Message loss under normal conditions | Zero | Kafka `acks=all` + idempotent producer + `ON CONFLICT DO NOTHING` in DB |
| WebSocket reconnect time | ≤ 5 seconds | `RECONNECT_DELAY_S = 5` constant in ingester; observable in logs |
| Bad data handling | Always tagged + routed to DLQ | Unit-testable: any `ValidationError`, duplicate ID, or out-of-order timestamp goes to `dead-letter` topic, never to `trades` |
| DLQ rate under normal market conditions | < 1% | `GET /stats` → `error_rate` field (= `dlq_last_60s / total_last_60s`) |

---

## Design Decisions

### Ingester deduplication: bounded ring-buffer vs. external store
A `deque(maxlen=10_000)` + set gives O(1) lookup without a Redis dependency.
The window covers the last 10,000 trade IDs (~30 minutes of BTC stream at
~5 msg/s). Exchange retransmissions within this window are caught; older
duplicates are unlikely in practice and are caught at the DB layer by the
unique index.

### Out-of-order detection: monotonic comparison, not windowing
Strict monotonic comparison is intentionally conservative — any tick with a
trade timestamp earlier than the previous one is routed to DLQ. Under normal
conditions this never fires. If relaxed reordering tolerance is needed, the
threshold can be made configurable.

### VWAP in processor, not DB
Computing VWAP in the processor (sliding deque) keeps the DB insert simple and
avoids expensive window queries on every write. The stored `vwap_1m` column is
a snapshot of the 1-min window *at the time of ingestion*, usable directly in
charts without recalculation.

### p99 latency via `PERCENTILE_CONT` in PostgreSQL
`received_at − time` (ingester receive time minus exchange trade time) captures
the true end-to-end ingestion latency including network round-trip. Computing
it directly in the DB means the API has no in-memory state to drift and
restarts produce correct numbers immediately.

### kafka-init service
A dedicated one-shot service creates both topics with explicit retention and
partition counts before any producer or consumer starts. This eliminates
auto-create races where a consumer can create a topic with wrong defaults if
it starts first.

### TimescaleDB hypertable
Automatic time-based partitioning keeps range queries on `time` fast without
manual partition management. The unique index on `(symbol, trade_id)` acts as a
second deduplication layer at the storage level.

---

## Repository Layout

```
realtime-market-pipeline/
├── init.sql              — TimescaleDB schema (hypertable + indexes)
├── docker-compose.yml    — Full stack orchestration
├── ingester/
│   ├── ingester.py       — WebSocket → Kafka producer
│   ├── Dockerfile
│   └── requirements.txt
├── processor/
│   ├── processor.py      — Kafka consumer → TimescaleDB
│   ├── Dockerfile
│   └── requirements.txt
├── api/
│   ├── main.py           — FastAPI /health + /stats
│   ├── Dockerfile
│   └── requirements.txt
└── dashboard/
    ├── app.py            — Streamlit live dashboard
    ├── Dockerfile
    └── requirements.txt
```

---

## How to Run

**Prerequisites:** Docker + Docker Compose v2, internet access for Binance WSS.

```bash
# Clone and start everything
git clone <repo>
cd realtime-market-pipeline
docker compose up --build
```

Startup takes ~60–90 seconds on first run (image pulls + TimescaleDB init).

| Service | URL |
|---|---|
| Dashboard | http://localhost:8501 |
| API | http://localhost:8000 |
| API docs | http://localhost:8000/docs |
| Kafka (host access) | localhost:9092 |
| TimescaleDB (host access) | localhost:5432 |

To stop cleanly:
```bash
docker compose down          # keeps timescale_data volume
docker compose down -v       # also deletes stored data
```

---

## How to Verify It Works

### 1. Pipeline is alive
```bash
curl http://localhost:8000/health
# {"status":"ok","timestamp":"...","db":"ok"}
```

### 2. Data is flowing
```bash
# Wait ~60s after startup, then:
curl http://localhost:8000/stats
```
Expected response:
```json
{
  "throughput_msgs_per_sec": 4.8,
  "error_rate": 0.0,
  "p99_latency_ms": 85.3,
  "dlq_count_total": 0,
  "trades_last_60s": 291,
  "dlq_last_60s": 0
}
```

### 3. Check success criteria directly
| Check | Command | Pass condition |
|---|---|---|
| p99 < 200ms | `curl .../stats \| jq .p99_latency_ms` | Value < 200 |
| Error rate < 1% | `curl .../stats \| jq .error_rate` | Value < 0.01 |
| DLQ not silently dropping | `docker logs <ingester>` | All bad ticks show `[WARNING] DLQ [reason]` |
| Reconnect within 5s | `docker compose restart ingester` then watch logs | "WebSocket connected" within 5s |

### 4. Query TimescaleDB directly
```bash
psql -h localhost -U postgres -d marketdata -c "
  SELECT time, price, vwap_1m, is_anomaly
  FROM trades ORDER BY time DESC LIMIT 5;
"
```

### 5. Inject a bad tick manually
```bash
# Connect to Kafka and produce a malformed message to the trades topic
# (the processor's ON CONFLICT guard and the ingester's DLQ routing
#  are both independently testable)
docker exec -it $(docker compose ps -q kafka) \
  kafka-console-producer --bootstrap-server localhost:9092 --topic dead-letter
# Type any JSON — processor will write it to dead_letter_trades
```

---

## Observability

| Signal | Where |
|---|---|
| Ingestion errors | `docker logs realtime-market-pipeline-ingester-1` |
| Anomaly events | `docker logs realtime-market-pipeline-processor-1` |
| DLQ reasons | `GET /stats` + `dead_letter_trades` table |
| Live charts | http://localhost:8501 |
| DB slow queries | `pg_stat_statements` (enable in TimescaleDB if needed) |
