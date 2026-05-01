"""
Kafka consumer that enriches trade records and persists them to TimescaleDB.

Responsibilities:
  - Consume from 'trades' topic: compute rolling 1-min VWAP, detect price
    anomalies (>2% change from previous tick), write enriched row to DB
  - Consume from 'dead-letter' topic: persist DLQ records to DB for API
    observability without data loss
  - Idempotent writes via ON CONFLICT DO NOTHING (handles processor restarts)
"""
import asyncio
import json
import logging
import os
import signal
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import Optional

import asyncpg
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaConnectionError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("processor")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/marketdata",
)
TRADES_TOPIC = "trades"
DLQ_TOPIC = "dead-letter"
VWAP_WINDOW_S = 60
ANOMALY_THRESHOLD = 0.02  # 2% price change triggers anomaly flag
DB_CONNECT_RETRIES = 30
DB_CONNECT_BACKOFF_S = 2
KAFKA_CONNECT_RETRIES = 30
KAFKA_CONNECT_BACKOFF_S = 2


# ── VWAP calculator ────────────────────────────────────────────────────────────

class VWAPCalculator:
    """Sliding-window VWAP: keeps (timestamp, pv, qty) tuples for the window."""

    def __init__(self, window_s: int = VWAP_WINDOW_S) -> None:
        self._window_s = window_s
        # Each entry: (trade_time: datetime, price*qty: float, qty: float)
        self._buf: deque[tuple[datetime, float, float]] = deque()

    def update(self, ts: datetime, price: float, qty: float) -> float:
        self._buf.append((ts, price * qty, qty))
        cutoff = ts - timedelta(seconds=self._window_s)
        while self._buf and self._buf[0][0] < cutoff:
            self._buf.popleft()

        total_pv = sum(pv for _, pv, _ in self._buf)
        total_q = sum(q for _, _, q in self._buf)
        return total_pv / total_q if total_q else price


# ── Anomaly detector ───────────────────────────────────────────────────────────

class AnomalyDetector:
    """Flags ticks where price moved more than threshold % from the prior tick."""

    def __init__(self, threshold: float = ANOMALY_THRESHOLD) -> None:
        self._threshold = threshold
        self._last_price: Optional[float] = None

    def check(self, price: float) -> bool:
        is_anomaly = False
        if self._last_price is not None:
            change = abs(price - self._last_price) / self._last_price
            is_anomaly = change > self._threshold
            if is_anomaly:
                logger.warning(
                    "Anomaly: price=%.4f prev=%.4f change=%.3f%%",
                    price, self._last_price, change * 100,
                )
        self._last_price = price
        return is_anomaly


# ── Processor ──────────────────────────────────────────────────────────────────

class Processor:
    def __init__(self) -> None:
        self.pool: Optional[asyncpg.Pool] = None
        self.consumer: Optional[AIOKafkaConsumer] = None
        self._vwap = VWAPCalculator()
        self._anomaly = AnomalyDetector()
        self._shutdown = asyncio.Event()

    # ── Startup / teardown ─────────────────────────────────────────────────────

    async def _connect_db(self) -> None:
        for attempt in range(1, DB_CONNECT_RETRIES + 1):
            try:
                self.pool = await asyncpg.create_pool(
                    DATABASE_URL,
                    min_size=2,
                    max_size=10,
                    command_timeout=30,
                )
                logger.info("Connected to TimescaleDB")
                return
            except Exception as exc:
                logger.warning("DB connect attempt %d/%d: %s", attempt, DB_CONNECT_RETRIES, exc)
                if attempt < DB_CONNECT_RETRIES:
                    await asyncio.sleep(DB_CONNECT_BACKOFF_S)
        raise RuntimeError("Could not connect to TimescaleDB after all retries")

    async def _connect_kafka(self) -> None:
        for attempt in range(1, KAFKA_CONNECT_RETRIES + 1):
            try:
                self.consumer = AIOKafkaConsumer(
                    TRADES_TOPIC,
                    DLQ_TOPIC,
                    bootstrap_servers=KAFKA_BOOTSTRAP,
                    group_id="processor-v1",
                    value_deserializer=lambda v: json.loads(v.decode()),
                    auto_offset_reset="latest",
                    enable_auto_commit=True,
                    session_timeout_ms=30_000,
                    heartbeat_interval_ms=3_000,
                )
                await self.consumer.start()
                logger.info("Kafka consumer started (topics: %s, %s)", TRADES_TOPIC, DLQ_TOPIC)
                return
            except KafkaConnectionError as exc:
                logger.warning("Kafka connect attempt %d/%d: %s", attempt, KAFKA_CONNECT_RETRIES, exc)
                if attempt < KAFKA_CONNECT_RETRIES:
                    await asyncio.sleep(KAFKA_CONNECT_BACKOFF_S)
        raise RuntimeError("Could not connect to Kafka after all retries")

    async def stop(self) -> None:
        self._shutdown.set()
        if self.consumer:
            await self.consumer.stop()
        if self.pool:
            await self.pool.close()

    # ── Per-record handlers ────────────────────────────────────────────────────

    async def _handle_trade(self, record: dict) -> None:
        trade_time = datetime.fromisoformat(record["trade_time"])
        received_at = datetime.fromisoformat(record["received_at"])
        price = float(record["price"])
        qty = float(record["quantity"])

        vwap_1m = self._vwap.update(trade_time, price, qty)
        is_anomaly = self._anomaly.check(price)

        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO trades
                    (time, received_at, symbol, trade_id, price,
                     quantity, is_buyer_maker, vwap_1m, is_anomaly)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT (symbol, trade_id) DO NOTHING
                """,
                trade_time,
                received_at,
                record["symbol"],
                int(record["trade_id"]),
                price,
                qty,
                bool(record["is_buyer_maker"]),
                vwap_1m,
                is_anomaly,
            )

    async def _handle_dlq(self, record: dict) -> None:
        received_at_raw = record.get("received_at")
        received_at = (
            datetime.fromisoformat(received_at_raw)
            if received_at_raw
            else datetime.now(timezone.utc)
        )
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO dead_letter_trades (received_at, raw_data, reason)
                VALUES ($1, $2, $3)
                """,
                received_at,
                json.dumps(record.get("raw", {})),
                str(record.get("reason", "unknown"))[:500],
            )

    # ── Main consume loop ──────────────────────────────────────────────────────

    async def run(self) -> None:
        await self._connect_db()
        await self._connect_kafka()
        logger.info("Processor running")

        async for msg in self.consumer:
            if self._shutdown.is_set():
                break
            try:
                if msg.topic == TRADES_TOPIC:
                    await self._handle_trade(msg.value)
                elif msg.topic == DLQ_TOPIC:
                    await self._handle_dlq(msg.value)
            except asyncpg.PostgresError as exc:
                logger.error("DB error on %s message: %s", msg.topic, exc)
            except (KeyError, ValueError, TypeError) as exc:
                logger.error("Malformed %s message: %s | value=%s",
                             msg.topic, exc, str(msg.value)[:200])
            except Exception as exc:
                logger.error("Unexpected error on %s: %s", msg.topic, exc, exc_info=True)


# ── Entry point ────────────────────────────────────────────────────────────────

async def main() -> None:
    processor = Processor()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.ensure_future(processor.stop()))

    try:
        await processor.run()
    finally:
        await processor.stop()
        logger.info("Processor shut down cleanly")


if __name__ == "__main__":
    asyncio.run(main())
