"""
WebSocket ingester for the Binance BTCUSDT real-time trade stream.

Responsibilities:
  - Connect and auto-reconnect to Binance WSS within 5 seconds of disconnect
  - Validate incoming tick schema via Pydantic
  - Deduplicate by trade_id using a bounded sliding window
  - Detect out-of-order events by monotonic trade timestamp comparison
  - Route valid trades → Kafka topic 'trades'
  - Route bad ticks with tagged reason → Kafka topic 'dead-letter'
"""
import asyncio
import json
import logging
import os
import signal
import time
from collections import deque
from datetime import datetime, timezone
from typing import Optional

import websockets
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError
from pydantic import BaseModel, field_validator, ValidationError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("ingester")

BINANCE_WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@trade"
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TRADES_TOPIC = "trades"
DLQ_TOPIC = "dead-letter"
RECONNECT_DELAY_S = 5
DEDUP_WINDOW_SIZE = 10_000  # bounded ring-buffer of recent trade IDs
KAFKA_CONNECT_RETRIES = 30
KAFKA_CONNECT_BACKOFF_S = 2


class BinanceTrade(BaseModel):
    e: str   # event type — must be "trade"
    E: int   # event time ms
    s: str   # symbol
    t: int   # trade ID
    p: str   # price (string from exchange)
    q: str   # quantity (string from exchange)
    T: int   # trade time ms
    m: bool  # is buyer the market maker

    @field_validator("e")
    @classmethod
    def event_type_must_be_trade(cls, v: str) -> str:
        if v != "trade":
            raise ValueError(f"expected 'trade', got '{v}'")
        return v

    @field_validator("p", "q")
    @classmethod
    def must_be_positive_numeric(cls, v: str) -> str:
        try:
            val = float(v)
        except ValueError:
            raise ValueError(f"not a number: {v!r}")
        if val <= 0:
            raise ValueError(f"must be positive, got {v}")
        return v


class Ingester:
    def __init__(self) -> None:
        self.producer: Optional[AIOKafkaProducer] = None
        # Bounded dedup window: deque tracks insertion order, set gives O(1) lookup
        self._dedup_deque: deque[int] = deque(maxlen=DEDUP_WINDOW_SIZE)
        self._dedup_set: set[int] = set()
        self._last_trade_time_ms: Optional[int] = None
        self._shutdown = asyncio.Event()

        self.stats = {
            "total_received": 0,
            "total_valid": 0,
            "total_dlq": 0,
            "start_time": time.monotonic(),
        }

    # ── Kafka lifecycle ────────────────────────────────────────────────────────

    async def _start_producer(self) -> None:
        for attempt in range(1, KAFKA_CONNECT_RETRIES + 1):
            try:
                self.producer = AIOKafkaProducer(
                    bootstrap_servers=KAFKA_BOOTSTRAP,
                    value_serializer=lambda v: json.dumps(v).encode(),
                    acks="all",
                    enable_idempotence=True,
                    compression_type="gzip",
                    request_timeout_ms=30_000,
                    retry_backoff_ms=500,
                )
                await self.producer.start()
                logger.info("Kafka producer connected to %s", KAFKA_BOOTSTRAP)
                return
            except KafkaConnectionError as exc:
                logger.warning(
                    "Kafka connect attempt %d/%d failed: %s",
                    attempt, KAFKA_CONNECT_RETRIES, exc,
                )
                if attempt < KAFKA_CONNECT_RETRIES:
                    await asyncio.sleep(KAFKA_CONNECT_BACKOFF_S)
        raise RuntimeError("Could not connect to Kafka after all retries")

    async def stop(self) -> None:
        self._shutdown.set()
        if self.producer:
            await self.producer.stop()

    # ── Dedup window management ────────────────────────────────────────────────

    def _is_duplicate(self, trade_id: int) -> bool:
        return trade_id in self._dedup_set

    def _record_trade_id(self, trade_id: int) -> None:
        if len(self._dedup_deque) == DEDUP_WINDOW_SIZE:
            evicted = self._dedup_deque[0]  # deque evicts from left on maxlen overflow
            self._dedup_set.discard(evicted)
        self._dedup_deque.append(trade_id)
        self._dedup_set.add(trade_id)

    # ── Message routing ────────────────────────────────────────────────────────

    async def _send_dlq(self, raw: dict, reason: str) -> None:
        payload = {
            "raw": raw,
            "reason": reason,
            "received_at": datetime.now(timezone.utc).isoformat(),
        }
        await self.producer.send_and_wait(DLQ_TOPIC, value=payload)
        self.stats["total_dlq"] += 1
        logger.warning("DLQ [%s] trade_id=%s", reason, raw.get("t", "?"))

    async def _send_trade(self, record: dict) -> None:
        await self.producer.send_and_wait(TRADES_TOPIC, value=record)
        self.stats["total_valid"] += 1

    # ── Per-message processing ─────────────────────────────────────────────────

    async def process_message(self, raw_msg: str) -> None:
        self.stats["total_received"] += 1
        received_at = datetime.now(timezone.utc)

        try:
            data = json.loads(raw_msg)
        except json.JSONDecodeError as exc:
            await self._send_dlq({"raw": raw_msg[:500]}, f"invalid_json:{exc}")
            return

        try:
            trade = BinanceTrade.model_validate(data)
        except ValidationError as exc:
            first_err = exc.errors(include_url=False)[0]
            await self._send_dlq(data, f"schema_error:{first_err['loc'][0]}:{first_err['msg']}")
            return

        if self._is_duplicate(trade.t):
            await self._send_dlq(data, f"duplicate:trade_id={trade.t}")
            return

        if (
            self._last_trade_time_ms is not None
            and trade.T < self._last_trade_time_ms
        ):
            await self._send_dlq(
                data,
                f"out_of_order:trade_T={trade.T}<last_T={self._last_trade_time_ms}",
            )
            return

        # Valid path
        self._record_trade_id(trade.t)
        self._last_trade_time_ms = trade.T

        record = {
            "trade_id": trade.t,
            "symbol": trade.s,
            "price": float(trade.p),
            "quantity": float(trade.q),
            "trade_time": datetime.fromtimestamp(
                trade.T / 1000, tz=timezone.utc
            ).isoformat(),
            "received_at": received_at.isoformat(),
            "is_buyer_maker": trade.m,
            "event_time": datetime.fromtimestamp(
                trade.E / 1000, tz=timezone.utc
            ).isoformat(),
        }
        await self._send_trade(record)

    # ── WebSocket connection loop ──────────────────────────────────────────────

    async def _ws_loop(self) -> None:
        while not self._shutdown.is_set():
            try:
                async with websockets.connect(
                    BINANCE_WS_URL,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=10,
                    max_size=2**20,
                ) as ws:
                    logger.info("WebSocket connected to %s", BINANCE_WS_URL)
                    async for message in ws:
                        if self._shutdown.is_set():
                            return
                        try:
                            await self.process_message(message)
                        except Exception as exc:
                            logger.error("Error processing message: %s", exc, exc_info=True)

            except websockets.ConnectionClosedError as exc:
                logger.warning("WebSocket closed unexpectedly: %s", exc)
            except websockets.InvalidURI as exc:
                logger.critical("Invalid WebSocket URI — shutting down: %s", exc)
                self._shutdown.set()
                return
            except OSError as exc:
                logger.warning("Network error: %s", exc)
            except Exception as exc:
                logger.error("Unexpected WebSocket error: %s", exc, exc_info=True)

            if not self._shutdown.is_set():
                logger.info("Reconnecting in %ds…", RECONNECT_DELAY_S)
                await asyncio.sleep(RECONNECT_DELAY_S)

    async def run(self) -> None:
        await self._start_producer()
        logger.info("Ingester started — routing to Kafka topics '%s' / '%s'",
                    TRADES_TOPIC, DLQ_TOPIC)
        await self._ws_loop()


# ── Entry point ────────────────────────────────────────────────────────────────

async def main() -> None:
    ingester = Ingester()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.ensure_future(ingester.stop()))

    try:
        await ingester.run()
    finally:
        await ingester.stop()
        uptime = time.monotonic() - ingester.stats["start_time"]
        logger.info(
            "Shutdown. uptime=%.1fs received=%d valid=%d dlq=%d",
            uptime,
            ingester.stats["total_received"],
            ingester.stats["total_valid"],
            ingester.stats["total_dlq"],
        )


if __name__ == "__main__":
    asyncio.run(main())
