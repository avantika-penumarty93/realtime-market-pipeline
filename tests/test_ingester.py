"""Unit tests for ingester validation, deduplication, and OOO detection.

All tests run without Kafka or a database — Kafka I/O is stubbed via AsyncMock.
"""
import asyncio
import json
import os
import sys
from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ingester"))

from ingester import BinanceTrade, Ingester  # noqa: E402


# ── Helpers ────────────────────────────────────────────────────────────────────

def _valid_raw(**overrides) -> dict:
    base = {
        "e": "trade",
        "E": 1_700_000_000_000,
        "s": "BTCUSDT",
        "t": 1001,
        "p": "42000.50",
        "q": "0.01",
        "T": 1_700_000_000_000,
        "m": False,
    }
    base.update(overrides)
    return base


def _make_ingester() -> Ingester:
    """Ingester with Kafka send methods replaced by async no-ops."""
    ing = Ingester()
    ing._send_dlq = AsyncMock()
    ing._send_trade = AsyncMock()
    return ing


def _run(coro):
    return asyncio.run(coro)


# ── Schema validation ──────────────────────────────────────────────────────────

class TestSchemaValidation:
    def test_missing_price_rejected(self):
        data = _valid_raw()
        del data["p"]
        with pytest.raises(ValidationError):
            BinanceTrade.model_validate(data)

    def test_negative_quantity_rejected(self):
        with pytest.raises(ValidationError):
            BinanceTrade.model_validate(_valid_raw(q="-0.5"))

    def test_zero_price_rejected(self):
        with pytest.raises(ValidationError):
            BinanceTrade.model_validate(_valid_raw(p="0"))

    def test_wrong_type_trade_id_rejected(self):
        # t must be an integer; a non-numeric string cannot be coerced
        with pytest.raises(ValidationError):
            BinanceTrade.model_validate(_valid_raw(t="not-an-integer"))

    def test_wrong_type_is_buyer_maker_rejected(self):
        # m must be bool; a list is not coercible
        with pytest.raises(ValidationError):
            BinanceTrade.model_validate(_valid_raw(m=[1, 2, 3]))

    def test_wrong_event_type_rejected(self):
        with pytest.raises(ValidationError):
            BinanceTrade.model_validate(_valid_raw(e="aggTrade"))


# ── Deduplication ──────────────────────────────────────────────────────────────

class TestDeduplication:
    def test_second_identical_trade_id_flagged(self):
        async def _run_test():
            ing = _make_ingester()
            msg = json.dumps(_valid_raw(t=9999))

            # First arrival — routed to trades
            await ing.process_message(msg)
            ing._send_trade.assert_called_once()
            ing._send_dlq.assert_not_called()

            # Second arrival — same trade_id → DLQ
            ing._send_trade.reset_mock()
            await ing.process_message(msg)

            ing._send_dlq.assert_called_once()
            reason = ing._send_dlq.call_args[0][1]
            assert "duplicate" in reason
            ing._send_trade.assert_not_called()

        _run(_run_test())


# ── Out-of-order detection ─────────────────────────────────────────────────────

class TestOutOfOrderDetection:
    def test_tick_with_earlier_timestamp_flagged(self):
        async def _run_test():
            ing = _make_ingester()

            # Tick at T=2_000_000 — accepted
            await ing.process_message(json.dumps(_valid_raw(t=1, T=2_000_000)))
            ing._send_trade.assert_called_once()
            ing._send_dlq.assert_not_called()

            # Tick at T=1_000_000 (earlier than previous) — OOO
            ing._send_trade.reset_mock()
            await ing.process_message(json.dumps(_valid_raw(t=2, T=1_000_000)))

            ing._send_dlq.assert_called_once()
            reason = ing._send_dlq.call_args[0][1]
            assert "out_of_order" in reason
            ing._send_trade.assert_not_called()

        _run(_run_test())


# ── Valid tick ─────────────────────────────────────────────────────────────────

class TestValidTick:
    def test_valid_tick_passes_all_checks(self):
        async def _run_test():
            ing = _make_ingester()
            await ing.process_message(json.dumps(_valid_raw()))

            ing._send_trade.assert_called_once()
            ing._send_dlq.assert_not_called()

        _run(_run_test())

    def test_valid_tick_increments_received_counter(self):
        async def _run_test():
            ing = _make_ingester()
            await ing.process_message(json.dumps(_valid_raw()))
            assert ing.stats["total_received"] == 1

        _run(_run_test())
