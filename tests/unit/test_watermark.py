"""
Unit Testing para WatermarkManager.

Cubre:
  - Creación de la tabla en __init__
  - get_last_timestamp: None en primera ejecución, valor en ejecuciones posteriores
  - get_last_rowversion: None en primera ejecución, valor en ejecuciones posteriores
  - update_timestamp / update_rowversion: UPSERT ejecutado correctamente
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, call

import pytest

from etl.utils.watermark import WatermarkManager
from tests.conftest import make_mock_engine


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_wm(fetchone_return=None) -> tuple[WatermarkManager, MagicMock]:
    """
    Devuelve (WatermarkManager, conn_mock).

    *fetchone_return* es lo que devuelve conn.execute(...).fetchone().
    """
    engine, conn = make_mock_engine()
    conn.execute.return_value.fetchone.return_value = fetchone_return
    wm = WatermarkManager(engine)
    return wm, conn


# ---------------------------------------------------------------------------
# Tests de inicialización
# ---------------------------------------------------------------------------

class TestInit:
    def test_creates_watermark_table_on_init(self):
        engine, conn = make_mock_engine()
        WatermarkManager(engine)

        # _ensure_table usa engine.begin() → conn.execute(CREATE TABLE ...)
        assert conn.execute.called
        sql_text = str(conn.execute.call_args[0][0])
        assert "etl_watermarks" in sql_text.lower()

    def test_create_uses_if_not_exists(self):
        engine, conn = make_mock_engine()
        WatermarkManager(engine)
        sql_text = str(conn.execute.call_args[0][0]).upper()
        assert "IF NOT EXISTS" in sql_text


# ---------------------------------------------------------------------------
# Tests de get_last_timestamp
# ---------------------------------------------------------------------------

class TestGetLastTimestamp:
    def test_returns_none_when_no_row_exists(self):
        wm, _ = _make_wm(fetchone_return=None)
        assert wm.get_last_timestamp("orders") is None

    def test_returns_none_when_row_value_is_none(self):
        wm, _ = _make_wm(fetchone_return=(None,))
        assert wm.get_last_timestamp("orders") is None

    def test_returns_datetime_when_watermark_exists(self):
        ts = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
        wm, _ = _make_wm(fetchone_return=(ts,))
        result = wm.get_last_timestamp("orders")
        assert result == ts

    def test_query_filters_by_table_name(self):
        engine, conn = make_mock_engine()
        conn.execute.return_value.fetchone.return_value = None
        wm = WatermarkManager(engine)

        # Resetear conteo de llamadas tras __init__
        conn.execute.reset_mock()
        wm.get_last_timestamp("my_table")

        call_params = conn.execute.call_args[0][1]
        assert call_params.get("t") == "my_table"


# ---------------------------------------------------------------------------
# Tests de get_last_rowversion
# ---------------------------------------------------------------------------

class TestGetLastRowversion:
    def test_returns_none_on_first_run(self):
        wm, _ = _make_wm(fetchone_return=None)
        assert wm.get_last_rowversion("accounts") is None

    def test_returns_integer_when_set(self):
        wm, _ = _make_wm(fetchone_return=(98765,))
        result = wm.get_last_rowversion("accounts")
        assert result == 98765


# ---------------------------------------------------------------------------
# Tests de update_timestamp
# ---------------------------------------------------------------------------

class TestUpdateTimestamp:
    def test_upsert_executed(self):
        engine, conn = make_mock_engine()
        wm = WatermarkManager(engine)
        conn.execute.reset_mock()

        ts = datetime(2024, 7, 4, tzinfo=timezone.utc)
        wm.update_timestamp("orders", ts)

        assert conn.execute.called

    def test_upsert_contains_on_conflict(self):
        engine, conn = make_mock_engine()
        wm = WatermarkManager(engine)
        conn.execute.reset_mock()

        wm.update_timestamp("orders", datetime(2024, 1, 1, tzinfo=timezone.utc))

        sql_text = str(conn.execute.call_args[0][0]).upper()
        assert "ON CONFLICT" in sql_text

    def test_update_passes_correct_value(self):
        engine, conn = make_mock_engine()
        wm = WatermarkManager(engine)
        conn.execute.reset_mock()

        ts = datetime(2025, 3, 15, tzinfo=timezone.utc)
        wm.update_timestamp("sales", ts)

        params = conn.execute.call_args[0][1]
        assert params["v"] == ts
        assert params["t"] == "sales"


# ---------------------------------------------------------------------------
# Tests de update_rowversion
# ---------------------------------------------------------------------------

class TestUpdateRowversion:
    def test_upsert_executed(self):
        engine, conn = make_mock_engine()
        wm = WatermarkManager(engine)
        conn.execute.reset_mock()

        wm.update_rowversion("accounts", 54321)

        assert conn.execute.called

    def test_update_passes_correct_value(self):
        engine, conn = make_mock_engine()
        wm = WatermarkManager(engine)
        conn.execute.reset_mock()

        wm.update_rowversion("accounts", 99999)

        params = conn.execute.call_args[0][1]
        assert params["v"] == 99999
        assert params["t"] == "accounts"
