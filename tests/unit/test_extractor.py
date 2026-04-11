"""
Unit Testing para IncrementalExtractor.

Cubre:
  - Caso exitoso: extracción por timestamp / rowversion con chunking
  - Fallos de la fuente: conexión rechazada, fallo de consulta, error durante iteración
  - Casos borde: fuente vacía, carga completa (sin watermark), carga incremental
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, call, patch

import polars as pl
import pytest
from sqlalchemy.exc import OperationalError

from etl.extractors.incremental import IncrementalExtractor
from tests.conftest import make_mock_engine


# ---------------------------------------------------------------------------
# Helper: configurar conn para devolver filas específicas
# ---------------------------------------------------------------------------

def _setup_result(conn: MagicMock, columns: list[str], rows: list[tuple]) -> MagicMock:
    """Conecta conn.execute(...) para devolver *rows* con *columns*."""
    mock_result = MagicMock(name="result")
    mock_result.keys.return_value = columns
    mock_result.__iter__ = MagicMock(return_value=iter(rows))
    conn.execute.return_value = mock_result
    return mock_result


# ---------------------------------------------------------------------------
# Caso exitoso: extracción por timestamp
# ---------------------------------------------------------------------------

class TestExtractByTimestamp:
    def test_full_load_yields_all_rows(self, minimal_config):
        engine, conn = make_mock_engine()
        rows = [(1, "Alice"), (2, "Bob"), (3, "Charlie")]
        _setup_result(conn, ["id", "name"], rows)

        extractor = IncrementalExtractor(engine, minimal_config)
        chunks = list(extractor.extract_by_timestamp("orders", "updated_at", last_value=None))

        # chunk_size=3 → exactamente un chunk con todas las filas
        assert len(chunks) == 1
        assert len(chunks[0]) == 3

    def test_chunking_splits_rows(self, minimal_config):
        engine, conn = make_mock_engine()
        # 7 filas con chunk_size=3 → 3 chunks (3, 3, 1)
        rows = [(i,) for i in range(7)]
        _setup_result(conn, ["id"], rows)

        extractor = IncrementalExtractor(engine, minimal_config)
        chunks = list(extractor.extract_by_timestamp("orders", "updated_at", last_value=None))

        assert len(chunks) == 3
        assert [len(c) for c in chunks] == [3, 3, 1]

    def test_incremental_load_passes_watermark_param(self, minimal_config):
        engine, conn = make_mock_engine()
        last_ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
        _setup_result(conn, ["id"], [(99,)])

        extractor = IncrementalExtractor(engine, minimal_config)
        list(extractor.extract_by_timestamp("orders", "updated_at", last_value=last_ts))

        # Verificar que el SQL se ejecutó con el watermark como parámetro vinculado
        call_args = conn.execute.call_args
        params = call_args[0][1]  # segundo argumento posicional de execute()
        assert params.get("last_ts") == last_ts

    def test_empty_source_yields_no_chunks(self, minimal_config):
        engine, conn = make_mock_engine()
        _setup_result(conn, ["id", "name"], [])

        extractor = IncrementalExtractor(engine, minimal_config)
        chunks = list(extractor.extract_by_timestamp("orders", "updated_at", last_value=None))

        assert chunks == []

    def test_returns_polars_dataframes(self, minimal_config):
        engine, conn = make_mock_engine()
        _setup_result(conn, ["id", "val"], [(1, 10.0)])

        extractor = IncrementalExtractor(engine, minimal_config)
        chunks = list(extractor.extract_by_timestamp("t", "updated_at", last_value=None))

        assert isinstance(chunks[0], pl.DataFrame)

    def test_dataframe_columns_match_query_result(self, minimal_config):
        engine, conn = make_mock_engine()
        _setup_result(conn, ["order_id", "total", "status"], [(1, 99.9, "open")])

        extractor = IncrementalExtractor(engine, minimal_config)
        chunks = list(extractor.extract_by_timestamp("orders", "updated_at", last_value=None))

        assert chunks[0].columns == ["order_id", "total", "status"]


# ---------------------------------------------------------------------------
# Caso exitoso: extracción por rowversion
# ---------------------------------------------------------------------------

class TestExtractByRowversion:
    def test_yields_rowver_column(self, minimal_config):
        engine, conn = make_mock_engine()
        rows = [(1, "Alice", 1001), (2, "Bob", 1002)]
        _setup_result(conn, ["id", "name", "__rowver"], rows)

        extractor = IncrementalExtractor(engine, minimal_config)
        chunks = list(
            extractor.extract_by_rowversion("accounts", "rowver", last_rowver=None)
        )

        assert "__rowver" in chunks[0].columns

    def test_incremental_rowversion_passes_param(self, minimal_config):
        engine, conn = make_mock_engine()
        _setup_result(conn, ["id", "__rowver"], [(5, 2000)])

        extractor = IncrementalExtractor(engine, minimal_config)
        list(extractor.extract_by_rowversion("accounts", "rowver", last_rowver=1500))

        call_args = conn.execute.call_args
        params = call_args[0][1]
        assert params.get("last_rv") == 1500


# ---------------------------------------------------------------------------
# Simulación de fallos de la fuente de origen
# ---------------------------------------------------------------------------

class TestSourceFailures:
    def test_connection_refused_raises_operational_error(self, minimal_config):
        engine = MagicMock(name="broken_engine")
        engine.connect.side_effect = OperationalError(
            "Connection refused", None, None
        )

        extractor = IncrementalExtractor(engine, minimal_config)
        with pytest.raises(OperationalError):
            list(extractor.extract_by_timestamp("orders", "updated_at", last_value=None))

    def test_query_execution_failure_raises(self, minimal_config):
        engine, conn = make_mock_engine()
        conn.execute.side_effect = OperationalError(
            "Invalid object name 'orders'", None, None
        )

        extractor = IncrementalExtractor(engine, minimal_config)
        with pytest.raises(OperationalError):
            list(extractor.extract_by_timestamp("orders", "updated_at", last_value=None))

    def test_mid_iteration_failure_raises(self, minimal_config):
        """La fuente corta la conexión tras enviar las primeras filas."""
        engine, conn = make_mock_engine()
        mock_result = MagicMock(name="result")
        mock_result.keys.return_value = ["id"]

        call_count = 0

        def flaky_iter():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                yield (1,)
                yield (2,)
                raise OperationalError("Connection lost mid-stream", None, None)

        mock_result.__iter__ = MagicMock(side_effect=flaky_iter)
        conn.execute.return_value = mock_result

        extractor = IncrementalExtractor(engine, minimal_config)
        with pytest.raises(OperationalError, match="Connection lost mid-stream"):
            list(extractor.extract_by_timestamp("orders", "updated_at", last_value=None))

    def test_connection_timeout_raises(self, minimal_config):
        engine = MagicMock(name="timeout_engine")
        engine.connect.side_effect = TimeoutError("Connection timed out after 30s")

        extractor = IncrementalExtractor(engine, minimal_config)
        with pytest.raises(TimeoutError):
            list(extractor.extract_by_timestamp("orders", "updated_at", last_value=None))

    def test_invalid_table_name_raises(self, minimal_config):
        engine, conn = make_mock_engine()
        conn.execute.side_effect = OperationalError(
            "Invalid object name 'does_not_exist'", None, None
        )

        extractor = IncrementalExtractor(engine, minimal_config)
        with pytest.raises(OperationalError):
            list(extractor.extract_by_timestamp("does_not_exist", "ts", last_value=None))

    def test_exception_propagates_not_swallowed(self, minimal_config):
        """Verificar que el extractor NO silencia las excepciones."""
        engine = MagicMock(name="engine")
        engine.connect.side_effect = RuntimeError("Unexpected DB error")

        extractor = IncrementalExtractor(engine, minimal_config)
        with pytest.raises(RuntimeError, match="Unexpected DB error"):
            list(extractor.extract_by_timestamp("t", "ts", last_value=None))
