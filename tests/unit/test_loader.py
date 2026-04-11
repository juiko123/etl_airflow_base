"""
Unit Testing para PostgresLoader.

Cubre:
  - Caso exitoso: chunks cargados, resumen devuelto
  - Comportamiento de reintentos: fallo transitorio seguido de éxito
  - Todos los reintentos agotados → chunks_failed incrementado
  - Columna __rowver eliminada antes del UPSERT
  - Generador vacío → cero filas / cero chunks
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, call, patch, PropertyMock

import polars as pl
import pytest
from tenacity import RetryError

from etl.loaders.postgres_loader import PostgresLoader
from tests.conftest import make_mock_engine


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

def _make_chunk(n: int = 3) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "id": list(range(1, n + 1)),
            "name": [f"row_{i}" for i in range(1, n + 1)],
            "updated_at": [datetime(2024, 1, i, tzinfo=timezone.utc) for i in range(1, n + 1)],
        }
    )


# ---------------------------------------------------------------------------
# Tests de caso exitoso
# ---------------------------------------------------------------------------

class TestLoadChunksSuccess:
    def test_returns_summary_dict(self, minimal_config):
        engine, conn = make_mock_engine()
        loader = PostgresLoader(engine, minimal_config)
        chunks = iter([_make_chunk(3)])

        result = loader.load_chunks(chunks, "orders", ["id"])

        assert "total_rows" in result
        assert "chunks_ok" in result
        assert "chunks_failed" in result
        assert "elapsed_s" in result

    def test_counts_rows_correctly(self, minimal_config):
        engine, conn = make_mock_engine()
        loader = PostgresLoader(engine, minimal_config)
        chunks = iter([_make_chunk(5), _make_chunk(5)])

        result = loader.load_chunks(chunks, "orders", ["id"])

        assert result["total_rows"] == 10
        assert result["chunks_ok"] == 2
        assert result["chunks_failed"] == 0

    def test_empty_generator_returns_zeros(self, minimal_config):
        engine, conn = make_mock_engine()
        loader = PostgresLoader(engine, minimal_config)

        result = loader.load_chunks(iter([]), "orders", ["id"])

        assert result["total_rows"] == 0
        assert result["chunks_ok"] == 0
        assert result["chunks_failed"] == 0

    def test_upsert_sql_executed(self, minimal_config):
        engine, conn = make_mock_engine()
        loader = PostgresLoader(engine, minimal_config)
        loader.load_chunks(iter([_make_chunk(2)]), "orders", ["id"])

        assert conn.execute.called
        sql_arg = str(conn.execute.call_args[0][0])
        assert "INSERT" in sql_arg.upper()
        assert "ON CONFLICT" in sql_arg.upper()

    def test_schema_included_in_upsert(self, minimal_config):
        engine, conn = make_mock_engine()
        loader = PostgresLoader(engine, minimal_config)
        loader.load_chunks(iter([_make_chunk(1)]), "orders", ["id"], schema="analytics")

        sql_arg = str(conn.execute.call_args[0][0])
        assert "analytics.orders" in sql_arg

    def test_rowver_column_stripped(self, minimal_config):
        engine, conn = make_mock_engine()
        loader = PostgresLoader(engine, minimal_config)

        df = _make_chunk(2).with_columns(pl.Series("__rowver", [1001, 1002]))
        loader.load_chunks(iter([df]), "orders", ["id"])

        sql_arg = str(conn.execute.call_args[0][0])
        assert "__rowver" not in sql_arg


# ---------------------------------------------------------------------------
# Tests de comportamiento de reintentos
# ---------------------------------------------------------------------------

class TestRetryBehaviour:
    def test_transient_failure_retried(self, minimal_config):
        """Primer llamado falla, segundo tiene éxito → chunk contado como OK."""
        engine = MagicMock(name="engine")
        conn = MagicMock(name="conn")

        # Dos context managers: el primero falla, el segundo tiene éxito
        fail_cm = MagicMock()
        fail_cm.__enter__ = MagicMock(side_effect=Exception("transient error"))
        fail_cm.__exit__ = MagicMock(return_value=False)

        ok_cm = MagicMock()
        ok_cm.__enter__ = MagicMock(return_value=conn)
        ok_cm.__exit__ = MagicMock(return_value=False)

        # side_effect como lista → se consume en orden
        engine.begin.side_effect = [fail_cm, ok_cm]

        loader = PostgresLoader(engine, minimal_config)
        result = loader.load_chunks(iter([_make_chunk(2)]), "orders", ["id"])

        assert result["chunks_ok"] == 1
        assert result["chunks_failed"] == 0

    def test_all_retries_exhausted_increments_chunks_failed(self, minimal_config):
        """Fallo persistente → RetryError → contado como chunks_failed."""
        engine, conn = make_mock_engine()
        conn.execute.side_effect = Exception("persistent DB error")

        loader = PostgresLoader(engine, minimal_config)
        result = loader.load_chunks(iter([_make_chunk(3)]), "orders", ["id"])

        assert result["chunks_failed"] == 1
        assert result["chunks_ok"] == 0
        assert result["total_rows"] == 0

    def test_mixed_chunks_partial_failure(self, minimal_config):
        """Primer chunk tiene éxito, segundo agota todos los reintentos."""
        engine = MagicMock(name="engine")
        conn_ok = MagicMock(name="conn_ok")
        conn_fail = MagicMock(name="conn_fail")
        conn_fail.execute.side_effect = Exception("bad chunk")

        call_count = {"n": 0}

        def begin_side_effect():
            cm = MagicMock()
            call_count["n"] += 1
            if call_count["n"] <= 1:
                cm.__enter__ = MagicMock(return_value=conn_ok)
            else:
                cm.__enter__ = MagicMock(return_value=conn_fail)
            cm.__exit__ = MagicMock(return_value=False)
            return cm

        engine.begin.side_effect = begin_side_effect

        loader = PostgresLoader(engine, minimal_config)
        result = loader.load_chunks(
            iter([_make_chunk(3), _make_chunk(3)]), "orders", ["id"]
        )

        assert result["chunks_ok"] == 1
        assert result["chunks_failed"] == 1
