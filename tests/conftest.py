"""
Fixtures compartidos de pytest para Unit Testing e integración.
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import polars as pl
import pytest

from etl.config.settings import ETLConfig, SourceConfig, TargetConfig


# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------

@pytest.fixture
def minimal_config() -> ETLConfig:
    """ETLConfig apto para tests — no requiere variables de entorno, reintentos rápidos."""
    return ETLConfig(
        source=SourceConfig(
            dialect="mssql",
            host="localhost",
            port=1433,
            database="testdb",
            username="sa",
            password="secret",
        ),
        target=TargetConfig(
            host="localhost",
            port=5432,
            database="testdb",
            username="etl",
            password="secret",
        ),
        chunk_size=3,        # chunks pequeños para probar la lógica de chunking
        max_workers=2,
        retry_attempts=2,
        retry_wait_min=0.01,  # reintentos casi instantáneos en tests
        retry_wait_max=0.05,
        retry_multiplier=1.0,
    )


# ---------------------------------------------------------------------------
# Helpers para engine mock
# ---------------------------------------------------------------------------

def make_mock_engine() -> tuple[MagicMock, MagicMock]:
    """
    Devuelve (engine, conn) con los protocolos de context manager configurados.

    El mismo mock *conn* es devuelto tanto por ``engine.connect()`` como por
    ``engine.begin()``, así los tests pueden fijar expectativas sobre un único objeto.
    ``conn.execution_options(...)`` devuelve el propio *conn* (patrón del extractor).
    """
    engine = MagicMock(name="engine")
    conn = MagicMock(name="conn")

    # engine.connect() usado como context manager
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)

    # engine.begin() usado como context manager (loader / watermark)
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    # el extractor reasigna conn = conn.execution_options(...)
    conn.execution_options.return_value = conn

    return engine, conn


@pytest.fixture
def mock_engine_pair() -> tuple[MagicMock, MagicMock]:
    return make_mock_engine()


# ---------------------------------------------------------------------------
# DataFrames de ejemplo
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_df() -> pl.DataFrame:
    """DataFrame de tres filas que coincide con el esquema de fixtures a continuación."""
    return pl.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "updated_at": [
                datetime(2024, 1, 1, tzinfo=timezone.utc),
                datetime(2024, 1, 2, tzinfo=timezone.utc),
                datetime(2024, 1, 3, tzinfo=timezone.utc),
            ],
            "amount": [100.0, 200.0, 300.0],
        }
    )


# ---------------------------------------------------------------------------
# Filas simuladas de information_schema
# (column_name, data_type, is_nullable, column_default)
# ---------------------------------------------------------------------------

@pytest.fixture
def pg_schema_rows() -> list[tuple]:
    """
    Coincide con sample_df: id NOT NULL, name nullable, updated_at NOT NULL,
    amount nullable.
    """
    return [
        ("id",         "integer",                    "NO",  None),
        ("name",       "character varying",           "YES", None),
        ("updated_at", "timestamp with time zone",   "NO",  None),
        ("amount",     "double precision",            "YES", None),
    ]
