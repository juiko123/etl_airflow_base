"""
Unit Testing para SchemaValidator.

Todos los tests usan un engine mock de SQLAlchemy — no se requiere base de datos real.
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from etl.validators.schema_validator import (
    SchemaValidationError,
    SchemaValidator,
    ValidationResult,
)
from tests.conftest import make_mock_engine


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_validator_with_schema(schema_rows: list[tuple]) -> SchemaValidator:
    """
    Devuelve un SchemaValidator cuyo ``_fetch_table_schema`` retorna *schema_rows*
    sin acceder a una base de datos real.
    """
    engine, conn = make_mock_engine()
    mock_result = MagicMock()
    mock_result.fetchall.return_value = schema_rows
    conn.execute.return_value = mock_result
    return SchemaValidator(engine)


# ---------------------------------------------------------------------------
# Tests de caso exitoso
# ---------------------------------------------------------------------------

class TestValidSchemas:
    def test_exact_match_passes(self, sample_df, pg_schema_rows):
        validator = _make_validator_with_schema(pg_schema_rows)
        result = validator.validate(sample_df, "orders")
        assert result.valid is True
        assert result.errors == []

    def test_returns_validation_result_instance(self, sample_df, pg_schema_rows):
        validator = _make_validator_with_schema(pg_schema_rows)
        result = validator.validate(sample_df, "orders")
        assert isinstance(result, ValidationResult)

    def test_no_errors_when_all_not_null_cols_present(self, pg_schema_rows):
        df = pl.DataFrame(
            {
                "id": [10],
                "name": ["Test"],
                "updated_at": [datetime(2024, 6, 1, tzinfo=timezone.utc)],
                "amount": [9.99],
            }
        )
        validator = _make_validator_with_schema(pg_schema_rows)
        result = validator.validate(df, "orders")
        assert result.valid is True

    def test_summary_shows_pass(self, sample_df, pg_schema_rows):
        validator = _make_validator_with_schema(pg_schema_rows)
        result = validator.validate(sample_df, "orders")
        assert "PASS" in result.summary()


# ---------------------------------------------------------------------------
# Tests de columnas faltantes
# ---------------------------------------------------------------------------

class TestMissingColumns:
    def test_missing_required_column_fails(self, pg_schema_rows):
        # Eliminar la columna NOT NULL 'updated_at'
        df = pl.DataFrame({"id": [1], "name": ["X"], "amount": [5.0]})
        validator = _make_validator_with_schema(pg_schema_rows)
        result = validator.validate(df, "orders")

        assert result.valid is False
        kinds = [e.kind for e in result.errors]
        assert "missing_required" in kinds

    def test_missing_required_error_names_column(self, pg_schema_rows):
        df = pl.DataFrame({"id": [1], "name": ["X"], "amount": [5.0]})
        validator = _make_validator_with_schema(pg_schema_rows)
        result = validator.validate(df, "orders")
        cols = [e.column for e in result.errors]
        assert "updated_at" in cols

    def test_missing_nullable_column_is_warning_not_error(self, pg_schema_rows):
        # Eliminar la columna nullable 'name' → debe ser advertencia, no error
        df = pl.DataFrame(
            {
                "id": [1],
                "updated_at": [datetime(2024, 1, 1, tzinfo=timezone.utc)],
                "amount": [5.0],
            }
        )
        validator = _make_validator_with_schema(pg_schema_rows)
        result = validator.validate(df, "orders")

        assert result.valid is True  # advertencia, no error
        warning_kinds = [w.kind for w in result.warnings]
        assert "missing_optional" in warning_kinds

    def test_missing_column_with_default_is_not_an_error(self):
        # Columna NOT NULL pero con default → advertencia, no error
        schema_rows = [
            ("id",         "integer",  "NO",  None),
            ("created_at", "timestamp","NO",  "NOW()"),  # NOT NULL + default
        ]
        df = pl.DataFrame({"id": [1]})
        validator = _make_validator_with_schema(schema_rows)
        result = validator.validate(df, "events")
        assert result.valid is True


# ---------------------------------------------------------------------------
# Tests de columnas extra
# ---------------------------------------------------------------------------

class TestExtraColumns:
    def test_extra_df_column_is_warning(self, sample_df, pg_schema_rows):
        df = sample_df.with_columns(pl.lit("extra").alias("mystery_col"))
        validator = _make_validator_with_schema(pg_schema_rows)
        result = validator.validate(df, "orders")

        assert result.valid is True  # columnas extra son advertencias, no errores
        warning_kinds = [w.kind for w in result.warnings]
        assert "extra_column" in warning_kinds

    def test_extra_column_named_in_warning(self, sample_df, pg_schema_rows):
        df = sample_df.with_columns(pl.lit(0).alias("__rowver"))
        validator = _make_validator_with_schema(pg_schema_rows)
        result = validator.validate(df, "orders")
        warning_cols = [w.column for w in result.warnings]
        assert "__rowver" in warning_cols


# ---------------------------------------------------------------------------
# Tests de incompatibilidad de tipos
# ---------------------------------------------------------------------------

class TestTypeMismatch:
    def test_string_in_integer_column_fails(self, pg_schema_rows):
        df = pl.DataFrame(
            {
                "id": ["not_an_int"],       # String vs integer → incompatible
                "name": ["X"],
                "updated_at": [datetime(2024, 1, 1, tzinfo=timezone.utc)],
                "amount": [1.0],
            }
        )
        validator = _make_validator_with_schema(pg_schema_rows)
        result = validator.validate(df, "orders")

        assert result.valid is False
        kinds = [e.kind for e in result.errors]
        assert "type_mismatch" in kinds

    def test_int_in_float_column_passes(self, pg_schema_rows):
        # Int64 es compatible con double precision (conversión implícita numérica)
        rows = [
            ("id",     "integer",        "NO",  None),
            ("score",  "double precision","YES", None),
        ]
        df = pl.DataFrame({"id": [1], "score": [42]})  # score es Int64
        validator = _make_validator_with_schema(rows)
        result = validator.validate(df, "scores")
        assert result.valid is True

    def test_datetime_compatible_with_timestamptz(self):
        rows = [("ts", "timestamp with time zone", "YES", None)]
        df = pl.DataFrame({"ts": [datetime(2024, 1, 1, tzinfo=timezone.utc)]})
        validator = _make_validator_with_schema(rows)
        result = validator.validate(df, "events")
        assert result.valid is True

    def test_utf8_compatible_with_varchar(self):
        rows = [("label", "character varying", "YES", None)]
        df = pl.DataFrame({"label": ["hello"]})
        validator = _make_validator_with_schema(rows)
        result = validator.validate(df, "tags")
        assert result.valid is True

    def test_bool_compatible_with_boolean(self):
        rows = [("active", "boolean", "YES", None)]
        df = pl.DataFrame({"active": [True, False]})
        validator = _make_validator_with_schema(rows)
        result = validator.validate(df, "flags")
        assert result.valid is True


# ---------------------------------------------------------------------------
# Tests de violación de restricción nullable
# ---------------------------------------------------------------------------

class TestNullableViolation:
    def test_null_in_not_null_column_fails(self, pg_schema_rows):
        df = pl.DataFrame(
            {
                "id": [1, None, 3],    # id es NOT NULL → violación
                "name": ["A", "B", "C"],
                "updated_at": [datetime(2024, 1, 1, tzinfo=timezone.utc)] * 3,
                "amount": [1.0, 2.0, 3.0],
            }
        )
        validator = _make_validator_with_schema(pg_schema_rows)
        result = validator.validate(df, "orders")

        assert result.valid is False
        kinds = [e.kind for e in result.errors]
        assert "nullable_violation" in kinds

    def test_null_in_nullable_column_passes(self, pg_schema_rows):
        df = pl.DataFrame(
            {
                "id": [1, 2],
                "name": [None, "Bob"],   # name es nullable → OK
                "updated_at": [datetime(2024, 1, 1, tzinfo=timezone.utc)] * 2,
                "amount": [None, 5.0],   # amount es nullable → OK
            }
        )
        validator = _make_validator_with_schema(pg_schema_rows)
        result = validator.validate(df, "orders")
        assert result.valid is True


# ---------------------------------------------------------------------------
# Tests de tabla no encontrada
# ---------------------------------------------------------------------------

class TestTableNotFound:
    def test_empty_schema_rows_produces_error(self):
        # Sin filas → tabla no encontrada
        validator = _make_validator_with_schema([])
        df = pl.DataFrame({"id": [1]})
        result = validator.validate(df, "nonexistent_table")

        assert result.valid is False
        assert result.errors[0].kind == "table_not_found"

    def test_summary_shows_fail_on_table_not_found(self):
        validator = _make_validator_with_schema([])
        df = pl.DataFrame({"id": [1]})
        result = validator.validate(df, "ghost")
        assert "FAIL" in result.summary()


# ---------------------------------------------------------------------------
# Tests de SchemaValidationError
# ---------------------------------------------------------------------------

class TestSchemaValidationError:
    def test_raises_with_result_attribute(self, pg_schema_rows):
        df = pl.DataFrame({"id": [1]})  # falta 'updated_at' requerida
        validator = _make_validator_with_schema(pg_schema_rows)
        result = validator.validate(df, "orders")
        exc = SchemaValidationError(result)
        assert exc.result is result

    def test_error_message_contains_column_name(self, pg_schema_rows):
        df = pl.DataFrame({"id": [1]})
        validator = _make_validator_with_schema(pg_schema_rows)
        result = validator.validate(df, "orders")
        exc = SchemaValidationError(result)
        assert "updated_at" in str(exc)
