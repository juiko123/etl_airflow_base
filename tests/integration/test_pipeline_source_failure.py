"""
Tests de integración: comportamiento del pipeline cuando la base de datos fuente falla.

Conectan la llamada completa a run_pipeline() pero reemplazan los engines reales
con mocks, por lo que no se requiere ninguna base de datos.

Escenarios cubiertos:
  1. Fuente inaccesible (conexión rechazada) → pipeline lanza excepción, no se carga
     ningún dato y el watermark NO se actualiza.
  2. Fuente falla a mitad de la extracción (conexión perdida tras el primer chunk) →
     pipeline lanza excepción, watermark NO se actualiza.
  3. Modo de pipeline desconocido → ValueError lanzado de inmediato.
  4. Fallo de Validación de Esquemas → SchemaValidationError lanzado antes de cualquier
     INSERT, watermark NO se actualiza.
  5. Caso exitoso con fuente + destino mockeados → watermark SÍ se actualiza.
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch, call

import polars as pl
import pytest
from sqlalchemy.exc import OperationalError

from etl.pipeline import run_pipeline
from etl.validators.schema_validator import SchemaValidationError
from tests.conftest import make_mock_engine


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_rows(n: int) -> list[tuple]:
    return [(i, f"name_{i}", datetime(2024, 1, 1, tzinfo=timezone.utc)) for i in range(1, n + 1)]


def _setup_source_conn(conn: MagicMock, columns: list[str], rows: list[tuple]) -> None:
    mock_result = MagicMock(name="source_result")
    mock_result.keys.return_value = columns
    mock_result.__iter__ = MagicMock(return_value=iter(rows))
    conn.execute.return_value = mock_result


def _setup_validator_pass(validator_mock: MagicMock) -> None:
    """Hace que validator.validate() devuelva un ValidationResult exitoso."""
    from etl.validators.schema_validator import ValidationResult
    ok = ValidationResult(valid=True)
    validator_mock.return_value.validate.return_value = ok


def _setup_watermark(wm_class_mock: MagicMock, last_ts=None, last_rv=None) -> MagicMock:
    wm = MagicMock(name="watermark_mgr")
    wm.get_last_timestamp.return_value = last_ts
    wm.get_last_rowversion.return_value = last_rv
    wm_class_mock.return_value = wm
    return wm


# ---------------------------------------------------------------------------
# Test: conexión rechazada en la fuente
# ---------------------------------------------------------------------------

class TestSourceConnectionRefused:
    @patch("etl.pipeline.SchemaValidator")
    @patch("etl.pipeline.WatermarkManager")
    @patch("etl.pipeline.build_engine")
    def test_pipeline_raises_operational_error(
        self, mock_build, mock_wm_cls, mock_validator_cls, minimal_config
    ):
        source_engine = MagicMock(name="source")
        source_engine.connect.side_effect = OperationalError(
            "Connection refused", None, None
        )
        target_engine, _ = make_mock_engine()

        mock_build.side_effect = [source_engine, target_engine]
        _setup_watermark(mock_wm_cls)
        _setup_validator_pass(mock_validator_cls)

        with pytest.raises(OperationalError):
            run_pipeline(
                "orders", "timestamp", "updated_at", ["id"],
                config=minimal_config, validate_schema=False,
            )

    @patch("etl.pipeline.SchemaValidator")
    @patch("etl.pipeline.WatermarkManager")
    @patch("etl.pipeline.build_engine")
    def test_watermark_not_updated_on_connection_failure(
        self, mock_build, mock_wm_cls, mock_validator_cls, minimal_config
    ):
        source_engine = MagicMock(name="source")
        source_engine.connect.side_effect = OperationalError(
            "Connection refused", None, None
        )
        target_engine, _ = make_mock_engine()
        mock_build.side_effect = [source_engine, target_engine]

        wm = _setup_watermark(mock_wm_cls)
        _setup_validator_pass(mock_validator_cls)

        with pytest.raises(OperationalError):
            run_pipeline(
                "orders", "timestamp", "updated_at", ["id"],
                config=minimal_config, validate_schema=False,
            )

        # El watermark NO debe actualizarse si la fuente falló
        wm.update_timestamp.assert_not_called()
        wm.update_rowversion.assert_not_called()

    @patch("etl.pipeline.SchemaValidator")
    @patch("etl.pipeline.WatermarkManager")
    @patch("etl.pipeline.build_engine")
    def test_engines_disposed_even_on_failure(
        self, mock_build, mock_wm_cls, mock_validator_cls, minimal_config
    ):
        source_engine = MagicMock(name="source")
        source_engine.connect.side_effect = OperationalError("refused", None, None)
        target_engine, _ = make_mock_engine()
        mock_build.side_effect = [source_engine, target_engine]

        _setup_watermark(mock_wm_cls)
        _setup_validator_pass(mock_validator_cls)

        with pytest.raises(OperationalError):
            run_pipeline(
                "orders", "timestamp", "updated_at", ["id"],
                config=minimal_config, validate_schema=False,
            )

        # Las conexiones deben liberarse incluso si hubo fallo
        source_engine.dispose.assert_called_once()
        target_engine.dispose.assert_called_once()


# ---------------------------------------------------------------------------
# Test: fallo a mitad de la extracción
# ---------------------------------------------------------------------------

class TestSourceMidExtractionFailure:
    @patch("etl.pipeline.SchemaValidator")
    @patch("etl.pipeline.WatermarkManager")
    @patch("etl.pipeline.build_engine")
    def test_mid_extraction_failure_raises(
        self, mock_build, mock_wm_cls, mock_validator_cls, minimal_config
    ):
        source_engine, source_conn = make_mock_engine()

        # El iterador produce una fila y luego lanza excepción
        def flaky_iter():
            yield (1, "Alice", datetime(2024, 1, 1, tzinfo=timezone.utc))
            raise OperationalError("Network partition", None, None)

        mock_result = MagicMock(name="result")
        mock_result.keys.return_value = ["id", "name", "updated_at"]
        mock_result.__iter__ = MagicMock(side_effect=flaky_iter)
        source_conn.execute.return_value = mock_result

        target_engine, _ = make_mock_engine()
        mock_build.side_effect = [source_engine, target_engine]

        wm = _setup_watermark(mock_wm_cls)
        _setup_validator_pass(mock_validator_cls)

        with pytest.raises(OperationalError, match="Network partition"):
            run_pipeline(
                "orders", "timestamp", "updated_at", ["id"],
                config=minimal_config, validate_schema=False,
            )

        wm.update_timestamp.assert_not_called()

    @patch("etl.pipeline.SchemaValidator")
    @patch("etl.pipeline.WatermarkManager")
    @patch("etl.pipeline.build_engine")
    def test_watermark_not_updated_on_mid_failure(
        self, mock_build, mock_wm_cls, mock_validator_cls, minimal_config
    ):
        source_engine, source_conn = make_mock_engine()

        def flaky_iter():
            yield (1, "Alice", datetime(2024, 1, 1, tzinfo=timezone.utc))
            raise OperationalError("Lost", None, None)

        mock_result = MagicMock()
        mock_result.keys.return_value = ["id", "name", "updated_at"]
        mock_result.__iter__ = MagicMock(side_effect=flaky_iter)
        source_conn.execute.return_value = mock_result

        target_engine, _ = make_mock_engine()
        mock_build.side_effect = [source_engine, target_engine]

        wm = _setup_watermark(mock_wm_cls)
        _setup_validator_pass(mock_validator_cls)

        with pytest.raises(OperationalError):
            run_pipeline(
                "orders", "timestamp", "updated_at", ["id"],
                config=minimal_config, validate_schema=False,
            )

        wm.update_timestamp.assert_not_called()


# ---------------------------------------------------------------------------
# Test: modo desconocido
# ---------------------------------------------------------------------------

class TestUnknownMode:
    @patch("etl.pipeline.SchemaValidator")
    @patch("etl.pipeline.WatermarkManager")
    @patch("etl.pipeline.build_engine")
    def test_invalid_mode_raises_value_error(
        self, mock_build, mock_wm_cls, mock_validator_cls, minimal_config
    ):
        source_engine, _ = make_mock_engine()
        target_engine, _ = make_mock_engine()
        mock_build.side_effect = [source_engine, target_engine]
        _setup_watermark(mock_wm_cls)

        with pytest.raises(ValueError, match="Unknown mode"):
            run_pipeline(
                "orders", "kafka", "updated_at", ["id"],
                config=minimal_config, validate_schema=False,
            )


# ---------------------------------------------------------------------------
# Test: fallo de Validación de Esquemas
# ---------------------------------------------------------------------------

class TestSchemaValidationFailure:
    @patch("etl.pipeline.SchemaValidator")
    @patch("etl.pipeline.WatermarkManager")
    @patch("etl.pipeline.build_engine")
    def test_schema_mismatch_raises_before_load(
        self, mock_build, mock_wm_cls, mock_validator_cls, minimal_config
    ):
        """SchemaValidationError debe abortar el pipeline antes de cualquier INSERT."""
        from etl.validators.schema_validator import ValidationResult

        source_engine, source_conn = make_mock_engine()
        rows = _make_rows(3)
        _setup_source_conn(source_conn, ["id", "name", "updated_at"], rows)

        target_engine, target_conn = make_mock_engine()
        mock_build.side_effect = [source_engine, target_engine]

        wm = _setup_watermark(mock_wm_cls)

        # El validador devuelve un resultado FALLIDO
        bad_result = ValidationResult(valid=False)
        bad_result.add_error("id", "type_mismatch", "Expected integer got varchar")
        mock_validator_cls.return_value.validate.return_value = bad_result

        with pytest.raises(SchemaValidationError):
            run_pipeline(
                "orders", "timestamp", "updated_at", ["id"],
                config=minimal_config, validate_schema=True,
            )

        # No debe haberse ejecutado ningún INSERT en la tabla destino
        insert_calls = [
            c for c in target_conn.execute.call_args_list
            if "INSERT INTO public.orders" in str(c).upper()
        ]
        assert insert_calls == [], "No debe insertarse ningún dato cuando falla la Validación de Esquemas"

    @patch("etl.pipeline.SchemaValidator")
    @patch("etl.pipeline.WatermarkManager")
    @patch("etl.pipeline.build_engine")
    def test_watermark_not_updated_on_schema_failure(
        self, mock_build, mock_wm_cls, mock_validator_cls, minimal_config
    ):
        from etl.validators.schema_validator import ValidationResult

        source_engine, source_conn = make_mock_engine()
        _setup_source_conn(source_conn, ["id", "name", "updated_at"], _make_rows(2))

        target_engine, _ = make_mock_engine()
        mock_build.side_effect = [source_engine, target_engine]

        wm = _setup_watermark(mock_wm_cls)

        bad_result = ValidationResult(valid=False)
        bad_result.add_error("id", "missing_required", "Columna ausente")
        mock_validator_cls.return_value.validate.return_value = bad_result

        with pytest.raises(SchemaValidationError):
            run_pipeline(
                "orders", "timestamp", "updated_at", ["id"],
                config=minimal_config, validate_schema=True,
            )

        wm.update_timestamp.assert_not_called()


# ---------------------------------------------------------------------------
# Test: caso exitoso (smoke test)
# ---------------------------------------------------------------------------

class TestHappyPath:
    @patch("etl.pipeline.SchemaValidator")
    @patch("etl.pipeline.WatermarkManager")
    @patch("etl.pipeline.build_engine")
    def test_successful_run_updates_watermark(
        self, mock_build, mock_wm_cls, mock_validator_cls, minimal_config
    ):
        source_engine, source_conn = make_mock_engine()
        rows = _make_rows(3)
        _setup_source_conn(source_conn, ["id", "name", "updated_at"], rows)

        target_engine, _ = make_mock_engine()
        mock_build.side_effect = [source_engine, target_engine]

        wm = _setup_watermark(mock_wm_cls)
        _setup_validator_pass(mock_validator_cls)

        result = run_pipeline(
            "orders", "timestamp", "updated_at", ["id"],
            config=minimal_config, validate_schema=True,
        )

        assert result["total_rows"] == 3
        wm.update_timestamp.assert_called_once()

    @patch("etl.pipeline.SchemaValidator")
    @patch("etl.pipeline.WatermarkManager")
    @patch("etl.pipeline.build_engine")
    def test_rowversion_mode_updates_rowver_watermark(
        self, mock_build, mock_wm_cls, mock_validator_cls, minimal_config
    ):
        source_engine, source_conn = make_mock_engine()
        rows = [(1, "Alice", 1001), (2, "Bob", 1002)]

        mock_result = MagicMock()
        mock_result.keys.return_value = ["id", "name", "__rowver"]
        mock_result.__iter__ = MagicMock(return_value=iter(rows))
        source_conn.execute.return_value = mock_result

        target_engine, _ = make_mock_engine()
        mock_build.side_effect = [source_engine, target_engine]

        wm = _setup_watermark(mock_wm_cls)
        _setup_validator_pass(mock_validator_cls)

        run_pipeline(
            "accounts", "rowversion", "rowver", ["id"],
            config=minimal_config, validate_schema=True,
        )

        wm.update_rowversion.assert_called_once()
        # update_rowversion(table_name, new_rowver) — verificar el máximo rowver del batch
        args = wm.update_rowversion.call_args[0]
        assert args[1] == 1002  # máximo rowver del batch
