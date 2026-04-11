"""
Orquestador del pipeline ETL — conecta extractor + loader + gestor de watermarks.

Uso:
    python -m etl.pipeline --table transactions --watermark-col updated_at
    python -m etl.pipeline --table accounts --mode rowversion --rowver-col rowver
"""
from __future__ import annotations

import argparse
import itertools
import sys
from datetime import datetime
from typing import Generator, Optional

import polars as pl
from sqlalchemy import create_engine, event, text
from sqlalchemy.pool import QueuePool

from etl.config.settings import load_config, ETLConfig
from etl.extractors.incremental import IncrementalExtractor
from etl.loaders.postgres_loader import PostgresLoader
from etl.utils.logging_setup import get_logger, setup_logging
from etl.utils.watermark import WatermarkManager
from etl.validators.schema_validator import SchemaValidator, SchemaValidationError

log = get_logger(__name__)


def build_engine(url: str, pool_size: int, max_overflow: int, pool_timeout: int, pool_recycle: int):
    engine = create_engine(
        url,
        poolclass=QueuePool,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_timeout=pool_timeout,
        pool_recycle=pool_recycle,
        pool_pre_ping=True,   # detecta conexiones obsoletas antes de usarlas
    )

    # Registrar cada checkout — ayuda al DBA a correlacionar agotamiento del pool con timestamps
    @event.listens_for(engine, "checkout")
    def on_checkout(dbapi_conn, conn_record, conn_proxy):
        log.debug("connection_checkout", pool_size=engine.pool.size(), checked_out=engine.pool.checkedout())

    @event.listens_for(engine, "connect")
    def on_connect(dbapi_conn, conn_record):
        log.info("new_db_connection_created")

    return engine


def _validate_first_chunk(
    chunks: Generator[pl.DataFrame, None, None],
    validator: SchemaValidator,
    target_table: str,
    schema: str,
) -> Generator[pl.DataFrame, None, None]:
    """
    Lee el primer chunk, lo valida contra el esquema destino y lo
    reincorpora al stream. Lanza SchemaValidationError si falla la
    Validación de Esquemas.
    """
    first = next(chunks, None)
    if first is None:
        return iter([])  # fuente vacía — nada que validar ni cargar

    result = validator.validate(first, target_table, schema)
    if not result.valid:
        raise SchemaValidationError(result)

    return itertools.chain([first], chunks)


def run_pipeline(
    table: str,
    mode: str,                   # "timestamp" | "rowversion"
    watermark_col: str,
    conflict_columns: list[str],
    target_table: Optional[str] = None,
    config: Optional[ETLConfig] = None,
    validate_schema: bool = True,
    target_schema: str = "public",
) -> dict:
    if config is None:
        config = load_config()

    target_table = target_table or table

    log.info(
        "pipeline_start",
        source_table=table,
        target_table=target_table,
        mode=mode,
        watermark_col=watermark_col,
    )

    # Crear engines — pools separados para fuente y destino
    source_engine = build_engine(
        config.source.get_url(),
        config.source.pool_size,
        config.source.max_overflow,
        config.source.pool_timeout,
        config.source.pool_recycle,
    )
    target_engine = build_engine(
        config.target.get_url(),
        config.target.pool_size,
        config.target.max_overflow,
        config.target.pool_timeout,
        config.target.pool_recycle,
    )

    watermark_mgr = WatermarkManager(target_engine)
    extractor = IncrementalExtractor(source_engine, config)
    loader = PostgresLoader(target_engine, config)
    validator = SchemaValidator(target_engine) if validate_schema else None

    try:
        if mode == "timestamp":
            last_ts = watermark_mgr.get_last_timestamp(table)
            chunks = extractor.extract_by_timestamp(
                table=table,
                watermark_col=watermark_col,
                last_value=last_ts,
            )
            if validator is not None:
                chunks = _validate_first_chunk(chunks, validator, target_table, target_schema)

            results = loader.load_chunks(chunks, target_table, conflict_columns, schema=target_schema)

            # Watermark = MAX(watermark_col) consultado directamente en la fuente.
            # Usar el reloj de la fuente (no del orquestador) garantiza que funcione
            # aunque la fuente y el orquestador estén en zonas horarias distintas.
            with source_engine.connect() as _conn:
                max_ts = _conn.execute(
                    text(f"SELECT MAX({watermark_col}) FROM {table}")
                ).scalar()
            if max_ts is not None:
                # Normalizar a datetime naive: Oracle devuelve TIMESTAMP sin tz,
                # así la comparación en la próxima ejecución no tiene conflictos de tz.
                if hasattr(max_ts, "tzinfo") and max_ts.tzinfo is not None:
                    max_ts = max_ts.replace(tzinfo=None)
                watermark_mgr.update_timestamp(table, max_ts)
            else:
                log.info("watermark_not_updated", reason="no_rows_extracted", table=table)

        elif mode == "rowversion":
            last_rv = watermark_mgr.get_last_rowversion(table)
            chunks = extractor.extract_by_rowversion(
                table=table,
                rowver_col=watermark_col,
                last_rowver=last_rv,
            )
            if validator is not None:
                chunks = _validate_first_chunk(chunks, validator, target_table, target_schema)

            # Necesitamos el máximo rowversion de los chunks cargados
            max_rowver = 0

            def tracking_chunks():
                nonlocal max_rowver
                for df in chunks:
                    if "__rowver" in df.columns:
                        chunk_max = df["__rowver"].max()
                        if chunk_max and chunk_max > max_rowver:
                            max_rowver = chunk_max
                    yield df

            results = loader.load_chunks(tracking_chunks(), target_table, conflict_columns, schema=target_schema)

            if max_rowver > 0:
                watermark_mgr.update_rowversion(table, max_rowver)

        else:
            raise ValueError(f"Unknown mode: {mode}. Use 'timestamp' or 'rowversion'.")

    except Exception as exc:
        log.error(
            "pipeline_failed",
            table=table,
            error=str(exc),
            exc_info=True,
        )
        raise
    finally:
        source_engine.dispose()
        target_engine.dispose()
        log.info("connections_disposed")

    log.info("pipeline_complete", table=table, **results)
    return results


def main() -> None:
    setup_logging()

    parser = argparse.ArgumentParser(description="Pipeline ETL incremental")
    parser.add_argument("--table", required=True, help="Nombre de la tabla fuente")
    parser.add_argument("--target-table", help="Nombre de la tabla destino (por defecto: igual que la fuente)")
    parser.add_argument("--mode", choices=["timestamp", "rowversion"], default="timestamp")
    parser.add_argument("--watermark-col", required=True, help="Columna para seguimiento incremental")
    parser.add_argument(
        "--conflict-cols",
        required=True,
        nargs="+",
        help="Columnas PK para el target del UPSERT ON CONFLICT",
    )
    args = parser.parse_args()

    results = run_pipeline(
        table=args.table,
        mode=args.mode,
        watermark_col=args.watermark_col,
        conflict_columns=args.conflict_cols,
        target_table=args.target_table,
    )

    failed = results.get("chunks_failed", 0)
    sys.exit(1 if failed > 0 else 0)


if __name__ == "__main__":
    main()
