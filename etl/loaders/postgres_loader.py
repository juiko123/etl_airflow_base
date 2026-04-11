"""
Cargador de Postgres — inserciones en chunks paralelas con lógica de reintentos.

Arquitectura:
    - ThreadPoolExecutor con semáforo acotado → paralelismo controlado
    - Cada worker tiene su propia conexión (sin estado compartido)
    - Tenacity maneja fallos transitorios de red/DB con backoff exponencial
    - UPSERT (INSERT ... ON CONFLICT DO UPDATE) garantiza idempotencia en re-ejecuciones
"""
from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from typing import Generator, List, Optional

import polars as pl
from sqlalchemy import text
from sqlalchemy.engine import Engine, Connection
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    before_sleep_log,
    RetryError,
)

from etl.config.settings import ETLConfig
from etl.utils.logging_setup import get_logger

log = get_logger(__name__)

# Reintentar en errores transitorios de DB/red
# (en producción, acotar a psycopg2.OperationalError)
RETRYABLE_EXCEPTIONS = (
    Exception,
)


class PostgresLoader:
    """
    Carga DataFrames de polars en Postgres con:
        - Workers paralelos (ThreadPoolExecutor)
        - Reintento por worker con backoff exponencial
        - UPSERT para garantizar idempotencia
    """

    def __init__(self, engine: Engine, config: ETLConfig):
        self.engine = engine
        self.config = config

    # ------------------------------------------------------------------
    # API pública
    # ------------------------------------------------------------------

    def load_chunks(
        self,
        chunks: Generator[pl.DataFrame, None, None],
        target_table: str,
        conflict_columns: List[str],
        schema: str = "public",
    ) -> dict:
        """
        Consume el generador de chunks y los despacha al pool de hilos.
        Devuelve resumen: {total_rows, chunks_ok, chunks_failed, elapsed_s}
        """
        start = time.monotonic()
        pending = []
        results = {"total_rows": 0, "chunks_ok": 0, "chunks_failed": 0}

        with ThreadPoolExecutor(max_workers=self.config.max_workers) as pool:
            for idx, chunk_df in enumerate(chunks):
                future = pool.submit(
                    self._load_one_chunk_with_retry,
                    chunk_df,
                    target_table,
                    conflict_columns,
                    schema,
                    idx,
                )
                pending.append((idx, future, len(chunk_df)))

            for idx, future, row_count in pending:
                try:
                    future.result()
                    results["chunks_ok"] += 1
                    results["total_rows"] += row_count
                    log.info(
                        "chunk_loaded",
                        table=target_table,
                        chunk=idx,
                        rows=row_count,
                    )
                except RetryError as exc:
                    results["chunks_failed"] += 1
                    log.error(
                        "chunk_failed_all_retries",
                        table=target_table,
                        chunk=idx,
                        rows=row_count,
                        error=str(exc),
                    )
                except Exception as exc:
                    results["chunks_failed"] += 1
                    log.error(
                        "chunk_unexpected_error",
                        table=target_table,
                        chunk=idx,
                        error=str(exc),
                        exc_info=True,
                    )

        results["elapsed_s"] = round(time.monotonic() - start, 2)
        self._log_summary(target_table, results)
        return results

    # ------------------------------------------------------------------
    # Métodos internos
    # ------------------------------------------------------------------

    def _load_one_chunk_with_retry(
        self,
        df: pl.DataFrame,
        table: str,
        conflict_columns: List[str],
        schema: str,
        chunk_idx: int,
    ) -> None:
        """Envuelve _load_one_chunk con reintentos de tenacity."""
        cfg = self.config

        @retry(
            retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS),
            stop=stop_after_attempt(cfg.retry_attempts),
            wait=wait_exponential(
                multiplier=cfg.retry_multiplier,
                min=cfg.retry_wait_min,
                max=cfg.retry_wait_max,
            ),
            before_sleep=lambda rs: log.warning(
                "chunk_load_retry",
                table=table,
                chunk=chunk_idx,
                attempt=rs.attempt_number,
                wait_s=getattr(rs.next_action, "sleep", None),
                error=str(rs.outcome.exception()) if rs.outcome else None,
            ),
            reraise=False,
        )
        def _with_retry() -> None:
            self._load_one_chunk(df, table, conflict_columns, schema, chunk_idx)

        _with_retry()

    def _load_one_chunk(
        self,
        df: pl.DataFrame,
        table: str,
        conflict_columns: List[str],
        schema: str,
        chunk_idx: int,
    ) -> None:
        """
        UPSERT del chunk en Postgres.
        Genera: INSERT INTO t (cols) VALUES (...) ON CONFLICT (pk) DO UPDATE SET ...
        Cada worker usa su propia conexión del pool — sin estado compartido.
        """
        cols = df.columns
        # Eliminar la columna de metadatos internos añadida por el extractor de rowversion
        if "__rowver" in cols:
            df = df.drop("__rowver")
            cols = df.columns

        placeholders = ", ".join(f":{c}" for c in cols)
        col_list = ", ".join(cols)
        conflict_target = ", ".join(conflict_columns)
        update_set = ", ".join(
            f"{c} = EXCLUDED.{c}"
            for c in cols
            if c not in conflict_columns
        )

        upsert_sql = f"""
            INSERT INTO {schema}.{table} ({col_list})
            VALUES ({placeholders})
            ON CONFLICT ({conflict_target})
            DO UPDATE SET {update_set}
        """

        rows = df.to_dicts()

        with self.engine.begin() as conn:
            conn.execute(text(upsert_sql), rows)

        log.debug(
            "chunk_upserted",
            table=table,
            chunk=chunk_idx,
            rows=len(df),
        )

    def _log_summary(self, table: str, results: dict) -> None:
        status = "success" if results["chunks_failed"] == 0 else "partial_failure"
        log.info(
            "load_summary",
            table=table,
            status=status,
            **results,
        )
