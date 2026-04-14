"""
Extractor incremental — soporta watermarks por timestamp y por rowversion.
Transmite filas en chunks para evitar cargar la tabla completa en memoria.
"""
from datetime import datetime, timezone
from typing import Generator, Optional, Union

import polars as pl
from sqlalchemy import text
from sqlalchemy.engine import Engine

from etl.config.settings import ETLConfig
from etl.utils.logging_setup import get_logger

log = get_logger(__name__)


class IncrementalExtractor:
    """
    Extrae únicamente las filas nuevas o modificadas de la DB heredada.

    Selección de estrategia:
        - timestamp: WHERE updated_at > :last_ts  (Oracle, MSSQL con columnas datetime)
        - rowversion: WHERE rowver > :last_rv      (rowversion / tipo timestamp de MSSQL)
    """

    def __init__(self, engine: Engine, config: ETLConfig):
        self.engine = engine
        self.config = config

    # ------------------------------------------------------------------
    # API pública
    # ------------------------------------------------------------------

    def extract_by_timestamp(
        self,
        table: str,
        watermark_col: str,
        last_value: Optional[datetime],
    ) -> Generator[pl.DataFrame, None, None]:
        """
        Produce DataFrames de polars con `chunk_size` filas cada uno.
        `last_value=None` → carga completa (primera ejecución).
        """
        if last_value is None:
            query = f"""
                SELECT *
                FROM   {table}
                ORDER  BY {watermark_col}
            """
            params: dict = {}
        else:
            query = f"""
                SELECT *
                FROM   {table}
                WHERE  {watermark_col} > :last_ts
                ORDER  BY {watermark_col}
            """
            params = {"last_ts": last_value}

        yield from self._stream_chunks(query, params, table)

    def extract_by_rowversion(
        self,
        table: str,
        rowver_col: str,
        last_rowver: Optional[int],
    ) -> Generator[pl.DataFrame, None, None]:
        """
        Extracción específica de MSSQL por rowversion.
        El rowversion se almacena como binary(8); se convierte a bigint para comparación.
        """
        if last_rowver is None:
            query = f"""
                SELECT *, CONVERT(BIGINT, {rowver_col}) AS __rowver
                FROM   {table}
                ORDER  BY {rowver_col}
            """
            params = {}
        else:
            query = f"""
                SELECT *, CONVERT(BIGINT, {rowver_col}) AS __rowver
                FROM   {table}
                WHERE  CONVERT(BIGINT, {rowver_col}) > :last_rv
                ORDER  BY {rowver_col}
            """
            params = {"last_rv": last_rowver}

        yield from self._stream_chunks(query, params, table)

    # ------------------------------------------------------------------
    # Streaming interno
    # ------------------------------------------------------------------

    def _stream_chunks(
        self,
        query: str,
        params: dict,
        table: str,
    ) -> Generator[pl.DataFrame, None, None]:
        """
        Usa cursor del lado del servidor (yield_per) para que solo `chunk_size`
        filas residan en memoria de Python a la vez.
        """
        chunk_size = self.config.chunk_size
        total_rows = 0
        chunk_index = 0

        with self.engine.connect() as conn:
            # stream_results → cursor del lado del servidor en drivers compatibles
            conn = conn.execution_options(stream_results=True, yield_per=chunk_size)
            result = conn.execute(text(query), params)
            columns = list(result.keys())

            batch: list[tuple] = []
            for row in result:
                batch.append(tuple(row))
                if len(batch) >= chunk_size:
                    df = pl.DataFrame(batch, schema=columns, orient="row")
                    total_rows += len(df)
                    log.info(
                        "chunk_extracted",
                        table=table,
                        chunk=chunk_index,
                        rows=len(df),
                        cumulative=total_rows,
                    )
                    yield df
                    batch = []
                    chunk_index += 1

            # vaciar el remanente
            if batch:
                df = pl.DataFrame(batch, schema=columns, orient="row")
                total_rows += len(df)
                log.info(
                    "chunk_extracted",
                    table=table,
                    chunk=chunk_index,
                    rows=len(df),
                    cumulative=total_rows,
                    final=True,
                )
                yield df

        log.info("extraction_complete", table=table, total_rows=total_rows)
