"""
Gestor de watermarks — registra el último timestamp extraído por tabla.
Persiste en Postgres para que los reinicios de Airflow mantengan consistencia.
"""
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine

from etl.utils.logging_setup import get_logger

log = get_logger(__name__)


CREATE_WATERMARK_TABLE = """
CREATE TABLE IF NOT EXISTS etl_watermarks (
    table_name    VARCHAR(255) PRIMARY KEY,
    last_value    TIMESTAMPTZ,
    last_rowver   BIGINT,           -- rowversion de MSSQL convertido a entero
    updated_at    TIMESTAMPTZ DEFAULT NOW()
);
"""


class WatermarkManager:
    def __init__(self, engine: Engine, schema: str = "public"):
        self.engine = engine
        self.schema = schema
        self._ensure_table()

    def _ensure_table(self) -> None:
        with self.engine.begin() as conn:
            conn.execute(text(CREATE_WATERMARK_TABLE))
        log.debug("watermark_table_ready")

    def get_last_timestamp(self, table_name: str) -> Optional[datetime]:
        with self.engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT last_value FROM etl_watermarks WHERE table_name = :t"
                ),
                {"t": table_name},
            ).fetchone()
        if row and row[0]:
            log.info("watermark_loaded", table=table_name, last_value=str(row[0]))
            return row[0]
        log.info("no_watermark_found", table=table_name, action="full_load")
        return None

    def get_last_rowversion(self, table_name: str) -> Optional[int]:
        with self.engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT last_rowver FROM etl_watermarks WHERE table_name = :t"
                ),
                {"t": table_name},
            ).fetchone()
        return row[0] if row else None

    def update_timestamp(self, table_name: str, new_value: datetime) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO etl_watermarks (table_name, last_value, updated_at)
                    VALUES (:t, :v, NOW())
                    ON CONFLICT (table_name)
                    DO UPDATE SET last_value = EXCLUDED.last_value,
                                  updated_at = NOW()
                """),
                {"t": table_name, "v": new_value},
            )
        log.info("watermark_updated", table=table_name, new_value=str(new_value))

    def update_rowversion(self, table_name: str, new_rowver: int) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO etl_watermarks (table_name, last_rowver, updated_at)
                    VALUES (:t, :v, NOW())
                    ON CONFLICT (table_name)
                    DO UPDATE SET last_rowver = EXCLUDED.last_rowver,
                                  updated_at = NOW()
                """),
                {"t": table_name, "v": new_rowver},
            )
        log.info("watermark_updated", table=table_name, new_rowver=new_rowver)
