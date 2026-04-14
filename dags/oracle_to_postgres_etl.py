"""
DAG: oracle_to_postgres_etl
============================
Orquesta el pipeline ETL incremental desde Oracle hacia PostgreSQL.

Configuración dinámica vía Airflow Variables
─────────────────────────────────────────────
Variable JSON  ``etl_oracle_tables``  (se crea automáticamente con los
defaults la primera vez que se ejecuta el DAG):

    [
      {
        "table": "ventas",
        "mode": "timestamp",
        "watermark_col": "updated_at",
        "conflict_columns": ["id"],
        "target_table": null        ← null = mismo nombre que la fuente
      }
    ]

Credenciales de BD vía Variables de entorno (montadas desde .env):
    SOURCE_DIALECT, SOURCE_HOST, SOURCE_PORT, SOURCE_DATABASE,
    SOURCE_USER, SOURCE_PASSWORD, TARGET_HOST, TARGET_PORT,
    TARGET_DATABASE, TARGET_USER, TARGET_PASSWORD, etc.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

log = logging.getLogger(__name__)

# ── Configuración por defecto de los DAGs ────────────────────────────────────

DEFAULT_ARGS = {
    "owner": "etl-team",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "email_on_failure": False,
    "email_on_retry": False,
}

# Tablas a sincronizar — se puede sobreescribir con la Variable Airflow
# ``etl_oracle_tables`` (JSON).
DEFAULT_TABLES: list[dict] = [
    {
        "table": "ventas",
        "mode": "timestamp",
        "watermark_col": "updated_at",
        "conflict_columns": ["id"],
        "target_table": None,
    },
]


# ── Helpers ───────────────────────────────────────────────────────────────────

def _load_etl_config():
    """Construye ETLConfig desde variables de entorno."""
    # Import diferido para no bloquear el parser de Airflow si las
    # dependencias del proyecto no están disponibles en el scheduler.
    from etl.config.settings import ETLConfig, SourceConfig, TargetConfig  # noqa: PLC0415

    source = SourceConfig(
        dialect=os.environ["SOURCE_DIALECT"],
        host=os.environ["SOURCE_HOST"],
        port=int(os.environ.get("SOURCE_PORT", 1521)),
        database=os.environ["SOURCE_DATABASE"],
        username=os.environ["SOURCE_USER"],
        password=os.environ["SOURCE_PASSWORD"],
        pool_size=int(os.environ.get("SOURCE_POOL_SIZE", 3)),
    )
    target = TargetConfig(
        host=os.environ["TARGET_HOST"],
        port=int(os.environ.get("TARGET_PORT", 5432)),
        database=os.environ["TARGET_DATABASE"],
        username=os.environ["TARGET_USER"],
        password=os.environ["TARGET_PASSWORD"],
        pool_size=int(os.environ.get("TARGET_POOL_SIZE", 5)),
    )
    return ETLConfig(
        source=source,
        target=target,
        chunk_size=int(os.environ.get("ETL_CHUNK_SIZE", 10_000)),
        max_workers=int(os.environ.get("ETL_MAX_WORKERS", 4)),
        retry_attempts=int(os.environ.get("ETL_RETRY_ATTEMPTS", 5)),
    )


def run_etl_table(
    table: str,
    mode: str,
    watermark_col: str,
    conflict_columns: list[str],
    target_table: str | None,
    **context: Any,
) -> dict:
    """
    Función Python que ejecuta el pipeline para una tabla.
    El resultado se pushea a XCom para trazabilidad.
    """
    from etl.pipeline import run_pipeline  # noqa: PLC0415
    from etl.utils.logging_setup import setup_logging  # noqa: PLC0415

    setup_logging()

    config = _load_etl_config()

    log.info(
        "Starting ETL pipeline | table=%s mode=%s watermark_col=%s",
        table, mode, watermark_col,
    )

    result = run_pipeline(
        table=table,
        mode=mode,
        watermark_col=watermark_col,
        conflict_columns=conflict_columns,
        target_table=target_table,
        config=config,
    )

    log.info(
        "ETL pipeline finished | table=%s total_rows=%s chunks_ok=%s chunks_failed=%s",
        table,
        result.get("total_rows", 0),
        result.get("chunks_ok", 0),
        result.get("chunks_failed", 0),
    )

    # Falla el task si hubo chunks con error (Airflow reintentará según DEFAULT_ARGS)
    if result.get("chunks_failed", 0) > 0:
        raise RuntimeError(
            f"Pipeline completed with {result['chunks_failed']} failed chunk(s). "
            f"Table: {table}. Full result: {result}"
        )

    return result


# ── Construcción dinámica del DAG ─────────────────────────────────────────────

def _get_table_configs() -> list[dict]:
    """Lee la configuración de tablas desde Airflow Variable (con fallback al default)."""
    try:
        raw = Variable.get("etl_oracle_tables", default_var=None)
        if raw:
            return json.loads(raw)
    except Exception as exc:  # noqa: BLE001
        log.warning("Could not read Variable 'etl_oracle_tables': %s — using defaults", exc)
    return DEFAULT_TABLES


with DAG(
    dag_id="oracle_to_postgres_etl",
    description="Pipeline ETL incremental Oracle → PostgreSQL",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 1, 1),
    schedule_interval="@hourly",       # cambiar a "0 */6 * * *" para cada 6 h, etc.
    catchup=False,                     # no backfill si el scheduler arranca tarde
    max_active_runs=1,                 # evita ejecuciones solapadas
    tags=["etl", "oracle", "postgres", "incremental"],
    doc_md=__doc__,
) as dag:

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end")

    table_configs = _get_table_configs()

    for cfg in table_configs:
        table_name = cfg["table"]

        with TaskGroup(group_id=f"etl_{table_name}") as tg:
            extract_load = PythonOperator(
                task_id="extract_load",
                python_callable=run_etl_table,
                op_kwargs={
                    "table":            cfg["table"],
                    "mode":             cfg.get("mode", "timestamp"),
                    "watermark_col":    cfg["watermark_col"],
                    "conflict_columns": cfg["conflict_columns"],
                    "target_table":     cfg.get("target_table"),
                },
                # El resultado queda en XCom automáticamente
            )

        start >> tg >> end
