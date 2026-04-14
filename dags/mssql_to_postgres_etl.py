"""
DAG: mssql_to_postgres_etl
===========================
Orquesta el pipeline ETL incremental desde Microsoft SQL Server hacia PostgreSQL.

Soporta dos modos de extracción incremental:
  - ``timestamp``  : filtra por columna datetime (ej. ``updated_at``)
  - ``rowversion`` : filtra por columna rowversion/timestamp de MSSQL (más preciso,
                     no depende del reloj de la aplicación)

Configuración dinámica vía Airflow Variable ``etl_mssql_tables`` (JSON).
Las credenciales se leen de variables de entorno (ver .env.example).
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

DEFAULT_TABLES: list[dict] = [
    # Ejemplo timestamp — igual que en Oracle
    # {
    #     "table": "orders",
    #     "mode": "timestamp",
    #     "watermark_col": "updated_at",
    #     "conflict_columns": ["order_id"],
    # },
    # Ejemplo rowversion — más robusto para MSSQL
    # {
    #     "table": "accounts",
    #     "mode": "rowversion",
    #     "watermark_col": "row_ver",
    #     "conflict_columns": ["account_id"],
    # },
]


def _load_etl_config_mssql():
    from etl.config.settings import ETLConfig, SourceConfig, TargetConfig  # noqa: PLC0415

    source = SourceConfig(
        dialect="mssql",
        host=os.environ["SOURCE_HOST"],
        port=int(os.environ.get("SOURCE_PORT", 1433)),
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


def run_etl_mssql_table(
    table: str,
    mode: str,
    watermark_col: str,
    conflict_columns: list[str],
    target_table: str | None,
    **context: Any,
) -> dict:
    from etl.pipeline import run_pipeline  # noqa: PLC0415
    from etl.utils.logging_setup import setup_logging  # noqa: PLC0415

    setup_logging()
    config = _load_etl_config_mssql()

    log.info(
        "Starting MSSQL ETL | table=%s mode=%s watermark_col=%s",
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
        "MSSQL ETL finished | table=%s total_rows=%s chunks_ok=%s chunks_failed=%s",
        table,
        result.get("total_rows", 0),
        result.get("chunks_ok", 0),
        result.get("chunks_failed", 0),
    )

    if result.get("chunks_failed", 0) > 0:
        raise RuntimeError(
            f"Pipeline completed with {result['chunks_failed']} failed chunk(s). "
            f"Table: {table}. Full result: {result}"
        )

    return result


def _get_table_configs() -> list[dict]:
    try:
        raw = Variable.get("etl_mssql_tables", default_var=None)
        if raw:
            return json.loads(raw)
    except Exception as exc:  # noqa: BLE001
        log.warning("Could not read Variable 'etl_mssql_tables': %s — using defaults", exc)
    return DEFAULT_TABLES


with DAG(
    dag_id="mssql_to_postgres_etl",
    description="Pipeline ETL incremental MSSQL → PostgreSQL (timestamp y rowversion)",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 1, 1),
    schedule_interval="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["etl", "mssql", "postgres", "incremental"],
    doc_md=__doc__,
) as dag:

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end")

    table_configs = _get_table_configs()

    if not table_configs:
        # DAG vacío — sólo visible en la UI para recordar que existe
        no_tables = EmptyOperator(task_id="no_tables_configured")
        start >> no_tables >> end
    else:
        for cfg in table_configs:
            table_name = cfg["table"]

            with TaskGroup(group_id=f"etl_{table_name}") as tg:
                PythonOperator(
                    task_id="extract_load",
                    python_callable=run_etl_mssql_table,
                    op_kwargs={
                        "table":            cfg["table"],
                        "mode":             cfg.get("mode", "timestamp"),
                        "watermark_col":    cfg["watermark_col"],
                        "conflict_columns": cfg["conflict_columns"],
                        "target_table":     cfg.get("target_table"),
                    },
                )

            start >> tg >> end
