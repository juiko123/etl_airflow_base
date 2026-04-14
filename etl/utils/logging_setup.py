"""
Logging estructurado para monitoreo del DBA.
Genera líneas JSON a archivo + stderr — parseable por Datadog, Splunk, grep.

Cuando se ejecuta dentro de Airflow (AIRFLOW_CTX_DAG_ID presente) se omite
la configuración de handlers para no interferir con el sistema de logging de
Airflow y evitar el loop de re-serialización JSON.
"""
import logging
import sys
import os
from pathlib import Path
import structlog
from structlog.types import Processor


def _inside_airflow() -> bool:
    return "AIRFLOW_CTX_DAG_ID" in os.environ


def setup_logging(log_dir: str = "etl/logs", level: str = "INFO") -> None:
    shared_processors: list[Processor] = [
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
    ]

    if _inside_airflow():
        # Dentro de Airflow: solo configurar structlog para que use el
        # logging estándar — Airflow ya gestiona handlers y formatters.
        structlog.configure(
            processors=shared_processors + [
                structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
            ],
            logger_factory=structlog.stdlib.LoggerFactory(),
            wrapper_class=structlog.stdlib.BoundLogger,
            cache_logger_on_first_use=True,
        )
        return

    # Fuera de Airflow: configuración completa con archivo JSONL + stderr
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    log_file = Path(log_dir) / "etl_pipeline.jsonl"

    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    fmt = logging.Formatter("%(message)s")

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(fmt)

    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setFormatter(fmt)

    root_logger.addHandler(file_handler)
    root_logger.addHandler(stderr_handler)

    structlog.configure(
        processors=shared_processors + [
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        processor=structlog.processors.JSONRenderer(),
        foreign_pre_chain=shared_processors,
    )

    for handler in root_logger.handlers:
        handler.setFormatter(formatter)


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    return structlog.get_logger(name)
