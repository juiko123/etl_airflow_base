"""
Configuración del ETL — cadenas de conexión, parámetros de chunking y política de reintentos.
"""
from dataclasses import dataclass, field
from typing import Optional
import os
from dotenv import load_dotenv

load_dotenv()


@dataclass
class SourceConfig:
    """Configuración de la fuente de datos heredada (MSSQL u Oracle)."""
    dialect: str          # "mssql" | "oracle"
    host: str
    port: int
    database: str
    username: str
    password: str
    pool_size: int = 3     # mantener pequeño — las DBs heredadas tienen límites estrechos de conexión
    max_overflow: int = 2
    pool_timeout: int = 30
    pool_recycle: int = 1800

    def get_url(self) -> str:
        if self.dialect == "mssql":
            return (
                f"mssql+pyodbc://{self.username}:{self.password}"
                f"@{self.host}:{self.port}/{self.database}"
                "?driver=ODBC+Driver+17+for+SQL+Server"
            )
        elif self.dialect == "oracle":
            # python-oracledb actúa como drop-in de cx_Oracle en modo thin
            # (sin Oracle Client instalado). SQLAlchemy 1.4 usa el dialecto
            # oracle+cx_oracle y verifica cx_Oracle.version >= "5.2".
            import sys
            import oracledb
            if "cx_Oracle" not in sys.modules:
                oracledb.version = "8.3.0"  # simula cx_Oracle >= 5.2
                sys.modules["cx_Oracle"] = oracledb
            return (
                f"oracle+cx_oracle://{self.username}:{self.password}"
                f"@{self.host}:{self.port}/?service_name={self.database}"
            )
        raise ValueError(f"Unsupported dialect: {self.dialect}")


@dataclass
class TargetConfig:
    """Configuración del destino en Postgres, consumido por Power BI."""
    host: str
    port: int
    database: str
    username: str
    password: str
    pool_size: int = 5
    max_overflow: int = 10
    pool_timeout: int = 30
    pool_recycle: int = 3600

    def get_url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.username}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )


@dataclass
class ETLConfig:
    """Parámetros de ejecución del ETL."""
    source: SourceConfig
    target: TargetConfig

    # Chunking
    chunk_size: int = 10_000       # filas por chunk
    max_workers: int = 4           # workers paralelos de inserción

    # Política de reintentos
    retry_attempts: int = 5
    retry_wait_min: float = 2.0    # segundos
    retry_wait_max: float = 30.0
    retry_multiplier: float = 1.5  # multiplicador de backoff exponencial

    # Watermark incremental
    watermark_table: str = "etl_watermarks"


def load_config() -> ETLConfig:
    """Construye la configuración a partir de variables de entorno."""
    source = SourceConfig(
        dialect=os.environ["SOURCE_DIALECT"],
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
