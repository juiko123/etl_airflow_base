"""
Capa de Validación de Esquemas — garantiza que un DataFrame de Polars sea
compatible con la tabla destino de PostgreSQL antes de iniciar cualquier carga.

Verificaciones realizadas:
  1. Columnas requeridas (NOT NULL sin default) presentes en el DataFrame.
  2. Compatibilidad de tipos: dtype de Polars vs data_type de PostgreSQL.
  3. Restricción de nulos: las columnas NOT NULL no deben contener valores nulos.
  4. Columnas extra en el DataFrame se reportan como advertencias (el loader las ignora).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

import polars as pl
from sqlalchemy import text
from sqlalchemy.engine import Engine

from etl.utils.logging_setup import get_logger

log = get_logger(__name__)


# ---------------------------------------------------------------------------
# Mapa de compatibilidad de tipos: tipo base de Polars → substrings de data_type en PG
# ---------------------------------------------------------------------------
_POLARS_TO_PG: dict[type, set[str]] = {
    # Todos los tipos enteros de Polars son aceptados por cualquier columna
    # entera o numérica de PG. El overflow lo reporta Postgres al insertar.
    pl.Int8:     {"smallint", "integer", "bigint", "numeric", "serial"},
    pl.Int16:    {"smallint", "integer", "bigint", "numeric", "serial"},
    pl.Int32:    {"smallint", "integer", "bigint", "numeric", "serial"},
    pl.Int64:    {"smallint", "integer", "bigint", "numeric", "int8", "serial",
                  "real", "float", "double"},   # enteros se convierten a float sin error
    pl.UInt8:    {"smallint", "integer", "bigint", "numeric"},
    pl.UInt16:   {"integer", "bigint", "numeric"},
    pl.UInt32:   {"integer", "bigint", "numeric"},
    pl.UInt64:   {"bigint", "numeric"},
    pl.Float32:  {"real", "float", "double", "numeric"},
    pl.Float64:  {"real", "float", "double", "numeric"},
    pl.Utf8:     {"char", "text", "varchar", "uuid", "json", "xml"},
    pl.String:   {"char", "text", "varchar", "uuid", "json", "xml"},
    pl.Boolean:  {"bool"},
    pl.Date:     {"date"},
    pl.Time:     {"time"},
    pl.Duration: {"interval"},
    pl.Binary:   {"bytea"},
}

# Datetime es parametrizado en Polars (Datetime(time_unit, time_zone));
# se identifica por el nombre de la clase base.
_DATETIME_BASE_NAMES = {"Datetime", "Date", "Time"}


# ---------------------------------------------------------------------------
# Tipos de resultado
# ---------------------------------------------------------------------------

@dataclass
class ColumnIssue:
    column: str
    kind: str    # "missing_required" | "extra" | "type_mismatch" | "nullable_violation" | ...
    detail: str


@dataclass
class ValidationResult:
    valid: bool = True
    errors: list[ColumnIssue] = field(default_factory=list)
    warnings: list[ColumnIssue] = field(default_factory=list)

    def add_error(self, column: str, kind: str, detail: str) -> None:
        self.errors.append(ColumnIssue(column, kind, detail))
        self.valid = False

    def add_warning(self, column: str, kind: str, detail: str) -> None:
        self.warnings.append(ColumnIssue(column, kind, detail))

    def summary(self) -> str:
        if self.valid:
            return f"PASS (advertencias={len(self.warnings)})"
        parts = [f"{e.kind}:{e.column}" for e in self.errors]
        return "FAIL — " + "; ".join(parts)


# ---------------------------------------------------------------------------
# Excepción personalizada
# ---------------------------------------------------------------------------

class SchemaValidationError(ValueError):
    """Se lanza cuando un DataFrame falla la Validación de Esquemas antes de la carga."""

    def __init__(self, result: ValidationResult) -> None:
        self.result = result
        details = "; ".join(f"[{e.kind}] {e.column}: {e.detail}" for e in result.errors)
        super().__init__(f"Validación de Esquemas fallida — {details}")


# ---------------------------------------------------------------------------
# Validador
# ---------------------------------------------------------------------------

class SchemaValidator:
    """
    Valida un DataFrame de Polars contra el esquema de una tabla destino en PostgreSQL.

    Uso::

        validator = SchemaValidator(target_engine)
        result = validator.validate(df, "transactions", schema="public")
        if not result.valid:
            raise SchemaValidationError(result)
    """

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    # ------------------------------------------------------------------
    # API pública
    # ------------------------------------------------------------------

    def validate(
        self,
        df: pl.DataFrame,
        target_table: str,
        schema: str = "public",
    ) -> ValidationResult:
        """
        Valida *df* contra ``schema.target_table`` en PostgreSQL.

        Devuelve un :class:`ValidationResult`; nunca lanza excepciones.
        El caller decide si elevar el error.
        """
        result = ValidationResult()
        log.info(
            "schema_validation_start",
            table=f"{schema}.{target_table}",
            df_rows=len(df),
            df_cols=df.columns,
        )

        try:
            table_cols = self._fetch_table_schema(target_table, schema)
        except ValueError as exc:
            result.add_error("*", "table_not_found", str(exc))
            return result

        df_col_set = set(df.columns)
        table_col_map: dict[str, dict] = {c["column_name"]: c for c in table_cols}
        table_col_set = set(table_col_map)

        # 1. ¿Las columnas requeridas están en el DataFrame?
        for col_info in table_cols:
            col = col_info["column_name"]
            is_required = not col_info["is_nullable"] and not col_info["has_default"]
            if col not in df_col_set:
                if is_required:
                    result.add_error(
                        col,
                        "missing_required",
                        f"La columna NOT NULL '{col}' (sin default) está ausente del DataFrame.",
                    )
                else:
                    result.add_warning(
                        col,
                        "missing_optional",
                        f"Columna nullable/con default '{col}' ausente; la DB usará default o NULL.",
                    )

        # 2. Columnas extra del DataFrame que no existen en la tabla → advertencias
        for col in sorted(df_col_set - table_col_set):
            result.add_warning(
                col,
                "extra_column",
                f"La columna '{col}' no existe en la tabla destino '{schema}.{target_table}'.",
            )

        # 3. Compatibilidad de tipos + restricción de nulos para columnas compartidas
        for col in sorted(df_col_set & table_col_set):
            col_info = table_col_map[col]
            pg_type = col_info["data_type"]
            polars_dtype = df[col].dtype

            if not self._types_compatible(polars_dtype, pg_type):
                result.add_error(
                    col,
                    "type_mismatch",
                    (
                        f"El dtype de Polars '{polars_dtype}' no es compatible "
                        f"con el tipo de PostgreSQL '{pg_type}'."
                    ),
                )

            if not col_info["is_nullable"] and df[col].null_count() > 0:
                result.add_error(
                    col,
                    "nullable_violation",
                    (
                        f"La columna '{col}' es NOT NULL en el destino pero el DataFrame "
                        f"contiene {df[col].null_count()} valor(es) nulo(s)."
                    ),
                )

        log.info(
            "schema_validation_complete",
            table=f"{schema}.{target_table}",
            valid=result.valid,
            errors=len(result.errors),
            warnings=len(result.warnings),
        )
        return result

    # ------------------------------------------------------------------
    # Métodos internos
    # ------------------------------------------------------------------

    def _fetch_table_schema(self, table: str, schema: str) -> list[dict]:
        """Consulta ``information_schema.columns`` para *schema.table*."""
        query = text("""
            SELECT
                column_name,
                data_type,
                is_nullable,
                column_default
            FROM information_schema.columns
            WHERE table_schema = :schema
              AND table_name   = :table
            ORDER BY ordinal_position
        """)
        with self._engine.connect() as conn:
            rows = conn.execute(query, {"schema": schema, "table": table}).fetchall()

        if not rows:
            raise ValueError(
                f"La tabla '{schema}.{table}' no se encontró en information_schema "
                f"(verificar nombre, esquema y permisos)."
            )

        return [
            {
                "column_name": r[0],
                "data_type": r[1].lower(),
                "is_nullable": r[2].upper() == "YES",
                "has_default": r[3] is not None,
            }
            for r in rows
        ]

    @staticmethod
    def _types_compatible(polars_dtype: pl.PolarsDataType, pg_type: str) -> bool:
        """
        Devuelve ``True`` si *polars_dtype* puede almacenarse en una columna PG de *pg_type*.

        Datetime es parametrizado en Polars (p.ej. ``Datetime(time_unit='us', …)``);
        se compara por nombre de clase base.  Los tipos compuestos desconocidos
        (Struct, List, …) se permiten en columnas json/text.
        """
        base_cls = type(polars_dtype)
        base_name = base_cls.__name__

        # Familia Datetime parametrizada
        if base_name in _DATETIME_BASE_NAMES:
            return any(t in pg_type for t in ("timestamp", "date", "time"))

        # Decimal parametrizado (Oracle NUMBER, MSSQL DECIMAL/NUMERIC)
        if base_name == "Decimal":
            return any(t in pg_type for t in ("numeric", "decimal", "real", "float", "double"))

        compatible = _POLARS_TO_PG.get(base_cls)
        if compatible is None:
            # Struct, List, Array, etc. → permitir almacenamiento en json/text
            return any(t in pg_type for t in ("json", "text", "char", "xml"))

        return any(pg_t in pg_type for pg_t in compatible)
