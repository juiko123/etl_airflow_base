"""
prepare_schemas.py
==================
Script de preparación de esquemas para el pipeline ETL Oracle → PostgreSQL.

Lo que hace:
  1. Oracle  — crea la tabla `ventas` e inserta filas de ejemplo
  2. Oracle  — crea tabla secundaria `clientes` para demostrar multi-tabla
  3. PostgreSQL — crea las tablas destino (`ventas`, `clientes`)
  4. PostgreSQL — crea la tabla de control `etl_watermarks` (usada por el pipeline)

Uso (desde la raíz del proyecto con el stack Docker activo):
    python scripts/prepare_schemas.py

    # Para solo preparar Oracle o PostgreSQL:
    python scripts/prepare_schemas.py --only oracle
    python scripts/prepare_schemas.py --only postgres

    # Para limpiar y recrear desde cero:
    python scripts/prepare_schemas.py --drop

Requisitos:
    docker compose up -d          # stack levantado
    pip install -r requirements.txt
"""

from __future__ import annotations

import argparse
import io
import sys
from datetime import datetime, timedelta
import random

# Forzar UTF-8 en stdout/stderr para que los caracteres Unicode funcionen
# en Windows sin necesitar el flag -X utf8
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

import oracledb
import psycopg2
from psycopg2.extras import execute_values

# ─────────────────────────────────────────────────────────────────────────────
# Configuración de conexiones (alineada con .env.example / docker-compose.yml)
# ─────────────────────────────────────────────────────────────────────────────

ORACLE_CONFIG = dict(
    host="localhost",
    port=1521,
    service_name="FREEPDB1",
    user="etl_user",
    password="secret",
)

PG_CONFIG = dict(
    host="localhost",
    port=5432,
    dbname="testdb",
    user="etl",
    password="secret",
)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers de conexión
# ─────────────────────────────────────────────────────────────────────────────

def oracle_connect() -> oracledb.Connection:
    """Abre conexión Oracle en modo thin (sin Oracle Client instalado)."""
    dsn = oracledb.makedsn(
        ORACLE_CONFIG["host"],
        ORACLE_CONFIG["port"],
        service_name=ORACLE_CONFIG["service_name"],
    )
    return oracledb.connect(
        user=ORACLE_CONFIG["user"],
        password=ORACLE_CONFIG["password"],
        dsn=dsn,
    )


def pg_connect() -> psycopg2.extensions.connection:
    """Abre conexión PostgreSQL."""
    return psycopg2.connect(**PG_CONFIG)


def section(title: str) -> None:
    print(f"\n{'─' * 60}")
    print(f"  {title}")
    print(f"{'─' * 60}")


# ─────────────────────────────────────────────────────────────────────────────
# Datos de ejemplo
# ─────────────────────────────────────────────────────────────────────────────

PRODUCTOS = [
    "Laptop Pro",    "Monitor 27\"",    "Teclado Mecánico",
    "Mouse Inalámbrico", "Headset USB",  "Webcam 4K",
    "Hub USB-C",     "SSD 1TB",         "Impresora Láser",
    "Tablet 10\"",   "Smartphone XL",   "Cargador 65W",
    "Cable HDMI 2m", "Memoria RAM 16GB","GPU RTX 4060",
]

CLIENTES = [
    ("ACME Corp",       "Ciudad de México", "ventas@acme.mx"),
    ("Tech Solutions",  "Guadalajara",      "compras@techsol.mx"),
    ("Global Imports",  "Monterrey",        "ops@globalimports.mx"),
    ("Distribuidora MX","Puebla",           "dir@distmx.mx"),
    ("Innovatech",      "Tijuana",          "contact@innovatech.mx"),
    ("DataSystems",     "Querétaro",        "info@datasys.mx"),
    ("AlphaTrade",      "León",             "alpha@trade.mx"),
    ("NetCommerce",     "Mérida",           "hello@netcommerce.mx"),
]

# Fecha base para las filas iniciales (5 días atrás, para que el watermark
# inicial sea anterior a "ahora" y el pipeline detecte las filas existentes)
BASE_TS = datetime.now() - timedelta(days=5)


def _random_ts(offset_minutes: int = 0) -> datetime:
    """Timestamp aleatorio dentro de los últimos 5 días."""
    spread = random.randint(0, 60 * 24 * 5)
    return BASE_TS + timedelta(minutes=spread + offset_minutes)


# ─────────────────────────────────────────────────────────────────────────────
# Preparación Oracle
# ─────────────────────────────────────────────────────────────────────────────

ORACLE_DDL = {
    "ventas": """
        CREATE TABLE ventas (
            id           NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            cliente_id   NUMBER,
            producto     VARCHAR2(100)   NOT NULL,
            cantidad     NUMBER(6)       DEFAULT 1,
            precio_unit  NUMBER(12, 2)   NOT NULL,
            total        NUMBER(14, 2)   GENERATED ALWAYS AS (cantidad * precio_unit) VIRTUAL,
            estado       VARCHAR2(20)    DEFAULT 'pendiente',
            updated_at   TIMESTAMP       DEFAULT LOCALTIMESTAMP
        )
    """,
    "clientes": """
        CREATE TABLE clientes (
            id           NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            nombre       VARCHAR2(100)   NOT NULL,
            ciudad       VARCHAR2(80),
            email        VARCHAR2(120),
            activo       NUMBER(1)       DEFAULT 1,
            updated_at   TIMESTAMP       DEFAULT LOCALTIMESTAMP
        )
    """,
}


def _drop_oracle_table(cursor: oracledb.Cursor, table: str) -> None:
    try:
        cursor.execute(f"DROP TABLE {table}")
        print(f"  [Oracle] Tabla '{table}' eliminada.")
    except oracledb.DatabaseError as e:
        err, = e.args
        if err.code == 942:   # ORA-00942: table or view does not exist
            print(f"  [Oracle] '{table}' no existía, se crea desde cero.")
        else:
            raise


def setup_oracle(drop: bool = False) -> None:
    section("ORACLE — preparación de esquemas")
    conn = oracle_connect()
    cur = conn.cursor()

    tables_order = ["ventas", "clientes"]   # ventas primero (tiene FK a clientes)

    # ── Drop ────────────────────────────────────────────────────────────────
    if drop:
        for table in tables_order:
            _drop_oracle_table(cur, table)
        conn.commit()

    # ── Crear tablas ─────────────────────────────────────────────────────────
    for table, ddl in ORACLE_DDL.items():
        try:
            cur.execute(ddl)
            print(f"  [Oracle] Tabla '{table}' creada.")
        except oracledb.DatabaseError as e:
            err, = e.args
            if err.code == 955:   # ORA-00955: name already used
                print(f"  [Oracle] '{table}' ya existe — se omite creación.")
            else:
                raise

    conn.commit()

    # ── Insertar clientes ────────────────────────────────────────────────────
    cur.execute("SELECT COUNT(*) FROM clientes")
    if cur.fetchone()[0] == 0:
        print(f"\n  [Oracle] Insertando {len(CLIENTES)} clientes...")
        for nombre, ciudad, email in CLIENTES:
            ts = _random_ts()
            cur.execute(
                "INSERT INTO clientes (nombre, ciudad, email, updated_at) "
                "VALUES (:1, :2, :3, :4)",
                (nombre, ciudad, email, ts),
            )
        conn.commit()
        print(f"  [Oracle] {len(CLIENTES)} clientes insertados.")
    else:
        print("  [Oracle] 'clientes' ya tiene datos — se omite inserción.")

    # ── Insertar ventas ──────────────────────────────────────────────────────
    cur.execute("SELECT COUNT(*) FROM ventas")
    if cur.fetchone()[0] == 0:
        n_ventas = 100000
        print(f"\n  [Oracle] Insertando {n_ventas} ventas de ejemplo...")
        estados = ["pendiente", "procesado", "enviado", "entregado", "cancelado"]

        cur.execute("SELECT id FROM clientes")
        cliente_ids = [r[0] for r in cur.fetchall()]

        for _ in range(n_ventas):
            ts       = _random_ts()
            cliente  = random.choice(cliente_ids)
            producto = random.choice(PRODUCTOS)
            cantidad = random.randint(1, 20)
            precio   = round(random.uniform(100, 15_000), 2)
            estado   = random.choice(estados)
            cur.execute(
                "INSERT INTO ventas (cliente_id, producto, cantidad, precio_unit, estado, updated_at) "
                "VALUES (:1, :2, :3, :4, :5, :6)",
                (cliente, producto, cantidad, precio, estado, ts),
            )

        conn.commit()
        print(f"  [Oracle] {n_ventas} ventas insertadas.")
    else:
        print("  [Oracle] 'ventas' ya tiene datos — se omite inserción.")

    # ── Resumen ──────────────────────────────────────────────────────────────
    print()
    for table in tables_order:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()[0]
        print(f"  [Oracle] {table:<12} → {count:>6} filas")

    cur.close()
    conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# Preparación PostgreSQL
# ─────────────────────────────────────────────────────────────────────────────

PG_DDL = {
    # Tabla de control del pipeline ETL (watermarks incrementales)
    "etl_watermarks": """
        CREATE TABLE IF NOT EXISTS etl_watermarks (
            table_name   TEXT        PRIMARY KEY,
            last_value   TIMESTAMPTZ,
            last_rowver  BIGINT,
            updated_at   TIMESTAMPTZ DEFAULT NOW()
        )
    """,
    # Destino de clientes
    "clientes": """
        CREATE TABLE IF NOT EXISTS clientes (
            id         INTEGER     PRIMARY KEY,
            nombre     TEXT        NOT NULL,
            ciudad     TEXT,
            email      TEXT,
            activo     SMALLINT    DEFAULT 1,
            updated_at TIMESTAMPTZ
        )
    """,
    # Destino de ventas
    "ventas": """
        CREATE TABLE IF NOT EXISTS ventas (
            id           INTEGER     PRIMARY KEY,
            cliente_id   INTEGER,
            producto     TEXT        NOT NULL,
            cantidad     INTEGER     DEFAULT 1,
            precio_unit  NUMERIC(12, 2) NOT NULL,
            total        NUMERIC(14, 2),
            estado       TEXT,
            updated_at   TIMESTAMPTZ
        )
    """,
}

PG_DROP_ORDER = ["ventas", "clientes", "etl_watermarks"]
PG_CREATE_ORDER = ["etl_watermarks", "clientes", "ventas"]


def setup_postgres(drop: bool = False) -> None:
    section("POSTGRESQL — preparación de esquemas")
    conn = pg_connect()
    conn.autocommit = False
    cur = conn.cursor()

    # ── Drop ────────────────────────────────────────────────────────────────
    if drop:
        for table in PG_DROP_ORDER:
            cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
            print(f"  [PostgreSQL] Tabla '{table}' eliminada (si existía).")
        conn.commit()

    # ── Crear tablas ─────────────────────────────────────────────────────────
    for table in PG_CREATE_ORDER:
        cur.execute(PG_DDL[table])
        print(f"  [PostgreSQL] Tabla '{table}' lista (CREATE IF NOT EXISTS).")

    conn.commit()

    # ── Resumen ──────────────────────────────────────────────────────────────
    print()
    for table in PG_CREATE_ORDER:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()[0]
        print(f"  [PostgreSQL] {table:<16} → {count:>6} filas")

    cur.close()
    conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# Modo incremental — añade datos nuevos para probar el DAG
# ─────────────────────────────────────────────────────────────────────────────

BATCH_SIZE_ORACLE = 500   # filas por executemany


def _insert_batch(cur: oracledb.Cursor, conn: oracledb.Connection,
                  rows: list[tuple], now: datetime) -> None:
    """
    executemany en lotes para Oracle con columna IDENTITY.
    INSERT ALL no es compatible con GENERATED ALWAYS AS IDENTITY
    (Oracle solo incrementa la secuencia una vez por statement).
    """
    sql = (
        "INSERT INTO ventas (cliente_id, producto, cantidad, precio_unit, estado, updated_at) "
        "VALUES (:1, :2, :3, :4, :5, :6)"
    )
    for start in range(0, len(rows), BATCH_SIZE_ORACLE):
        cur.executemany(sql, rows[start : start + BATCH_SIZE_ORACLE])
    conn.commit()


def seed_oracle(n_inserts: int = 1_000, n_updates: int = 200) -> None:
    """
    Añade filas nuevas y modifica filas existentes en Oracle con
    updated_at = NOW(), de modo que el pipeline incremental las detecte.
    """
    section(f"ORACLE — seed incremental  (+{n_inserts} inserts / {n_updates} updates)")
    conn = oracle_connect()
    cur  = conn.cursor()

    from datetime import timezone
    now    = datetime.now(timezone.utc).replace(tzinfo=None)  # naive UTC para Oracle
    estados = ["pendiente", "procesado", "enviado", "entregado", "cancelado"]

    cur.execute("SELECT id FROM clientes")
    cliente_ids = [r[0] for r in cur.fetchall()]
    if not cliente_ids:
        print("  [Oracle] Sin clientes — ejecuta primero sin --seed para crear el esquema.")
        cur.close(); conn.close(); return

    # ── Inserts nuevos ───────────────────────────────────────────────────────
    print(f"\n  Insertando {n_inserts:,} filas nuevas con updated_at = NOW()...")
    new_rows = [
        (
            random.choice(cliente_ids),
            random.choice(PRODUCTOS),
            random.randint(1, 20),
            round(random.uniform(100, 15_000), 2),
            random.choice(estados),
            now,
        )
        for _ in range(n_inserts)
    ]
    _insert_batch(cur, conn, new_rows, now)
    print(f"  {n_inserts:,} filas insertadas.")

    # ── Updates (simula modificaciones en la fuente) ─────────────────────────
    if n_updates > 0:
        cur.execute("SELECT id FROM ventas ORDER BY DBMS_RANDOM.VALUE FETCH FIRST :n ROWS ONLY",
                    {"n": n_updates})
        ids_to_update = [r[0] for r in cur.fetchall()]
        print(f"\n  Actualizando {len(ids_to_update):,} filas existentes (updated_at = NOW())...")
        for row_id in ids_to_update:
            cur.execute(
                "UPDATE ventas SET "
                "  precio_unit = :p, "
                "  estado      = :e, "
                "  updated_at  = :ts "
                "WHERE id = :id",
                {
                    "p":  round(random.uniform(100, 15_000), 2),
                    "e":  random.choice(estados),
                    "ts": now,
                    "id": row_id,
                },
            )
        conn.commit()
        print(f"  {len(ids_to_update):,} filas actualizadas.")

    # ── Resumen ──────────────────────────────────────────────────────────────
    cur.execute("SELECT COUNT(*) FROM ventas")
    total = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM ventas WHERE updated_at >= :ts", {"ts": now})
    above_wm = cur.fetchone()[0]

    print(f"\n  [Oracle] ventas total              : {total:>8,}")
    print(f"  [Oracle] filas con updated_at>=NOW : {above_wm:>8,}  ← el DAG procesará estas")

    cur.close()
    conn.close()


def trigger_dag(dag_id: str = "oracle_to_postgres_etl",
                airflow_url: str = "http://localhost:8080",
                user: str = "admin", password: str = "admin") -> None:
    """Dispara el DAG vía Airflow REST API."""
    import urllib.request, json as _json, base64

    section(f"AIRFLOW — trigger DAG '{dag_id}'")
    url     = f"{airflow_url}/api/v1/dags/{dag_id}/dagRuns"
    payload = _json.dumps({"conf": {}}).encode()
    creds   = base64.b64encode(f"{user}:{password}".encode()).decode()
    req     = urllib.request.Request(
        url, data=payload,
        headers={"Content-Type": "application/json", "Authorization": f"Basic {creds}"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            body = _json.loads(resp.read())
        print(f"  run_id : {body['dag_run_id']}")
        print(f"  state  : {body['state']}")
        print(f"\n  Airflow UI: {airflow_url}  (admin/admin)")
    except Exception as e:
        print(f"  No se pudo disparar el DAG: {e}", file=sys.stderr)
        print(f"  Dispara manualmente:")
        print(f"  curl -X POST {url} -H 'Content-Type: application/json' -u {user}:{password} -d '{{\"conf\":{{}}}}'")


# ─────────────────────────────────────────────────────────────────────────────
# Entrypoint
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Gestiona los esquemas Oracle/PostgreSQL para el pipeline ETL.\n\n"
            "Modos de uso:\n"
            "  Sin flags      — crea tablas si no existen, inserta datos iniciales\n"
            "  --drop         — elimina y recrea todo desde cero\n"
            "  --seed         — añade filas nuevas/modificadas para probar el DAG incremental\n"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--drop",
        action="store_true",
        help="Eliminar tablas y recrear desde cero (carga inicial).",
    )
    parser.add_argument(
        "--seed",
        action="store_true",
        help="Añadir datos nuevos a Oracle sin borrar lo existente (prueba incremental).",
    )
    parser.add_argument(
        "--inserts", type=int, default=1_000, metavar="N",
        help="Filas nuevas a insertar en modo --seed (default: 1000).",
    )
    parser.add_argument(
        "--updates", type=int, default=200, metavar="N",
        help="Filas existentes a modificar en modo --seed (default: 200).",
    )
    parser.add_argument(
        "--trigger",
        action="store_true",
        help="Disparar el DAG en Airflow al terminar.",
    )
    parser.add_argument(
        "--only",
        choices=["oracle", "postgres"],
        help="(setup) Ejecutar solo Oracle o solo PostgreSQL.",
    )
    args = parser.parse_args()

    try:
        if args.seed:
            # ── Modo incremental: solo añade datos a Oracle ──────────────────
            seed_oracle(n_inserts=args.inserts, n_updates=args.updates)
        else:
            # ── Modo setup: crear/recrear esquemas ───────────────────────────
            run_oracle   = args.only in (None, "oracle")
            run_postgres = args.only in (None, "postgres")

            if args.drop:
                print("\n  ADVERTENCIA: --drop eliminará todas las tablas y sus datos.")

            if run_oracle:
                setup_oracle(drop=args.drop)
            if run_postgres:
                setup_postgres(drop=args.drop)

            section("LISTO")
            print("  Esquemas listos. Siguiente paso:")
            print()
            print("  # Añadir datos incrementales y disparar el DAG:")
            print("  python -X utf8 scripts/prepare_schemas.py --seed --trigger")
            print()

        if args.trigger:
            trigger_dag()

    except oracledb.DatabaseError as e:
        print(f"\n  ERROR Oracle: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"\n  ERROR: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
