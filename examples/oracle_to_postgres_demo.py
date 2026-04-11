"""
Demo iterativo — Oracle → PostgreSQL con Docker
================================================

Demuestra el comportamiento de carga incremental por watermark (updated_at):
  - Carga inicial de 10 000 filas en chunks
  - 5 iteraciones donde cada una inserta ~500 filas nuevas y actualiza ~100
  - Cada ejecución del pipeline solo procesa las filas nuevas/modificadas
    desde el último watermark — nunca re-lee toda la tabla

Requisitos previos:
    docker compose up -d
    pip install -r requirements.txt

Uso:
    python examples/oracle_to_postgres_demo.py
"""

import random
import sys
import time
from pathlib import Path

# Asegura que la raíz del proyecto esté en el path al correr el script directamente
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import create_engine, text

from etl.config.settings import ETLConfig, SourceConfig, TargetConfig
from etl.pipeline import run_pipeline

# ---------------------------------------------------------------------------
# Conexiones (contenedores Docker)
# ---------------------------------------------------------------------------

SOURCE_URL = "oracle+oracledb://etl_user:secret@localhost:1521/?service_name=FREEPDB1"
TARGET_URL = "postgresql+psycopg2://etl:secret@localhost:5432/testdb"

CONFIG = ETLConfig(
    source=SourceConfig(
        dialect="oracle",
        host="localhost",
        port=1521,
        database="FREEPDB1",
        username="etl_user",
        password="secret",
    ),
    target=TargetConfig(
        host="localhost",
        port=5432,
        database="testdb",
        username="etl",
        password="secret",
    ),
    chunk_size=1000,   # procesa 1 000 filas por chunk
    max_workers=2,
    retry_attempts=3,
    retry_wait_min=0.5,
    retry_wait_max=5.0,
)

PRODUCTOS = [
    "Laptop", "Monitor", "Teclado", "Mouse", "Auriculares",
    "Webcam", "Hub USB", "SSD Externo", "Impresora", "Tablet",
    "Smartphone", "Cargador", "Cable HDMI", "Memoria RAM", "GPU",
]

INITIAL_ROWS  = 10_000
BATCH_INSERTS = 500    # filas nuevas por iteración
BATCH_UPDATES = 100    # filas modificadas por iteración
ITERATIONS    = 5


# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

def setup_oracle(engine):
    """Recrea la tabla ventas e inserta INITIAL_ROWS filas."""
    with engine.begin() as conn:
        try:
            conn.execute(text("DROP TABLE ventas"))
        except Exception:
            pass
        conn.execute(text("""
            CREATE TABLE ventas (
                id         NUMBER PRIMARY KEY,
                producto   VARCHAR2(100),
                monto      NUMBER(10,2),
                updated_at TIMESTAMP DEFAULT LOCALTIMESTAMP
            )
        """))

    print(f"[Oracle] Tabla recreada. Insertando {INITIAL_ROWS:,} filas iniciales...")
    t0 = time.time()

    batch_size = 500
    with engine.begin() as conn:
        for start in range(1, INITIAL_ROWS + 1, batch_size):
            end = min(start + batch_size, INITIAL_ROWS + 1)
            # Oracle no soporta INSERT INTO ... VALUES (a),(b) — usa INSERT ALL
            into_clauses = " ".join(
                f"INTO ventas (id, producto, monto, updated_at) "
                f"VALUES ({i}, '{random.choice(PRODUCTOS)}', "
                f"{round(random.uniform(5, 3000), 2)}, LOCALTIMESTAMP)"
                for i in range(start, end)
            )
            conn.execute(text(f"INSERT ALL {into_clauses} SELECT 1 FROM dual"))

    elapsed = time.time() - t0
    print(f"[Oracle] {INITIAL_ROWS:,} filas insertadas en {elapsed:.1f}s")


def setup_postgres(engine):
    """Recrea la tabla destino y limpia watermarks."""
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS ventas"))
        conn.execute(text("DROP TABLE IF EXISTS etl_watermarks"))
        conn.execute(text("""
            CREATE TABLE ventas (
                id         INTEGER PRIMARY KEY,
                producto   VARCHAR(100),
                monto      NUMERIC(10,2),
                updated_at TIMESTAMPTZ
            )
        """))
    print("[PostgreSQL] Tabla 'ventas' creada (vacía). Watermarks limpiados.")


# ---------------------------------------------------------------------------
# Operaciones de cambio en Oracle (simulan actividad real)
# ---------------------------------------------------------------------------

def insert_new_rows(engine, next_id: int, count: int) -> int:
    """Inserta `count` filas nuevas. Devuelve el siguiente ID disponible."""
    batch_size = 500
    with engine.begin() as conn:
        for start in range(0, count, batch_size):
            end = min(start + batch_size, count)
            into_clauses = " ".join(
                f"INTO ventas (id, producto, monto, updated_at) "
                f"VALUES ({next_id + start + i}, '{random.choice(PRODUCTOS)}', "
                f"{round(random.uniform(5, 3000), 2)}, LOCALTIMESTAMP)"
                for i in range(end - start)
            )
            conn.execute(text(f"INSERT ALL {into_clauses} SELECT 1 FROM dual"))
    return next_id + count


def update_random_rows(engine, max_id: int, count: int):
    """Actualiza `count` filas aleatorias (simula modificaciones en la fuente)."""
    ids = random.sample(range(1, max_id + 1), min(count, max_id))
    with engine.begin() as conn:
        for row_id in ids:
            nuevo_monto = round(random.uniform(5, 3000), 2)
            conn.execute(text(
                "UPDATE ventas SET monto = :m, updated_at = LOCALTIMESTAMP WHERE id = :id"
            ), {"m": nuevo_monto, "id": row_id})


# ---------------------------------------------------------------------------
# Resumen
# ---------------------------------------------------------------------------

def print_separator(title: str):
    width = 60
    print("\n" + "=" * width)
    print(f"  {title}")
    print("=" * width)


def show_counts(pg_engine, oracle_engine):
    with pg_engine.connect() as conn:
        pg_count = conn.execute(text("SELECT COUNT(*) FROM ventas")).scalar()
    with oracle_engine.connect() as conn:
        ora_count = conn.execute(text("SELECT COUNT(*) FROM ventas")).scalar()
    print(f"  Oracle:     {ora_count:>8,} filas")
    print(f"  PostgreSQL: {pg_count:>8,} filas")


def show_sample(pg_engine, label: str, limit: int = 5):
    with pg_engine.connect() as conn:
        rows = conn.execute(text(
            f"SELECT id, producto, monto, updated_at FROM ventas "
            f"ORDER BY updated_at DESC LIMIT {limit}"
        )).fetchall()
    print(f"\n  Ultimas {limit} filas en PostgreSQL ({label}):")
    print(f"  {'ID':<8} {'Producto':<15} {'Monto':>10}  {'updated_at'}")
    print("  " + "-" * 58)
    for r in rows:
        print(f"  {r[0]:<8} {r[1]:<15} {r[2]:>10}  {r[3]}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    src = create_engine(SOURCE_URL)
    tgt = create_engine(TARGET_URL)

    # ── PREPARACIÓN ────────────────────────────────────────────────────────
    print_separator("PREPARACIÓN: Crear tablas y datos iniciales")
    setup_oracle(src)
    time.sleep(3)
    setup_postgres(tgt)

    # ── CARGA INICIAL COMPLETA ─────────────────────────────────────────────
    print_separator(f"CARGA INICIAL — {INITIAL_ROWS:,} filas (sin watermark)")
    t0 = time.time()
    result = run_pipeline(
        table="ventas",
        mode="timestamp",
        watermark_col="updated_at",
        conflict_columns=["id"],
        config=CONFIG,
    )
    elapsed = time.time() - t0
    print(f"\n  Resultado:")
    print(f"    Filas migradas : {result['total_rows']:,}")
    print(f"    Chunks OK      : {result['chunks_ok']}")
    print(f"    Chunks fallidos: {result['chunks_failed']}")
    print(f"    Tiempo total   : {elapsed:.2f}s")

    show_counts(tgt, src)
    show_sample(tgt, "carga inicial")

    # ── ITERACIONES INCREMENTALES ──────────────────────────────────────────
    next_id = INITIAL_ROWS + 1

    for iteration in range(1, ITERATIONS + 1):
        print_separator(
            f"ITERACIÓN {iteration}/{ITERATIONS} — "
            f"+{BATCH_INSERTS} nuevas  /  {BATCH_UPDATES} actualizadas"
        )

        # Pausa para que updated_at sea estrictamente posterior al watermark
        time.sleep(1)

        # Cambios en la fuente
        next_id = insert_new_rows(src, next_id, BATCH_INSERTS)
        update_random_rows(src, next_id - 1, BATCH_UPDATES)

        total_in_oracle = INITIAL_ROWS + (iteration * BATCH_INSERTS)
        # Diagnóstico: ver cuántas filas de Oracle superan el watermark actual
        with tgt.connect() as conn:
            wm = conn.execute(text(
                "SELECT last_value FROM etl_watermarks WHERE table_name = 'ventas'"
            )).scalar()
        # Strippear tz antes de enviar a Oracle (TIMESTAMP sin timezone)
        wm_naive = wm.replace(tzinfo=None) if wm and getattr(wm, "tzinfo", None) else wm
        with src.connect() as conn:
            ora_count_over_wm = conn.execute(text(
                "SELECT COUNT(*) FROM ventas WHERE updated_at > :wm"
            ), {"wm": wm_naive}).scalar()
            ora_now = conn.execute(text("SELECT LOCALTIMESTAMP FROM dual")).scalar()
        print(f"\n  Watermark guardado     : {wm}")
        print(f"  SYSTIMESTAMP Oracle    : {ora_now}")
        print(f"  Filas Oracle > wm      : {ora_count_over_wm}  "
              f"(esperado ~{BATCH_INSERTS + BATCH_UPDATES})")
        print(f"  Oracle total           : {total_in_oracle:,} filas")
        print("  Ejecutando pipeline incremental (solo filas nuevas/modificadas)...")

        t0 = time.time()
        result = run_pipeline(
            table="ventas",
            mode="timestamp",
            watermark_col="updated_at",
            conflict_columns=["id"],
            config=CONFIG,
        )
        elapsed = time.time() - t0

        expected = BATCH_INSERTS + BATCH_UPDATES
        print(f"\n  Resultado:")
        print(f"    Filas procesadas : {result['total_rows']:,}  "
              f"(esperado ~{expected})")
        print(f"    Chunks OK        : {result['chunks_ok']}")
        print(f"    Chunks fallidos  : {result['chunks_failed']}")
        print(f"    Tiempo           : {elapsed:.2f}s  "
              f"({'INCREMENTAL - no releyó toda la tabla' if result['total_rows'] < total_in_oracle else ''})")
    
        show_counts(tgt, src)
        show_sample(tgt, f"iteración {iteration}")

    # ── RESUMEN FINAL ──────────────────────────────────────────────────────
    print_separator("RESUMEN FINAL")
    total_oracle = INITIAL_ROWS + (ITERATIONS * BATCH_INSERTS)
    print(f"  Filas totales en Oracle    : {total_oracle:,}")
    with tgt.connect() as conn:
        pg_total = conn.execute(text("SELECT COUNT(*) FROM ventas")).scalar()
    print(f"  Filas totales en PostgreSQL: {pg_total:,}")
    print(f"\n  El watermark garantizó que cada ejecución procesara")
    print(f"  solo ~{BATCH_INSERTS + BATCH_UPDATES} filas en lugar de {total_oracle:,}.")
    print()

    src.dispose()
    tgt.dispose()


if __name__ == "__main__":
    main()
