# ETL Pipeline — Oracle / MSSQL → PostgreSQL con Airflow

Pipeline ETL incremental que replica datos desde bases de datos heredadas (Oracle o MSSQL) hacia PostgreSQL, orquestado con Apache Airflow y desplegado en Docker.

**Versión:** 0.1.0

---

## ¿Qué hace?

Extrae tablas de una base de datos fuente y las carga en PostgreSQL de forma **incremental**: en cada ejecución solo se procesan las filas nuevas o modificadas desde la última vez, sin releer toda la tabla.

```
Run 1 → carga inicial completa   (ej. 100 000 filas)
Run 2 → solo filas nuevas        (ej.   1 200 filas)
Run 3 → solo filas nuevas        (ej.     800 filas)
```

El mecanismo es un **watermark**: al finalizar cada run se guarda el `MAX(updated_at)` procesado. El siguiente run filtra `WHERE updated_at > <último watermark>`.

---

## Arquitectura

```
┌──────────────────┐   pool x3 / chunks    ┌────────────────────┐
│  Oracle / MSSQL  │ ─────────────────────► │ IncrementalExtractor│
│  (fuente)        │   cursor server-side   └────────┬───────────┘
└──────────────────┘                                 │ Polars DataFrame
                                                     ▼
                                           ┌────────────────────┐
                                           │  SchemaValidator   │
                                           └────────┬───────────┘
                                                    │ válido
                                                    ▼
┌──────────────────┐   pool x5 / UPSERT    ┌────────────────────┐
│  PostgreSQL      │ ◄──────────────────── │  PostgresLoader    │
│  (destino)       │  ON CONFLICT DO UPDATE│  ThreadPoolExecutor│
└──────────────────┘                       └────────────────────┘
         ▲
         │  Orquestación y scheduling
┌────────┴─────────┐
│  Apache Airflow  │
│  (LocalExecutor) │
└──────────────────┘
```

| Componente | Responsabilidad |
|---|---|
| `IncrementalExtractor` | Lee la fuente en chunks por timestamp o rowversion |
| `SchemaValidator` | Verifica compatibilidad de tipos antes de cargar |
| `PostgresLoader` | UPSERT paralelo en PostgreSQL |
| `WatermarkManager` | Guarda y recupera el punto de control por tabla |

**Modos de extracción:**
- `timestamp` — `WHERE updated_at > :last_ts` (Oracle y MSSQL)
- `rowversion` — `WHERE rowver > :last_rv` (MSSQL con columna rowversion nativa)

---

## Requisitos

- Docker Desktop
- Python 3.11+
- `pip install -r requirements.txt` (para el script de preparación)

---

## Inicio rápido

### 1. Configurar variables de entorno

```bash
cp .env.example .env
# Editar .env si se necesitan credenciales distintas a los defaults
```

### 2. Construir la imagen de Airflow

```bash
docker compose build airflow-webserver airflow-scheduler
```

### 3. Inicializar Airflow

```bash
docker compose up airflow-init
```

### 4. Levantar el stack completo

```bash
docker compose up -d
```

Verificar que todos los servicios estén healthy:

```bash
docker compose ps
```

> Oracle tarda ~2 minutos en arrancar por primera vez.

### 5. Preparar los esquemas de base de datos

```bash
python -X utf8 scripts/prepare_schemas.py --drop
```

Esto crea las tablas en Oracle con datos de prueba (100 000 filas) y las tablas destino vacías en PostgreSQL.

### 6. Ejecutar el DAG

**Via API REST:**
```bash
curl -X POST http://localhost:8080/api/v1/dags/oracle_to_postgres_etl/dagRuns \
     -H "Content-Type: application/json" \
     -u admin:admin \
     -d '{"conf":{}}'
```

**Via Airflow UI:**
```
http://localhost:8080  →  admin / admin
DAG: oracle_to_postgres_etl  →  Trigger DAG ▶
```

### 7. Verificar resultado

```bash
docker compose exec postgres psql -U etl -d testdb -c \
  "SELECT COUNT(*), MAX(updated_at) FROM ventas;"

docker compose exec postgres psql -U etl -d testdb -c \
  "SELECT * FROM etl_watermarks;"
```

---

## Stack Docker

| Servicio | Imagen | Puerto | Descripción |
|---|---|---|---|
| `postgres` | postgres:16 | 5432 | Base de datos destino ETL |
| `oracle` | gvenzl/oracle-free:23-slim | 1521 | Base de datos fuente |
| `airflow-metadata` | postgres:16 | — | Metadatos internos de Airflow |
| `airflow-webserver` | etl_airflow_base-airflow | 8080 | UI y API REST de Airflow |
| `airflow-scheduler` | etl_airflow_base-airflow | — | Scheduler con LocalExecutor |

La imagen `etl_airflow_base-airflow` extiende `apache/airflow:2.10.4-python3.11` con las dependencias del proyecto (oracledb, polars, structlog, tenacity).

---

## DAGs

### `oracle_to_postgres_etl`

Pipeline incremental Oracle → PostgreSQL.

```
start → etl_{tabla}.extract_load → end
```

- Schedule: `@hourly`
- Las tablas a sincronizar se configuran via **Airflow Variable** `etl_oracle_tables` (JSON). Si no existe, usa los defaults definidos en el DAG.
- Cada tabla corre en su propio `TaskGroup`, permitiendo paralelismo entre tablas.

**Estructura de la variable `etl_oracle_tables`:**
```json
[
  {
    "table": "ventas",
    "mode": "timestamp",
    "watermark_col": "updated_at",
    "conflict_columns": ["id"],
    "target_table": null
  }
]
```

### `mssql_to_postgres_etl`

Mismo patrón para fuentes MSSQL. Configurable via Variable `etl_mssql_tables`. Soporta además el modo `rowversion`.

---

## Script de preparación de esquemas

`scripts/prepare_schemas.py` gestiona los esquemas de base de datos para pruebas y desarrollo.

```bash
# Setup inicial — crea tablas y carga datos (100k filas en Oracle)
python -X utf8 scripts/prepare_schemas.py --drop

# Idempotente — crea solo si no existen
python -X utf8 scripts/prepare_schemas.py

# Seed incremental — añade filas nuevas para probar el DAG
python -X utf8 scripts/prepare_schemas.py --seed

# Seed con volumen personalizado
python -X utf8 scripts/prepare_schemas.py --seed --inserts 5000 --updates 1000

# Seed y disparar el DAG automáticamente
python -X utf8 scripts/prepare_schemas.py --seed --trigger

# Solo preparar Oracle o PostgreSQL
python -X utf8 scripts/prepare_schemas.py --only oracle
python -X utf8 scripts/prepare_schemas.py --only postgres
```

**Flujo típico de prueba incremental:**

| Comando | Oracle | PostgreSQL | DAG procesa |
|---|---|---|---|
| `--drop` | 100 000 filas | vacío | — |
| trigger DAG | 100 000 | 100 000 | 100 000 (full load) |
| `--seed --trigger` | +1 200 filas/updates | — | ~1 200 |
| `--seed --trigger` | +1 200 filas/updates | — | ~1 200 |

---

## Estructura del proyecto

```
etl/
├── config/
│   └── settings.py           # Configuración desde variables de entorno
├── extractors/
│   └── incremental.py        # Extractor por timestamp y rowversion
├── loaders/
│   └── postgres_loader.py    # UPSERT paralelo a PostgreSQL
├── validators/
│   └── schema_validator.py   # Validación de tipos antes de cargar
├── utils/
│   ├── logging_setup.py      # Logging estructurado (structlog)
│   └── watermark.py          # Control de marcas de agua
└── pipeline.py               # Orquestador principal

dags/
├── oracle_to_postgres_etl.py # DAG incremental Oracle → PostgreSQL
└── mssql_to_postgres_etl.py  # DAG incremental MSSQL → PostgreSQL

scripts/
└── prepare_schemas.py        # Setup y seed de esquemas para desarrollo

tests/
├── unit/                     # Tests unitarios
└── integration/              # Tests de integración
```

---

## Variables de entorno

Todas las variables tienen defaults en `docker-compose.yml`. Para sobreescribir, editar `.env`:

```env
# Fuente Oracle
SOURCE_DIALECT=oracle
SOURCE_HOST=oracle            # nombre del servicio Docker
SOURCE_PORT=1521
SOURCE_DATABASE=FREEPDB1
SOURCE_USER=etl_user
SOURCE_PASSWORD=secret
SOURCE_POOL_SIZE=3

# Destino PostgreSQL
TARGET_HOST=postgres
TARGET_PORT=5432
TARGET_DATABASE=testdb
TARGET_USER=etl
TARGET_PASSWORD=secret
TARGET_POOL_SIZE=5

# Parámetros ETL
ETL_CHUNK_SIZE=10000
ETL_MAX_WORKERS=4
ETL_RETRY_ATTEMPTS=5

# Airflow
AIRFLOW__CORE__FERNET_KEY=<generar con python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())">
_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=admin
```

---

## Uso por línea de comandos (sin Airflow)

El pipeline también puede ejecutarse directamente:

```bash
# Extracción por timestamp
python -m etl.pipeline \
  --table ventas \
  --watermark-col updated_at \
  --conflict-cols id

# Extracción por rowversion (MSSQL)
python -m etl.pipeline \
  --table accounts \
  --mode rowversion \
  --watermark-col rowver \
  --conflict-cols id
```

---

## Tests

```bash
# Levantar PostgreSQL para tests de integración
docker compose up -d postgres

# Correr todos los tests
pytest tests/ -v --cov=etl --cov-report=term-missing
```

Cobertura actual: **88%**

---

## CI/CD

Azure Pipelines ejecuta los tests automáticamente en cada push a `main` y `develop`.

- Cobertura mínima requerida: **80%**
- Python: **3.11**
- Resultados en la pestaña **Tests** y **Coverage** de Azure DevOps

---

## Reset completo

```bash
docker compose down -v   # elimina contenedores y volúmenes
docker compose build airflow-webserver airflow-scheduler
docker compose up airflow-init
docker compose up -d
python -X utf8 scripts/prepare_schemas.py --drop
```
