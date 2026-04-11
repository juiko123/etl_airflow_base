# ETL Pipeline — MSSQL / Oracle → PostgreSQL

Pipeline ETL incremental para replicar datos desde bases de datos heredadas (MSSQL / Oracle) hacia PostgreSQL, optimizado para consumo con Power BI.

**Versión:** 0.1.0

---

## ¿Qué hace este proyecto?

Extrae tablas de una base de datos heredada (MSSQL u Oracle) y las carga en PostgreSQL de forma **incremental**: en cada ejecución solo se procesan las filas nuevas o modificadas desde la última vez, sin releer toda la tabla.

```
Ejecución 1 → lee 10 000 filas (carga inicial completa)
Ejecución 2 → lee    600 filas (solo las nuevas/modificadas)
Ejecución 3 → lee    600 filas (solo las nuevas/modificadas)
```

El mecanismo que hace esto posible se llama **watermark**: al finalizar cada ejecución se guarda el `updated_at` máximo procesado. La siguiente ejecución filtra `WHERE updated_at > <último watermark>`.

---

## Arquitectura

```
┌─────────────────┐     Connection Pool      ┌──────────────────┐
│  MSSQL / Oracle │ ──── QueuePool (x3) ───► │  Incremental     │
│  (fuente)       │   cursor server-side      │  Extractor       │
└─────────────────┘   chunks de N filas       └────────┬─────────┘
                                                       │ Polars DataFrame
                                                       ▼
                                              ┌──────────────────┐
                                              │  Schema          │
                                              │  Validator       │
                                              └────────┬─────────┘
                                                       │ válido
                                                       ▼
┌─────────────────┐     Connection Pool      ┌──────────────────┐
│  PostgreSQL     │ ◄─── QueuePool (x5) ──── │  Postgres        │
│  (destino)      │   UPSERT por lotes        │  Loader          │
│  (Power BI)     │   ON CONFLICT DO UPDATE   │  ThreadPoolExec  │
└─────────────────┘                           └──────────────────┘
```

**Componentes clave:**

| Componente | Qué hace |
|---|---|
| `IncrementalExtractor` | Lee la fuente en chunks para no cargar toda la tabla en memoria |
| `SchemaValidator` | Verifica que los tipos de columnas sean compatibles antes de cargar |
| `PostgresLoader` | Inserta en paralelo usando UPSERT (`ON CONFLICT DO UPDATE`) |
| `WatermarkManager` | Guarda y recupera el punto de control de cada tabla |

**Modos de extracción:**
- `timestamp` — `WHERE updated_at > :last_ts` (Oracle y MSSQL)
- `rowversion` — `WHERE rowver > :last_rv` (MSSQL con columna rowversion)

---

## Demo con Docker

La forma más rápida de ver el pipeline en acción: levanta Oracle + PostgreSQL en Docker y ejecuta el demo iterativo.

### 1. Requisitos

- Docker Desktop
- Python 3.13+

### 2. Levantar los contenedores

```bash
docker compose up -d
```

Espera hasta que ambos contenedores muestren `healthy`:

```bash
docker compose ps
```

> Oracle tarda ~2 minutos en arrancar la primera vez.

### 3. Instalar dependencias

```bash
pip install -r requirements.txt
```

### 4. Ejecutar el demo

```bash
python examples/oracle_to_postgres_demo.py
```

**Qué ocurre durante el demo:**

| Paso | Acción | Filas procesadas | PostgreSQL total |
|---|---|---|---|
| Carga inicial | Inserta 10 000 filas en Oracle y las migra | 10 000 | 10 000 |
| Iteración 1 | +500 nuevas / 100 actualizadas | ~600 | 10 500 |
| Iteración 2 | +500 nuevas / 100 actualizadas | ~600 | 11 000 |
| Iteración 3 | +500 nuevas / 100 actualizadas | ~600 | 11 500 |
| Iteración 4 | +500 nuevas / 100 actualizadas | ~600 | 12 000 |
| Iteración 5 | +500 nuevas / 100 actualizadas | ~600 | 12 500 |

> Las 100 actualizaciones no agregan filas nuevas — el UPSERT (`ON CONFLICT DO UPDATE`) sobreescribe las existentes.
> En cada iteración el pipeline procesa ~600 filas en lugar de releer las 10 000+.

**Salida esperada:**

```
============================================================
  CARGA INICIAL — 10,000 filas (sin watermark)
============================================================

  Resultado:
    Filas migradas : 10,000
    Chunks OK      : 10
    Tiempo total   : 2.43s
  Oracle:        10,000 filas
  PostgreSQL:    10,000 filas

============================================================
  ITERACIÓN 1/5 — +500 nuevas  /  100 actualizadas
============================================================

  Resultado:
    Filas procesadas : 600  (esperado ~600)
    Chunks OK        : 1
    Tiempo           : 0.31s  (INCREMENTAL - no releyó toda la tabla)
  Oracle:        10,500 filas
  PostgreSQL:    10,500 filas

...

============================================================
  RESUMEN FINAL
============================================================
  Filas totales en Oracle    :   12,500
  Filas totales en PostgreSQL:   12,500

  El watermark garantizó que cada ejecución procesara
  solo ~600 filas en lugar de 12,500.
```

---

## Estructura del proyecto

```
etl/
├── config/
│   └── settings.py         # Configuración y variables de entorno
├── extractors/
│   └── incremental.py      # Extractor por timestamp y rowversion
├── loaders/
│   └── postgres_loader.py  # Carga UPSERT paralela a PostgreSQL
├── validators/
│   └── schema_validator.py # Validación de esquema antes de cargar
├── utils/
│   ├── logging_setup.py    # Logging estructurado (structlog)
│   └── watermark.py        # Gestión de marcas de agua incrementales
└── pipeline.py             # Orquestador principal
examples/
└── oracle_to_postgres_demo.py  # Demo iterativo con Docker
tests/
├── unit/                   # Pruebas unitarias (mocks)
└── integration/            # Pruebas de integración
```

---

## Instalación

```bash
pip install -r requirements.txt
```

Para desarrollo:

```bash
pip install -r requirements-dev.txt
```

---

## Variables de entorno

Crea un archivo `.env` en la raíz del proyecto:

```env
# Fuente
SOURCE_DIALECT=oracle         # mssql | oracle
SOURCE_HOST=localhost
SOURCE_PORT=1521
SOURCE_DATABASE=FREEPDB1
SOURCE_USER=etl_user
SOURCE_PASSWORD=secret
SOURCE_POOL_SIZE=3

# Destino
TARGET_HOST=localhost
TARGET_PORT=5432
TARGET_DATABASE=testdb
TARGET_USER=etl
TARGET_PASSWORD=secret
TARGET_POOL_SIZE=5

# ETL
ETL_CHUNK_SIZE=10000
ETL_MAX_WORKERS=4
ETL_RETRY_ATTEMPTS=5
```

---

## Uso por línea de comandos

```bash
# Extracción por timestamp
python -m etl.pipeline \
  --table transactions \
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

## Pruebas

```bash
# Levantar PostgreSQL para tests de integración
docker compose up -d postgres

# Correr todos los tests con cobertura
pytest tests/ -v --cov=etl --cov-report=term-missing
```

Cobertura actual: **88%**

---

## CI/CD

Azure Pipelines ejecuta automáticamente las pruebas en cada push y Pull Request hacia `main` y `develop`.

- Cobertura mínima requerida: **80%**
- Python: **3.13**
- Resultados visibles en la pestaña **Tests** y **Coverage** de Azure DevOps
