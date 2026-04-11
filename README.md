# ETL Pipeline — MSSQL / Oracle → PostgreSQL

Pipeline ETL incremental para replicar datos desde bases de datos heredadas (MSSQL / Oracle) hacia PostgreSQL, optimizado para consumo con Power BI.

**Versión:** 0.1.0

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

**Modos de extracción:**
- `timestamp` — extracción incremental por columna de fecha/hora
- `rowversion` — extracción incremental por columna rowversion (MSSQL)

---

## Estructura del proyecto

```
etl/
├── config/
│   └── settings.py        # Configuración y variables de entorno
├── extractors/
│   └── incremental.py     # Extractor por timestamp y rowversion
├── loaders/
│   └── postgres_loader.py # Carga UPSERT paralela a PostgreSQL
├── validators/
│   └── schema_validator.py# Validación de esquema antes de cargar
├── utils/
│   ├── logging_setup.py   # Logging estructurado (structlog)
│   └── watermark.py       # Gestión de marcas de agua incrementales
└── pipeline.py            # Orquestador principal
tests/
├── unit/                  # Pruebas unitarias
└── integration/           # Pruebas de integración
```

---

## Requisitos

- Python 3.13+
- PostgreSQL (destino)
- MSSQL o Oracle (fuente)
- ODBC Driver 17 for SQL Server (si usas MSSQL)

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
SOURCE_DIALECT=mssql          # mssql | oracle
SOURCE_HOST=localhost
SOURCE_PORT=1433
SOURCE_DATABASE=mi_base
SOURCE_USER=usuario
SOURCE_PASSWORD=contraseña
SOURCE_POOL_SIZE=3

# Destino
TARGET_HOST=localhost
TARGET_PORT=5432
TARGET_DATABASE=mi_postgres
TARGET_USER=usuario
TARGET_PASSWORD=contraseña
TARGET_POOL_SIZE=5

# ETL
ETL_CHUNK_SIZE=10000
ETL_MAX_WORKERS=4
ETL_RETRY_ATTEMPTS=5
```

---

## Uso

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
pytest tests/ -v --cov=etl --cov-fail-under=80
```

---

## CI/CD

Azure Pipelines ejecuta automáticamente las pruebas en cada push y Pull Request hacia `main` y `develop`.

- Cobertura mínima requerida: **80%**
- Python: **3.13**
- Resultados visibles en la pestaña **Tests** y **Coverage** de Azure DevOps
