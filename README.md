# Trabajo Integrador — IA en Producción

Pipeline de ML Engineering para pronóstico de producción de hidrocarburos.
El foco está en **reproducibilidad, trazabilidad y automatización**, no en maximizar la precisión del modelo.

## Servicios

| Servicio | URL | Credenciales |
|---|---|---|
| API REST | http://localhost:8000 | — |
| Swagger UI | http://localhost:8000/docs | — |
| Airflow | http://localhost:8080 | airflow / airflow |
| MLflow | http://localhost:9090 | — |
| PostgreSQL | localhost:5432 | airflow / airflow |

## Setup inicial

**1. Clonar y crear el archivo de entorno**

```bash
git clone <repo-url>
cd mia204_01
cp .env.example .env
```

Editar `.env` y completar las claves de Airflow. Para generarlas:

```bash
# AIRFLOW__CORE__FERNET_KEY
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# AIRFLOW__API__SECRET_KEY
python -c "import secrets; print(secrets.token_hex(32))"
```

**2. Levantar el stack**

```bash
docker compose up --build -d
```

La primera vez, el init script `db/init-featurestore.sql` crea automáticamente las bases `featurestore` y `mlflow` en PostgreSQL. Si el volumen de Postgres ya existía, ejecutarlo manualmente:

```bash
docker compose exec postgres psql -U airflow -f /docker-entrypoint-initdb.d/init-featurestore.sql
```

## Pipeline ML (Airflow DAG)

El DAG `ml_pipeline` ejecuta el pipeline completo de extremo a extremo:

```
ingest_csvs → build_features → list_experiments → train_model[0..2] → promote_best
```

- **ingest_csvs**: descarga los CSVs de producción y pozos de datos.gob.ar (si no están en `data/raw/`) y los carga en la tabla `production` y `wells` de PostgreSQL.
- **build_features**: genera las features y las persiste en la tabla `features`.
- **train_model**: entrena 3 experimentos de RandomForestRegressor en paralelo, registra parámetros y métricas (RMSE, R², MAE) en MLflow.
- **promote_best**: asigna el alias `production` en el Model Registry al experimento con menor RMSE.

Disparar desde la UI de Airflow (http://localhost:8080) o por CLI:

```bash
docker compose exec airflow-apiserver airflow dags trigger ml_pipeline
```

## Entrenamiento standalone

`scripts/train.py` permite entrenar sin Airflow, usando los features ya cargados en el Feature Store.

```bash
# Entrenar con datos hasta una fecha específica
docker compose exec airflow-worker python /opt/airflow/scripts/train.py --date "2023-10-01"

# Entrenar con todos los datos disponibles
docker compose exec airflow-worker python /opt/airflow/scripts/train.py --date "2026-01-01"
```

El parámetro `--date` define la fecha de corte: solo se usan registros con `fecha <= date`. Misma fecha → mismos datos → mismas métricas (reproducible).

Ver más ejemplos en `scripts/ejemplos_uso.sh`.

## API REST

### `GET /api/v1/wells`

Lista los pozos disponibles para una fecha de consulta.

```bash
curl "http://localhost:8000/api/v1/wells?date_query=2026-01-01"
```

```json
[
  { "id_well": "114889" },
  { "id_well": "96630" }
]
```

### `GET /api/v1/forecast`

Devuelve la serie histórica de producción de gas (`prod_gas`) para un pozo en un rango de fechas.

```bash
curl "http://localhost:8000/api/v1/forecast?id_well=96630&date_start=2022-01-01&date_end=2022-12-01"
```

```json
{
  "id_well": "96630",
  "data": [
    { "date": "2022-01-01", "prod": 1523.4 },
    { "date": "2022-02-01", "prod": 1490.1 }
  ]
}
```

## Estructura del repositorio

```
├── airflow/
│   ├── Dockerfile              # imagen Airflow con pandas/sklearn/mlflow
│   ├── requirements.txt
│   ├── dags/
│   │   └── dag_pozos.py        # DAG ml_pipeline
│   └── plugins/
│       └── ml_pipeline/
│           ├── config.py       # constantes, experimentos, URLs de descarga
│           ├── data_ingestion.py
│           ├── feature_engineering.py
│           ├── training.py
│           └── db.py           # engine SQLAlchemy del feature store
├── api/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── app/
│   │   ├── main.py             # FastAPI app
│   │   ├── routes.py           # endpoints /wells y /forecast
│   │   ├── schemas.py          # modelos Pydantic
│   │   ├── repository.py       # acceso a datos (Postgres + Fake para tests)
│   │   └── config.py           # configuración vía variables de entorno
│   └── tests/
│       ├── test_wells.py
│       ├── test_forecast.py
│       └── test_integration_api_postgres.py
├── mlflow/
│   └── Dockerfile              # imagen MLflow con psycopg2
├── db/
│   └── init-featurestore.sql   # crea bases featurestore y mlflow en Postgres
├── scripts/
│   ├── train.py                # entrenamiento standalone (--date YYYY-MM-DD)
│   ├── ejemplos_uso.sh         # ejemplos de uso del sistema
│   └── test_e2e.sh             # smoke test de los 3 servicios
├── data/
│   └── raw/                    # CSVs descargados por el DAG
├── docker-compose.yaml
└── .env.example
```

## Tests

```bash
# Tests unitarios de la API (sin Docker)
cd api && python -m pytest tests/test_wells.py tests/test_forecast.py -v

# Tests de integración contra Postgres (requiere stack levantado)
cd api && RUN_INTEGRATION=1 python -m pytest tests/test_integration_api_postgres.py -v
```

## Datasets

Los datos se descargan automáticamente desde [datos.gob.ar](https://datos.gob.ar/dataset/energia-produccion-petroleo-gas-por-pozo-capitulo-iv) la primera vez que corre el DAG. También pueden colocarse manualmente en `data/raw/`:

- `produccin-de-pozos-de-gas-y-petrleo-no-convencional.csv` — producción mensual por pozo
- `capitulo-iv-pozos.csv` — metadatos de pozos (empresa, provincia, cuenca)
