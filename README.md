# Trabajo Integrador — IA en Producción
## Pronóstico de Producción de Hidrocarburos

Plataforma de ML Engineering para predecir la producción mensual de gas de pozos no convencionales, expuesta como API REST y orquestada con Apache Airflow.

> **Foco:** ML Engineering (reproducibilidad, trazabilidad, automatización), no precisión del modelo.

---

## Arquitectura

```
┌────────────────────────────────────────────────────────────────────┐
│                         docker-compose up                          │
├──────────┬──────────┬──────────┬───────────┬──────────┬───────────┤
│ postgres │  redis   │ airflow  │  mlflow   │   api    │ ray-serve │
│  :5433   │          │  :8080   │  :9090    │  :8000   │  :8265    │
└──────────┴──────────┴────┬─────┴───────────┴────┬─────┴───────────┘
                           │                       │
              ┌────────────┴──────────┐   ┌────────┴────────────┐
              │   DAG ml_pipeline     │   │   GET /forecast     │
              │   (1° de cada mes)    │   │   GET /wells        │
              │                       │   │   GET /health       │
              │  descarga CSV         │   └─────────────────────┘
              │  → preprocesa         │
              │  → Feature Store      │   ┌─────────────────────┐
              │  → entrena 4 modelos  │   │   DAG drift_report  │
              │  → promueve mejor     │   │   (lunes 06:00 UTC) │
              └───────────────────────┘   │                     │
                                          │  PSI + KS por feat. │
                                          │  model decay (RMSE) │
                                          │  → auto-retraining  │
                                          └─────────────────────┘
```

### Servicios

| Servicio | Puerto | Descripción |
|---|---|---|
| `postgres` | 5433 | Metadata Airflow + Feature Store (`featurestore`) + backend MLflow |
| `redis` | — | Broker Celery para workers de Airflow |
| `airflow-apiserver` | 8080 | UI y API REST de Airflow (credenciales: `airflow / airflow`) |
| `mlflow` | 9090 | Experiment tracking y Model Registry |
| `ray-serve` | 8265 (dashboard), 8001 (interno) | Inferencia distribuida (2 réplicas) |
| `api` | 8000 | API REST pública (`/api/v1/forecast`, `/api/v1/wells`) |

---

## Quickstart

### Requisitos
- Docker >= 24 y Docker Compose v2
- 8 GB RAM disponibles (Airflow + Ray usan bastante)

### 1. Clonar y configurar

```bash
git clone <repo-url>
cd mia204_01
cp .env.example .env
# Editar .env: completar AIRFLOW__CORE__FERNET_KEY, POSTGRES_PASSWORD, etc.
```

Generar las claves necesarias:

```bash
# FERNET_KEY
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
# SECRET_KEY
python -c "import secrets, base64; print(base64.b64encode(secrets.token_bytes(16)).decode())"
```

### 2. Levantar el stack

```bash
docker compose up --build
```

Esperar ~2 minutos a que `airflow-init` termine y todos los servicios estén healthy.

### 3. Ejecutar el pipeline

**Opción A — Desde la UI de Airflow:**
1. Ir a http://localhost:8080 (user: `airflow`, password: del `.env`)
2. Activar el DAG `ml_pipeline` y triggerear manualmente

**Opción B — Desde CLI:**
```bash
docker compose exec airflow-worker airflow dags trigger ml_pipeline
```

El pipeline descarga el CSV público del gobierno, aplica el shift temporal, persiste en el Feature Store y entrena 4 variantes del modelo. El de menor RMSE queda con el alias `production` en MLflow.

### 4. Verificar

```bash
# Health check
curl http://localhost:8000/health

# Predicción para un pozo
curl "http://localhost:8000/api/v1/forecast?id_well=96688&date_start=2022-01-01&date_end=2022-06-01"

# Listado de pozos
curl "http://localhost:8000/api/v1/wells?date_query=2022-01-01"

# Docs interactivos
open http://localhost:8000/docs
```

### 5. Entrenamiento standalone (reproducible)

```bash
docker compose exec airflow-worker python /opt/airflow/scripts/train.py --date "2023-06-30"
```

Usa el Feature Store con point-in-time correctness: solo features con `fecha <= 2023-06-30`.

---

## API Reference

### `GET /api/v1/forecast`

Devuelve predicciones mensuales de producción de gas para un pozo.

**Parámetros:**

| Parámetro | Tipo | Requerido | Descripción |
|---|---|---|---|
| `id_well` | string | ✅ | ID del pozo (ej: `96688`) |
| `date_start` | YYYY-MM-DD | ✅ | Inicio del rango |
| `date_end` | YYYY-MM-DD | ✅ | Fin del rango |

**Respuesta:**

```json
{
  "id_well": "96688",
  "data": [
    { "date": "2022-01-01", "prod": 61.5 },
    { "date": "2022-02-01", "prod": 49.8 }
  ]
}
```

> **Nota sobre el shift temporal:** cada punto usa las features del mes indicado en `date` para predecir la producción del mes siguiente. El modelo fue entrenado con este desplazamiento para evitar data leakage.

### `GET /api/v1/wells`

Lista los pozos con datos disponibles hasta una fecha dada.

**Parámetros:**

| Parámetro | Tipo | Requerido | Descripción |
|---|---|---|---|
| `date_query` | YYYY-MM-DD | ✅ | Fecha de corte |
| `limit` | int | — | Máximo de resultados (default: 100) |
| `offset` | int | — | Paginación (default: 0) |

**Respuesta:**

```json
[{ "id_well": "96688" }, { "id_well": "96630" }]
```

---

## Pipeline de ML

### DAG `ml_pipeline` — schedule: `0 2 1 * *` (mensual)

```
start → download_dataset → preprocess → persist_features
      → read_features_from_store → train_experiments → promote_model
```

| Task | Descripción |
|---|---|
| `download_dataset` | Descarga el CSV de pozos no convencionales del portal datos.gob.ar |
| `preprocess` | Filtra fechas, codifica `tipoextraccion`, aplica **shift temporal** (-1 mes por pozo) |
| `persist_features` | Guarda en el Feature Store (DELETE-then-INSERT idempotente por rango de fechas) |
| `read_features_from_store` | Valida disponibilidad de datos; devuelve solo metadata para no saturar XCom |
| `train_experiments` | Lee del Feature Store directamente, entrena 4 variantes de RandomForest, loguea en MLflow |
| `promote_model` | Asigna alias `production` al modelo con menor RMSE de test |

**Shift temporal:** en el preprocesado, `prod_gas` se desplaza -1 mes por pozo (`groupby('idpozo')['prod_gas'].shift(-1)`). Así el modelo aprende: *dadas las condiciones operativas de este mes, ¿cuánto gas producirá el pozo el mes siguiente?* El último mes de cada pozo se descarta (sin target futuro).

### DAG `drift_report` — schedule: `0 6 * * 1` (lunes)

```
cargar_datos_referencia ─┐
                          ├─→ calcular_drift ─┐
cargar_datos_recientes  ─┘                    ├─→ decidir_retraining
                          ──→ evaluar_decay ──┘
```

| Task | Descripción |
|---|---|
| `cargar_datos_referencia` | Lee todo el Feature Store (distribución de entrenamiento) |
| `cargar_datos_recientes` | Consulta features de los últimos 90 días |
| `calcular_drift` | PSI y KS test por feature; loguea en MLflow (`drift_monitoring`) |
| `evaluar_model_decay` | RMSE sobre datos recientes vs baseline taggeado en MLflow |
| `decidir_retraining` | Si hay drift (PSI > 0.2 o KS p < 0.05) o decay (RMSE > 15%), dispara `ml_pipeline` vía API REST |

---

## Feature Store

Implementado sobre PostgreSQL (tabla `features` en la base `featurestore`). La clase `FeatureStore` en `airflow/plugins/ml_pipeline/feature_store.py` es la **única interfaz** al Feature Store — ningún otro módulo ejecuta SQL sobre esa tabla directamente.

### Schema

```sql
CREATE TABLE features (
    id SERIAL PRIMARY KEY,
    id_pozo VARCHAR NOT NULL,
    fecha DATE NOT NULL,      -- primer día del mes
    tipoextraccion INT,       -- codificado con TIPOEXTRACCION_MAP estático
    prod_gas FLOAT,           -- target (producción del mes siguiente, post-shift)
    prod_agua FLOAT,
    tef FLOAT,
    prod_pet FLOAT,
    profundidad FLOAT
);
```

### Métodos

| Método | Uso |
|---|---|
| `save_features(df, fecha_min, fecha_max)` | DELETE-then-INSERT por rango. Idempotente. |
| `get_training_features(cutoff_date)` | Point-in-time correctness para reproducibilidad |
| `get_inference_features(id_pozo, start, end)` | Online store para la API |
| `count()` | Utilidad para verificar estado |

---

## Experiment Tracking (MLflow)

Disponible en http://localhost:9090

Cada run registra:
- **Parámetros:** `n_estimators`, `max_depth`, `features`, `training_date`, tamaño del dataset
- **Métricas:** `train_rmse`, `train_mae`, `train_r2`, `test_rmse`, `test_mae`, `test_r2`
- **Artefactos:** modelo serializado, gráfico de feature importance, firma del modelo (input/output schema)
- **Tags:** `feature_set` (all / reduced), `data_source=featurestore`

El modelo promovido recibe el alias `production` en el Model Registry y queda accesible como `models:/hydrocarbon_forecast@production`.

---

## Infraestructura de Inferencia (Ray Serve)

Ray Serve corre en un contenedor separado con 2 réplicas del deployment `HydrocarbonForecast`. Al iniciar, carga el modelo con alias `production` desde MLflow.

El endpoint `/api/v1/forecast` de la API principal envía un POST a `ray-serve:8001/predict` con las features del período solicitado. Si Ray Serve no está disponible, devuelve `503`.

Dashboard de Ray: http://localhost:8265

---

## Tests

```bash
# Tests unitarios de la API (sin DB)
cd api && python -m pytest tests/ -v --ignore=tests/test_integration_api_postgres.py

# Tests de integración contra Docker (requiere stack levantado)
cd api && RUN_INTEGRATION=1 python -m pytest tests/test_integration_api_postgres.py -v

# Tests del módulo de drift
docker compose exec airflow-worker python -m pytest /opt/airflow/tests/ -v
```

**Suite actual:** 35 tests (17 unitarios API + 15 drift + 3 integración)

---

## Estructura del Repositorio

```
mia204_01/
├── airflow/
│   ├── dags/
│   │   ├── dag_pozos.py          # DAG ml_pipeline (entrenamiento mensual)
│   │   └── dag_drift_report.py   # DAG drift_report (monitoreo semanal)
│   ├── plugins/ml_pipeline/
│   │   ├── config.py             # Constantes centralizadas (umbrales, schedules, features)
│   │   ├── db.py                 # Engine SQLAlchemy (FEATURESTORE_DB_URL)
│   │   ├── feature_store.py      # Clase FeatureStore — única interfaz a la tabla features
│   │   ├── training.py           # train_and_log(), promote_best_model()
│   │   └── drift.py              # calcular_psi(), calcular_ks(), detectar_model_decay()
│   └── tests/
│       ├── conftest.py
│       └── test_drift.py         # 15 tests (PSI, KS, decay, reporte)
├── api/
│   ├── app/
│   │   ├── api/v1/endpoints/
│   │   │   ├── forecast.py       # GET /forecast → Ray Serve → predicción
│   │   │   └── wells.py          # GET /wells
│   │   ├── repositories/
│   │   │   ├── base.py           # Interfaz abstracta WellRepository
│   │   │   ├── postgres.py       # Implementación PostgreSQL
│   │   │   └── fake.py           # Implementación para tests
│   │   ├── core/
│   │   │   ├── config.py         # Settings (pydantic-settings)
│   │   │   ├── dependencies.py   # Inyección de dependencias FastAPI
│   │   │   └── model.py          # MODEL_FEATURES
│   │   ├── schemas.py            # Pydantic models (ForecastPoint, ForecastResponse, WellItem)
│   │   └── main.py               # create_app()
│   └── tests/
│       ├── test_forecast.py      # 11 tests unitarios
│       ├── test_wells.py         # 6 tests unitarios
│       └── test_integration_api_postgres.py  # 3 tests integración
├── ray/
│   └── serve_app.py              # Deployment Ray Serve (HydrocarbonForecast)
├── db/
│   └── init-featurestore.sql     # Bootstrap automático de tablas (production, wells, features)
├── scripts/
│   └── train.py                  # Entrenamiento standalone: python train.py --date YYYY-MM-DD
├── docker-compose.yaml
├── .env.example
└── README.md
```

---

## Variables de Entorno

Copiar `.env.example` a `.env` y completar:

| Variable | Descripción |
|---|---|
| `AIRFLOW_UID` | UID del usuario host (Linux: `id -u`) |
| `AIRFLOW__CORE__FERNET_KEY` | Clave Fernet para encriptar conexiones |
| `AIRFLOW__API__SECRET_KEY` | Secret JWT para la API de Airflow |
| `POSTGRES_PASSWORD` | Contraseña de PostgreSQL |
| `FEATURESTORE_DB_URL` | URL SQLAlchemy al Feature Store |
| `AIRFLOW_API_URL` | URL del API server de Airflow (para auto-trigger de drift) |
| `AIRFLOW_API_USER` / `AIRFLOW_API_PASSWORD` | Credenciales para disparar retraining |
| `MLFLOW_TRACKING_URI` | URL del tracking server MLflow |

---

## Equipo

| Integrante | GitHub | Contribuciones principales |
|---|---|---|
| **Hernan** | [hernangm](https://github.com/hernangm) | Pipeline Airflow, Feature Store PostgreSQL, MLflow, Ray Serve |
| **Matias** | [matiascaccia](https://github.com/matiascaccia) | Docker Compose, API FastAPI, repositorios SQLAlchemy, tests |
