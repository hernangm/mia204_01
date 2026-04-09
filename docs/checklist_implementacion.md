# Checklist de Implementación (estado actual)

Este checklist se deriva del README del proyecto y refleja el estado actual observado en el repositorio.

Convención usada:
- [x] Implementado
- [ ] Pendiente o parcial

## Entrega Parcial

- [x] Sistema dockerizado (postgres, redis, airflow apiserver/scheduler/worker/triggerer/dag-processor, mlflow, api)
- [x] API funcional (FastAPI con `PostgresWellRepository` consultando `featurestore.wells` y `featurestore.production`)
- [x] Experiment tracking (MLflow server con backend Postgres y artefactos en `./mlruns`)
- [x] Logging de métricas (rmse, r2, mae logueadas por cada run en [training.py](airflow/plugins/ml_pipeline/training.py))
- [x] Feature Store persistente (tabla `features` en Postgres, poblada por [feature_engineering.py](airflow/plugins/ml_pipeline/feature_engineering.py))
- [x] Training reproducible (TIPOEXTRACCION_MAP estático + `RANDOM_STATE=204` en [training.py:33](airflow/plugins/ml_pipeline/training.py#L33))
- [x] Pipeline modular (DAG delgado + módulos en `airflow/plugins/ml_pipeline/`)
- [x] Registro y promoción de modelo (training registra `hydrocarbon_forecast`; `promote_best_run` asigna alias `production` al mejor rmse)

## Entrega Final

- [ ] Orquestación automática (DAG con `schedule=None` en [dag_pozos.py:25](airflow/dags/dag_pozos.py#L25); falta schedule cron)
- [ ] Retraining periódico (depende del punto anterior)
- [ ] Model decay (sin job ni métricas de degradación)
- [ ] Data drift / concept drift (sin DAG de drift ni reportes Evidently/KS/PSI)
- [ ] Infraestructura escalable (sin Ray Serve u otra capa de serving escalable)
- [ ] Inferencia desde modelo registrado (el endpoint `/forecast` devuelve valores históricos de `production`, no carga `models:/hydrocarbon_forecast@production`)

## Requisitos técnicos

- [x] Docker & Docker Compose
- [x] API REST (`/api/v1/wells`, `/api/v1/forecast` conectados al featurestore vía repositorio)
- [x] Feature Store (PostgreSQL con tablas `production`, `wells`, `features`)
- [x] Model Registry (MLflow: registro automático + alias `production` en la promoción)
- [x] Experiment Tracking
- [ ] Orquestación (parcial: Airflow operativo y DAG `ml_pipeline` con tasks dinámicas, pero ejecución manual)

## API especificada en README

- [x] Base URL /api/v1
- [x] GET /wells
- [x] GET /forecast (devuelve serie histórica; pendiente servir predicciones del modelo MLflow)

## Nota de alcance

- Los puntos de "Entrega Final" permanecen abiertos porque requieren automatización de schedule, monitoreo de drift/decay y una capa de serving escalable.
- El flujo de entrenamiento → registro → promoción está cerrado (el DAG ya mapea dinámicamente sobre `EXPERIMENTS` y asigna el alias `production` al run con menor rmse), pero el servicio de inferencia todavía no consume ese modelo promovido.

## Verificación end-to-end (2026-04-09)

Ejecución manual del DAG `ml_pipeline` (run `manual__2026-04-09T22:47:57`) — todas las tasks en estado `success`:

- `ingest_csvs` → `build_features` → `list_experiments` → `train_model[0..3]` → `promote_best`

Resultados de los 4 experimentos logueados en MLflow (experimento `hydrocarbon_forecast`):

| map | n_estimators | features              | rmse       | r2     | version |
|-----|--------------|-----------------------|------------|--------|---------|
| 0   | 50           | ALL_FEATURES_GAS      | **808.71** | 0.7484 | v1      |
| 1   | 100          | ALL_FEATURES_GAS      | 820.97     | 0.7532 | v3      |
| 2   | 200          | ALL_FEATURES_GAS      | 820.52     | 0.7534 | v4      |
| 3   | 100          | REDUCED_FEATURES_GAS  | 964.14     | 0.6596 | v2      |

`promote_best_run` asignó el alias `production` al modelo registrado `hydrocarbon_forecast` → **version 1** (menor rmse = 808.71, `n_estimators=50`, `ALL_FEATURES_GAS`). Los 4 runs con sus parámetros y métricas son comparables desde la UI de MLflow en http://localhost:9090.
