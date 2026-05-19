# Checklist de Implementación (estado actual)

Cada item esta vinculado al codigo que lo cumple. Si esta marcado `[x]` se
puede verificar siguiendo la referencia. Si esta `[ ]` hay un puntero al
gap concreto.

Convención:
- [x] Implementado — incluye `file:line` o nombre de DAG/servicio como evidencia
- [ ] Pendiente o parcial — incluye una nota explicando que falta

## Entrega Parcial (16/4)

- [x] Sistema dockerizado — [docker-compose.yaml](../docker-compose.yaml) levanta postgres, redis, airflow (apiserver/scheduler/worker/dag-processor/triggerer), minio, mlflow, ray-serve y api
- [x] API funcional conforme OpenAPI — [api/app/api/v1/router.py](../api/app/api/v1/router.py), endpoints `/wells` y `/forecast` con ejemplos `2023-01-01` / `96688`
- [x] Experiment tracking reproducible (sabe cual esta productivo) — [training.py:171-212](../airflow/plugins/ml_pipeline/training.py) promueve el run con menor RMSE via alias `production`
- [x] Logging de metricas y artefactos — [training.py:91-112](../airflow/plugins/ml_pipeline/training.py): train/test RMSE/MAE/R², feature_importance.png, signature, input_example, dataset logging
- [x] Features persistidas en feature store y usadas en inferencia — [dag_pozos.py persist_features task](../airflow/dags/dag_pozos.py), [api/app/repositories/postgres.py:56-90 get_features](../api/app/repositories/postgres.py)
- [x] Entrenamiento consume del feature store — [dag_pozos.py read_features_from_store task](../airflow/dags/dag_pozos.py), `data_source=featurestore` taggeado en cada run de MLflow
- [x] Training reproducible con un comando para cualquier dia — `docker compose exec airflow-scheduler airflow dags trigger ml_pipeline --conf '{"date_from":"YYYY-MM-DD","date_to":"YYYY-MM-DD"}'`; el DAG honra esos params en `preprocess` y `read_features_from_store`

## Entrega Final (28/5)

- [x] Orquestación automática del entrenamiento — [config.py PIPELINE_SCHEDULE](../airflow/plugins/ml_pipeline/config.py) = `0 2 1 * *` (mensual, alineado al ciclo del dataset)
- [x] Reporte de model decay + drift con ≥2 métricas — [dag_drift_report.py](../airflow/dags/dag_drift_report.py): PSI, KS p-value y RMSE-degradation (3 metricas). Logueadas a MLflow `drift_monitoring`, PNG como artefacto. Auto-dispara `ml_pipeline` si detecta drift o decay.
- [x] Infraestructura escalable para inferencia — [ray/serve_app.py](../ray/serve_app.py): Ray Serve con `num_replicas=2`, dashboard en 8265, FastAPI proxy en [api/app/api/v1/endpoints/forecast.py](../api/app/api/v1/endpoints/forecast.py)

## Requisitos técnicos

- [x] Docker & Docker Compose
- [x] API REST (base `/api/v1`)
- [x] Feature Store con `features` table poblada por el DAG — [db/init-featurestore.sql:41-53](../db/init-featurestore.sql), `INSERT` idempotente via `DELETE WHERE fecha BETWEEN`
- [x] Model Registry (`hydrocarbon_forecast` con alias `production`)
- [x] Experiment Tracking (MLflow + Postgres backend)
- [x] Orquestación con dos DAGs — `ml_pipeline` (entrenamiento mensual) y `drift_report` (monitoreo semanal con auto-retrigger)

## API especificada en OpenAPI ([docs/openapi.yaml](openapi.yaml))

- [x] `GET /api/v1/wells` — params `date_query`, `limit`, `offset`; respuesta `[{id_well}]`
- [x] `GET /api/v1/forecast` — params `id_well`, `date_start`, `date_end`; respuesta `{id_well, data: [{date, prod}]}`
- [x] `GET /health` — chequeo de liveness

## Aspectos a defender en la entrega final

- Feedback de la entrega parcial — "Feature Store para entrenamiento: Puede mejorar. No usan 'feature store'. Usan simplemente una DB." → Decisión de diseño: PostgreSQL como feature store (ver README sección "Decisiones de diseño" cuando se agregue) es suficiente para el alcance educativo. Trade-offs vs Feast/Tecton/Hopsworks documentados.
- Sin online/offline split, sin point-in-time joins, sin feature versioning — el alcance (inferencia batch mensual, single-tenant) no lo justifica.

## Verificacion E2E

- [x] [scripts/test_e2e.sh](../scripts/test_e2e.sh) — chequea health de API/Airflow/MLflow/MinIO **y** valida que `/forecast` devuelva data no-vacia (prueba que el feature store fue poblado + Ray Serve responde)

## Notas

- El alcance del MVP final esta implementado en `etapa-3-ray-serve` (con esta rama `missing-features` agregando el wiring real del feature store) y queda pendiente el merge a `main`.
- Las dos partes de la rúbrica de proceso (PR-history distribuido entre Hernan y Matias, README con seccion "Equipo") se completan con commits adicionales en esta rama.
