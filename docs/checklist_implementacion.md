# Checklist de Implementación (estado actual)

Este checklist se deriva del README del proyecto y refleja el estado actual observado en el repositorio.

Convención usada:
- [x] Implementado
- [ ] Pendiente o parcial

## Entrega Parcial

- [x] Sistema dockerizado
- [x] API funcional (endpoints con date format y examples en Swagger, modelo productivo cargado desde MLflow)
- [x] Experiment tracking
- [x] Logging de métricas
- [x] Feature Store persistente
- [x] Training reproducible

## Entrega Final

- [ ] Orquestación automática
- [ ] Retraining periódico
- [ ] Model decay
- [ ] Data drift / concept drift
- [ ] Infraestructura escalable

## Requisitos técnicos

- [x] Docker & Docker Compose
- [x] API REST (base)
- [x] Feature Store
- [x] Model Registry (modelos registrados automáticamente, alias `production` asignado al mejor modelo por RMSE de test)
- [x] Experiment Tracking
- [x] Orquestación (DAG `ml_pipeline` ejecuta pipeline completo: descarga → preprocesamiento → entrenamiento de 4 experimentos → promoción del mejor modelo)

## API especificada en README

- [x] Base URL /api/v1
- [x] GET /wells
- [x] GET /forecast (sirve predicciones del modelo `hydrocarbon_forecast@production` de MLflow sobre las features de cada `(id_well, fecha)`)

## Nota de alcance

- La entrega parcial está completa: pipeline end-to-end funcional con MLflow integration (train/test split, feature importance, model signature, dataset logging, tags, promoción automática).
- Los puntos de "Entrega Final" permanecen abiertos porque requieren automatizaciones (scheduling periódico) y monitoreo avanzados (drift, decay) que aún no están implementados.
