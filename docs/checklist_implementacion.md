# Checklist de Implementación (estado actual)

Este checklist se deriva del README del proyecto y refleja el estado actual observado en el repositorio.

Convención usada:
- [x] Implementado
- [ ] Pendiente o parcial

## Entrega Parcial

- [x] Sistema dockerizado
- [ ] API funcional (parcial: estructura y endpoints base listos, falta validación end-to-end con datos/modelo productivo)
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
- [ ] Model Registry (parcial: MLflow desplegado, falta evidencia de flujo de registro/promoción en producción)
- [x] Experiment Tracking
- [ ] Orquestación (parcial: Airflow presente, falta automatización periódica de extremo a extremo)

## API especificada en README

- [x] Base URL /api/v1
- [x] GET /wells
- [x] GET /forecast (sirve predicciones del modelo `hydrocarbon_forecast@production` de MLflow sobre las features de cada `(id_well, fecha)`)

## Nota de alcance

- Los puntos de "Entrega Final" permanecen abiertos porque requieren automatizaciones y monitoreo avanzados que aún no están implementados end-to-end.
- Se desmarcaron ítems que estaban en estado parcial para que el checklist represente mejor el avance real.
