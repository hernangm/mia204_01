# Guión — Demo Video (6 minutos)

**Proyecto:** Pronóstico de Producción de Hidrocarburos  
**Materia:** IA en Producción — MIA 204  
**Integrantes:** Hernan Marano · Matias Caccia  
**Formato:** pantalla compartida + voz en off, no hace falta cámara

---

## Distribución de tiempo

| Segmento | Tiempo | Quién habla |
|---|---|---|
| 1. Introducción y contexto | 0:00 – 0:45 | Hernan |
| 2. Arquitectura del sistema | 0:45 – 1:30 | Hernan |
| 3. Levantar el stack | 1:30 – 2:00 | Matias |
| 4. Pipeline Airflow de punta a punta | 2:00 – 3:15 | Hernan |
| 5. MLflow: experimentos y modelo productivo | 3:15 – 3:50 | Hernan |
| 6. API REST y Ray Serve | 3:50 – 4:40 | Matias |
| 7. Drift monitoring y auto-retraining | 4:40 – 5:20 | Hernan |
| 8. Tests y cierre | 5:20 – 6:00 | Matias |

---

## Guión detallado

### Segmento 1 — Introducción (0:00 – 0:45) · Hernan

*[Pantalla: slide o README con el título del proyecto]*

> "El proyecto que vamos a mostrar hoy es una plataforma de ML Engineering para predecir la producción mensual de gas de pozos de petróleo no convencional en Argentina. Usamos datos públicos del Ministerio de Energía — producción por pozo mes a mes.
>
> El objetivo **no** es tener el modelo más preciso. El foco está en construir la ingeniería alrededor del modelo: reproducibilidad, trazabilidad, automatización y escalabilidad. Todo lo que hace que un modelo de ML pueda operar en producción de forma confiable."

---

### Segmento 2 — Arquitectura (0:45 – 1:30) · Hernan

*[Pantalla: diagrama del README o el docker-compose.yaml abierto en el editor]*

> "El sistema completo levanta con un solo comando: `docker compose up`. Tenemos ocho servicios:
>
> - **PostgreSQL** hace tres trabajos: guarda la metadata de Airflow, el Feature Store con las features del modelo, y el backend de MLflow.
> - **Airflow** orquesta dos DAGs: uno de entrenamiento que corre el primero de cada mes, y uno de monitoreo que corre todos los lunes.
> - **MLflow** registra cada experimento y gestiona el Model Registry. El modelo en producción vive bajo el alias `production`.
> - **Ray Serve** corre la inferencia distribuida con dos réplicas del modelo. Es un contenedor separado de la API.
> - **La API FastAPI** expone los endpoints `/forecast` y `/wells` al mundo exterior."

---

### Segmento 3 — Levantar el stack (1:30 – 2:00) · Matias

*[Pantalla: terminal en la raíz del proyecto]*

> "Arrancamos el stack. Un solo comando:"

```bash
docker compose up --build
```

> "Mientras levanta, muestro los servicios corriendo:"

```bash
docker compose ps
```

> "Vemos todos los contenedores en estado `healthy`. Nótese que el puerto de postgres está en 5433 — lo movimos para no pisar una instalación local. La API está en 8000, MLflow en 9090, el dashboard de Ray en 8265 y Airflow en 8080."

---

### Segmento 4 — Pipeline Airflow de punta a punta (2:00 – 3:15) · Hernan

*[Pantalla: Airflow UI en http://localhost:8080, luego logs de las tasks]*

> "Vamos a Airflow. Acá vemos los dos DAGs: `ml_pipeline` y `drift_report`.
>
> Triggereo el pipeline de entrenamiento:"

*[Hacer click en Trigger DAG o mostrar que ya corrió exitosamente]*

> "El pipeline tiene seis tasks. Les explico las más importantes:
>
> La task `preprocess` descarga el CSV con la producción de todos los pozos y aplica un **shift temporal**: desplazamos `prod_gas` un mes hacia adelante por pozo. Esto es importante — si no hiciéramos el shift, el modelo usaría la producción de agua y petróleo del mismo mes para predecir el gas del mismo mes, lo que es data leakage. En producción, esa información no existe al principio del mes. Con el shift, el modelo aprende: *dadas las condiciones de enero, ¿cuánto gas producirá el pozo en febrero?*
>
> La task `persist_features` guarda el resultado en el **Feature Store**, que es una tabla PostgreSQL. La escritura es idempotente: primero borra el rango de fechas a insertar y después inserta. Así podemos re-ejecutar el pipeline sin duplicar datos.
>
> La task `train_experiments` lee directo del Feature Store — no viaja el dataset por XCom — y entrena cuatro variantes del modelo con distintos hiperparámetros. Cada una queda registrada como un run en MLflow.
>
> Finalmente, `promote_model` elige el de menor RMSE y le asigna el alias `production`."

*[Mostrar el log de `promote_model` con el RMSE y versión]*

---

### Segmento 5 — MLflow (3:15 – 3:50) · Hernan

*[Pantalla: MLflow UI en http://localhost:9090]*

> "Acá en MLflow vemos el experimento `hydrocarbon_forecast`. Cada fila es un run — uno por variante del modelo. Están logueados los parámetros, las métricas de train y test, y los artefactos.
>
> Entro a un run:"

*[Click en un run, mostrar métricas y artefactos]*

> "Vemos `test_rmse`, `test_mae`, `test_r2`. Como artefacto, además del modelo serializado, tenemos un gráfico de importancia de features — `prod_pet` y `tef` son las más relevantes.
>
> En el Model Registry, el modelo ganador tiene el alias `production`. Ray Serve lo carga automáticamente al iniciar."

---

### Segmento 6 — API REST y Ray Serve (3:50 – 4:40) · Matias

*[Pantalla: terminal o navegador en http://localhost:8000/docs]*

> "La API tiene tres endpoints. Muestro el Swagger:"

*[Abrir http://localhost:8000/docs]*

> "Pruebo `/forecast` con un pozo real:"

```bash
curl "http://localhost:8000/api/v1/forecast?id_well=96688&date_start=2022-01-01&date_end=2022-06-01"
```

> "La respuesta tiene un array de puntos mensuales. Cada punto tiene `date` — que es el primer día del mes cuyas features se usaron como input — y `prod` — que es la predicción de gas para el mes siguiente, en miles de metros cúbicos.
>
> Internamente, esta request llegó a la API FastAPI, que consultó el Feature Store para obtener las features del pozo en ese rango, y mandó esas features a Ray Serve mediante un POST a `/predict`. Ray Serve corrió la inferencia con el modelo de MLflow y devolvió las predicciones.
>
> Pruebo también `/wells`:"

```bash
curl "http://localhost:8000/api/v1/wells?date_query=2022-01-01&limit=5"
```

> "Devuelve los pozos que tienen datos hasta esa fecha. Tiene paginación con `limit` y `offset`.
>
> El dashboard de Ray lo podemos ver en el puerto 8265 — muestra los dos replicas del deployment corriendo."

*[Opcional: abrir http://localhost:8265 brevemente]*

---

### Segmento 7 — Drift monitoring y auto-retraining (4:40 – 5:20) · Hernan

*[Pantalla: Airflow UI, DAG drift_report / MLflow experimento drift_monitoring]*

> "El segundo DAG es `drift_report`, que corre todos los lunes a las 6 AM UTC.
>
> Lo que hace es comparar la distribución de features de los últimos 90 días contra todo el histórico del Feature Store — que es exactamente lo que vio el modelo al entrenar.
>
> Usa dos tests estadísticos:
> - **PSI** — Population Stability Index. Mide cuánto cambió la distribución de cada feature. Umbral: mayor a 0.2 es drift severo.
> - **KS test** — Kolmogorov-Smirnov. P-value menor a 0.05 indica cambio significativo.
>
> Además evalúa el **model decay**: carga el modelo productivo, predice sobre datos recientes y compara el RMSE contra el baseline que se taggeó en MLflow cuando el modelo fue promovido.
>
> Todos estos resultados quedan registrados en MLflow bajo el experimento `drift_monitoring`."

*[Mostrar el experimento drift_monitoring en MLflow si hay datos, o el código del DAG]*

> "Si detecta drift o decay, la última task llama a la API REST de Airflow y dispara automáticamente el DAG `ml_pipeline` para reentrenar con los datos más recientes. Es un ciclo cerrado."

---

### Segmento 8 — Tests y cierre (5:20 – 6:00) · Matias

*[Pantalla: terminal]*

> "El sistema tiene 35 tests en total. Los corro:"

```bash
# Tests unitarios de la API (con repositorio fake y Ray mockeado)
cd api && python -m pytest tests/ -v --ignore=tests/test_integration_api_postgres.py
```

*[Mostrar salida: 17 passed]*

```bash
# Tests de integración contra el stack real
RUN_INTEGRATION=1 python -m pytest tests/test_integration_api_postgres.py -v
```

*[Mostrar salida: 3 passed]*

```bash
# Tests del módulo de drift (PSI, KS, model decay)
docker compose exec airflow-worker python -m pytest /opt/airflow/tests/ -v
```

*[Mostrar salida: 15 passed]*

> "Para cerrar: el sistema que mostramos hoy cubre el ciclo completo de ML en producción. Ingesta y Feature Store con idempotencia y point-in-time correctness. Entrenamiento reproducible con shift temporal para evitar data leakage. Experiment tracking y Model Registry en MLflow. Serving distribuido con Ray Serve. Y monitoreo automático de drift que cierra el ciclo con retraining. Todo dockerizado, con un solo comando."

---

## Checklist antes de grabar

- [ ] Stack levantado y todos los servicios en `healthy`
- [ ] Pipeline `ml_pipeline` corrido al menos una vez (modelo `production` disponible)
- [ ] MLflow con al menos 4 runs del experimento `hydrocarbon_forecast`
- [ ] Ray Serve en estado `healthy` (cargó el modelo)
- [ ] `curl` del `/forecast` devuelve datos reales (no 503)
- [ ] Tests corriendo localmente sin errores
- [ ] Resolución de pantalla legible (font size terminal >= 16pt)
- [ ] Sin notificaciones ni ventanas extrañas abiertas

## Tips para la grabación

- Mostrar las URLs en el navegador siempre que sea posible (más visual que solo terminal)
- Para MLflow, hacer zoom en las métricas de un run — los números son más elocuentes que el código
- Si el pipeline tarda (descarga del CSV), tener una corrida anterior ya en "success" para mostrar
- Los logs de Airflow dentro de cada task son buenos para mostrar el shift: buscar la línea `"Shift temporal aplicado"`
