# Forecast de Produccion de Hidrocarburos

Sistema de ML para predecir la produccion de gas y petroleo de pozos no convencionales.
El foco del proyecto es ML Engineering: reproducibilidad, trazabilidad y despliegue, no la sofisticacion del modelo.

Dataset: [Produccion de pozos de gas y petroleo no convencional](http://datos.energia.gob.ar/dataset/c846e79c-026c-4040-897f-1ad3543b407c/resource/b5b58cdc-9e07-41f9-b392-fb9ec68b0725)
— Secretaria de Energia, Gobierno de Argentina.

---

## Arquitectura

```
datos crudos  -->  Feature Store (PostgreSQL)  -->  Entrenamiento  -->  MLflow Tracking
                                                                              |
                                                                     Model Registry
                                                                              |
                                                          API REST (FastAPI) <--
```

Servicios principales:

| Servicio    | Puerto | Descripcion                        |
|-------------|--------|------------------------------------|
| API REST    | 8000   | Endpoints de prediccion y pozos    |
| MLflow      | 5000   | Tracking de experimentos y modelos |
| PostgreSQL  | 5432   | Feature Store (interno)            |
| Airflow     | 8080   | Orquestacion (entrega final)       |

---

## Requisitos

- Docker >= 24.0
- Docker Compose >= 2.17 (por uso de `dockerfile_inline`)
- Python 3.11+ (solo para desarrollo local sin Docker)

---

## Quick Start

### 1. Clonar el repositorio

```bash
git clone <url-del-repo>
cd mia204_01
```

### 2. Configurar variables de entorno

```bash
cp .env.example .env
```

Los valores por defecto del `.env.example` funcionan sin modificacion para desarrollo local.

### 3. Levantar los servicios

```bash
docker compose up -d
```

Esto levanta PostgreSQL, MLflow, la API y Airflow. Verificar que esten corriendo:

```bash
docker compose ps
```

### 4. Entrenar el modelo

```bash
docker compose run --rm training python scripts/train.py --date 2024-10-15
```

El parametro `--date` define hasta que fecha se usan los datos. Mismo codigo + misma fecha = mismo modelo.

### 5. Hacer predicciones

```bash
curl "http://localhost:8000/api/v1/forecast?id_well=POZO-001&date_start=2024-11-01&date_end=2024-11-30"
```

---

## Endpoints de la API

### GET /api/v1/forecast

Devuelve predicciones de produccion para un pozo en un rango de fechas.

Parametros:

| Nombre     | Tipo   | Requerido | Descripcion                      |
|------------|--------|-----------|----------------------------------|
| id_well    | string | si        | Identificador del pozo           |
| date_start | string | si        | Fecha inicio (YYYY-MM-DD)        |
| date_end   | string | si        | Fecha fin (YYYY-MM-DD)           |

Respuesta de ejemplo:

```json
{
  "id_well": "POZO-001",
  "data": [
    {"date": "2024-11-01", "prod": 12.4},
    {"date": "2024-11-02", "prod": 11.9}
  ]
}
```

### GET /api/v1/wells

Devuelve la lista de pozos disponibles en el Feature Store para una fecha dada.

Parametros:

| Nombre     | Tipo   | Requerido | Descripcion               |
|------------|--------|-----------|---------------------------|
| date_query | string | si        | Fecha de consulta (YYYY-MM-DD) |

Respuesta de ejemplo:

```json
[
  {"id_well": "POZO-001"},
  {"id_well": "POZO-002"}
]
```

Documentacion interactiva disponible en `http://localhost:8000/docs`.

---

## Estructura del Proyecto

```
.
├── docker-compose.yaml         # Todos los servicios (API, MLflow, PostgreSQL, Airflow)
├── requirements.txt
├── .env / .env.example
│
├── docker/
│   ├── Dockerfile.api          # Imagen FastAPI con hot-reload
│   ├── Dockerfile.training     # Imagen para correr el script de entrenamiento
│   └── postgres/
│       └── init.sql            # Crea la DB "forecast" al primer arranque
│
├── src/
│   ├── api/                    # FastAPI: rutas, schemas, dependencias
│   ├── data/                   # Carga y validacion de datos
│   ├── features/               # Feature engineering y Feature Store
│   ├── models/                 # Entrenamiento, evaluacion y registro
│   └── inference/              # Prediccion con modelo registrado en MLflow
│
├── scripts/
│   └── train.py                # Entrenamiento reproducible por fecha
│
├── tests/                      # Tests unitarios e integracion
├── dags/                       # DAGs de Airflow (entrega final)
├── data/                       # Ignorado en git
└── logs/                       # Ignorado en git
```

---

## Flujo de Entrenamiento

```
1. Descargar datos del gobierno (datos.energia.gob.ar)
2. Validar y limpiar el dataset
3. Generar features y persistir en Feature Store (PostgreSQL)
4. Entrenar modelo consumiendo el Feature Store
5. Registrar parametros, metricas y artefactos en MLflow
6. Promover a produccion si supera al modelo actual
```

---

## Estado del Proyecto — Entrega Parcial (16/04)

### Hecho

- [x] `docker compose up -d` levanta todos los servicios
- [x] PostgreSQL con dos bases de datos: `airflow` (Airflow) y `forecast` (Feature Store + MLflow)
- [x] MLflow corriendo en `http://localhost:5000` con backend en PostgreSQL y artefactos en volumen persistente
- [x] API FastAPI corriendo en `http://localhost:8000` con hot-reload activo
- [x] Airflow (CeleryExecutor) corriendo en `http://localhost:8080`
- [x] Servicio `training` configurado para correr bajo demanda con `docker compose run --rm training`
- [x] Estructura base de `src/` y `requirements.txt`
- [x] Endpoints `/api/v1/forecast` y `/api/v1/wells` declarados (responden, sin logica aun)
- [x] DAG exploratorio en `dags/dag_pozos.py`

### Falta para la entrega

- [ ] **Data loader**: `src/data/loader.py` — descarga y valida el CSV de datos.gob.ar
- [ ] **Feature engineering**: `src/features/engineering.py` — genera features a partir de datos crudos
- [ ] **Feature Store**: `src/features/feature_store.py` — persiste y consulta features en PostgreSQL
- [ ] **Script de entrenamiento**: `scripts/train.py --date YYYY-MM-DD` — reproducible con seed fijo, consume el feature store, registra en MLflow
- [ ] **Logica de prediccion**: `src/inference/predictor.py` — carga modelo desde MLflow y predice
- [ ] **API completa**: conectar `/api/v1/forecast` y `/api/v1/wells` al feature store y al predictor
- [ ] **Tests basicos**: `tests/test_api.py`, `tests/test_features.py`
- [ ] **`.env.example`** con los valores por defecto documentados
- [ ] **README** con instrucciones finales verificadas (este archivo)

---

## Desarrollo Local (sin Docker)

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Requiere PostgreSQL y MLflow corriendo (usar docker compose up postgres mlflow)
export DATABASE_URL=postgresql://forecast:forecast@localhost:5432/forecast
export MLFLOW_TRACKING_URI=http://localhost:5000

python scripts/train.py --date 2024-10-15
uvicorn src.api.app:app --reload
```

---

## Tests

```bash
pytest tests/ -v
pytest tests/ --cov=src --cov-report=term
```

---

## Troubleshooting

**La API no responde:**
```bash
docker compose ps
docker compose logs api
```

**El entrenamiento falla:**
```bash
docker compose logs training
```

**MLflow no registra experimentos:**
```bash
docker compose logs mlflow
# Verificar que MLFLOW_TRACKING_URI este correctamente seteado en .env
```

**Puerto 5432 ya en uso:**
El contenedor de PostgreSQL no expone el puerto al host intencionalmente.
Los servicios se comunican por la red interna de Docker.

---

## Equipo

Trabajo integrador — Materia IA en Produccion (MIA204), UdeSA.
