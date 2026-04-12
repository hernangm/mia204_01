# ml_pipeline/config.py — fuente única de verdad para constantes del pipeline

# ---------------------------------------------------------------------------
# URLs de descarga de datasets públicos (datos.gob.ar)
# Se usan como fallback si los CSVs no están presentes en data/raw/.
# ---------------------------------------------------------------------------

# Producción mensual por pozo (no convencional) — ~164 MB
DATASET_DOWNLOAD_URL = (
    "http://datos.energia.gob.ar/dataset/c846e79c-026c-4040-897f-1ad3543b407c"
    "/resource/b5b58cdc-9e07-41f9-b392-fb9ec68b0725/download/"
    "produccin-de-pozos-de-gas-y-petrleo-no-convencional.csv"
)

# Listado de pozos con metadatos (empresa, provincia, cuenca, etc.)
WELLS_DOWNLOAD_URL = (
    "http://datos.energia.gob.ar/dataset/c846e79c-026c-4040-897f-1ad3543b407c"
    "/resource/cb5c0f04-7835-45cd-b982-3e25ca7d7751/download/capitulo-iv-pozos.csv"
)

# ---------------------------------------------------------------------------
# MLflow
# ---------------------------------------------------------------------------

# Nombre del experimento en el servidor MLflow.
# Todos los runs del pipeline se agrupan bajo este experimento.
MLFLOW_EXPERIMENT_NAME = "hydrocarbon_forecast"

# ---------------------------------------------------------------------------
# Feature sets
# ---------------------------------------------------------------------------

# Variables predictoras para modelos de producción de gas.
# Incluye producción de otros fluidos (pet, agua), tiempo efectivo (tef),
# profundidad del pozo y tipo de extracción (codificado numéricamente).
ALL_FEATURES_GAS = ["prod_pet", "prod_agua", "tef", "profundidad", "tipoextraccion"]

# ---------------------------------------------------------------------------
# Experimentos de entrenamiento
# ---------------------------------------------------------------------------
# Cada entrada define un run de MLflow independiente.
# El pipeline entrena todos en paralelo (dynamic task mapping en Airflow)
# y promueve el de menor RMSE al alias "production" en el Model Registry.
#
# Parámetros de performance ajustados para entorno de desarrollo:
#   - n_estimators reducido (10-30) para minimizar tiempo de cómputo
#   - max_depth acotado para evitar sobreajuste y reducir memoria
#   - max_samples=0.30: usa el 30% del dataset en cada entrenamiento,
#     suficiente para comparar modelos en una demo sin saturar recursos
#   - n_jobs=1: un solo core por experimento (Airflow ya paraleliza los runs)
# ---------------------------------------------------------------------------
EXPERIMENTS = [
    {
        "model_type": "random_forest",
        "model_params": {
            "n_estimators": 10,   # bosque pequeño, entrena en segundos
            "max_depth": 5,       # árboles poco profundos, bajo consumo de RAM
            "max_samples": 0.30,  # 30% del dataset por árbol
            "n_jobs": 1,          # un core; Airflow paraleliza entre experimentos
            "random_state": 204,  # semilla fija para reproducibilidad
        },
        "target": "prod_gas",
        "features": ALL_FEATURES_GAS,
    },
    {
        "model_type": "random_forest",
        "model_params": {
            "n_estimators": 20,   # más árboles que exp 1 para comparar
            "max_depth": 8,
            "max_samples": 0.30,
            "n_jobs": 1,
            "random_state": 204,
        },
        "target": "prod_gas",
        "features": ALL_FEATURES_GAS,
    },
    {
        "model_type": "random_forest",
        "model_params": {
            "n_estimators": 30,   # más árboles, mayor profundidad; tope de la demo
            "max_depth": 10,
            "max_samples": 0.30,
            "n_jobs": 1,
            "random_state": 204,
        },
        "target": "prod_gas",
        "features": ALL_FEATURES_GAS,
    },
]

# ---------------------------------------------------------------------------
# Encoding map para tipoextraccion
# ---------------------------------------------------------------------------
# Mapeo estático entero → categoría. Fijo en código para garantizar que
# el mismo valor numérico represente la misma categoría en cualquier run,
# independientemente del orden en que aparezcan en el CSV.
TIPOEXTRACCION_MAP = {
    "Bombeo Hidráulico": 0,
    "Bombeo Mecánico": 1,
    "Cavidad Progresiva": 2,
    "Electrosumergible": 3,
    "Gas Lift": 4,
    "Jet Pump": 5,
    "Otros Tipos de Extracción": 6,
    "Pistoneo (Swabbing)": 7,
    "Plunger Lift": 8,
    "Sin Sistema de Extracción": 9,
    "Surgencia Natural": 10,
}
