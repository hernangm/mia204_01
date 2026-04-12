# ml_pipeline/config.py — single source of truth for pipeline constants

DATASET_DOWNLOAD_URL = (
    "http://datos.energia.gob.ar/dataset/c846e79c-026c-4040-897f-1ad3543b407c"
    "/resource/b5b58cdc-9e07-41f9-b392-fb9ec68b0725/download/"
    "produccin-de-pozos-de-gas-y-petrleo-no-convencional.csv"
)

MLFLOW_EXPERIMENT_NAME = "hydrocarbon_forecast"

# ---------- Feature sets ----------
ALL_FEATURES_GAS = ['prod_pet', 'prod_agua', 'tef', 'profundidad', 'tipoextraccion']
REDUCED_FEATURES_GAS = ['prod_pet', 'tef', 'profundidad']

# ---------- Experiments to run ----------
EXPERIMENTS = [
    {
        'model_type': 'random_forest',
        'model_params': {'n_estimators': 50, 'random_state': 204},
        'target': 'prod_gas',
        'features': ALL_FEATURES_GAS,
    },
    {
        'model_type': 'random_forest',
        'model_params': {'n_estimators': 100, 'random_state': 204},
        'target': 'prod_gas',
        'features': ALL_FEATURES_GAS,
    },
    {
        'model_type': 'random_forest',
        'model_params': {'n_estimators': 200, 'random_state': 204},
        'target': 'prod_gas',
        'features': ALL_FEATURES_GAS,
    },
    {
        'model_type': 'random_forest',
        'model_params': {'n_estimators': 100, 'random_state': 204},
        'target': 'prod_gas',
        'features': REDUCED_FEATURES_GAS,
    },
]

# ---------- Encoding map (static for reproducibility) ----------
TIPOEXTRACCION_MAP = {
    'Bombeo Hidráulico': 0,
    'Bombeo Mecánico': 1,
    'Cavidad Progresiva': 2,
    'Electrosumergible': 3,
    'Gas Lift': 4,
    'Jet Pump': 5,
    'Otros Tipos de Extracción': 6,
    'Pistoneo (Swabbing)': 7,
    'Plunger Lift': 8,
    'Sin Sistema de Extracción': 9,
    'Surgencia Natural': 10,
}

# ---------- Orquestacion automatica ----------
# Expresion cron para retraining periodico (primer dia de cada mes, 02:00 UTC).
PIPELINE_SCHEDULE = "0 2 1 * *"

# ---------- Deteccion de drift ----------
# Umbral PSI: < 0.1 = sin drift, 0.1-0.2 = drift moderado, > 0.2 = drift severo.
PSI_THRESHOLD = 0.2
# Umbral p-value para test KS: si p < KS_PVALUE_THRESHOLD la distribucion cambio.
KS_PVALUE_THRESHOLD = 0.05
# Numero de bins para calcular PSI (estandar de industria: 10 o 20).
PSI_BINS = 10
# Degradacion de RMSE aceptable antes de considerar model decay (proporcion).
MODEL_DECAY_RMSE_THRESHOLD = 0.15
# Ventana de datos recientes para comparar distribuciones (en dias).
DRIFT_WINDOW_DAYS = 90
# Features continuas que se monitorizan para data drift.
DRIFT_FEATURES = ['prod_pet', 'prod_agua', 'tef', 'profundidad']
# Schedule del DAG de drift (todos los lunes a las 06:00 UTC).
DRIFT_SCHEDULE = "0 6 * * 1"
# Nombre del experimento MLflow para reportes de drift.
DRIFT_EXPERIMENT_NAME = "drift_monitoring"
# Tag en MLflow donde se guarda el RMSE baseline del modelo en produccion.
MLFLOW_BASELINE_RMSE_TAG = "baseline_test_rmse"
