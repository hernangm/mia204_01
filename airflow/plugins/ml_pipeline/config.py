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
