"""Centralized configuration for the ML pipeline.

All tuneable constants and environment-dependent values live here so that
every module imports from a single source of truth.
"""

import os

# ---------------------------------------------------------------------------
# Data sources
# ---------------------------------------------------------------------------
PRODUCTION_URL = (
    "http://datos.energia.gob.ar/dataset/c846e79c-026c-4040-897f-1ad3543b407c"
    "/resource/b5b58cdc-9e07-41f9-b392-fb9ec68b0725/download/"
    "produccin-de-pozos-de-gas-y-petrleo-no-convencional.csv"
)

WELLS_URL = (
    "http://datos.energia.gob.ar/dataset/c846e79c-026c-4040-897f-1ad3543b407c"
    "/resource/cbfa4d79-ffb3-4096-bab5-eb0dde9a8385/download/"
    "listado-de-pozos-cargados-por-empresas-operadoras.csv"
)

DATA_DIR = os.environ.get("DATA_DIR", "/opt/airflow/data/raw")

# ---------------------------------------------------------------------------
# Feature engineering
# ---------------------------------------------------------------------------
FEATURE_COLUMNS: list[str] = ["tipoextraccion", "prod_gas", "prod_agua", "tef"]
TARGET_COLUMN: str = "prod_pet"

TIPO_EXTRACCION_MAP: dict[str, int] = {
    "CONVENCIONAL": 0,
    "NO CONVENCIONAL": 1,
}
TIPO_EXTRACCION_DEFAULT: int = -1

# ---------------------------------------------------------------------------
# MLflow
# ---------------------------------------------------------------------------
MLFLOW_TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000")
MLFLOW_EXPERIMENT = os.environ.get("MLFLOW_EXPERIMENT", "hydrocarbon_forecast")
MODEL_NAME = "hydrocarbon_forecast_model"

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql+psycopg2://airflow:airflow@postgres:5432/featurestore",
)
