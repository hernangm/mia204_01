"""ML Pipeline DAG — hydrocarbon production forecast.

Tasks:
  1. download_raw_data   — fetch production & wells CSVs
  2. ingest_wells        — load wells into feature store
  3. compute_features    — engineer features from production data, persist
  4. train_model         — train model, log to MLflow
  5. promote_model       — tag model version as 'production'
"""

import logging
from datetime import datetime

from airflow.decorators import dag, task
from airflow.models.param import Param

logger = logging.getLogger(__name__)


@dag(
    dag_id="ml_pipeline",
    description="Pipeline de ML para pronóstico de producción de hidrocarburos",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    params={
        "date_from": Param(
            default=None,
            type=["null", "string"],
            description="Fecha inicio del rango (YYYY-MM-DD). Vacío = sin filtro.",
        ),
        "date_to": Param(
            default=None,
            type=["null", "string"],
            description="Fecha fin del rango (YYYY-MM-DD). Vacío = sin filtro.",
        ),
    },
)
def ml_pipeline():

    @task
    def download_raw_data() -> dict:
        """Download both CSV datasets to /opt/airflow/data/."""
        from ml_pipeline.data_ingestion import download_production, download_wells

        prod_path = download_production()
        wells_path = download_wells()
        return {"production_csv": prod_path, "wells_csv": wells_path}

    @task
    def ingest_wells(paths: dict) -> int:
        """Load wells CSV into the feature-store database."""
        from ml_pipeline.data_ingestion import ingest_wells as _ingest
        from ml_pipeline.db import get_engine

        engine = get_engine()
        return _ingest(engine, csv_path=paths["wells_csv"])

    @task
    def compute_features(paths: dict) -> int:
        """Engineer features from production CSV and write to feature store."""
        from airflow.operators.python import get_current_context

        from ml_pipeline.db import get_engine
        from ml_pipeline.feature_engineering import compute_features as _compute

        context = get_current_context()
        params = context["params"]

        engine = get_engine()
        return _compute(
            engine,
            csv_path=paths["production_csv"],
            date_from=params.get("date_from"),
            date_to=params.get("date_to"),
        )

    @task
    def train_model() -> dict:
        """Train a RandomForest model and log to MLflow."""
        from ml_pipeline.db import get_engine
        from ml_pipeline.feature_engineering import load_training_data
        from ml_pipeline.training import train_and_log

        engine = get_engine()
        df = load_training_data(engine)
        logger.info("Training on %d rows", len(df))
        return train_and_log(df)

    @task
    def promote_model(train_result: dict) -> None:
        """Promote the trained model version with alias 'production'."""
        from ml_pipeline.training import promote_model as _promote

        _promote(run_id=train_result["run_id"])

    # --- DAG wiring ---
    paths = download_raw_data()
    wells_done = ingest_wells(paths)
    features_done = compute_features(paths)

    # Training starts after features are ready (wells ingest can run in parallel)
    result = train_model()
    features_done >> result
    wells_done >> result

    promote_model(result)


ml_pipeline()
