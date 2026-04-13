"""End-to-end hydrocarbon production forecast pipeline.

This file is intentionally a thin orchestrator: every task delegates to a
function in airflow/plugins/ml_pipeline/{data_ingestion,feature_engineering,
training}.py. The plugins directory is auto-added to sys.path by Airflow, so
imports look like `from ml_pipeline.… import …` and live INSIDE task bodies
(per the imports-inside-tasks convention used throughout this project).

Pipeline stages:
  1. ingest_csvs        — load production + wells from data/raw into postgres
  2. build_features     — derive the features table from production
  3. train_model[0..N]  — dynamically mapped over EXPERIMENTS, each logs to MLflow
  4. promote_best       — assigns the 'production' alias to the lowest-rmse model

Configuración via dag_run.conf (trigger con JSON):
  force_ingest (bool): fuerza recarga de CSVs aunque production ya tenga datos
  force_build  (bool): fuerza regeneración de features aunque ya existan
  Ejemplo: {"force_ingest": true, "force_build": true}
"""

from datetime import datetime

from airflow.decorators import dag, task


@dag(
    dag_id="ml_pipeline",
    description="End-to-end hydrocarbon production forecast pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["mia204", "ml"],
)
def ml_pipeline():

    @task
    def ingest_csvs() -> dict:
        from airflow.operators.python import get_current_context
        from ml_pipeline.data_ingestion import ingest

        conf = get_current_context()["dag_run"].conf or {}
        force = bool(conf.get("force_ingest", False))
        return ingest(force=force)

    @task
    def build_features(_upstream: dict) -> dict:
        # _upstream gates this task on ingest_csvs without using the data.
        from airflow.operators.python import get_current_context
        from ml_pipeline.feature_engineering import build

        conf = get_current_context()["dag_run"].conf or {}
        force = bool(conf.get("force_build", False))
        return build(force=force)

    @task
    def list_experiments(_upstream: dict) -> list[dict]:
        # Wrapped in a task so EXPERIMENTS isn't imported at DAG-parse time.
        from ml_pipeline.config import EXPERIMENTS

        return list(EXPERIMENTS)

    @task
    def train_model(experiment: dict) -> dict:
        from ml_pipeline.training import train_one_experiment

        return train_one_experiment(experiment)

    @task
    def promote_best(results: list[dict]) -> dict:
        from ml_pipeline.training import promote_best_run

        return promote_best_run(results)

    ingested = ingest_csvs()
    features_ready = build_features(ingested)
    experiments = list_experiments(features_ready)
    results = train_model.expand(experiment=experiments)
    promote_best(results)


ml_pipeline()
