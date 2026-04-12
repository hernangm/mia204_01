"""Stage 3 of the ml_pipeline DAG: train models and log to MLflow.

`train_one_experiment` is the per-experiment trainer designed to be called
from a dynamically-mapped Airflow task (one mapped instance per dict in
ml_pipeline.config.EXPERIMENTS). Each call:
  * trains a RandomForestRegressor with the experiment's params
  * logs params + metrics + the fitted model under MLFLOW_EXPERIMENT_NAME
  * registers the model under REGISTERED_MODEL_NAME

`promote_best_run` consumes the list of mapped results and tags the lowest-rmse
version of the registered model with the alias 'production'.
"""

import logging
import os

import mlflow
import mlflow.sklearn
import pandas as pd
from mlflow import MlflowClient
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split

from ml_pipeline.config import MLFLOW_EXPERIMENT_NAME
from ml_pipeline.db import get_engine

log = logging.getLogger(__name__)

REGISTERED_MODEL_NAME = "hydrocarbon_forecast"
PRODUCTION_ALIAS = "production"
TEST_SIZE = 0.2
RANDOM_STATE = 204


def _setup_mlflow() -> None:
    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI")
    if not tracking_uri:
        raise RuntimeError("MLFLOW_TRACKING_URI is not set")
    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(MLFLOW_EXPERIMENT_NAME)


def _load_features() -> pd.DataFrame:
    engine = get_engine()
    try:
        return pd.read_sql(
            "SELECT id_pozo, fecha, tipoextraccion, prod_gas, prod_agua, "
            "tef, prod_pet, profundidad FROM features",
            engine,
        )
    except Exception as exc:
        raise RuntimeError(f"Failed to read features table: {exc}") from exc


def train_one_experiment(experiment: dict) -> dict:
    """Train one experiment and return {run_id, version, rmse}.

    `experiment` is one entry from ml_pipeline.config.EXPERIMENTS.
    """
    _setup_mlflow()
    df = _load_features()
    if df.empty:
        raise RuntimeError("features table is empty; did build_features run?")

    target = experiment["target"]
    features = experiment["features"]
    params = experiment["model_params"]
    log.info("Training experiment: target=%s features=%s params=%s", target, features, params)

    X = df[features]
    y = df[target]
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=TEST_SIZE, random_state=RANDOM_STATE
    )

    with mlflow.start_run() as run:
        mlflow.log_param("model_type", experiment["model_type"])
        mlflow.log_param("target", target)
        mlflow.log_param("features", ",".join(features))
        for key, value in params.items():
            mlflow.log_param(key, value)

        model = RandomForestRegressor(**params)
        model.fit(X_train, y_train)
        preds = model.predict(X_test)

        rmse = float(mean_squared_error(y_test, preds) ** 0.5)
        r2 = float(r2_score(y_test, preds))
        mae = float(mean_absolute_error(y_test, preds))
        mlflow.log_metric("rmse", rmse)
        mlflow.log_metric("r2", r2)
        mlflow.log_metric("mae", mae)

        info = mlflow.sklearn.log_model(
            sk_model=model,
            name="model",
            registered_model_name=REGISTERED_MODEL_NAME,
        )

        version = info.registered_model_version
        if version is None:
            # Fallback path if log_model didn't surface the version directly.
            client = MlflowClient()
            matches = client.search_model_versions(f"run_id='{run.info.run_id}'")
            if not matches:
                raise RuntimeError(
                    f"log_model registered no version for run {run.info.run_id}"
                )
            version = matches[0].version

        log.info(
            "Run %s registered as version %s (rmse=%.4f r2=%.4f)",
            run.info.run_id,
            version,
            rmse,
            r2,
        )
        return {"run_id": run.info.run_id, "version": int(version), "rmse": rmse}


def promote_best_run(results: list[dict]) -> dict:
    """Pick the lowest-rmse mapped result and assign the production alias to it."""
    if not results:
        raise RuntimeError("No training results to promote")
    best = min(results, key=lambda r: r["rmse"])

    _setup_mlflow()
    client = MlflowClient()
    client.set_registered_model_alias(
        name=REGISTERED_MODEL_NAME,
        alias=PRODUCTION_ALIAS,
        version=str(best["version"]),
    )
    log.info(
        "Promoted version %s (run_id=%s, rmse=%.4f) as alias '%s'",
        best["version"],
        best["run_id"],
        best["rmse"],
        PRODUCTION_ALIAS,
    )
    return best
