"""Model training with MLflow experiment tracking."""

import logging

import mlflow
import pandas as pd
from mlflow.exceptions import MlflowException
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split

from ml_pipeline.config import (
    FEATURE_COLUMNS,
    MLFLOW_EXPERIMENT,
    MLFLOW_TRACKING_URI,
    MODEL_NAME,
    TARGET_COLUMN,
)

logger = logging.getLogger(__name__)


def train_and_log(
    df: pd.DataFrame,
    n_estimators: int = 100,
    test_size: float = 0.2,
    random_state: int = 42,
) -> dict:
    """Train a RandomForestRegressor, log params/metrics/model to MLflow.

    Returns a dict with run_id, metrics, and model_uri.
    Raises ``RuntimeError`` on MLflow or training errors.
    """
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(MLFLOW_EXPERIMENT)

    X = df[FEATURE_COLUMNS]
    y = df[TARGET_COLUMN]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state
    )

    try:
        with mlflow.start_run() as run:
            # Log parameters
            mlflow.log_param("n_estimators", n_estimators)
            mlflow.log_param("test_size", test_size)
            mlflow.log_param("random_state", random_state)
            mlflow.log_param("n_features", len(FEATURE_COLUMNS))
            mlflow.log_param("n_samples_train", len(X_train))
            mlflow.log_param("n_samples_test", len(X_test))
            mlflow.log_param("features", FEATURE_COLUMNS)
            mlflow.log_param("target", TARGET_COLUMN)

            # Train
            model = RandomForestRegressor(
                n_estimators=n_estimators, random_state=random_state
            )
            model.fit(X_train, y_train)

            # Evaluate
            y_pred = model.predict(X_test)
            mse = mean_squared_error(y_test, y_pred)
            mae = mean_absolute_error(y_test, y_pred)
            r2 = r2_score(y_test, y_pred)

            mlflow.log_metric("mse", mse)
            mlflow.log_metric("mae", mae)
            mlflow.log_metric("r2", r2)

            logger.info("Metrics — MSE: %.4f  MAE: %.4f  R2: %.4f", mse, mae, r2)

            # Log model
            mlflow.sklearn.log_model(
                model,
                artifact_path="model",
                registered_model_name=MODEL_NAME,
            )

            return {
                "run_id": run.info.run_id,
                "mse": mse,
                "mae": mae,
                "r2": r2,
                "model_uri": f"runs:/{run.info.run_id}/model",
            }
    except MlflowException as exc:
        raise RuntimeError(f"MLflow error during training: {exc}") from exc


def promote_model(run_id: str, alias: str = "production") -> None:
    """Assign the *alias* to the model version produced by *run_id*.

    Raises ``RuntimeError`` if no model version is found or MLflow fails.
    """
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    client = mlflow.tracking.MlflowClient()

    try:
        # Find the version created by this run
        filter_str = f"run_id='{run_id}'"
        versions = client.search_model_versions(filter_str)
        if not versions:
            raise ValueError(f"No model version found for run_id={run_id}")

        latest = max(versions, key=lambda v: int(v.version))
        client.set_registered_model_alias(MODEL_NAME, alias, latest.version)
        logger.info(
            "Model %s v%s promoted with alias '%s'",
            MODEL_NAME,
            latest.version,
            alias,
        )
    except MlflowException as exc:
        raise RuntimeError(f"MLflow error during model promotion: {exc}") from exc
