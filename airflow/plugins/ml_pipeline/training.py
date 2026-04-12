"""Módulo de entrenamiento con integración MLflow.

Entrena modelos scikit-learn y registra parámetros, métricas y
artefactos en el tracking server de MLflow.
"""

import logging

logger = logging.getLogger(__name__)


def _feature_set_tag(features):
    """Devuelve un tag descriptivo según el conjunto de features usado."""
    from ml_pipeline.config import ALL_FEATURES_GAS, REDUCED_FEATURES_GAS

    if features == ALL_FEATURES_GAS:
        return "all"
    if features == REDUCED_FEATURES_GAS:
        return "reduced"
    return "custom"


def train_and_log(data_dict, experiment_cfg):
    """Entrena un modelo y registra todo en MLflow.

    Args:
        data_dict: dict de listas (dataset preprocesado, serializable JSON).
        experiment_cfg: dict con claves model_type, model_params, target, features.

    Returns:
        dict con run_id, rmse, mae, r2.
    """
    import mlflow
    import mlflow.sklearn
    import numpy as np
    import pandas as pd
    from mlflow.models import infer_signature
    from sklearn.ensemble import RandomForestRegressor
    from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
    from sklearn.model_selection import train_test_split

    from ml_pipeline.config import MLFLOW_EXPERIMENT_NAME

    # Silenciar mensajes informativos de MLflow que Airflow captura como ERROR
    logging.getLogger("mlflow").setLevel(logging.WARNING)
    logging.getLogger("mlflow.sklearn").setLevel(logging.WARNING)
    logging.getLogger("mlflow.store.model_registry").setLevel(logging.WARNING)

    df = pd.DataFrame(data_dict)

    target = experiment_cfg["target"]
    features = experiment_cfg["features"]
    model_params = experiment_cfg["model_params"]

    X = df[features]
    y = df[target]

    # --- Train / test split (80/20) ---
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=model_params.get("random_state", 42)
    )

    mlflow.set_experiment(MLFLOW_EXPERIMENT_NAME)

    with mlflow.start_run() as run:
        # --- Tags ---
        mlflow.set_tag("feature_set", _feature_set_tag(features))
        mlflow.set_tag("model_type", experiment_cfg.get("model_type", "random_forest"))

        # --- Dataset ---
        dataset = mlflow.data.from_pandas(
            df[features + [target]],
            name="produccion_no_convencional",
            targets=target,
        )
        mlflow.log_input(dataset, context="training")

        # --- Parámetros ---
        mlflow.log_param("target", target)
        mlflow.log_param("features", ", ".join(features))
        mlflow.log_param("n_features", len(features))
        mlflow.log_param("n_samples_total", len(df))
        mlflow.log_param("n_samples_train", len(X_train))
        mlflow.log_param("n_samples_test", len(X_test))
        mlflow.log_params(model_params)

        # --- Entrenamiento ---
        modelo = RandomForestRegressor(**model_params)
        modelo.fit(X_train, y_train)

        # --- Métricas sobre train ---
        y_pred_train = modelo.predict(X_train)
        train_rmse = float(np.sqrt(mean_squared_error(y_train, y_pred_train)))
        train_mae = float(mean_absolute_error(y_train, y_pred_train))
        train_r2 = float(r2_score(y_train, y_pred_train))

        mlflow.log_metric("train_rmse", train_rmse)
        mlflow.log_metric("train_mae", train_mae)
        mlflow.log_metric("train_r2", train_r2)

        # --- Métricas sobre test (las que importan) ---
        y_pred_test = modelo.predict(X_test)
        test_rmse = float(np.sqrt(mean_squared_error(y_test, y_pred_test)))
        test_mae = float(mean_absolute_error(y_test, y_pred_test))
        test_r2 = float(r2_score(y_test, y_pred_test))

        mlflow.log_metric("test_rmse", test_rmse)
        mlflow.log_metric("test_mae", test_mae)
        mlflow.log_metric("test_r2", test_r2)

        # --- Feature importance (gráfico) ---
        _log_feature_importance(modelo, features)

        # --- Firma del modelo (input/output schema) ---
        signature = infer_signature(X_train, y_pred_train)

        # --- Registro del modelo ---
        mlflow.sklearn.log_model(
            modelo,
            name="model",
            signature=signature,
            input_example=X_train.head(3),
            registered_model_name=MLFLOW_EXPERIMENT_NAME,
        )

        logger.info(
            "Run %s — train RMSE=%.4f / test RMSE=%.4f  R²=%.4f",
            run.info.run_id,
            train_rmse,
            test_rmse,
            test_r2,
        )

        return {
            "run_id": run.info.run_id,
            "rmse": test_rmse,
            "mae": test_mae,
            "r2": test_r2,
        }


def _log_feature_importance(modelo, features):
    """Genera y registra un gráfico de importancia de features como artefacto."""
    import os
    import tempfile

    import mlflow
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import numpy as np

    importances = modelo.feature_importances_
    idx = np.argsort(importances)

    fig, ax = plt.subplots(figsize=(8, max(3, len(features) * 0.5)))
    ax.barh(range(len(features)), importances[idx])
    ax.set_yticks(range(len(features)))
    ax.set_yticklabels([features[i] for i in idx])
    ax.set_xlabel("Importancia")
    ax.set_title("Feature Importance — RandomForest")
    fig.tight_layout()

    tmp_path = os.path.join(tempfile.gettempdir(), "feature_importance.png")
    fig.savefig(tmp_path, dpi=100)
    plt.close(fig)

    mlflow.log_artifact(tmp_path, artifact_path="plots")


def promote_best_model(results):
    """Promueve el modelo con menor RMSE (test) asignándole el alias ``production``.

    Args:
        results: lista de dicts retornados por ``train_and_log``.

    Returns:
        dict del mejor resultado con la version promovida.
    """
    import mlflow

    from ml_pipeline.config import MLFLOW_EXPERIMENT_NAME

    best = min(results, key=lambda r: r["rmse"])
    client = mlflow.MlflowClient()

    # Buscamos la versión del modelo que corresponde al mejor run
    model_name = MLFLOW_EXPERIMENT_NAME
    versions = client.search_model_versions(f"name='{model_name}'")
    matching = [v for v in versions if v.run_id == best["run_id"]]

    if matching:
        version = matching[0].version
        client.set_registered_model_alias(model_name, "production", version)
        logger.info(
            "Modelo v%s (run %s, RMSE=%.4f) promovido como 'production'",
            version,
            best["run_id"],
            best["rmse"],
        )
        best["version"] = version
    else:
        logger.warning(
            "No se encontró versión registrada para run %s", best["run_id"]
        )

    return best
