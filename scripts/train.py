"""Script de entrenamiento standalone.

Entrena modelos de pronóstico de producción de hidrocarburos usando los
features ya persistidos en el Feature Store (tabla `features` en Postgres).

Uso:
    python scripts/train.py --date "2023-10-01"

El parámetro --date define la fecha de corte: solo se usan registros con
fecha <= date. Esto permite reproducir exactamente el modelo que se habría
entrenado en cualquier momento histórico.

Requisitos previos:
    - Stack levantado con: docker-compose up
    - Pipeline de ingesta y feature engineering ejecutado al menos una vez
      (via el DAG `ml_pipeline` en Airflow, o corriendo los pasos manualmente)
    - Variable de entorno MLFLOW_TRACKING_URI y FEATURESTORE_DATABASE_URL
      configuradas (o usar los valores por defecto de ml_pipeline.db)
"""

import argparse
import logging
import os
import sys
from datetime import date
from pathlib import Path

import mlflow
import mlflow.sklearn
import pandas as pd
from mlflow import MlflowClient
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split

# Agrega airflow/plugins al path para poder importar ml_pipeline.
# La ruta difiere según el entorno:
#   - Host:      <repo>/airflow/plugins
#   - Container: /opt/airflow/plugins  (bind mount; parent del script ya es /opt/airflow)
_REPO_ROOT = Path(__file__).resolve().parent.parent
for _plugins_candidate in (
    _REPO_ROOT / "airflow" / "plugins",  # ejecución desde el host
    _REPO_ROOT / "plugins",              # ejecución dentro del container
):
    if _plugins_candidate.exists():
        sys.path.insert(0, str(_plugins_candidate))
        break

from ml_pipeline.config import EXPERIMENTS, MLFLOW_EXPERIMENT_NAME  # noqa: E402
from ml_pipeline.db import get_engine  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
log = logging.getLogger(__name__)

REGISTERED_MODEL_NAME = "hydrocarbon_forecast"
PRODUCTION_ALIAS = "production"
TEST_SIZE = 0.2
RANDOM_STATE = 204


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    """Parsea los argumentos de línea de comando."""
    parser = argparse.ArgumentParser(
        description="Entrena y registra modelos de pronóstico de producción."
    )
    parser.add_argument(
        "--date",
        required=True,
        metavar="YYYY-MM-DD",
        help="Fecha de corte para features. Solo se usan registros con fecha <= date.",
    )
    return parser.parse_args()


def parse_date(date_str: str) -> date:
    """
    Convierte una string YYYY-MM-DD a un objeto date.

    Args:
        date_str: Fecha en formato YYYY-MM-DD.

    Returns:
        Objeto date correspondiente.

    Raises:
        SystemExit: Si el formato es inválido.
    """
    try:
        return date.fromisoformat(date_str)
    except ValueError:
        log.error("Formato de fecha inválido: '%s'. Use YYYY-MM-DD.", date_str)
        sys.exit(1)


# ---------------------------------------------------------------------------
# Feature loading
# ---------------------------------------------------------------------------

def load_features(cutoff: date) -> pd.DataFrame:
    """
    Carga features del Feature Store filtrando por fecha de corte.

    Args:
        cutoff: Solo se incluyen registros con fecha <= cutoff.

    Returns:
        DataFrame con columnas de features listas para entrenar.

    Raises:
        RuntimeError: Si no se pueden leer los datos del Feature Store.
    """
    engine = get_engine()
    query = (
        "SELECT id_pozo, fecha, tipoextraccion, prod_gas, prod_agua, "
        "tef, prod_pet, profundidad "
        "FROM features "
        "WHERE fecha <= %(cutoff)s"
    )
    try:
        df = pd.read_sql(query, engine, params={"cutoff": cutoff})
    except Exception as exc:
        raise RuntimeError(f"Error al leer el Feature Store: {exc}") from exc

    log.info("Features cargados: %d filas (fecha <= %s)", len(df), cutoff)
    return df


# ---------------------------------------------------------------------------
# Training
# ---------------------------------------------------------------------------

def _setup_mlflow() -> None:
    """Configura el tracking URI de MLflow desde la variable de entorno."""
    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI")
    if not tracking_uri:
        raise RuntimeError(
            "MLFLOW_TRACKING_URI no está configurado. "
            "Asegurate de que el archivo .env esté cargado o de exportar la variable."
        )
    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(MLFLOW_EXPERIMENT_NAME)
    log.info("MLflow tracking URI: %s — experimento: %s", tracking_uri, MLFLOW_EXPERIMENT_NAME)


def train_experiment(experiment: dict, df: pd.DataFrame, training_date: date) -> dict:
    """
    Entrena un experimento, loguea métricas y registra el modelo en MLflow.

    Args:
        experiment: Configuración del experimento (de ml_pipeline.config.EXPERIMENTS).
        df: DataFrame de features ya filtrado por fecha de corte.
        training_date: Fecha de corte usada para el entrenamiento (se loguea como parámetro).

    Returns:
        Diccionario con run_id, version y rmse del experimento.
    """
    target = experiment["target"]
    features = experiment["features"]
    params = experiment["model_params"]

    X = df[features]
    y = df[target]
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=TEST_SIZE, random_state=RANDOM_STATE
    )

    with mlflow.start_run() as run:
        # Loguear parámetros del experimento
        mlflow.log_param("training_date", str(training_date))
        mlflow.log_param("model_type", experiment["model_type"])
        mlflow.log_param("target", target)
        mlflow.log_param("features", ",".join(features))
        for key, value in params.items():
            mlflow.log_param(key, value)

        # Entrenar modelo
        model = RandomForestRegressor(**params)
        model.fit(X_train, y_train)
        preds = model.predict(X_test)

        # Calcular y loguear métricas
        rmse = float(mean_squared_error(y_test, preds) ** 0.5)
        r2 = float(r2_score(y_test, preds))
        mae = float(mean_absolute_error(y_test, preds))
        mlflow.log_metric("rmse", rmse)
        mlflow.log_metric("r2", r2)
        mlflow.log_metric("mae", mae)

        # Registrar modelo
        info = mlflow.sklearn.log_model(
            sk_model=model,
            name="model",
            registered_model_name=REGISTERED_MODEL_NAME,
        )
        version = info.registered_model_version
        if version is None:
            # Fallback: buscar la versión por run_id
            client = MlflowClient()
            matches = client.search_model_versions(f"run_id='{run.info.run_id}'")
            if not matches:
                raise RuntimeError(
                    f"No se registró ninguna versión para el run {run.info.run_id}"
                )
            version = matches[0].version

        log.info(
            "Experimento completado — target=%s n_estimators=%s rmse=%.4f r2=%.4f mae=%.4f version=%s",
            target,
            params.get("n_estimators"),
            rmse,
            r2,
            mae,
            version,
        )
        return {"run_id": run.info.run_id, "version": int(version), "rmse": rmse}


def promote_best(results: list[dict]) -> None:
    """
    Asigna el alias 'production' al modelo con menor RMSE.

    Args:
        results: Lista de resultados de train_experiment (run_id, version, rmse).
    """
    best = min(results, key=lambda r: r["rmse"])
    client = MlflowClient()
    client.set_registered_model_alias(
        name=REGISTERED_MODEL_NAME,
        alias=PRODUCTION_ALIAS,
        version=str(best["version"]),
    )
    log.info(
        "Modelo promovido: versión %s (run_id=%s, rmse=%.4f) → alias '%s'",
        best["version"],
        best["run_id"],
        best["rmse"],
        PRODUCTION_ALIAS,
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()
    cutoff = parse_date(args.date)

    log.info("=== Inicio de entrenamiento (fecha de corte: %s) ===", cutoff)

    _setup_mlflow()

    df = load_features(cutoff)
    if df.empty:
        log.error(
            "No hay features disponibles para fecha <= %s. "
            "Ejecutá primero el pipeline de ingesta y feature engineering.",
            cutoff,
        )
        sys.exit(1)

    results = []
    for experiment in EXPERIMENTS:
        result = train_experiment(experiment, df, cutoff)
        results.append(result)

    promote_best(results)

    log.info(
        "=== Entrenamiento finalizado: %d experimentos registrados en MLflow ===",
        len(results),
    )


if __name__ == "__main__":
    main()
