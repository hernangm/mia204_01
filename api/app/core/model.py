"""Loader del modelo productivo de MLflow para el endpoint de forecast.

Carga ``models:/<name>@<alias>`` al iniciar la aplicacion y cachea el
resultado. El endpoint depende de ``get_model()`` via FastAPI Depends
para que los tests puedan reemplazarlo por un fake.
"""

import logging
from typing import Any

from app.core.config import get_settings

logger = logging.getLogger(__name__)

# El orden DEBE coincidir con el orden de features usado en el
# entrenamiento (airflow/plugins/ml_pipeline/config.py::ALL_FEATURES_GAS).
MODEL_FEATURES: list[str] = [
    "prod_pet",
    "prod_agua",
    "tef",
    "profundidad",
    "tipoextraccion",
]

_cached_model: Any = None


def load_model_at_startup() -> None:
    """Carga el modelo de MLflow al iniciar la app (llamado desde lifespan)."""
    global _cached_model
    try:
        import mlflow
    except ImportError:
        logger.info("mlflow no instalado, modelo no cargado (modo test)")
        return

    settings = get_settings()
    mlflow.set_tracking_uri(settings.mlflow_tracking_uri)
    model_uri = f"models:/{settings.mlflow_model_name}@{settings.mlflow_model_alias}"
    logger.info("Cargando modelo desde %s ...", model_uri)
    try:
        _cached_model = mlflow.pyfunc.load_model(model_uri)
        logger.info("Modelo cargado exitosamente")
    except Exception as exc:
        logger.warning("No se pudo cargar el modelo al inicio: %s", exc)


def get_model() -> Any:
    """Dependency de FastAPI que retorna el modelo productivo cacheado."""
    if _cached_model is None:
        raise RuntimeError(
            "Modelo no disponible. Verifique que exista un modelo "
            "registrado con alias 'production' en MLflow."
        )
    return _cached_model
