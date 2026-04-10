"""Loader del modelo productivo de MLflow para el endpoint de forecast.

Carga `models:/<name>@<alias>` una única vez y cachea el PyFuncModel
resultante. El endpoint depende de `get_model()` vía FastAPI Depends
para que los tests puedan reemplazarlo por un fake. mlflow se importa
de forma diferida para que los tests unitarios que overridan
`get_model` no necesiten mlflow instalado en el entorno de test.
"""

from functools import lru_cache
from typing import Any

from app.core.config import get_settings

# El orden DEBE coincidir con el orden de features usado en el
# entrenamiento (airflow/plugins/ml_pipeline/config.py::ALL_FEATURES_GAS).
# El modelo actualmente registrado como `production` fue entrenado con
# exactamente este orden de columnas.
MODEL_FEATURES: list[str] = [
    "prod_pet",
    "prod_agua",
    "tef",
    "profundidad",
    "tipoextraccion",
]


@lru_cache(maxsize=1)
def _load_model() -> Any:
    import mlflow

    settings = get_settings()
    mlflow.set_tracking_uri(settings.mlflow_tracking_uri)
    model_uri = f"models:/{settings.mlflow_model_name}@{settings.mlflow_model_alias}"
    try:
        return mlflow.pyfunc.load_model(model_uri)
    except Exception as exc:
        raise RuntimeError(
            f"No se pudo cargar el modelo MLflow desde {model_uri}: {exc}"
        ) from exc


def get_model() -> Any:
    """Dependency de FastAPI que retorna el modelo productivo cacheado."""
    return _load_model()
