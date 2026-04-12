"""Servidor de inferencia distribuida con Ray Serve.

Carga el modelo registrado en MLflow con alias 'production' al iniciar
y expone un endpoint POST /predict para inferencia batch.

Este modulo se ejecuta como proceso principal del contenedor ray-serve.
"""

import logging
import os
import time

import mlflow
import pandas as pd
import ray
from fastapi import FastAPI
from ray import serve

logger = logging.getLogger(__name__)

# Aplicacion FastAPI interna de Ray Serve (no confundir con la API principal).
# Solo expone /predict — no tiene documentacion Swagger ni logica de negocio.
_app = FastAPI(title="Ray Serve Inference", docs_url=None)

# Orden de features (debe coincidir con entrenamiento).
MODEL_FEATURES = ["prod_pet", "prod_agua", "tef", "profundidad", "tipoextraccion"]


@serve.deployment(
    num_replicas=2,
    ray_actor_options={"num_cpus": 0.5},
)
@serve.ingress(_app)
class HydrocarbonForecast:
    """Deployment de Ray Serve para el modelo de pronostico de hidrocarburos.

    Al instanciarse (una vez por replica), carga el modelo de MLflow.
    El metodo predict maneja las solicitudes de inferencia de forma concurrente.
    """

    def __init__(self):
        """Inicializa la replica cargando el modelo productivo desde MLflow."""
        tracking_uri = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:9090")
        mlflow.set_tracking_uri(tracking_uri)
        model_uri = "models:/hydrocarbon_forecast@production"
        logger.info("Replica iniciando — cargando modelo desde %s", model_uri)
        self._model = mlflow.pyfunc.load_model(model_uri)
        logger.info("Modelo cargado exitosamente en replica")

    @_app.post("/predict")
    def predict(self, payload: list[dict]) -> list[float]:
        """Realiza inferencia sobre un batch de filas de features.

        Args:
            payload: lista de dicts, cada uno con las features del modelo.
                     Ejemplo: [{"prod_pet": 10.0, "prod_agua": 5.0, ...}]

        Returns:
            Lista de floats con las predicciones de prod_gas.
        """
        df = pd.DataFrame(payload)[MODEL_FEATURES]
        predictions = self._model.predict(df)
        return [float(p) for p in predictions]


if __name__ == "__main__":
    ray.init(dashboard_host="0.0.0.0", dashboard_port=8265)
    serve.start(http_options={"host": "0.0.0.0", "port": 8001})
    serve.run(HydrocarbonForecast.bind())
    logger.info("Ray Serve iniciado en puerto 8001, dashboard en 8265")

    # Mantener el proceso vivo
    while True:
        time.sleep(60)
