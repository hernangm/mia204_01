from fastapi import FastAPI

app = FastAPI(
    title="Forecast de Producción de Hidrocarburos",
    description="API REST para predicción de producción de pozos de gas y petróleo.",
    version="0.1.0",
)


@app.get("/health", tags=["health"])
def health():
    """Endpoint de salud para el healthcheck del contenedor."""
    return {"status": "ok"}


@app.get("/api/v1/wells", tags=["forecast"])
def get_wells(date_query: str):
    """
    Devuelve los pozos disponibles en el Feature Store para una fecha dada.

    Args:
        date_query: Fecha de consulta en formato YYYY-MM-DD.
    """
    # TODO: consultar Feature Store
    return []


@app.get("/api/v1/forecast", tags=["forecast"])
def get_forecast(id_well: str, date_start: str, date_end: str):
    """
    Devuelve predicciones de producción para un pozo en un rango de fechas.

    Args:
        id_well: Identificador del pozo.
        date_start: Fecha inicio (YYYY-MM-DD).
        date_end: Fecha fin (YYYY-MM-DD).
    """
    # TODO: cargar modelo desde MLflow y predecir
    return {"id_well": id_well, "data": []}
