from datetime import date
from functools import lru_cache

from fastapi import Depends, FastAPI, Query

from app.config import Settings
from app.postgres_repository import PostgresWellRepository
from app.repository import WellRepository
from app.schemas import ForecastPoint, ForecastResponse, WellItem

app = FastAPI(
    title="Oil & Gas Forecast API",
    version="1.0.0",
    description="API simple para consultar el listado de pozos y sus pronósticos de producción.",
)


@lru_cache
def get_settings() -> Settings:
    return Settings()


def get_repository(settings: Settings = Depends(get_settings)) -> WellRepository:
    return PostgresWellRepository(settings.database_url)


@app.get("/api/v1/wells", response_model=list[WellItem])
def get_wells(
    date_query: date = Query(..., description="Fecha para la cual se hace la consulta (YYYY-MM-DD)."),
    repo: WellRepository = Depends(get_repository),
):
    rows = repo.get_wells(date_query)
    return [WellItem(**row) for row in rows]


@app.get("/api/v1/forecast", response_model=ForecastResponse)
def get_forecast(
    id_well: str = Query(..., description="Identificador del pozo."),
    date_start: date = Query(..., description="Fecha de inicio (YYYY-MM-DD)."),
    date_end: date = Query(..., description="Fecha de fin (YYYY-MM-DD)."),
    repo: WellRepository = Depends(get_repository),
):
    rows = repo.get_forecast(id_well, date_start, date_end)
    return ForecastResponse(
        id_well=id_well,
        data=[ForecastPoint(**row) for row in rows],
    )
