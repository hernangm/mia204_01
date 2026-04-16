"""Endpoints de la API: /api/v1/wells y /api/v1/forecast.

Ambas rutas se registran en el mismo router para mantener todo en un solo lugar.
El prefijo /api/v1 se agrega en main.py al montar el router.
"""

from datetime import date
from functools import lru_cache

from fastapi import APIRouter, Depends, HTTPException, Query, status

from app.config import Settings
from app.repository import PostgresWellRepository, WellRepository
from app.schemas import ForecastPoint, ForecastResponse, WellItem

router = APIRouter()


# El repositorio se cachea para que el TTL-cache interno de list_wells
# (y el pool del engine SQLAlchemy) sobrevivan entre requests.
@lru_cache(maxsize=1)
def get_repository() -> WellRepository:
    """Dependency injection: devuelve el repositorio de producción configurado."""
    settings = Settings()
    return PostgresWellRepository(
        database_url=settings.database_url,
        forecast_measure=settings.forecast_measure,
        wells_cache_ttl_seconds=settings.wells_cache_ttl_seconds,
    )


@router.get("/wells", response_model=list[WellItem], tags=["wells"])
def list_wells(
    date_query: date = Query(
        ...,
        description="Fecha de consulta (YYYY-MM-DD)",
        json_schema_extra={"example": "2023-01-01"},
    ),
    limit: int = Query(100, ge=1, le=1000, description="Cantidad máxima de resultados"),
    offset: int = Query(0, ge=0, description="Resultados a omitir (paginación)"),
    repo: WellRepository = Depends(get_repository),
) -> list[WellItem]:
    """Lista todos los pozos disponibles para una fecha de consulta."""
    try:
        wells = repo.list_wells(date_query, limit=limit, offset=offset)
    except RuntimeError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Wells backend is temporarily unavailable",
        ) from exc
    return [WellItem(id_well=w) for w in wells]


@router.get("/forecast", response_model=ForecastResponse, tags=["forecast"])
def get_forecast(
    id_well: str = Query(
        ...,
        min_length=1,
        description="ID del pozo",
        json_schema_extra={"example": "96688"},
    ),
    date_start: date = Query(
        ...,
        description="Fecha inicio del rango (YYYY-MM-DD)",
        json_schema_extra={"example": "2023-01-01"},
    ),
    date_end: date = Query(
        ...,
        description="Fecha fin del rango (YYYY-MM-DD)",
        json_schema_extra={"example": "2023-12-31"},
    ),
    repo: WellRepository = Depends(get_repository),
) -> ForecastResponse:
    """Devuelve la serie de producción histórica para un pozo en un rango de fechas."""
    if date_start > date_end:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="date_start must be less than or equal to date_end",
        )
    try:
        points = repo.get_forecast(id_well, date_start, date_end)
    except RuntimeError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Forecast backend is temporarily unavailable",
        ) from exc
    return ForecastResponse(
        id_well=id_well,
        data=[ForecastPoint(**p) for p in points],
    )
