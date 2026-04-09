from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query, status

from app.core.dependencies import get_repository
from app.repositories.base import WellRepository
from app.schemas import ForecastPoint, ForecastResponse

router = APIRouter(tags=["forecast"])


@router.get("/forecast", response_model=ForecastResponse)
def get_forecast(
    id_well: str = Query(..., min_length=1, description="ID del pozo"),
    date_start: date = Query(..., description="Fecha inicio del rango"),
    date_end: date = Query(..., description="Fecha fin del rango"),
    repository: WellRepository = Depends(get_repository),
) -> ForecastResponse:
    if date_start > date_end:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="date_start must be less than or equal to date_end",
        )

    try:
        points = repository.get_forecast(id_well, date_start, date_end)
    except RuntimeError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Forecast backend is temporarily unavailable",
        ) from exc

    return ForecastResponse(
        id_well=id_well,
        data=[ForecastPoint(**point) for point in points],
    )

