from datetime import date

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Query, status

from app.core.dependencies import get_model, get_repository
from app.core.model import MODEL_FEATURES
from app.repositories.base import WellRepository
from app.schemas import ForecastPoint, ForecastResponse

router = APIRouter(tags=["forecast"])


@router.get("/forecast", response_model=ForecastResponse)
def get_forecast(
    id_well: str = Query(..., min_length=1, description="ID del pozo"),
    date_start: date = Query(..., description="Fecha inicio del rango"),
    date_end: date = Query(..., description="Fecha fin del rango"),
    repository: WellRepository = Depends(get_repository),
    model=Depends(get_model),
) -> ForecastResponse:
    if date_start > date_end:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="date_start must be less than or equal to date_end",
        )

    try:
        feature_rows = repository.get_features(id_well, date_start, date_end)
    except RuntimeError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Forecast backend is temporarily unavailable",
        ) from exc

    if not feature_rows:
        return ForecastResponse(id_well=id_well, data=[])

    features_df = pd.DataFrame(feature_rows)[MODEL_FEATURES]

    try:
        predictions = model.predict(features_df)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Forecast model is temporarily unavailable",
        ) from exc

    points = [
        ForecastPoint(date=row["date"], prod=float(pred))
        for row, pred in zip(feature_rows, predictions)
    ]
    return ForecastResponse(id_well=id_well, data=points)
