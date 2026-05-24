from datetime import date

import pandas as pd
import requests as http_client
from fastapi import APIRouter, Depends, HTTPException, Query, status

from app.core.config import get_settings
from app.core.dependencies import get_repository
from app.core.model import MODEL_FEATURES
from app.repositories.base import WellRepository
from app.schemas import ForecastPoint, ForecastResponse

router = APIRouter(tags=["forecast"])


@router.get("/forecast", response_model=ForecastResponse)
def get_forecast(
    id_well: str = Query(..., min_length=1, description="ID del pozo", json_schema_extra={"example": "96688"}),
    date_start: date = Query(..., description="Fecha inicio del rango", json_schema_extra={"example": "2023-01-01"}),
    date_end: date = Query(..., description="Fecha fin del rango", json_schema_extra={"example": "2023-12-31"}),
    repository: WellRepository = Depends(get_repository),
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

    # Inferencia via Ray Serve (inferencia distribuida)
    settings = get_settings()
    payload = features_df.to_dict(orient="records")
    try:
        resp = http_client.post(
            f"{settings.ray_serve_url}/predict",
            json=payload,
            timeout=10,
        )
        resp.raise_for_status()
        predictions = resp.json()
    except http_client.RequestException as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Servicio de inferencia no disponible",
        ) from exc

    points = [
        ForecastPoint(date=row["date"], prod=float(pred))
        for row, pred in zip(feature_rows, predictions)
    ]
    return ForecastResponse(id_well=id_well, data=points)
