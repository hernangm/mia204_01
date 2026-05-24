from datetime import date as Date
from typing import Annotated

from pydantic import BaseModel, Field


class WellItem(BaseModel):
    id_well: str


class ForecastPoint(BaseModel):
    date: Annotated[
        Date,
        Field(
            description=(
                "Primer día del mes cuyas features se usaron como input del modelo (YYYY-MM-01). "
                "El modelo fue entrenado con un shift temporal de 1 mes: las features del mes T "
                "predicen la producción de gas del mes T+1."
            ),
        ),
    ]
    prod: Annotated[
        float,
        Field(description="Producción mensual de gas predicha para el mes siguiente al indicado en 'date' (miles de m³)."),
    ]


class ForecastResponse(BaseModel):
    id_well: str
    data: Annotated[
        list[ForecastPoint],
        Field(description="Serie temporal mensual ordenada cronológicamente. Un punto por mes de features disponibles."),
    ]
