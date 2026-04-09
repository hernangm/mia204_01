from datetime import date

from pydantic import BaseModel


class WellItem(BaseModel):
    id_well: str


class ForecastPoint(BaseModel):
    date: date
    prod: float


class ForecastResponse(BaseModel):
    id_well: str
    data: list[ForecastPoint]
