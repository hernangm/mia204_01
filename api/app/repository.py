"""Acceso a datos para pozos y producción.

Contiene:
  - WellRepository: interfaz abstracta (permite inyección de dependencias en tests)
  - PostgresWellRepository: implementación real contra la base featurestore
  - FakeWellRepository: implementación en memoria para tests unitarios
"""

import threading
import time
from abc import ABC, abstractmethod
from datetime import date

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError


class WellRepository(ABC):
    @abstractmethod
    def list_wells(self, date_query: date, limit: int = 100, offset: int = 0) -> list[str]:
        raise NotImplementedError

    @abstractmethod
    def get_forecast(self, id_well: str, date_start: date, date_end: date) -> list[dict[str, object]]:
        raise NotImplementedError


class PostgresWellRepository(WellRepository):
    """Lee pozos y producción desde PostgreSQL (base featurestore)."""

    ALLOWED_MEASURES = {"prod_gas", "prod_pet", "prod_agua", "tef"}

    def __init__(
        self,
        database_url: str,
        forecast_measure: str = "prod_gas",
        wells_cache_ttl_seconds: int = 300,
    ) -> None:
        if forecast_measure not in self.ALLOWED_MEASURES:
            raise ValueError(f"Unsupported forecast measure: {forecast_measure}")
        self._engine = create_engine(database_url, pool_pre_ping=True)
        self._measure = forecast_measure
        self._wells_cache_ttl = wells_cache_ttl_seconds
        self._wells_cache: dict[tuple[date, int, int], tuple[float, list[str]]] = {}
        self._wells_cache_lock = threading.Lock()

    def list_wells(self, date_query: date, limit: int = 100, offset: int = 0) -> list[str]:
        cache_key = (date_query, limit, offset)
        if self._wells_cache_ttl > 0:
            with self._wells_cache_lock:
                hit = self._wells_cache.get(cache_key)
                if hit is not None and time.monotonic() - hit[0] < self._wells_cache_ttl:
                    return hit[1]
        try:
            with self._engine.connect() as conn:
                rows = conn.execute(
                    text(
                        """
                        SELECT DISTINCT idpozo
                        FROM wells
                        WHERE fecha_data IS NULL OR fecha_data <= :date_query
                        ORDER BY idpozo
                        LIMIT :limit OFFSET :offset
                        """
                    ),
                    {"date_query": date_query, "limit": limit, "offset": offset},
                )
                wells = [r.idpozo for r in rows]
        except SQLAlchemyError as exc:
            raise RuntimeError("Unable to fetch wells from PostgreSQL") from exc
        if self._wells_cache_ttl > 0:
            with self._wells_cache_lock:
                self._wells_cache[cache_key] = (time.monotonic(), wells)
        return wells

    def get_forecast(self, id_well: str, date_start: date, date_end: date) -> list[dict[str, object]]:
        try:
            with self._engine.connect() as conn:
                rows = conn.execute(
                    text(
                        f"""
                        SELECT fecha AS date, {self._measure} AS prod
                        FROM production
                        WHERE idpozo = :id_well
                          AND fecha BETWEEN :date_start AND :date_end
                        ORDER BY fecha
                        """
                    ),
                    {"id_well": id_well, "date_start": date_start, "date_end": date_end},
                )
                return [{"date": r.date, "prod": r.prod} for r in rows]
        except SQLAlchemyError as exc:
            raise RuntimeError("Unable to fetch forecast data from PostgreSQL") from exc


class FakeWellRepository(WellRepository):
    """Repositorio en memoria para tests unitarios."""

    def __init__(self) -> None:
        self._wells = [
            {"id_well": "POZO-001", "date_data": date(2023, 9, 1)},
            {"id_well": "POZO-002", "date_data": date(2023, 10, 1)},
            {"id_well": "POZO-003", "date_data": date(2024, 1, 1)},
        ]
        self._production = [
            {"id_well": "POZO-001", "date": date(2023, 10, 1), "prod": 150.5},
            {"id_well": "POZO-001", "date": date(2023, 11, 1), "prod": 152.0},
            {"id_well": "POZO-001", "date": date(2023, 12, 1), "prod": 149.75},
            {"id_well": "POZO-002", "date": date(2023, 10, 1), "prod": 98.0},
            {"id_well": "POZO-002", "date": date(2023, 11, 1), "prod": 101.25},
        ]

    def list_wells(self, date_query: date, limit: int = 100, offset: int = 0) -> list[str]:
        wells = [w["id_well"] for w in self._wells if w["date_data"] <= date_query]
        unique = sorted(dict.fromkeys(wells))
        return unique[offset : offset + limit]

    def get_forecast(self, id_well: str, date_start: date, date_end: date) -> list[dict[str, object]]:
        return [
            {"date": p["date"], "prod": p["prod"]}
            for p in self._production
            if p["id_well"] == id_well and date_start <= p["date"] <= date_end
        ]
