from datetime import date

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from app.repositories.base import WellRepository


class PostgresWellRepository(WellRepository):
    def __init__(self, database_url: str, forecast_measure: str = "prod_gas") -> None:
        allowed_measures = {"prod_gas", "prod_pet", "prod_agua", "tef"}
        if forecast_measure not in allowed_measures:
            raise ValueError(f"Unsupported forecast measure: {forecast_measure}")

        self._engine = create_engine(database_url, pool_pre_ping=True)
        self._forecast_measure = forecast_measure

    def list_wells(self, date_query: date, limit: int = 100, offset: int = 0) -> list[str]:
        try:
            with self._engine.connect() as connection:
                result = connection.execute(
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
                return [row.idpozo for row in result]
        except SQLAlchemyError as exc:
            raise RuntimeError("Unable to fetch wells from PostgreSQL") from exc

    def get_forecast(self, id_well: str, date_start: date, date_end: date) -> list[dict[str, object]]:
        try:
            with self._engine.connect() as connection:
                result = connection.execute(
                    text(
                        f"""
                        SELECT fecha AS date, {self._forecast_measure} AS prod
                        FROM production
                        WHERE idpozo = :id_well
                          AND fecha BETWEEN :date_start AND :date_end
                        ORDER BY fecha
                        """
                    ),
                    {"id_well": id_well, "date_start": date_start, "date_end": date_end},
                )
                return [{"date": row.date, "prod": row.prod} for row in result]
        except SQLAlchemyError as exc:
            raise RuntimeError("Unable to fetch forecast data from PostgreSQL") from exc

    def get_features(
        self, id_well: str, date_start: date, date_end: date
    ) -> list[dict[str, object]]:
        try:
            with self._engine.connect() as connection:
                result = connection.execute(
                    text(
                        """
                        SELECT fecha AS date,
                               prod_pet,
                               prod_agua,
                               tef,
                               profundidad,
                               tipoextraccion
                        FROM features
                        WHERE id_pozo = :id_well
                          AND fecha BETWEEN :date_start AND :date_end
                        ORDER BY fecha
                        """
                    ),
                    {"id_well": id_well, "date_start": date_start, "date_end": date_end},
                )
                return [
                    {
                        "date": row.date,
                        "prod_pet": row.prod_pet,
                        "prod_agua": row.prod_agua,
                        "tef": row.tef,
                        "profundidad": row.profundidad,
                        "tipoextraccion": row.tipoextraccion,
                    }
                    for row in result
                ]
        except SQLAlchemyError as exc:
            raise RuntimeError("Unable to fetch feature data from PostgreSQL") from exc
