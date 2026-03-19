from datetime import date

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from app.repository import WellRepository


class PostgresWellRepository(WellRepository):
    def __init__(self, database_url: str):
        self._engine: Engine = create_engine(database_url)

    def get_wells(self, date_query: date) -> list[dict]:
        query = text(
            "SELECT DISTINCT idpozo FROM production WHERE fecha = :fecha ORDER BY idpozo"
        )
        with self._engine.connect() as conn:
            rows = conn.execute(query, {"fecha": date_query}).fetchall()
        return [{"id_well": row[0]} for row in rows]

    def get_forecast(
        self, id_well: str, date_start: date, date_end: date
    ) -> list[dict]:
        query = text(
            "SELECT fecha, prod_pet FROM production "
            "WHERE idpozo = :idpozo AND fecha BETWEEN :date_start AND :date_end "
            "ORDER BY fecha"
        )
        with self._engine.connect() as conn:
            rows = conn.execute(
                query,
                {"idpozo": id_well, "date_start": date_start, "date_end": date_end},
            ).fetchall()
        return [{"date": row[0], "prod": row[1]} for row in rows]
