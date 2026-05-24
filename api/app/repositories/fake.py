from datetime import date

from app.repositories.base import WellRepository


class FakeWellRepository(WellRepository):
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
        self._features = [
            {
                "id_well": "POZO-001",
                "date": date(2023, 10, 1),
                "prod_pet": 10.0,
                "prod_agua": 5.0,
                "tef": 30.0,
                "profundidad": 2500.0,
                "tipoextraccion": 8,
            },
            {
                "id_well": "POZO-001",
                "date": date(2023, 11, 1),
                "prod_pet": 11.0,
                "prod_agua": 5.5,
                "tef": 30.0,
                "profundidad": 2500.0,
                "tipoextraccion": 8,
            },
            {
                "id_well": "POZO-001",
                "date": date(2023, 12, 1),
                "prod_pet": 9.5,
                "prod_agua": 4.0,
                "tef": 31.0,
                "profundidad": 2500.0,
                "tipoextraccion": 8,
            },
            {
                "id_well": "POZO-002",
                "date": date(2023, 10, 1),
                "prod_pet": 7.0,
                "prod_agua": 3.0,
                "tef": 28.0,
                "profundidad": 2200.0,
                "tipoextraccion": 10,
            },
        ]

    def list_wells(self, date_query: date, limit: int = 100, offset: int = 0) -> list[str]:
        wells = [well["id_well"] for well in self._wells if well["date_data"] <= date_query]
        wells = sorted(dict.fromkeys(wells))
        return wells[offset:offset + limit]

    def get_forecast(self, id_well: str, date_start: date, date_end: date) -> list[dict[str, object]]:
        return [
            {"date": point["date"], "prod": point["prod"]}
            for point in self._production
            if point["id_well"] == id_well and date_start <= point["date"] <= date_end
        ]

    def get_features(
        self, id_well: str, date_start: date, date_end: date
    ) -> list[dict[str, object]]:
        return [
            {
                "date": row["date"],
                "prod_pet": row["prod_pet"],
                "prod_agua": row["prod_agua"],
                "tef": row["tef"],
                "profundidad": row["profundidad"],
                "tipoextraccion": row["tipoextraccion"],
            }
            for row in self._features
            if row["id_well"] == id_well and date_start <= row["date"] <= date_end
        ]
