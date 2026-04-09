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

    def list_wells(self, date_query: date) -> list[str]:
        wells = [well["id_well"] for well in self._wells if well["date_data"] <= date_query]
        return sorted(dict.fromkeys(wells))

    def get_forecast(self, id_well: str, date_start: date, date_end: date) -> list[dict[str, object]]:
        return [
            {"date": point["date"], "prod": point["prod"]}
            for point in self._production
            if point["id_well"] == id_well and date_start <= point["date"] <= date_end
        ]
