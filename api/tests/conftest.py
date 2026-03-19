"""Shared fixtures for API tests."""

from datetime import date

import pytest
from fastapi.testclient import TestClient

from app.main import app, get_repository
from app.repository import WellRepository


class FakeWellRepository(WellRepository):
    """In-memory repository for testing — no database needed."""

    def __init__(self):
        self.wells_data = [
            {"id_well": "POZO-001"},
            {"id_well": "POZO-002"},
            {"id_well": "POZO-003"},
        ]
        self.forecast_data = [
            {"date": date(2024, 1, 1), "prod": 150.5},
            {"date": date(2024, 2, 1), "prod": 142.3},
            {"date": date(2024, 3, 1), "prod": 138.0},
        ]

    def get_wells(self, date_query: date) -> list[dict]:
        return self.wells_data

    def get_forecast(
        self, id_well: str, date_start: date, date_end: date
    ) -> list[dict]:
        return [
            row
            for row in self.forecast_data
            if date_start <= row["date"] <= date_end
        ]


@pytest.fixture()
def fake_repo():
    return FakeWellRepository()


@pytest.fixture()
def client(fake_repo):
    """TestClient with the repository dependency overridden."""
    app.dependency_overrides[get_repository] = lambda: fake_repo
    yield TestClient(app)
    app.dependency_overrides.clear()
