"""Tests for GET /api/v1/forecast."""


def test_forecast_returns_well_id(client):
    resp = client.get(
        "/api/v1/forecast",
        params={"id_well": "POZO-001", "date_start": "2024-01-01", "date_end": "2024-12-31"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["id_well"] == "POZO-001"


def test_forecast_returns_data_array(client):
    resp = client.get(
        "/api/v1/forecast",
        params={"id_well": "POZO-001", "date_start": "2024-01-01", "date_end": "2024-12-31"},
    )
    data = resp.json()
    assert isinstance(data["data"], list)
    assert len(data["data"]) == 3


def test_forecast_data_points_have_correct_fields(client):
    resp = client.get(
        "/api/v1/forecast",
        params={"id_well": "POZO-001", "date_start": "2024-01-01", "date_end": "2024-12-31"},
    )
    for point in resp.json()["data"]:
        assert "date" in point
        assert "prod" in point
        assert isinstance(point["prod"], float)


def test_forecast_filters_by_date_range(client):
    resp = client.get(
        "/api/v1/forecast",
        params={"id_well": "POZO-001", "date_start": "2024-01-01", "date_end": "2024-01-31"},
    )
    data = resp.json()["data"]
    assert len(data) == 1
    assert data[0]["date"] == "2024-01-01"


def test_forecast_empty_range_returns_empty_data(client):
    resp = client.get(
        "/api/v1/forecast",
        params={"id_well": "POZO-001", "date_start": "2025-01-01", "date_end": "2025-12-31"},
    )
    assert resp.status_code == 200
    assert resp.json()["data"] == []


def test_forecast_missing_params_returns_422(client):
    resp = client.get("/api/v1/forecast")
    assert resp.status_code == 422


def test_forecast_missing_id_well_returns_422(client):
    resp = client.get(
        "/api/v1/forecast",
        params={"date_start": "2024-01-01", "date_end": "2024-12-31"},
    )
    assert resp.status_code == 422


def test_forecast_invalid_date_returns_422(client):
    resp = client.get(
        "/api/v1/forecast",
        params={"id_well": "POZO-001", "date_start": "bad", "date_end": "2024-12-31"},
    )
    assert resp.status_code == 422
