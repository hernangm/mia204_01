def test_forecast_returns_well_id(client):
    response = client.get(
        "/api/v1/forecast",
        params={"id_well": "POZO-001", "date_start": "2023-10-01", "date_end": "2023-12-31"},
    )

    assert response.status_code == 200
    assert response.json()["id_well"] == "POZO-001"


def test_forecast_returns_data_array(client):
    response = client.get(
        "/api/v1/forecast",
        params={"id_well": "POZO-001", "date_start": "2023-10-01", "date_end": "2023-12-31"},
    )

    assert response.status_code == 200
    assert isinstance(response.json()["data"], list)


def test_forecast_data_points_have_correct_fields(client):
    response = client.get(
        "/api/v1/forecast",
        params={"id_well": "POZO-001", "date_start": "2023-10-01", "date_end": "2023-12-31"},
    )

    assert response.status_code == 200
    data_point = response.json()["data"][0]
    assert set(data_point) == {"date", "prod"}


def test_forecast_filters_by_date_range(client):
    # FakeModel devuelve prod_pet * 10; la fila fake de features para
    # POZO-001 en 2023-11-01 tiene prod_pet=11.0, por lo tanto la
    # predicción esperada es 110.0.
    response = client.get(
        "/api/v1/forecast",
        params={"id_well": "POZO-001", "date_start": "2023-11-01", "date_end": "2023-11-30"},
    )

    assert response.status_code == 200
    assert response.json()["data"] == [{"date": "2023-11-01", "prod": 110.0}]


def test_forecast_empty_range_returns_empty_data(client):
    response = client.get(
        "/api/v1/forecast",
        params={"id_well": "POZO-001", "date_start": "2025-01-01", "date_end": "2025-12-31"},
    )

    assert response.status_code == 200
    assert response.json()["data"] == []


def test_forecast_missing_id_well_returns_422(client):
    response = client.get(
        "/api/v1/forecast",
        params={"date_start": "2023-10-01", "date_end": "2023-12-31"},
    )

    assert response.status_code == 422


def test_forecast_missing_params_returns_422(client):
    response = client.get("/api/v1/forecast", params={"id_well": "POZO-001"})

    assert response.status_code == 422


def test_forecast_invalid_date_returns_422(client):
    response = client.get(
        "/api/v1/forecast",
        params={"id_well": "POZO-001", "date_start": "bad-date", "date_end": "2023-12-31"},
    )

    assert response.status_code == 422


def test_forecast_date_range_invalid_returns_422(client):
    response = client.get(
        "/api/v1/forecast",
        params={"id_well": "POZO-001", "date_start": "2023-12-31", "date_end": "2023-10-01"},
    )

    assert response.status_code == 422
    assert response.json()["detail"] == "date_start must be less than or equal to date_end"


def test_forecast_repository_failure_returns_503(client, fake_repository):
    def failing_get_features(*args, **kwargs):
        raise RuntimeError("db unavailable")

    fake_repository.get_features = failing_get_features

    response = client.get(
        "/api/v1/forecast",
        params={"id_well": "POZO-001", "date_start": "2023-10-01", "date_end": "2023-12-31"},
    )

    assert response.status_code == 503
    assert response.json()["detail"] == "Forecast backend is temporarily unavailable"


def test_forecast_model_failure_returns_503(client, fake_model):
    def failing_predict(*args, **kwargs):
        raise RuntimeError("model unavailable")

    fake_model.predict = failing_predict

    response = client.get(
        "/api/v1/forecast",
        params={"id_well": "POZO-001", "date_start": "2023-10-01", "date_end": "2023-12-31"},
    )

    assert response.status_code == 503
    assert response.json()["detail"] == "Forecast model is temporarily unavailable"
