def test_wells_returns_list(client):
    response = client.get("/api/v1/wells", params={"date_query": "2023-10-15"})

    assert response.status_code == 200
    assert response.json() == [{"id_well": "POZO-001"}, {"id_well": "POZO-002"}]


def test_wells_returns_expected_ids(client):
    response = client.get("/api/v1/wells", params={"date_query": "2024-01-15"})

    assert response.status_code == 200
    payload = response.json()
    assert [item["id_well"] for item in payload] == ["POZO-001", "POZO-002", "POZO-003"]


def test_wells_item_has_id_well(client):
    response = client.get("/api/v1/wells", params={"date_query": "2023-10-15"})

    assert response.status_code == 200
    assert all("id_well" in item for item in response.json())


def test_wells_missing_date_query_returns_422(client):
    response = client.get("/api/v1/wells")

    assert response.status_code == 422


def test_wells_invalid_date_format_returns_422(client):
    response = client.get("/api/v1/wells", params={"date_query": "not-a-date"})

    assert response.status_code == 422


def test_wells_repository_failure_returns_503(client, fake_repository):
    def failing_list_wells(*args, **kwargs):
        raise RuntimeError("db unavailable")

    fake_repository.list_wells = failing_list_wells

    response = client.get("/api/v1/wells", params={"date_query": "2023-10-15"})

    assert response.status_code == 503
    assert response.json()["detail"] == "Wells backend is temporarily unavailable"
