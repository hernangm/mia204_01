"""Tests for GET /api/v1/wells."""


def test_wells_returns_list(client):
    resp = client.get("/api/v1/wells", params={"date_query": "2024-01-01"})
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)
    assert len(data) == 3


def test_wells_item_has_id_well(client):
    resp = client.get("/api/v1/wells", params={"date_query": "2024-01-01"})
    for item in resp.json():
        assert "id_well" in item
        assert isinstance(item["id_well"], str)


def test_wells_returns_expected_ids(client):
    resp = client.get("/api/v1/wells", params={"date_query": "2024-01-01"})
    ids = [item["id_well"] for item in resp.json()]
    assert ids == ["POZO-001", "POZO-002", "POZO-003"]


def test_wells_missing_date_query_returns_422(client):
    resp = client.get("/api/v1/wells")
    assert resp.status_code == 422


def test_wells_invalid_date_format_returns_422(client):
    resp = client.get("/api/v1/wells", params={"date_query": "not-a-date"})
    assert resp.status_code == 422
