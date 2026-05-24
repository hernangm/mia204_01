from collections.abc import Generator
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from app.core.dependencies import get_repository
from app.main import create_app
from app.repositories.fake import FakeWellRepository


def _fake_ray_predict(url, json=None, timeout=None):
    """Simula la respuesta de Ray Serve (POST /predict).

    Replica la logica de FakeModel: devuelve prod_pet * 10 por fila
    para que los tests puedan asertar valores exactos.
    """

    class FakeResponse:
        status_code = 200

        def raise_for_status(self):
            pass

        def json(self):
            return [float(row["prod_pet"]) * 10 for row in json]

    return FakeResponse()


@pytest.fixture()
def fake_repository() -> FakeWellRepository:
    return FakeWellRepository()


@pytest.fixture()
def client(
    fake_repository: FakeWellRepository,
) -> Generator[TestClient, None, None]:
    app = create_app()
    app.dependency_overrides[get_repository] = lambda: fake_repository
    with patch("app.api.v1.endpoints.forecast.http_client.post", side_effect=_fake_ray_predict):
        with TestClient(app) as test_client:
            yield test_client
