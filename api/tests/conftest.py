from collections.abc import Generator

import pytest
from fastapi.testclient import TestClient

from app.core.dependencies import get_model, get_repository
from app.main import create_app
from app.repositories.fake import FakeWellRepository


class FakeModel:
    """Reemplazo del PyFuncModel de MLflow usado en los tests unitarios.

    Devuelve una predicción determinística (`prod_pet * 10`) por fila
    para que los tests puedan asertar valores exactos sin necesitar
    cargar un modelo real.
    """

    def predict(self, df):
        return [float(row["prod_pet"]) * 10 for _, row in df.iterrows()]


@pytest.fixture()
def fake_repository() -> FakeWellRepository:
    return FakeWellRepository()


@pytest.fixture()
def fake_model() -> FakeModel:
    return FakeModel()


@pytest.fixture()
def client(
    fake_repository: FakeWellRepository, fake_model: FakeModel
) -> Generator[TestClient, None, None]:
    app = create_app()
    app.dependency_overrides[get_repository] = lambda: fake_repository
    app.dependency_overrides[get_model] = lambda: fake_model
    with TestClient(app) as test_client:
        yield test_client
