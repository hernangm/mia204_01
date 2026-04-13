from collections.abc import Generator

import pytest
from fastapi.testclient import TestClient

from app.main import create_app
from app.repository import FakeWellRepository
from app.routes import get_repository


@pytest.fixture()
def fake_repository() -> FakeWellRepository:
    return FakeWellRepository()


@pytest.fixture()
def client(fake_repository: FakeWellRepository) -> Generator[TestClient, None, None]:
    app = create_app()
    app.dependency_overrides[get_repository] = lambda: fake_repository
    with TestClient(app) as test_client:
        yield test_client
