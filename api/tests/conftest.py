from collections.abc import Generator

import pytest
from fastapi.testclient import TestClient

from app.core.dependencies import get_repository
from app.main import create_app
from app.repositories.fake import FakeWellRepository


@pytest.fixture()
def fake_repository() -> FakeWellRepository:
    return FakeWellRepository()


@pytest.fixture()
def client(fake_repository: FakeWellRepository) -> Generator[TestClient, None, None]:
    app = create_app()
    app.dependency_overrides[get_repository] = lambda: fake_repository
    with TestClient(app) as test_client:
        yield test_client
