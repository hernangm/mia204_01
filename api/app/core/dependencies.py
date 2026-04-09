from app.core.config import get_settings
from app.repositories.base import WellRepository
from app.repositories.postgres import PostgresWellRepository


def get_repository() -> WellRepository:
    settings = get_settings()
    return PostgresWellRepository(
        database_url=settings.database_url,
        forecast_measure=settings.forecast_measure,
    )
