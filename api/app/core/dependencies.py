from app.core.config import get_settings
from app.core.model import get_model as _get_model
from app.repositories.base import WellRepository
from app.repositories.postgres import PostgresWellRepository


def get_repository() -> WellRepository:
    settings = get_settings()
    return PostgresWellRepository(
        database_url=settings.database_url,
        forecast_measure=settings.forecast_measure,
    )


def get_model():
    """Indirección para que los tests puedan overridar esta dependency
    sin tocar el loader cacheado de app.core.model."""
    return _get_model()
