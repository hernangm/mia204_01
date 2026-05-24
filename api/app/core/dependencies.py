from functools import lru_cache

from app.core.config import get_settings
from app.core.model import get_model as _get_model
from app.repositories.base import WellRepository
from app.repositories.postgres import PostgresWellRepository


# Cachear el repo entre requests preserva el pool del engine SQLAlchemy y el
# TTL-cache interno de list_wells.
@lru_cache(maxsize=1)
def get_repository() -> WellRepository:
    settings = get_settings()
    return PostgresWellRepository(
        database_url=settings.database_url,
        forecast_measure=settings.forecast_measure,
        wells_cache_ttl_seconds=settings.wells_cache_ttl_seconds,
    )


def get_model():
    """Indirección para que los tests puedan overridar esta dependency
    sin tocar el loader cacheado de app.core.model."""
    return _get_model()
