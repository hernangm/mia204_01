"""SQLAlchemy engine factory for the feature-store database."""

import logging

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from ml_pipeline.config import DATABASE_URL

logger = logging.getLogger(__name__)


def get_engine(url: str | None = None) -> Engine:
    """Return a SQLAlchemy engine for the feature-store DB.

    Falls back to ``DATABASE_URL`` from config (env var → default).
    """
    url = url or DATABASE_URL
    logger.info("Creating database engine for %s", url.split("@")[-1])
    return create_engine(url)
