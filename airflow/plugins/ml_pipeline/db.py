"""Single source of truth for the featurestore SQLAlchemy engine.

Every module in ml_pipeline that needs to talk to the featurestore should call
get_engine() rather than building its own connection string. This keeps the
default URL in one place and lets tests/CI override it via env var.
"""

import os

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

DEFAULT_URL = "postgresql+psycopg2://airflow:airflow@postgres:5432/featurestore"


def get_engine() -> Engine:
    url = os.environ.get("FEATURESTORE_DATABASE_URL", DEFAULT_URL)
    return create_engine(url, pool_pre_ping=True)
