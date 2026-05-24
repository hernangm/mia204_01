"""Fuente única para el engine SQLAlchemy del featurestore.

Todos los módulos de ml_pipeline que necesiten hablar con el featurestore
deben llamar a get_engine() en lugar de construir su propia conexión.
Esto mantiene la URL en un solo lugar y permite overridearla con env vars.
"""

import os

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

# missing-features usa FEATURESTORE_DB_URL (vs FEATURESTORE_DATABASE_URL en main)
DEFAULT_URL = "postgresql+psycopg2://airflow:airflow@postgres:5432/featurestore"


def get_engine() -> Engine:
    url = os.environ.get("FEATURESTORE_DB_URL", DEFAULT_URL)
    return create_engine(url, pool_pre_ping=True)
