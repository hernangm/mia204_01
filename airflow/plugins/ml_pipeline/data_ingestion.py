"""Download and load raw datasets into the feature-store database."""

import logging
import os

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from ml_pipeline.config import DATA_DIR, PRODUCTION_URL, WELLS_URL

logger = logging.getLogger(__name__)


def download_csv(url: str, dest_path: str) -> str:
    """Download a CSV from *url* and save it to *dest_path*.

    Raises ``RuntimeError`` if the download or write fails.
    """
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    logger.info("Downloading %s -> %s", url, dest_path)
    try:
        df = pd.read_csv(url)
    except Exception as exc:
        raise RuntimeError(f"Failed to download CSV from {url}: {exc}") from exc
    try:
        df.to_csv(dest_path, index=False)
    except OSError as exc:
        raise RuntimeError(f"Failed to write CSV to {dest_path}: {exc}") from exc
    logger.info("Saved %d rows to %s", len(df), dest_path)
    return dest_path


def download_production(dest: str | None = None) -> str:
    """Download the production CSV and return the local path."""
    dest = dest or os.path.join(DATA_DIR, "produccion-pozos-no-convencional.csv")
    return download_csv(PRODUCTION_URL, dest)


def download_wells(dest: str | None = None) -> str:
    """Download the wells CSV and return the local path."""
    dest = dest or os.path.join(DATA_DIR, "listado-pozos-empresas-operadoras.csv")
    return download_csv(WELLS_URL, dest)


def ingest_wells(engine: Engine, csv_path: str | None = None) -> int:
    """Parse the wells CSV and upsert rows into *featurestore.wells*.

    Raises ``RuntimeError`` on CSV parsing or database errors.
    """
    csv_path = csv_path or os.path.join(DATA_DIR, "listado-pozos-empresas-operadoras.csv")
    logger.info("Reading wells from %s", csv_path)
    try:
        df = pd.read_csv(csv_path, encoding="utf-8")
    except (FileNotFoundError, pd.errors.ParserError) as exc:
        raise RuntimeError(f"Failed to read wells CSV {csv_path}: {exc}") from exc

    col_map = {
        "idpozo": "idpozo",
        "sigla": "sigla",
    }
    for optional in ("empresa", "provincia", "cuenca", "areayacimiento", "fecha_data"):
        if optional in df.columns:
            col_map[optional] = optional

    subset = df[list(col_map.keys())].rename(columns=col_map).copy()
    subset["idpozo"] = subset["idpozo"].astype(str)
    if "fecha_data" in subset.columns:
        subset["fecha_data"] = pd.to_datetime(subset["fecha_data"]).dt.date

    try:
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE wells"))
        subset.to_sql("wells", engine, if_exists="append", index=False, method="multi", chunksize=5000)
    except SQLAlchemyError as exc:
        raise RuntimeError(f"Database error while ingesting wells: {exc}") from exc

    logger.info("Ingested %d wells rows", len(subset))
    return len(subset)
