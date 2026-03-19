"""Feature computation and persistence into the feature store."""

import logging
import os

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from ml_pipeline.config import (
    DATA_DIR,
    FEATURE_COLUMNS,
    TARGET_COLUMN,
    TIPO_EXTRACCION_DEFAULT,
    TIPO_EXTRACCION_MAP,
)

logger = logging.getLogger(__name__)


def compute_features(
    engine: Engine,
    csv_path: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
) -> int:
    """Read the raw production CSV, engineer features, and write them to the
    *features* table in the feature-store database.

    Returns the number of rows written.
    Raises ``RuntimeError`` on CSV parsing or database errors.
    """
    csv_path = csv_path or os.path.join(DATA_DIR, "produccion-pozos-no-convencional.csv")
    logger.info("Reading production CSV from %s", csv_path)
    try:
        df = pd.read_csv(csv_path, encoding="utf-8")
    except (FileNotFoundError, pd.errors.ParserError) as exc:
        raise RuntimeError(f"Failed to read production CSV {csv_path}: {exc}") from exc
    logger.info("Raw rows: %d", len(df))

    # Derive fecha from anio + mes
    df["fecha"] = pd.to_datetime(
        df["anio"].astype(str) + "-" + df["mes"].astype(str).str.zfill(2) + "-01"
    )

    # Optional date filtering
    if date_from:
        df = df[df["fecha"] >= pd.to_datetime(date_from)]
        logger.info("Filtered from %s: %d rows", date_from, len(df))
    if date_to:
        df = df[df["fecha"] <= pd.to_datetime(date_to)]
        logger.info("Filtered to %s: %d rows", date_to, len(df))

    # Select and clean
    keep = ["idpozo", "fecha"] + FEATURE_COLUMNS + [TARGET_COLUMN]
    df = df[keep].dropna().copy()
    df["idpozo"] = df["idpozo"].astype(str)

    # Encode tipoextraccion with a static map
    df["tipoextraccion"] = (
        df["tipoextraccion"]
        .str.upper()
        .str.strip()
        .map(TIPO_EXTRACCION_MAP)
        .fillna(TIPO_EXTRACCION_DEFAULT)
        .astype(int)
    )

    # Persist to feature store
    try:
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE features RESTART IDENTITY"))
        df.rename(columns={"idpozo": "id_pozo"}).to_sql(
            "features", engine, if_exists="append", index=False, method="multi", chunksize=5000
        )
    except SQLAlchemyError as exc:
        raise RuntimeError(f"Database error while writing features: {exc}") from exc

    logger.info("Wrote %d feature rows", len(df))
    return len(df)


def load_training_data(engine: Engine) -> pd.DataFrame:
    """Load the feature table into a DataFrame for training.

    Raises ``RuntimeError`` if the query fails.
    """
    try:
        query = "SELECT * FROM features"
        return pd.read_sql(query, engine)
    except SQLAlchemyError as exc:
        raise RuntimeError(f"Failed to load training data: {exc}") from exc
