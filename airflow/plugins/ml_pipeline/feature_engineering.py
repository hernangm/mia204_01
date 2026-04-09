"""Stage 2 of the ml_pipeline DAG: build the features table from production.

Reads the (already-populated) production table, drops rows with nulls in any
modeling column, encodes tipoextraccion via the static map in
ml_pipeline.config.TIPOEXTRACCION_MAP, and bulk-inserts the result into the
features table. Idempotent: TRUNCATEs features first.
"""

import logging

import pandas as pd
from sqlalchemy import text

from ml_pipeline.config import TIPOEXTRACCION_MAP
from ml_pipeline.db import get_engine

log = logging.getLogger(__name__)

PRODUCTION_QUERY = (
    "SELECT idpozo, fecha, prod_pet, prod_gas, prod_agua, tef, "
    "tipoextraccion, profundidad FROM production"
)

REQUIRED_COLS = ["prod_pet", "prod_gas", "prod_agua", "tef", "profundidad", "tipoextraccion"]
OUTPUT_COLS = [
    "id_pozo",
    "fecha",
    "tipoextraccion",
    "prod_gas",
    "prod_agua",
    "tef",
    "prod_pet",
    "profundidad",
]


def build() -> dict:
    engine = get_engine()

    log.info("Reading production rows from featurestore")
    try:
        df = pd.read_sql(PRODUCTION_QUERY, engine)
    except Exception as exc:
        raise RuntimeError(f"Failed to read production table: {exc}") from exc
    raw_count = len(df)
    log.info("Read %d production rows", raw_count)

    df = df.dropna(subset=REQUIRED_COLS)
    log.info("After dropna on required columns: %d rows", len(df))

    df["tipoextraccion"] = df["tipoextraccion"].map(TIPOEXTRACCION_MAP)
    unmapped = df["tipoextraccion"].isna().sum()
    if unmapped:
        log.warning("Dropping %d rows with unmapped tipoextraccion values", int(unmapped))
    df = df.dropna(subset=["tipoextraccion"])
    df["tipoextraccion"] = df["tipoextraccion"].astype(int)

    features_df = df.rename(columns={"idpozo": "id_pozo"})[OUTPUT_COLS]

    log.info("Truncating features table")
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE features RESTART IDENTITY"))

    log.info("Inserting %d feature rows", len(features_df))
    try:
        features_df.to_sql(
            "features",
            engine,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=5000,
        )
    except Exception as exc:
        raise RuntimeError(f"Failed to bulk-insert features: {exc}") from exc

    return {"raw_rows": raw_count, "feature_rows": len(features_df)}
