"""Stage 1 of the ml_pipeline DAG: ingest raw CSVs into the featurestore.

Reads from /opt/airflow/data/raw (host bind-mount of ./data/raw). If the
production CSV is missing, falls back to streaming-download from the public
gob.ar URL defined in ml_pipeline.config.

Idempotent: TRUNCATEs production and wells before reloading.
"""

import logging
from pathlib import Path

import pandas as pd
import requests
from sqlalchemy import text

from ml_pipeline.config import DATASET_DOWNLOAD_URL, WELLS_DOWNLOAD_URL
from ml_pipeline.db import get_engine

log = logging.getLogger(__name__)

DATA_DIR = Path("/opt/airflow/data/raw")
PRODUCTION_CSV = DATA_DIR / "produccion-pozos-no-convencional.csv"
WELLS_CSV = DATA_DIR / "listado-pozos-empresas-operadoras.csv"


def download_if_missing() -> None:
    """Hybrid: prefer the locally-mounted CSVs, fall back to download."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    if not PRODUCTION_CSV.exists():
        log.info("Production CSV missing, streaming download from %s", DATASET_DOWNLOAD_URL)
        try:
            with requests.get(DATASET_DOWNLOAD_URL, stream=True, timeout=300) as response:
                response.raise_for_status()
                with open(PRODUCTION_CSV, "wb") as fh:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            fh.write(chunk)
            log.info("Downloaded production CSV to %s", PRODUCTION_CSV)
        except requests.RequestException as exc:
            raise RuntimeError(f"Failed to download production CSV: {exc}") from exc
    else:
        log.info("Using existing production CSV at %s", PRODUCTION_CSV)

    if not WELLS_CSV.exists():
        log.info("Wells CSV missing, streaming download from %s", WELLS_DOWNLOAD_URL)
        try:
            with requests.get(WELLS_DOWNLOAD_URL, stream=True, timeout=300) as response:
                response.raise_for_status()
                with open(WELLS_CSV, "wb") as fh:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            fh.write(chunk)
            log.info("Downloaded wells CSV to %s", WELLS_CSV)
        except requests.RequestException as exc:
            raise RuntimeError(f"Failed to download wells CSV: {exc}") from exc
    else:
        log.info("Using existing wells CSV at %s", WELLS_CSV)


def _load_production(engine) -> int:
    log.info("Reading %s", PRODUCTION_CSV)
    try:
        df = pd.read_csv(PRODUCTION_CSV)
    except Exception as exc:
        raise RuntimeError(f"Failed to read production CSV: {exc}") from exc

    # Derive a single fecha column from anio + mes (first day of month).
    df["fecha"] = pd.to_datetime(
        df["anio"].astype(str) + "-" + df["mes"].astype(str).str.zfill(2) + "-01"
    )

    cols = [
        "idpozo",
        "fecha",
        "prod_pet",
        "prod_gas",
        "prod_agua",
        "tef",
        "tipoextraccion",
        "profundidad",
    ]
    subset = df[cols].copy()
    subset["idpozo"] = subset["idpozo"].astype(str)

    log.info("Inserting %d production rows", len(subset))
    try:
        subset.to_sql(
            "production",
            engine,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=5000,
        )
    except Exception as exc:
        raise RuntimeError(f"Failed to bulk-insert production rows: {exc}") from exc
    return len(subset)


def _load_wells(engine) -> int:
    log.info("Reading %s", WELLS_CSV)
    try:
        df = pd.read_csv(WELLS_CSV)
    except Exception as exc:
        raise RuntimeError(f"Failed to read wells CSV: {exc}") from exc

    candidate_cols = ("idpozo", "sigla", "empresa", "provincia", "cuenca", "areayacimiento")
    cols = [c for c in candidate_cols if c in df.columns]
    subset = df[cols].copy()
    subset["idpozo"] = subset["idpozo"].astype(str)

    if "fecha_data" in df.columns:
        subset["fecha_data"] = pd.to_datetime(df["fecha_data"], errors="coerce").dt.date

    log.info("Inserting %d wells rows", len(subset))
    try:
        subset.to_sql(
            "wells",
            engine,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=5000,
        )
    except Exception as exc:
        raise RuntimeError(f"Failed to bulk-insert wells rows: {exc}") from exc
    return len(subset)


def ingest() -> dict:
    """Truncate production + wells and reload from the raw CSVs."""
    download_if_missing()
    engine = get_engine()

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE production RESTART IDENTITY"))
        conn.execute(text("TRUNCATE TABLE wells"))

    production_rows = _load_production(engine)
    wells_rows = _load_wells(engine)
    log.info("Ingest complete: production=%d wells=%d", production_rows, wells_rows)
    return {"production_rows": production_rows, "wells_rows": wells_rows}
