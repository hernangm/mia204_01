"""Load CSV data into the featurestore PostgreSQL database.

Usage:
    python data/load_data.py

Requires DATABASE_URL env var or defaults to:
    postgresql+psycopg2://airflow:airflow@localhost:5432/featurestore

When running inside docker-compose, use 'postgres' as host instead of 'localhost'.
"""

import os
from datetime import date

import pandas as pd
from sqlalchemy import create_engine, text

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql+psycopg2://airflow:airflow@localhost:5432/featurestore",
)

DATA_DIR = os.path.join(os.path.dirname(__file__), "raw")

PRODUCTION_CSV = os.path.join(DATA_DIR, "produccion-pozos-no-convencional.csv")
WELLS_CSV = os.path.join(DATA_DIR, "listado-pozos-empresas-operadoras.csv")


def load_production(engine):
    print(f"Reading {PRODUCTION_CSV} ...")
    df = pd.read_csv(PRODUCTION_CSV, encoding="utf-8")

    # Derive fecha from anio + mes (first day of month)
    df["fecha"] = pd.to_datetime(
        df["anio"].astype(str) + "-" + df["mes"].astype(str).str.zfill(2) + "-01"
    )

    cols = {
        "idpozo": "idpozo",
        "fecha": "fecha",
        "prod_pet": "prod_pet",
        "prod_gas": "prod_gas",
        "prod_agua": "prod_agua",
        "tef": "tef",
        "tipoextraccion": "tipoextraccion",
    }
    subset = df[list(cols.keys())].rename(columns=cols)

    # Convert idpozo to string
    subset["idpozo"] = subset["idpozo"].astype(str)

    print(f"Loading {len(subset)} production rows into database...")
    subset.to_sql("production", engine, if_exists="append", index=False, method="multi", chunksize=5000)
    print("Production data loaded.")


def load_wells(engine):
    print(f"Reading {WELLS_CSV} ...")
    df = pd.read_csv(WELLS_CSV, encoding="utf-8")

    cols = {
        "idpozo": "idpozo",
        "sigla": "sigla",
        "empresa": "empresa" if "empresa" in df.columns else None,
        "provincia": "provincia" if "provincia" in df.columns else None,
        "cuenca": "cuenca" if "cuenca" in df.columns else None,
        "areayacimiento": "areayacimiento" if "areayacimiento" in df.columns else None,
    }
    # Filter out columns not present in CSV
    cols = {k: v for k, v in cols.items() if v is not None}

    subset = df[list(cols.keys())].rename(columns=cols)
    subset["idpozo"] = subset["idpozo"].astype(str)

    # Add fecha_data if present
    if "fecha_data" in df.columns:
        subset["fecha_data"] = pd.to_datetime(df["fecha_data"]).dt.date

    print(f"Loading {len(subset)} well rows into database...")
    subset.to_sql("wells", engine, if_exists="append", index=False, method="multi", chunksize=5000)
    print("Wells data loaded.")


def main():
    engine = create_engine(DATABASE_URL)

    # Clear existing data
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE production RESTART IDENTITY"))
        conn.execute(text("TRUNCATE TABLE wells"))

    load_production(engine)
    load_wells(engine)
    print("Done.")


if __name__ == "__main__":
    main()
