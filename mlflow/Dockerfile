FROM ghcr.io/mlflow/mlflow:v3.10.1

# Stock image lacks psycopg2 — required to use PostgreSQL as the backend store.
RUN pip install --no-cache-dir psycopg2-binary
