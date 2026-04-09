-- Feature-store + MLflow database bootstrap.
-- Runs via postgres' /docker-entrypoint-initdb.d/ on first start of a fresh volume.
-- Idempotent so it survives a `docker-compose down -v` + re-up.

-- Create featurestore database if it doesn't already exist.
SELECT 'CREATE DATABASE featurestore'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'featurestore')\gexec

-- Create mlflow backend database if it doesn't already exist.
SELECT 'CREATE DATABASE mlflow'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'mlflow')\gexec

\c featurestore;

CREATE TABLE IF NOT EXISTS production (
    id SERIAL PRIMARY KEY,
    idpozo VARCHAR NOT NULL,
    fecha DATE NOT NULL,
    prod_pet FLOAT,
    prod_gas FLOAT,
    prod_agua FLOAT,
    tef FLOAT,
    tipoextraccion VARCHAR,
    profundidad FLOAT
);

CREATE INDEX IF NOT EXISTS idx_prod_pozo_fecha ON production(idpozo, fecha);

CREATE TABLE IF NOT EXISTS wells (
    idpozo VARCHAR NOT NULL,
    sigla VARCHAR,
    empresa VARCHAR,
    provincia VARCHAR,
    cuenca VARCHAR,
    areayacimiento VARCHAR,
    fecha_data DATE
);

CREATE INDEX IF NOT EXISTS idx_wells_pozo ON wells(idpozo);
CREATE INDEX IF NOT EXISTS idx_wells_fecha ON wells(fecha_data);

CREATE TABLE IF NOT EXISTS features (
    id SERIAL PRIMARY KEY,
    id_pozo VARCHAR NOT NULL,
    fecha DATE NOT NULL,
    tipoextraccion INT,
    prod_gas FLOAT,
    prod_agua FLOAT,
    tef FLOAT,
    prod_pet FLOAT,
    profundidad FLOAT
);

CREATE INDEX IF NOT EXISTS idx_features_pozo_fecha ON features(id_pozo, fecha);
