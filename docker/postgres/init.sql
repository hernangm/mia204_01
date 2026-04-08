-- Script de inicialización ejecutado por PostgreSQL al arrancar por primera vez.
-- Crea el usuario y la base de datos del Feature Store / MLflow.
-- La base de datos "airflow" la crea automáticamente la imagen postgres:16
-- a partir de las variables POSTGRES_USER / POSTGRES_DB del docker-compose.

-- Usuario para el Feature Store y MLflow
CREATE USER forecast WITH PASSWORD 'forecast';

-- Base de datos principal del proyecto
CREATE DATABASE forecast OWNER forecast;

-- Permisos completos sobre la base de datos
GRANT ALL PRIVILEGES ON DATABASE forecast TO forecast;
