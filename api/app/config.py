"""Configuración centralizada de la API.

Lee variables de entorno (o del archivo .env).
"""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_name: str = "MIA204 API"
    database_url: str = "postgresql+psycopg2://airflow:airflow@postgres:5432/featurestore"
    # Métrica de producción que devuelve /forecast (prod_gas | prod_pet | prod_agua | tef)
    forecast_measure: str = "prod_gas"
    # TTL del cache de /wells en segundos (0 deshabilita el cache)
    wells_cache_ttl_seconds: int = 300
