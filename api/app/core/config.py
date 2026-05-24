from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_name: str = "MIA204 API"
    api_v1_prefix: str = "/api/v1"
    database_url: str = "postgresql+psycopg2://airflow:airflow@postgres:5432/featurestore"
    forecast_measure: str = "prod_gas"
    wells_cache_ttl_seconds: int = 300
    mlflow_tracking_uri: str = "http://mlflow:9090"
    mlflow_model_name: str = "hydrocarbon_forecast"
    mlflow_model_alias: str = "production"
    ray_serve_url: str = "http://ray-serve:8001"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
