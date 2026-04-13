"""Punto de entrada de la API FastAPI.

Crea la app, monta el router con el prefijo /api/v1 y expone el endpoint /health.
"""

from fastapi import FastAPI

from app.config import Settings
from app.routes import router


def create_app() -> FastAPI:
    settings = Settings()
    app = FastAPI(title=settings.app_name)
    app.include_router(router, prefix="/api/v1")

    @app.get("/health", tags=["health"])
    def health() -> dict[str, str]:
        return {"status": "ok"}

    return app


app = create_app()
