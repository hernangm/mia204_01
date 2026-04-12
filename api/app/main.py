from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.v1.router import api_router
from app.core.config import get_settings


def _patch_openapi_for_3_0(app: FastAPI) -> None:
    """Corrige el schema OpenAPI para compatibilidad total con Swagger UI 3.0.3.

    FastAPI genera ``anyOf`` en ValidationError.loc (sintaxis 3.1);
    Swagger UI para 3.0 no lo soporta y muestra errores de renderizado.
    """
    original = app.openapi

    def patched_openapi():
        schema = original()
        val_err = (
            schema.get("components", {})
            .get("schemas", {})
            .get("ValidationError", {})
        )
        loc = val_err.get("properties", {}).get("loc", {})
        items = loc.get("items", {})
        if "anyOf" in items:
            items.pop("anyOf")
            items["type"] = "string"
        return schema

    app.openapi = patched_openapi


@asynccontextmanager
async def lifespan(app: FastAPI):
    from app.core.model import load_model_at_startup

    load_model_at_startup()
    yield


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(
        title=settings.app_name,
        openapi_version="3.0.3",
        lifespan=lifespan,
    )
    app.include_router(api_router, prefix=settings.api_v1_prefix)

    @app.get("/health", tags=["health"])
    def health() -> dict[str, str]:
        return {"status": "ok"}

    _patch_openapi_for_3_0(app)

    return app


app = create_app()
