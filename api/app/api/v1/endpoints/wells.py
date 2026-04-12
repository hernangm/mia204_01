from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query, status

from app.core.dependencies import get_repository
from app.repositories.base import WellRepository
from app.schemas import WellItem

router = APIRouter(tags=["wells"])


@router.get("/wells", response_model=list[WellItem])
def list_wells(
    date_query: date = Query(..., description="Fecha de consulta", json_schema_extra={"example": "2023-01-01"}),
    limit: int = Query(100, ge=1, le=1000, description="Cantidad máxima de resultados"),
    offset: int = Query(0, ge=0, description="Cantidad de resultados a omitir (paginación)"),
    repository: WellRepository = Depends(get_repository),
) -> list[WellItem]:
    try:
        wells = repository.list_wells(date_query, limit=limit, offset=offset)
    except RuntimeError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Wells backend is temporarily unavailable",
        ) from exc

    return [WellItem(id_well=id_well) for id_well in wells]
