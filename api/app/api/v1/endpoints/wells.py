from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query, status

from app.core.dependencies import get_repository
from app.repositories.base import WellRepository
from app.schemas import WellItem

router = APIRouter(tags=["wells"])


@router.get("/wells", response_model=list[WellItem])
def list_wells(
    date_query: date = Query(..., description="Fecha de consulta"),
    repository: WellRepository = Depends(get_repository),
) -> list[WellItem]:
    try:
        wells = repository.list_wells(date_query)
    except RuntimeError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Wells backend is temporarily unavailable",
        ) from exc

    return [WellItem(id_well=id_well) for id_well in wells]
