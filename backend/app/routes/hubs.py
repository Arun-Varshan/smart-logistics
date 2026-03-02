from typing import List

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.orm import Session

from ..auth import get_current_token_data
from ..database import get_db
from ..models import Hub


router = APIRouter(prefix="/api", tags=["hubs"])


class HubOut(BaseModel):
    id: int
    name: str
    location: str | None

    class Config:
        orm_mode = True


@router.get("/hubs", response_model=List[HubOut])
def list_hubs(
    token_data=Depends(get_current_token_data),
    db: Session = Depends(get_db),
):
    hubs = (
        db.query(Hub)
        .filter(Hub.tenant_id == token_data.tenant_id)
        .order_by(Hub.id.asc())
        .all()
    )
    return hubs

