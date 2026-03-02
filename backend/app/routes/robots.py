from typing import List

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.orm import Session

from ..auth import get_current_token_data
from ..database import get_db
from ..models import Robot


router = APIRouter(prefix="/api", tags=["robots"])


class RobotOut(BaseModel):
    id: int
    name: str
    status: str
    x: float
    y: float
    utilization_pct: float

    class Config:
        orm_mode = True


@router.get("/hub/{hub_id}/robots", response_model=List[RobotOut])
def get_hub_robots(
    hub_id: int,
    token_data=Depends(get_current_token_data),
    db: Session = Depends(get_db),
):
    robots = (
        db.query(Robot)
        .filter(
            Robot.hub_id == hub_id,
            Robot.tenant_id == token_data.tenant_id,
        )
        .order_by(Robot.id.asc())
        .all()
    )
    return robots

