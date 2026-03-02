from datetime import datetime

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import func
from sqlalchemy.orm import Session

from ..database import get_db
from ..models import Hub, HubTimeseries, Tenant
from ..simulator.run import is_simulator_running


router = APIRouter(prefix="/api/debug", tags=["debug"])


class DbStatus(BaseModel):
    total_timeseries_rows: int
    last_timestamp: str | None
    tenant_count: int
    hub_count: int


class SimulatorStatus(BaseModel):
    running: bool


@router.get("/db/status", response_model=DbStatus)
def get_db_status(db: Session = Depends(get_db)):
    total_rows = db.query(func.count(HubTimeseries.id)).scalar() or 0
    last_ts = (
        db.query(func.max(HubTimeseries.timestamp))
        .scalar()
    )
    tenants = db.query(func.count(Tenant.id)).scalar() or 0
    hubs = db.query(func.count(Hub.id)).scalar() or 0

    return DbStatus(
        total_timeseries_rows=total_rows,
        last_timestamp=last_ts.isoformat() if isinstance(last_ts, datetime) else None,
        tenant_count=tenants,
        hub_count=hubs,
    )


@router.get("/simulator/state", response_model=SimulatorStatus)
def get_simulator_state():
    return SimulatorStatus(running=is_simulator_running())

