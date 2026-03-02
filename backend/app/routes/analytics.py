from datetime import datetime, timedelta
from typing import List

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from sqlalchemy import func
from sqlalchemy.orm import Session

from ..auth import get_current_token_data
from ..database import get_db
from ..models import Alert, HubTimeseries


router = APIRouter(prefix="/api", tags=["analytics"])


class TimeseriesPoint(BaseModel):
    timestamp: datetime
    total_parcels: int
    parcels_processed: int
    active_robots: int
    utilization_pct: float

    class Config:
        orm_mode = True


class AlertOut(BaseModel):
    id: int
    timestamp: datetime
    type: str
    message: str
    severity: str

    class Config:
        orm_mode = True


class AnalyticsSummary(BaseModel):
    range: str
    points: List[TimeseriesPoint]
    alerts: List[AlertOut]


def _range_to_timedelta(range_value: str) -> timedelta:
    if range_value == "1h":
        return timedelta(hours=1)
    if range_value == "24h":
        return timedelta(hours=24)
    if range_value == "7d":
        return timedelta(days=7)
    return timedelta(hours=24)


@router.get("/hub/{hub_id}/timeseries", response_model=List[TimeseriesPoint])
def get_hub_timeseries(
    hub_id: int,
    range: str = Query("1h", regex="^(1h|24h|7d)$"),
    token_data=Depends(get_current_token_data),
    db: Session = Depends(get_db),
):
    delta = _range_to_timedelta(range)
    now = datetime.utcnow()
    start_time = now - delta

    rows = (
        db.query(HubTimeseries)
        .filter(
            HubTimeseries.hub_id == hub_id,
            HubTimeseries.tenant_id == token_data.tenant_id,
            HubTimeseries.timestamp >= start_time,
        )
        .order_by(HubTimeseries.timestamp.asc())
        .all()
    )
    return [
        TimeseriesPoint(
            timestamp=r.timestamp,
            total_parcels=r.total_parcels,
            parcels_processed=r.parcels_processed,
            active_robots=r.active_robots,
            utilization_pct=r.utilization_pct,
        )
        for r in rows
    ]


@router.get("/hub/{hub_id}/alerts", response_model=List[AlertOut])
def get_hub_alerts(
    hub_id: int,
    token_data=Depends(get_current_token_data),
    db: Session = Depends(get_db),
):
    alerts = (
        db.query(Alert)
        .filter(
            Alert.hub_id == hub_id,
            Alert.tenant_id == token_data.tenant_id,
        )
        .order_by(Alert.timestamp.desc())
        .limit(100)
        .all()
    )
    return alerts


@router.get("/hub/{hub_id}/analytics", response_model=AnalyticsSummary)
def get_hub_analytics(
    hub_id: int,
    range: str = Query("24h", regex="^(1h|24h|7d)$"),
    token_data=Depends(get_current_token_data),
    db: Session = Depends(get_db),
):
    delta = _range_to_timedelta(range)
    now = datetime.utcnow()
    start_time = now - delta

    timeseries_rows = (
        db.query(HubTimeseries)
        .filter(
            HubTimeseries.hub_id == hub_id,
            HubTimeseries.tenant_id == token_data.tenant_id,
            HubTimeseries.timestamp >= start_time,
        )
        .order_by(HubTimeseries.timestamp.asc())
        .all()
    )

    alerts = (
        db.query(Alert)
        .filter(
            Alert.hub_id == hub_id,
            Alert.tenant_id == token_data.tenant_id,
            Alert.timestamp >= start_time,
        )
        .order_by(Alert.timestamp.desc())
        .limit(100)
        .all()
    )

    points = [
        TimeseriesPoint(
            timestamp=r.timestamp,
            total_parcels=r.total_parcels,
            parcels_processed=r.parcels_processed,
            active_robots=r.active_robots,
            utilization_pct=r.utilization_pct,
        )
        for r in timeseries_rows
    ]
    alert_out = [
        AlertOut(
            id=a.id,
            timestamp=a.timestamp,
            type=a.type,
            message=a.message,
            severity=a.severity,
        )
        for a in alerts
    ]

    return AnalyticsSummary(range=range, points=points, alerts=alert_out)

