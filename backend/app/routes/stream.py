import asyncio
import json
from datetime import datetime, timedelta
from typing import Any, Dict, List

from fastapi import APIRouter, Depends
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session

from ..auth import get_current_token_data
from ..database import get_db
from ..models import Alert, Hub, HubTimeseries, Robot


router = APIRouter(prefix="/api", tags=["stream"])


async def _build_payload(
    db: Session,
    hub: Hub,
) -> Dict[str, Any]:
    now = datetime.utcnow()
    since = now - timedelta(minutes=5)

    latest = (
        db.query(HubTimeseries)
        .filter(
            HubTimeseries.hub_id == hub.id,
            HubTimeseries.tenant_id == hub.tenant_id,
        )
        .order_by(HubTimeseries.timestamp.desc())
        .first()
    )

    if latest is None:
        kpis = {}
    else:
        kpis = {
            "total_parcels": latest.total_parcels,
            "parcels_processed": latest.parcels_processed,
            "active_robots": latest.active_robots,
            "avg_processing_time": latest.avg_processing_time,
            "utilization_pct": latest.utilization_pct,
        }

    robots = (
        db.query(Robot)
        .filter(
            Robot.hub_id == hub.id,
            Robot.tenant_id == hub.tenant_id,
        )
        .order_by(Robot.id.asc())
        .all()
    )

    robot_payload = [
        {
            "id": r.id,
            "name": r.name,
            "status": r.status,
            "x": r.x,
            "y": r.y,
            "utilization_pct": r.utilization_pct,
        }
        for r in robots
    ]

    recent_rows: List[HubTimeseries] = (
        db.query(HubTimeseries)
        .filter(
            HubTimeseries.hub_id == hub.id,
            HubTimeseries.tenant_id == hub.tenant_id,
            HubTimeseries.timestamp >= since,
        )
        .order_by(HubTimeseries.timestamp.desc())
        .limit(1)
        .all()
    )

    zone_load = {}
    if recent_rows:
        row = recent_rows[0]
        zone_load = {
            "medical": row.zone_medical_load,
            "fragile": row.zone_fragile_load,
            "general": row.zone_general_load,
        }

    alerts = (
        db.query(Alert)
        .filter(
            Alert.hub_id == hub.id,
            Alert.tenant_id == hub.tenant_id,
            Alert.timestamp >= since,
        )
        .order_by(Alert.timestamp.desc())
        .limit(20)
        .all()
    )

    alerts_payload = [
        {
            "id": a.id,
            "timestamp": a.timestamp.isoformat(),
            "type": a.type,
            "message": a.message,
            "severity": a.severity,
        }
        for a in alerts
    ]

    return {
        "timestamp": datetime.utcnow().isoformat(),
        "kpis": kpis,
        "robots": robot_payload,
        "zone_load": zone_load,
        "alerts": alerts_payload,
    }


@router.get("/stream/hub/{hub_id}")
async def stream_hub(
    hub_id: int,
    token_data=Depends(get_current_token_data),
    db: Session = Depends(get_db),
):
    hub = (
        db.query(Hub)
        .filter(Hub.id == hub_id, Hub.tenant_id == token_data.tenant_id)
        .first()
    )
    if not hub:
        return StreamingResponse(iter(()), media_type="text/event-stream")

    async def event_generator():
        while True:
            payload = await _build_payload(db, hub)
            data = json.dumps(payload)
            yield f"data: {data}\n\n"
            await asyncio.sleep(3.0)

    return StreamingResponse(event_generator(), media_type="text/event-stream")

