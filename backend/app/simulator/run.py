import math
import threading
import time
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

from sqlalchemy.orm import Session

from ..database import SessionLocal
from ..models import Alert, Hub, HubTimeseries, Robot, Tenant


class SimulatorState:
    def __init__(self):
        self.thread = None
        self.running = False


state = SimulatorState()


def _get_or_create_default_tenant_and_hub(db: Session) -> Tuple[Tenant, Hub, List[Robot]]:
    tenant = db.query(Tenant).filter(Tenant.name == "Wiztric Demo").first()
    if not tenant:
        tenant = Tenant(
            name="Wiztric Demo",
            company_name="Wiztric Technologies – Smart Logistics Hub",
            api_key="wiztric-demo-key",
        )
        db.add(tenant)
        db.flush()

    hub = (
        db.query(Hub)
        .filter(Hub.tenant_id == tenant.id)
        .order_by(Hub.id.asc())
        .first()
    )
    if not hub:
        hub = Hub(
            name="Demo Hub 1",
            location="Wiztric Demo Location",
            tenant_id=tenant.id,
        )
        db.add(hub)
        db.flush()

    robots = (
        db.query(Robot)
        .filter(Robot.tenant_id == tenant.id, Robot.hub_id == hub.id)
        .order_by(Robot.id.asc())
        .all()
    )
    if not robots:
        robots = []
        for i in range(5):
            r = Robot(
                name=f"R-{i+1}",
                hub_id=hub.id,
                tenant_id=tenant.id,
                status="idle",
                x=float(5 * i),
                y=0.0,
                utilization_pct=0.0,
            )
            db.add(r)
            robots.append(r)
        db.flush()

    db.commit()
    for r in robots:
        db.refresh(r)
    db.refresh(tenant)
    db.refresh(hub)
    return tenant, hub, robots


def _volume_factor_for_now(now: datetime) -> float:
    hour = now.hour
    weekday = now.weekday()

    if 6 <= hour < 9:
        base = 0.6
    elif 10 <= hour < 16:
        base = 1.0
    elif 17 <= hour < 20:
        base = 0.6
    else:
        base = 0.2

    if weekday < 5:
        base *= 1.0
    elif weekday == 6:
        base *= 0.5
    else:
        base *= 0.7

    return base


def _zone_distribution(factor: float) -> Dict[str, int]:
    total = int(50 * factor) + 1
    medical = int(total * 0.15)
    fragile = int(total * 0.25)
    general = total - medical - fragile
    return {
        "medical": medical,
        "fragile": fragile,
        "general": general,
    }


def _simulate_tick(db: Session, tenant: Tenant, hub: Hub, robots: List[Robot]) -> None:
    now = datetime.utcnow()
    factor = _volume_factor_for_now(now)
    zones = _zone_distribution(factor)

    active_robots = max(1, min(len(robots), int(1 + factor * len(robots))))

    base_parcels = int(40 * factor)
    processed = int(base_parcels * 0.8)
    total_parcels = base_parcels + processed

    utilization = min(1.0, factor + 0.2) * 100.0
    avg_processing_time = max(1.0, 5.0 - factor * 2.0)

    for idx, robot in enumerate(robots):
        angle = (now.timestamp() / 30.0) + idx
        robot.x = 10.0 * math.cos(angle) + idx * 2.0
        robot.y = 10.0 * math.sin(angle)
        robot.status = "active" if idx < active_robots else "idle"
        robot.utilization_pct = utilization if idx < active_robots else utilization * 0.3
        robot.last_update = now
        db.add(robot)

    overload = any(v > 60 for v in zones.values())
    if overload:
        message = "Zone overload detected: " + ", ".join(
            f"{name}={value}" for name, value in zones.items()
        )
        alert = Alert(
            hub_id=hub.id,
            tenant_id=tenant.id,
            timestamp=now,
            type="ZONE_OVERLOAD",
            message=message,
            severity="high",
        )
        db.add(alert)

    record = HubTimeseries(
        hub_id=hub.id,
        tenant_id=tenant.id,
        timestamp=now,
        total_parcels=total_parcels,
        parcels_processed=processed,
        active_robots=active_robots,
        avg_processing_time=avg_processing_time,
        zone_medical_load=zones["medical"],
        zone_fragile_load=zones["fragile"],
        zone_general_load=zones["general"],
        utilization_pct=utilization,
    )
    db.add(record)
    db.commit()


def _warmup_history(db: Session, tenant: Tenant, hub: Hub, robots: List[Robot]) -> None:
    count = (
        db.query(HubTimeseries)
        .filter(HubTimeseries.tenant_id == tenant.id, HubTimeseries.hub_id == hub.id)
        .count()
    )
    if count >= 20:
        return

    now = datetime.utcnow()
    start_time = now - timedelta(minutes=19)
    for i in range(20):
        t = start_time + timedelta(minutes=i)
        factor = _volume_factor_for_now(t)
        zones = _zone_distribution(factor)
        active_robots = max(1, min(len(robots), int(1 + factor * len(robots))))
        base_parcels = int(40 * factor)
        processed = int(base_parcels * 0.8)
        total_parcels = base_parcels + processed
        utilization = min(1.0, factor + 0.2) * 100.0
        avg_processing_time = max(1.0, 5.0 - factor * 2.0)

        record = HubTimeseries(
            hub_id=hub.id,
            tenant_id=tenant.id,
            timestamp=t,
            total_parcels=total_parcels,
            parcels_processed=processed,
            active_robots=active_robots,
            avg_processing_time=avg_processing_time,
            zone_medical_load=zones["medical"],
            zone_fragile_load=zones["fragile"],
            zone_general_load=zones["general"],
            utilization_pct=utilization,
        )
        db.add(record)

    db.commit()


def simulator_loop() -> None:
    db = SessionLocal()
    try:
        tenant, hub, robots = _get_or_create_default_tenant_and_hub(db)
        _warmup_history(db, tenant, hub, robots)

        state.running = True
        while state.running:
            _simulate_tick(db, tenant, hub, robots)
            time.sleep(3.0)
    finally:
        db.close()
        state.running = False


def start_simulator_thread() -> None:
    if state.thread and state.thread.is_alive():
        return
    t = threading.Thread(target=simulator_loop, daemon=True)
    state.thread = t
    t.start()


def is_simulator_running() -> bool:
    return bool(state.thread and state.thread.is_alive())

