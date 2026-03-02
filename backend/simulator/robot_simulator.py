import time

from .. import db


def _compute_robot_utilization(robots):
    if not robots:
        return []

    active_states = {"moving_to_zone", "delivering", "returning"}
    result = []
    for r in robots:
        status = getattr(r, "status", "idle")
        active = status in active_states
        utilization = 100.0 if active else 20.0 if status == "idle" else 0.0
        result.append(
            {
                "id": getattr(r, "id", None),
                "status": status,
                "assigned_parcel": None,
                "current_zone": getattr(r, "target_zone", None),
                "utilization_percentage": utilization,
            }
        )
    return result


def sync_robots_to_db(sim_engine, interval_seconds=3.0):
    while True:
        robots = getattr(sim_engine, "robots", [])
        for r in _compute_robot_utilization(robots):
            db.upsert_robot(r)
        time.sleep(interval_seconds)

