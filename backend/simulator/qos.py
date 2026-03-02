import random
from datetime import datetime, timedelta

SEVERITIES = ["minor", "moderate", "severe"]
ZONES = ["General", "Fragile", "Medical", "Quarantine"]

def gen_qos_records(n: int = 120):
    now = datetime.utcnow()
    records = []
    for i in range(n):
        ts = (now - timedelta(minutes=random.randint(1, 720))).isoformat(timespec="seconds")
        parcel_id = f"P{10000 + i}"
        zone = random.choice(ZONES)
        damaged = random.random() < (0.18 if zone != "Quarantine" else 0.65)
        severity = random.choices(SEVERITIES, weights=[60, 30, 10])[0] if damaged else "none"
        confidence = round(random.uniform(0.55, 0.98), 2) if damaged else 0.0
        robot_id = f"R-{random.randint(1, 12)}"
        records.append({
            "timestamp": ts,
            "parcel_id": parcel_id,
            "zone": zone,
            "damaged": damaged,
            "severity": severity,
            "confidence": confidence,
            "robot_id": robot_id,
        })
    return records

def summarize_qos(records):
    total = len(records)
    dmg = len([r for r in records if r["damaged"]])
    crit = len([r for r in records if r["severity"] == "severe"])
    ratio = round((dmg / total * 100.0), 1) if total else 0.0
    zone_counts = {}
    robot_counts = {}
    severity_counts = {"minor": 0, "moderate": 0, "severe": 0}
    for r in records:
        zone_counts[r["zone"]] = zone_counts.get(r["zone"], 0) + (1 if r["damaged"] else 0)
        robot_counts[r["robot_id"]] = robot_counts.get(r["robot_id"], 0) + (1 if r["damaged"] else 0)
        if r["damaged"] and r["severity"] in severity_counts:
            severity_counts[r["severity"]] += 1
    top_zones = sorted(zone_counts.items(), key=lambda x: x[1], reverse=True)[:3]
    return {
        "total": total,
        "damaged_ratio": ratio,
        "safe_count": total - dmg,
        "critical_count": crit,
        "top_zones": [z for z, _ in top_zones],
        "zone_counts": zone_counts,
        "robot_counts": robot_counts,
        "severity_counts": severity_counts,
    }
