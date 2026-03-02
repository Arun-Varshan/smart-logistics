import random

try:
    import networkx as nx
except ImportError:
    nx = None

from datetime import datetime

from .. import db


def get_robot_count_for_volume(volume):
    if volume is None:
        return 5
    if volume < 800:
        return 5
    if volume <= 1500:
        return 8
    if volume <= 2200:
        return 10
    return 12


def get_ingestion_rate(volume):
    if volume is None:
        return {"label": "medium", "parcels_per_tick": 2}
    if volume < 800:
        return {"label": "low", "parcels_per_tick": 1}
    if volume <= 1500:
        return {"label": "medium", "parcels_per_tick": 2}
    return {"label": "high", "parcels_per_tick": 3}


def assign_zone(parcel_type, priority):
    ptype = (parcel_type or "").lower()
    if "medical" in ptype:
        return "Medical"
    if "fragile" in ptype:
        return "Fragile"
    if "electronics" in ptype:
        return "Electronics"
    if "perishable" in ptype:
        return "Fragile" # Defaulting perishable to Fragile if no Perishable zone
    if "heavy" in ptype:
        return "General"
    if (priority or "").lower() == "high":
        return "Fragile" # Priority items to Fragile for careful handling
    return "General"


def assign_zone_balanced(parcel_type, priority, predicted_volume, zone_loads):
    base_zone = assign_zone(parcel_type, priority)
    
    # Simple overflow logic: if zone has more than 50 parcels, overflow to General
    if zone_loads.get(base_zone, 0) >= 50 and base_zone != "General":
        return "General"
        
    return base_zone


def optimize_routes(zones):
    if not zones:
        return {
            "routes": [],
            "co2_savings_percent": 0,
            "updated_at": datetime.utcnow().isoformat(),
        }

    zone_names = list(zones.keys())

    if nx is None or len(zone_names) <= 1:
        routes = [f"Route {chr(65 + i)}: Dock -> {name} -> Dock" for i, name in enumerate(zone_names[:4])]
        return {
            "routes": routes,
            "co2_savings_percent": random.randint(10, 30),
            "updated_at": datetime.utcnow().isoformat(),
        }

    g = nx.Graph()
    g.add_node("Dock")
    for name in zone_names:
        g.add_node(name)
        g.add_edge("Dock", name, weight=1)

    routes = []
    for idx, name in enumerate(zone_names[:6]):
        try:
            path = nx.dijkstra_path(g, "Dock", name)
            path_back = list(reversed(path[:-1]))
            full_path = path + path_back
            routes.append(f"Route {chr(65 + idx)}: " + " -> ".join(full_path))
        except Exception:
            routes.append(f"Route {chr(65 + idx)}: Dock -> {name} -> Dock")

    savings = random.randint(12, 28)
    return {
        "routes": routes,
        "co2_savings_percent": savings,
        "updated_at": datetime.utcnow().isoformat(),
    }


def log_action(message, source):
    msg = f"[{source}] {message}"
    db.insert_log(msg, source=source)
    return msg
