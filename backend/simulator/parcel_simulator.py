import random
import time
from datetime import datetime

from .. import db
from . import decision_engine


PARCEL_TYPES = [
    "Medical Supplies",
    "Fragile Glassware",
    "Electronics",
    "Perishable Goods",
    "Heavy Machinery",
    "General Merchandise",
]

PRIORITIES = ["High", "Medium", "Low"]


class ParcelSimulator:
    def __init__(self, sim_engine, tick_seconds=3.0):
        self.sim_engine = sim_engine
        self.tick_seconds = tick_seconds
        self.running = True
        self.latest_volume = None

    def update_prediction_volume(self, volume):
        self.latest_volume = volume

    def _generate_parcel(self, idx):
        ptype = random.choice(PARCEL_TYPES)
        priority = random.choices(PRIORITIES, weights=[0.25, 0.5, 0.25])[0]
        zone = decision_engine.assign_zone(ptype, priority)
        pid = f"P{int(time.time())}{idx:02d}"

        parcel = {
            "id": pid,
            "type": ptype,
            "priority": priority,
            "zone": zone,
            "status": "intake",
            "assigned_robot": None,
            "created_at": datetime.utcnow().isoformat(timespec="seconds"),
        }
        return parcel

    def step(self):
        rate = decision_engine.get_ingestion_rate(self.latest_volume)
        count = rate["parcels_per_tick"]

        batch = []
        for i in range(count):
            parcel = self._generate_parcel(i)
            db.insert_parcel(parcel)
            batch.append(
                {
                    "id": parcel["id"],
                    "zone": parcel["zone"],
                    "priority": parcel["priority"],
                    "weight_kg": random.uniform(3.0, 15.0),
                    "destination_city": "Hub",
                    "status": "Intake",
                }
            )

        if batch:
            self.sim_engine.add_parcels(batch)
            decision_engine.log_action(
                f"Auto-ingested {len(batch)} parcels ({rate['label']})",
                source="ParcelSimulator",
            )

    def run_loop(self):
        while self.running:
            self.step()
            time.sleep(self.tick_seconds)

