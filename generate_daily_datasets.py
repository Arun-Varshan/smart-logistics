import pandas as pd
import random
import os
from datetime import datetime, timedelta

# Configuration
OUTPUT_DIR = "generated_manifests"
NUM_DATASETS = 20
ROWS_PER_DATASET = 100

ZONES = ["Medical", "Fragile", "Electronics", "Perishable", "Heavy", "General"]
PRIORITIES = ["Low", "Medium", "High"]
CITIES = ["Delhi", "Mumbai", "Bangalore", "Chennai", "Kolkata", "Hyderabad", "Pune", "Ahmedabad"]

def generate_dataset(dataset_id):
    data = []
    start_time = datetime(2026, 2, 27, 7, 30, 0)
    
    for i in range(ROWS_PER_DATASET):
        parcel_id = f"P-{random.randint(10000, 99999)}"
        zone = random.choice(ZONES)
        priority = random.choice(PRIORITIES)
        city = random.choice(CITIES)
        weight = round(random.uniform(1.0, 50.0), 2)
        volume = round(random.uniform(0.1, 2.5), 2)
        created_at = (start_time + timedelta(minutes=i)).isoformat(timespec="seconds")
        status = "RECEIVED_AT_HUB"
        amount = round(random.uniform(200.0, 2500.0), 2)
        godown = f"Godown-{city[:3].upper()}"
        
        data.append({
            "id": parcel_id,
            "zone": zone,
            "priority": priority,
            "destination_city": city,
            "weight_kg": weight,
            "volume_m3": volume,
            "created_at": created_at,
            "status": status,
            "amount_to_pay": amount,
            "godown": godown,
            "damage_flag": 0
        })
    
    df = pd.DataFrame(data)
    filename = f"daily_manifest_batch_{dataset_id}.csv"
    filepath = os.path.join(OUTPUT_DIR, filename)
    df.to_csv(filepath, index=False)
    print(f"Generated {filepath}")

if __name__ == "__main__":
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
    
    for i in range(1, NUM_DATASETS + 1):
        generate_dataset(i)
    
    print(f"\nSuccessfully generated {NUM_DATASETS} datasets in '{OUTPUT_DIR}' folder.")
