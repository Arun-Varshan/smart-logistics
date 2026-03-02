import pandas as pd
import random
import os
from datetime import datetime, timedelta

def generate_csv(filename, rows=400):
    zones = ["Medical", "Fragile", "Electronics", "Perishable", "Heavy", "General"]
    cities = ["New Delhi", "Mumbai", "Bangalore", "Hyderabad", "Chennai", "Kolkata", "Pune"]
    priorities = ["High", "Medium", "Low"]
    
    data = []
    base_time = datetime.now()
    
    for i in range(rows):
        parcel_id = f"P-{random.randint(10000, 99999)}"
        zone = random.choice(zones)
        city = random.choice(cities)
        priority = random.choice(priorities)
        weight = round(random.uniform(0.5, 25.0), 2)
        volume = round(random.uniform(0.1, 2.0), 2)
        
        # Staggered creation times
        created_at = (base_time + timedelta(minutes=i)).isoformat(timespec="seconds")
        
        data.append({
            "parcel_id": parcel_id,
            "zone": zone,
            "priority": priority,
            "destination_city": city,
            "weight_kg": weight,
            "volume": volume,
            "created_at": created_at,
            "damage_flag": 1 if random.random() < 0.05 else 0
        })
    
    df = pd.DataFrame(data)
    df.to_csv(filename, index=False)
    print(f"Generated {filename} with {rows} rows.")

if __name__ == "__main__":
    output_dir = "data_samples"
    os.makedirs(output_dir, exist_ok=True)
    
    for i in range(1, 5):
        fname = os.path.join(output_dir, f"daily_operations_batch_{i}.csv")
        generate_csv(fname, 400)
