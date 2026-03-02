import pandas as pd
import random
from datetime import datetime

# -----------------------------
# Column definitions (as asked)
# -----------------------------
columns = [
    "ds",                 # Date of arrival (Day-Month-Year)
    "parcel_id",          # Unique parcel ID
    "volume",             # Number of items
    "zone",               # Target zone
    "weight_kg",          # Weight of parcel
    "destination_city",   # Destination city
    "priority",           # Processing priority
    "image_path"          # Optional image reference
]

# -----------------------------
# Sample values
# -----------------------------
zones = ["Medical", "Fragile", "Electronics", "Perishable", "Heavy", "General"]
cities = ["Hyderabad", "Delhi", "Mumbai", "Kolkata", "Bangalore", "Chennai"]
priorities = ["High", "Medium", "Low"]

data = []
today = datetime.now().strftime("%d-%m-%Y")

# -----------------------------
# Generate sample rows
# -----------------------------
for i in range(1, 21):  # 20 parcels
    parcel_id = f"P{i:05d}"
    volume = 1
    zone = random.choice(zones)
    weight_kg = round(random.uniform(0.5, 30.0), 2)
    destination_city = random.choice(cities)

    # Priority logic
    if zone in ["Medical", "Perishable"]:
        priority = "High"
    elif zone == "Fragile":
        priority = random.choice(["High", "Medium"])
    else:
        priority = random.choice(priorities)

    image_path = f"parcel_{i}.jpg"  # optional reference

    data.append([
        today,
        parcel_id,
        volume,
        zone,
        weight_kg,
        destination_city,
        priority,
        image_path
    ])

# -----------------------------
# Create DataFrame and save CSV
# -----------------------------
df = pd.DataFrame(data, columns=columns)
df.to_csv("parcel_operations_data.csv", index=False)

print("✅ File created: parcel_operations_data.csv")
print(df.head())
