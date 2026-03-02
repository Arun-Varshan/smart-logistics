import requests
import time
import json

BASE_URL = "http://127.0.0.1:5000"

def run_full_flow():
    print("=== PHASE 0: FORECAST & PREPARATION ===")
    # Login
    login_resp = requests.post(f"{BASE_URL}/auth/login", json={"email": "admin@wiztric.demo", "password": "admin123"})
    if login_resp.status_code != 200:
        print(f"Login failed: {login_resp.status_code} {login_resp.text}")
        return
    token = login_resp.json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}
    
    # Advisor Greeting
    greeting_resp = requests.get(f"{BASE_URL}/api/ai/advisor_greeting", headers=headers)
    if greeting_resp.status_code != 200:
        print(f"Greeting failed: {greeting_resp.status_code} {greeting_resp.text}")
        return
    print(f"Advisor: {greeting_resp.json()['greeting']}")
    
    print("\n=== PHASE 1: PARCEL RECEIVING STATION (CSV UPLOAD) ===")
    with open("daily_manifest.csv", "rb") as f:
        upload_resp = requests.post(f"{BASE_URL}/upload_csv", headers=headers, files={"file": f})
    if upload_resp.status_code != 200:
        print(f"Upload failed: {upload_resp.status_code} {upload_resp.text}")
        return
    print(f"Upload Status: {upload_resp.json()['message']}")
    
    print("Waiting for ingestion (5s)...")
    time.sleep(5) 
    
    print("\n=== PHASE 2: CENTRAL COMMAND (START PROCESSING) ===")
    process_resp = requests.post(f"{BASE_URL}/api/admin/initiate_processing", headers=headers)
    if process_resp.status_code != 200:
        print(f"Central Command Error: {process_resp.status_code} {process_resp.text}")
        # Try to find why - check parcel status
        parcels = requests.get(f"{BASE_URL}/api/intake/parcels", headers=headers).json()
        print(f"DEBUG: Recent parcels status: {[p['status'] for p in parcels['parcels'][:5]]}")
        return
    print(f"Central Command: {process_resp.json()['message']}")
    
    print("\n=== PHASE 3-5: QOS & ROBOTICS MOVEMENT (LIVE LOGS) ===")
    for i in range(6):
        time.sleep(5)
        logs = requests.get(f"{BASE_URL}/api/admin/notifications", headers=headers).json()
        print(f"--- Iteration {i+1} Logs ---")
        for log in logs["notifications"][-3:]:
            print(f"[{log['timestamp']}] {log['message']}")
            
    print("\n=== PHASE 6: DELIVERY DISPATCH ===")
    delivery_resp = requests.post(f"{BASE_URL}/api/admin/initiate_delivery", headers=headers)
    if delivery_resp.status_code != 200:
        print(f"Delivery Error: {delivery_resp.status_code} {delivery_resp.text}")
    else:
        print(f"Logistics: {delivery_resp.json()['message']}")
    
    print("\n=== PHASE 8: CUSTOMER TRACKING ===")
    time.sleep(5)
    track_resp = requests.get(f"{BASE_URL}/api/customer/track?parcel_id=P10001", headers=headers)
    print(f"Parcel P10001 Tracking: {json.dumps(track_resp.json(), indent=2)}")

if __name__ == "__main__":
    run_full_flow()
