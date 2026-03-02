import requests
import json

BASE_URL = "http://127.0.0.1:5000"

def test_endpoint(endpoint):
    try:
        url = f"{BASE_URL}{endpoint}"
        print(f"Testing {url}...")
        resp = requests.get(url)
        print(f"Status: {resp.status_code}")
        try:
            data = resp.json()
            print(f"Response (first 100 chars): {str(data)[:200]}")
            return data
        except:
            print(f"Response text: {resp.text[:200]}")
            return None
    except Exception as e:
        print(f"Error: {e}")
        return None

print("--- DIAGNOSTICS ---")
test_endpoint("/forecast")
test_endpoint("/digital_twin_state")
test_endpoint("/parcels")
test_endpoint("/robots")
print("-------------------")
