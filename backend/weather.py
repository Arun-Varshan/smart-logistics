import json
import time
from typing import Any, Dict, Optional

try:
    import redis  # type: ignore
except Exception:
    redis = None

import requests

CACHE_TTL_SECONDS = 600
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"


class WeatherCache:
    def __init__(self) -> None:
        self._mem: Dict[str, Dict[str, Any]] = {}
        self._r = None
        if redis is not None:
            try:
                self._r = redis.Redis(host="localhost", port=6379, db=0)
                self._r.ping()
            except Exception:
                self._r = None

    def get(self, key: str) -> Optional[Dict[str, Any]]:
        if self._r:
            try:
                val = self._r.get(key)
                if val:
                    return json.loads(val.decode("utf-8"))
            except Exception:
                pass
        item = self._mem.get(key)
        if item and (time.time() - item.get("_ts", 0)) < CACHE_TTL_SECONDS:
            return item
        return None

    def set(self, key: str, value: Dict[str, Any]) -> None:
        value["_ts"] = time.time()
        if self._r:
            try:
                self._r.setex(key, CACHE_TTL_SECONDS, json.dumps(value))
                return
            except Exception:
                pass
        self._mem[key] = value


cache = WeatherCache()


def fetch_weather(lat: float, lon: float) -> Dict[str, Any]:
    key = f"wx:{lat:.3f}:{lon:.3f}"
    cached = cache.get(key)
    if cached:
        return cached
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m,precipitation",
        "timezone": "Asia/Kolkata",
    }
    try:
        resp = requests.get(OPEN_METEO_URL, params=params, timeout=8)
        resp.raise_for_status()
        data = resp.json()
    except Exception:
        data = {"error": "weather_unavailable"}
    cache.set(key, data)
    return data
