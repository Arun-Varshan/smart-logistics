import os
import json
import time
import random
import pickle
import joblib
from datetime import datetime, timedelta, timezone
from threading import Thread, Lock, Event
import queue

# --- SYSTEM EVENT BRIDGE ---
class SystemEventBridge:
    def __init__(self):
        self.listeners = []
        self.lock = Lock()

    def subscribe(self):
        q = queue.Queue(maxsize=50)
        with self.lock:
            self.listeners.append(q)
        return q

    def emit(self, event_type, data):
        payload = {"type": event_type, "data": data, "timestamp": datetime.utcnow().isoformat()}
        with self.lock:
            for q in self.listeners[:]:
                try:
                    q.put_nowait(payload)
                except queue.Full:
                    self.listeners.remove(q)

event_bridge = SystemEventBridge()
from functools import wraps

from flask import Flask, request, jsonify, g, send_from_directory, Response
from flask_cors import CORS
import pandas as pd
import numpy as np
try:
    from prometheus_client import Counter, Gauge, generate_latest, CONTENT_TYPE_LATEST
    PROMETHEUS_ENABLED = True
except Exception:
    PROMETHEUS_ENABLED = False
    class Counter:  # type: ignore
        def labels(self, **kwargs):
            return self
        def inc(self, n=1):
            pass
    class Gauge:  # type: ignore
        def set(self, v):
            pass
    def generate_latest():  # type: ignore
        return b""
    CONTENT_TYPE_LATEST = "text/plain"  # type: ignore
try:
    import structlog
    HAS_STRUCTLOG = True
except Exception:
    HAS_STRUCTLOG = False

from jose import jwt, JWTError
from passlib.context import CryptContext

try:
    from ultralytics import YOLO
    HAS_YOLO = True
except Exception as e:
    YOLO = None
    HAS_YOLO = False
    print(f"Damage model disabled (ultralytics/torch import failed: {e})")

try:
    from .simulation import sim_engine
except Exception:
    try:
        from simulation import sim_engine
    except Exception:
        raise

try:
    from . import db
    from .simulator import ParcelSimulator, decision_engine, robot_simulator
except Exception:
    import db
    from simulator import ParcelSimulator, decision_engine, robot_simulator
from zoneinfo import ZoneInfo

# --- 1. CONFIGURATION & SETUP ---
app = Flask(__name__)
# Maximum permissiveness for local development to ensure zero connection issues
CORS(app, resources={r"/*": {
    "origins": "*",
    "methods": ["GET", "POST", "OPTIONS"],
    "allow_headers": ["Content-Type", "Authorization", "X-Tenant-Id", "X-Role"]
}})
import logging
if HAS_STRUCTLOG:
    structlog.configure(wrapper_class=structlog.make_filtering_bound_logger(logging.INFO))
    logger = structlog.get_logger("wiztric")
else:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("wiztric")

DATA_DIR = "data"
UPLOAD_DIR = "uploads"
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(UPLOAD_DIR, exist_ok=True)
DATA_FILE = os.path.join(DATA_DIR, "parcel_history.csv")
DAILY_PARCELS_PATH = os.path.join(DATA_DIR, "daily_parcels.csv")

SIMULATION_ENABLED = os.environ.get("SIMULATION_ENABLED", "true").lower() == "true"
SIM_TICK_SECONDS = float(os.environ.get("SIMULATION_TICK_SECONDS", "3.0"))
IST = ZoneInfo("Asia/Kolkata")
DEFAULT_COMPANY_ID = os.environ.get("COMPANY_ID", "demo_company")

JWT_SECRET = os.environ.get("WIZTRIC_JWT_SECRET", "CHANGE_ME_SUPER_SECRET_32+_CHARS")
JWT_ALG = "HS256"
ACCESS_TOKEN_MIN = 30

pwd_context = CryptContext(schemes=["pbkdf2_sha256"], deprecated="auto")

DAMAGE_MODEL_PATH = os.environ.get("DAMAGE_MODEL_PATH", r"C:\Users\Omen\Downloads\Parcel Box Damage Classification.v2i.yolov8\runs\detect\train2\weights\best.pt")
damage_model = None
damage_registry = set()
scan_history = []
sim_qos_records = []

# Prometheus metrics
REQ_COUNTER = Counter("wiztric_requests_total", "Total API requests", ["endpoint"])
PARCEL_GAUGE = Gauge("wiztric_parcels_total", "Total parcels (recent)")
ROBOT_GAUGE = Gauge("wiztric_robots_total", "Total robots")


def get_damage_model():
    global damage_model
    if not HAS_YOLO:
        return None
    if damage_model is None:
        if os.path.exists(DAMAGE_MODEL_PATH):
            damage_model = YOLO(DAMAGE_MODEL_PATH)
            print(f"Loaded YOLOv8 damage model from {DAMAGE_MODEL_PATH}")
        else:
            print(f"Damage model not found at {DAMAGE_MODEL_PATH}")
            return None
    return damage_model


def detect_damage_image(path, conf_thr=0.25):
    model = get_damage_model()
    if not model:
        # Fallback to random if model fails to load for demo purposes
        dmg = random.random() < 0.2
        return {
            "damaged": dmg,
            "confidence": round(random.uniform(0.6, 0.9), 2) if dmg else 0.0,
            "severity": random.choice(["minor", "moderate", "severe"]) if dmg else "none",
            "annotated_path": None
        }
    
    results = model(path, conf=conf_thr, imgsz=640, verbose=False)
    max_conf = 0.0
    first = None
    for r in results:
        if first is None:
            first = r
        if getattr(r, "boxes", None) is None:
            continue
        confs = r.boxes.conf.cpu().numpy().tolist()
        if confs:
            cmax = max(confs)
            if cmax > max_conf:
                max_conf = cmax
    
    damaged = max_conf >= conf_thr
    severity = "none"
    if damaged:
        if max_conf > 0.8: severity = "severe"
        elif max_conf > 0.5: severity = "moderate"
        else: severity = "minor"

    annotated_path = None
    if first is not None:
        try:
            import cv2
            os.makedirs(UPLOAD_DIR, exist_ok=True)
            fname = f"ann_{os.path.basename(path)}"
            out_path = os.path.join(UPLOAD_DIR, fname)
            im = first.plot()
            cv2.imwrite(out_path, im) # Save to UPLOAD_DIR
            annotated_path = f"/uploads/{fname}"
            
            # STORE IN DB (Persistent history)
            parcel_id = os.path.basename(path).split('_')[1] if '_' in os.path.basename(path) else "P-UNKNOWN"
            db.insert_log(f"QOS SCAN: Parcel {parcel_id} {severity.upper()} damage detected (conf {max_conf:.2f}). Image: {annotated_path}", source="QOS_AGENT")
            
        except Exception as e:
            print(f"Failed to write annotated damage image or log to DB: {e}")
            
    return {
        "damaged": damaged, 
        "confidence": float(max_conf), 
        "severity": severity,
        "annotated_path": annotated_path
    }

# Global State for Simulation
simulation_state = {
    "logs": [],
    "parcels": []
}
state_lock = Lock()
route_state = {"routes": [], "co2_savings_percent": 0, "updated_at": None}
intake_index = 0
threads_started = False
parcel_simulator = None
latest_tomorrow_volume = None
last_stream_cache = None
last_stream_lock = Lock()

# --- 2. MODULE IMPORTS ---

# Global flag to ensure threads only start once
_threads_initialized = False

@app.before_request
def start_threads_on_first_request():
    global _threads_initialized
    if not _threads_initialized:
        print("Initializing background workers for the first time...")
        start_background_workers()
        _threads_initialized = True

# B. Prophet for Forecasting
try:
    from prophet import Prophet
    from prophet.serialize import model_from_json
    HAS_PROPHET = True
except ImportError:
    HAS_PROPHET = False
    print("Warning: 'prophet' not installed. Using mock forecasting.")

# C. OR-Tools for Route Optimization
try:
    from ortools.constraint_solver import routing_enums_pb2
    from ortools.constraint_solver import pywrapcp
    HAS_ORTOOLS = True
except ImportError:
    HAS_ORTOOLS = False
    print("Warning: 'ortools' not installed. Using mock routing.")

# --- 3. AGENTS ---

class Agent:
    def __init__(self, name, role):
        self.name = name
        self.role = role

    def log(self, message, company_id=None):
        entry = f"[{datetime.now().strftime('%H:%M:%S')}] [{self.name}] {message}"
        with state_lock:
            simulation_state["logs"].append(entry)
            if len(simulation_state["logs"]) > 50:
                simulation_state["logs"].pop(0)
        db.insert_log(entry, source=self.name, company_id=company_id)
        return entry


class PredictionAgent(Agent):
    def analyze(self, forecast_val):
        if forecast_val > 1500:
            self.log(f"High volume predicted ({forecast_val}). Requesting more robots.")
            return "increase_robots"
        return "normal_operations"


class AssignmentAgent(Agent):
    def assign_zone(self, parcel_type, priority):
        return decision_engine.assign_zone(parcel_type, priority)


class OptimizationAgent(Agent):
    def optimize(self, zones):
        state = decision_engine.optimize_routes(zones)
        self.log(
            f"Optimized routes. CO2 savings: {state['co2_savings_percent']}%"
        )
        return state


class DetectionAgent(Agent):
    def inspect(self, parcel_id, damaged, confidence):
        status = "damaged" if damaged else "clear"
        self.log(f"Parcel {parcel_id} {status} (conf {confidence:.2f})")


agents = {
    "predictor": PredictionAgent("LogiMind AI", "Prediction"),
    "assigner": AssignmentAgent("ParcelSense AI", "Assignment"),
    "optimizer": OptimizationAgent("Wiztric Flow AI", "Optimization"),
    "detector": DetectionAgent("Damage QOS Agent", "Quality"),
}


def hash_password(p):
    return pwd_context.hash(p)


def verify_password(plain, hashed):
    import hashlib
    # Try SHA256 Hex (used for our specific seeded users)
    if hashlib.sha256(plain.encode()).hexdigest() == hashed:
        return True
    # Try PBKDF2 (if passlib is available and hashed starts with $)
    if hashed.startswith("$"):
        try:
            return pwd_context.verify(plain, hashed)
        except Exception:
            return False
    return False


def create_access_token(user_id, role="manager"):
    now = datetime.now(timezone.utc)
    payload = {
        "sub": user_id,
        "role": role,
        "type": "access",
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(minutes=ACCESS_TOKEN_MIN)).timestamp()),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALG)


def decode_token(token):
    return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALG])


def require_auth(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        REQ_COUNTER.labels(endpoint=request.path).inc()
        auth = request.headers.get("Authorization", "")
        token = None
        if auth.startswith("Bearer "):
            token = auth.split(" ", 1)[1]
        else:
            token = request.args.get("access_token")
        if not token:
            return jsonify({"error": "Missing token"}), 401
        try:
            payload = decode_token(token)
            if payload.get("type") != "access":
                return jsonify({"error": "Invalid token type"}), 401
        except JWTError:
            return jsonify({"error": "Invalid or expired token"}), 401
        req_role = (request.headers.get("X-Role") or "").lower()
        g.current_user = {"id": payload.get("sub"), "role": (req_role or (payload.get("role") or "").lower())}
        requested_tenant = request.headers.get("X-Tenant-Id") or request.args.get("tenant_id")
        requested_tenant = request.headers.get("X-Tenant-Id") or request.args.get("tenant_id")
        if not requested_tenant:
            user = db.get_user_by_id(payload.get("sub"))
            requested_tenant = (user or {}).get("company_id") or DEFAULT_COMPANY_ID
        g.company_id = requested_tenant
        return fn(*args, **kwargs)

    return wrapper

def ensure_role(*allowed):
    role = (getattr(g, "current_user", {}) or {}).get("role") or ""
    if role not in [r.lower() for r in allowed]:
        return jsonify({"error": "Forbidden", "required_roles": allowed}), 403
    return None

# --- DEMO SEEDER (DISABLED FOR CSV SOURCE OF TRUTH) ---
def seed_demo_if_needed():
    pass

@app.route("/metrics", methods=["GET"])
def metrics():
    return generate_latest(), 200, {"Content-Type": CONTENT_TYPE_LATEST}


def ensure_default_admin():
    user = db.get_user_by_email("admin@wiztric.demo")
    if user:
        return
    admin_user = {
        "id": "admin-1",
        "company_id": DEFAULT_COMPANY_ID,
        "email": "admin@wiztric.demo",
        "password_hash": hash_password("admin123"),
        "role": "admin",
    }
    db.upsert_user(admin_user)

def ensure_role_users():
    users = [
        ("intake@hub.com", "intake123", "intake"),
        ("qos@hub.com", "qos123", "qos"),
        ("robotics@hub.com", "robot123", "robotics"),
        ("logistics@hub.com", "log123", "logistics"),
        ("planning@hub.com", "plan123", "planning"),
        ("executive@hub.com", "exec123", "admin"),
    ]
    for email, pwd, role in users:
        u = db.get_user_by_email(email)
        if not u:
            db.upsert_user({
                "id": f"user-{role}",
                "company_id": DEFAULT_COMPANY_ID,
                "email": email,
                "password_hash": hash_password(pwd),
                "role": role,
            })


ensure_default_admin()
ensure_role_users()
try:
    from .simulator.qos import gen_qos_records, summarize_qos
except Exception:
    from backend.simulator.qos import gen_qos_records, summarize_qos
if not sim_qos_records:
    sim_qos_records = gen_qos_records(160)

# --- PROCESS DAILY UPLOAD (THREADED) ---
is_processing_daily = False

_auto_started_companies = set()

def auto_start_processing(company_id, max_count=100):
    try:
        # Avoid repeated kickoff per tenant during session
        if company_id in _auto_started_companies:
            return
        recent = db.get_recent_parcels(limit=500, company_id=company_id)
        # Select only fresh parcels
        candidates = [p for p in recent if (p.get("status") or "").upper() == "RECEIVED_AT_HUB"]
        if not candidates:
            return
        random.shuffle(candidates)
        to_process = candidates[:max_count]
        start_background_processing(to_process, company_id)
        _auto_started_companies.add(company_id)
        agents["predictor"].log(f"Auto-started processing for {len(to_process)} parcels on site load.", company_id=company_id)
    except Exception as e:
        print(f"Auto-start processing error: {e}")

def process_daily_upload(df, company_id):
    global is_processing_daily
    is_processing_daily = True
    batch_id = f"BATCH-{int(time.time())}"
    
    total_parcels = len(df)
    print(f"DEBUG: Starting upload of {total_parcels} parcels for {company_id}...")
    
    # Calculate KPIs directly from CSV (Phase 1)
    zones = {}
    priorities = {}
    total_weight = 0.0
    total_volume = 0.0
    
    # Generate mock vehicle info for inbound delivery (since it's not in CSV)
    vehicle_id = f"TRUCK-IN-{random.randint(100, 999)}"
    driver_name = random.choice(["Ramesh", "Suresh", "Vikram", "Anita", "Priya"])
    
    parcel_records = []
    for _, row in df.iterrows():
        try:
            z = str(row.get("zone", "General")).strip()
            p = str(row.get("priority", "Medium")).strip()
            w = float(row.get("weight_kg", 0.0))
            v = float(row.get("volume_m3", row.get("volume", 0.0)))
            
            zones[z] = zones.get(z, 0) + 1
            priorities[p] = priorities.get(p, 0) + 1
            total_weight += w
            total_volume += v
            
            parcel_id = str(row.get("parcel_id", row.get("id", f"P{random.randint(10000,99999)}"))).strip()
            dest = str(row.get("destination_city", "Unknown")).strip()
            godown = str(row.get("godown", f"Godown-{dest[:3].upper()}")).strip()
            amount = float(row.get("amount_to_pay", 500.0))
            dmg_flag = int(row.get("damage_flag", 0))

            parcel_record = {
                "id": parcel_id,
                "batch_id": batch_id,
                "company_id": company_id,
                "type": z,
                "priority": p,
                "zone": z,
                "status": "RECEIVED_AT_HUB",
                "quality_status": "PENDING",
                "delivery_status": "HUB_INTAKE",
                "volume": v,
                "weight_kg": w,
                "destination_city": dest,
                "godown": godown,
                "amount_to_pay": amount,
                "damage_flag": dmg_flag,
                "created_at": datetime.utcnow().isoformat(timespec="seconds"),
            }
            parcel_records.append(parcel_record)
        except Exception as row_err:
            print(f"DEBUG: Skipping invalid row: {row_err}")
    
    try:
        # Bulk insert for efficiency
        db.bulk_insert_parcels(parcel_records, company_id=company_id)
        # Store batch summary (Phase 1)
        db.insert_batch(batch_id, total_parcels, company_id=company_id, vehicle_id=vehicle_id, driver_name=driver_name)
        
        # Notify about clustering and vehicle assignment
        event_bridge.emit("PARCEL_BATCH_RECEIVED", {
            "batch_id": batch_id,
            "count": total_parcels,
            "vehicle_id": vehicle_id,
            "message": f"Daily parcels sorted by location. All {total_parcels} parcels assigned to vehicle {vehicle_id} for hub intake."
        })
        
        # Initialize simulation fleet based on this volume
        sim_engine.company_id = company_id
        sim_engine.update_forecast(total_parcels)
        
        agents["predictor"].log(
            f"New parcel batch received: {batch_id} from {vehicle_id}. Total: {total_parcels} parcels. "
            f"Weight: {total_weight:.1f}kg, Volume: {total_volume:.1f}. Status: RECEIVED_AT_HUB.",
            company_id=company_id
        )
        print(f"DEBUG: Successfully inserted {len(parcel_records)} parcels into {db.DB_TYPE} database.")
    except Exception as db_err:
        print(f"CRITICAL ERROR during DB insertion: {db_err}")
        import traceback
        traceback.print_exc()
    
    is_processing_daily = False
    # Notify Central Command
    agents["predictor"].log(f"Batch {batch_id} ready for processing trigger by Hub Operations Manager.", company_id=company_id)
    # Immediately kick off processing for all parcels from this batch
    try:
        batch_parcels = [ {"id": r["id"], "type": r["type"], "priority": r["priority"]} for r in parcel_records ]
        start_background_processing(batch_parcels, company_id)
        agents["predictor"].log(f"Auto-started robotic sorting for entire batch {batch_id} ({len(batch_parcels)} parcels).", company_id=company_id)
    except Exception as e:
        print(f"Auto-start after upload failed: {e}")

def run_qos_flow(parcel_id, ptype, prio, company_id=None):
    time.sleep(random.uniform(3, 7)) # Simulate processing delay
    p = db.get_parcel_by_id(parcel_id, company_id=company_id)
    if not p: return

    # QOS Inspection logic (Step 3)
    # 10% chance of damage if not already flagged in CSV
    is_damaged = bool(p.get("damage_flag", 0)) or (random.random() < 0.1)
    
    if is_damaged:
        roll = random.random()
        q_status = "CRITICAL_DAMAGE" if roll < 0.3 else "MINOR_DAMAGE"
    else:
        q_status = "SAFE"
        
    db.update_parcel_status(parcel_id, quality_status=q_status, company_id=company_id)
    agents["detector"].log(f"Parcel {parcel_id} inspection result: {q_status}", company_id=company_id)
    
    if q_status == "CRITICAL_DAMAGE":
        # Block from delivery and redirect robot (Step 3)
        agents["assigner"].log(f"ALERT: Parcel {parcel_id} CRITICAL DAMAGE. Redirecting to Quarantine.", company_id=company_id)
        for r in sim_engine.robots:
            if r.current_parcel and r.current_parcel["id"] == parcel_id:
                r.assign_task("Quarantine", r.current_parcel)
                break
    else:
        # Cleared for handling (Step 3)
        # We don't change status to DELIVERY_READY yet, because it must also be IN_ZONE (Step 6)
        pass

def run_robotics_flow(parcel_id, ptype, prio, company_id=None):
    # In a real system, this would wait for an available robot
    # For simulation, we add it to the sim engine which handles robot assignment
    sim_engine.add_parcels([{"id": parcel_id, "zone": ptype, "priority": prio, "status": "CLEARED_FOR_HANDLING", "company_id": company_id}])
    agents["assigner"].log(f"Parcel {parcel_id} cleared for robotic movement to {ptype} zone.", company_id=company_id)
    
    # The sim_engine will eventually move it to "IN ZONE" via on_parcel_delivered callback.
    pass

def run_delivery_flow(parcel_id, company_id=None):
    time.sleep(5)
    db.update_parcel_status(parcel_id, status="OUT FOR DELIVERY", delivery_status="IN_TRANSIT", company_id=company_id)
    agents["optimizer"].log(f"Parcel {parcel_id} is out for delivery.", company_id=company_id)
    
    time.sleep(random.uniform(15, 30))
    db.update_parcel_status(parcel_id, status="DELIVERED", delivery_status="COMPLETED", company_id=company_id)
    agents["optimizer"].log(f"Parcel {parcel_id} delivered successfully.", company_id=company_id)


# --- FORECAST LOGIC (ROBUST) ---
PRETRAINED_MODEL_PATH = os.environ.get(
    "PROPHET_MODEL_PATH",
    r"C:\Users\jayas\Downloads\prophet_parcel_model (1).pkl",
)
loaded_prophet_model = None

def load_or_train_model():
    global loaded_prophet_model
    if not HAS_PROPHET: return

    # 1. Try Loading Pretrained
    if os.path.exists(PRETRAINED_MODEL_PATH):
        try:
            with open(PRETRAINED_MODEL_PATH, 'rb') as f:
                loaded_prophet_model = pickle.load(f)
            print(f"Loaded pretrained model (pickle): {PRETRAINED_MODEL_PATH}")
            return
        except Exception as e:
            print(f"Pickle load failed: {e}. Trying joblib...")
            try:
                loaded_prophet_model = joblib.load(PRETRAINED_MODEL_PATH)
                print(f"Loaded pretrained model (joblib): {PRETRAINED_MODEL_PATH}")
                return
            except Exception as e2:
                print(f"Joblib load failed: {e2}")

    # 2. Train from CSV if Pretrained fails/missing
    if os.path.exists(DATA_FILE):
        try:
            print("Training Prophet model from CSV...")
            df = pd.read_csv(DATA_FILE)
            df.columns = [c.lower() for c in df.columns]
            if 'ds' in df.columns and ('y' in df.columns or 'volume' in df.columns):
                if 'y' not in df.columns:
                    df['y'] = df['volume']
                df['ds'] = pd.to_datetime(df['ds'], dayfirst=False, errors='coerce')
                df = df.dropna(subset=['ds'])
                daily_df = df.groupby('ds', as_index=False)['y'].sum()
                daily_df = daily_df.sort_values('ds').drop_duplicates(subset='ds')
                m = Prophet()
                m.fit(daily_df)
                loaded_prophet_model = m
                print("Model trained successfully.")
        except Exception as e:
            print(f"Training failed: {e}")

# Load on startup
load_or_train_model()

def run_forecast_logic():
    """Returns: forecast_data (list), tomorrow_val (int), high_vol (bool)"""
    global loaded_prophet_model, latest_tomorrow_volume, parcel_simulator
    forecast_data = []
    tomorrow_val = 1600
    
    if loaded_prophet_model:
        try:
            today = datetime.now(IST).date()
            tomorrow = today + timedelta(days=1)

            future = loaded_prophet_model.make_future_dataframe(periods=30)
            fcst = loaded_prophet_model.predict(future)

            mask = (fcst["ds"].dt.date >= (today - timedelta(days=7))) & (
                fcst["ds"].dt.date <= (today + timedelta(days=7))
            )
            window = fcst.loc[mask].copy()

            if window.empty:
                window = fcst.tail(14)

            forecast_data = [
                {
                    "ds": str(r["ds"].date()),
                    "yhat": int(r["yhat"]),
                    "type": "forecast" if r["ds"].date() >= today else "history",
                }
                for _, r in window.iterrows()
            ]

            tomorrow_row = fcst[fcst["ds"].dt.date == tomorrow]
            if not tomorrow_row.empty:
                tomorrow_val = int(tomorrow_row.iloc[0]["yhat"])
            else:
                tomorrow_val = int(fcst.iloc[-1]["yhat"])

        except Exception as e:
            print(f"Forecast error: {e}")

    if not forecast_data:
        today = datetime.now(IST).date()
        history = []
        for i in range(7, 0, -1):
            d = today - timedelta(days=i)
            y = max(200, int(tomorrow_val * (0.8 + 0.12 * random.random())))
            history.append(
                {
                    "ds": str(d),
                    "yhat": y,
                    "type": "history",
                }
            )
        future = []
        for i in range(0, 7):
            d = today + timedelta(days=i)
            y = max(200, int(tomorrow_val * (0.9 + 0.15 * random.random())))
            future.append(
                {
                    "ds": str(d),
                    "yhat": y,
                    "type": "forecast" if d >= today else "history",
                }
            )
        forecast_data = history + future

    low_bound = int(tomorrow_val * 0.95)
    upper_bound = int(tomorrow_val * 1.05)

    target_robots = decision_engine.get_robot_count_for_volume(tomorrow_val)
    
    # Calculate zone distribution for pre-assignment (Step 0)
    # Forecasted distribution: Medical 30%, Perishable 30%, Fragile 20%, General 20%
    zone_dist = {
        "Medical": int(target_robots * 0.3),
        "Perishable": int(target_robots * 0.3),
        "Fragile": int(target_robots * 0.2),
        "General": target_robots - int(target_robots * 0.3)*2 - int(target_robots * 0.2)
    }
    sim_engine.pre_assign_robots(zone_dist)

    latest_tomorrow_volume = tomorrow_val
    if parcel_simulator is not None:
        parcel_simulator.update_prediction_volume(tomorrow_val)

    if tomorrow_val > 1500:
        agents["predictor"].log(
            f"High forecast ({tomorrow_val}). Increasing robots to {target_robots}."
        )
    elif tomorrow_val < 800:
        agents["predictor"].log(
            f"Low forecast ({tomorrow_val}). Adjusting robots to {target_robots}."
        )
    else:
        agents["predictor"].log(
            f"Normal forecast ({tomorrow_val}). Setting robots to {target_robots}."
        )

    tomorrow_date_str = (datetime.now(IST) + timedelta(days=1)).strftime("%d-%m-%Y")
    db.save_prediction(tomorrow_date_str, tomorrow_val, low_bound, upper_bound)

    return forecast_data, tomorrow_val, (tomorrow_val > 1500), tomorrow_date_str


# --- 4. BACKEND ENDPOINTS ---


@app.route("/auth/login", methods=["POST"])
def login():
    body = request.get_json(silent=True) or {}
    email = (body.get("email") or body.get("username") or "").strip().lower()
    password = body.get("password") or ""
    if not email or not password:
        return jsonify({"error": "Email and password required"}), 400

    user = db.get_user_by_email(email)
    if not user or not verify_password(password, user.get("password_hash") or ""):
        return jsonify({"error": "Invalid credentials"}), 401

    role = user.get("role") or "GUEST"
    token = create_access_token(user_id=user["id"], role=role)
    
    # Resolve redirect path based on role (Final Mapping)
    role_paths = {
        "HUB_MANAGER": "/manager/dashboard.html",
        "QOS": "/qos/dashboard.html",
        "ROBOTICS": "/robots/dashboard.html",
        "DELIVERY": "/delivery/dashboard.html",
        "ADMIN": "/admin/dashboard.html",
        "FINANCE": "/finance/dashboard.html"
    }
    redirect_path = role_paths.get(role, "/index.html")

    return jsonify({
        "access_token": token,
        "token_type": "bearer",
        "tenant_id": user.get("company_id") or DEFAULT_COMPANY_ID,
        "role": role,
        "redirect_url": redirect_path
    })

@app.route("/upload_history", methods=["POST"])
@require_auth
def upload_history():
    guard = ensure_role("admin", "planning")
    if guard: return guard
    if "file" not in request.files:
        return jsonify({"error": "No file uploaded"}), 400
    file = request.files["file"]
    if not file.filename:
        return jsonify({"error": "No file selected"}), 400
    try:
        df = pd.read_csv(file)
        df.columns = [c.strip().lower() for c in df.columns]
        df.to_csv(DATA_FILE, index=False)
        load_or_train_model()
        return jsonify({"message": f"Historical data loaded with {len(df)} rows. Model trained."})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/upload_csv", methods=["POST"])
@require_auth
def upload_csv():
    # Manager, Admin, and QOS can upload (though typically Manager/Intake)
    guard = ensure_role("HUB_MANAGER", "ADMIN", "QOS")
    if guard: return guard
    if "file" not in request.files:
        return jsonify({"error": "No file uploaded"}), 400
    
    file = request.files["file"]
    if not file.filename:
        return jsonify({"error": "No file selected"}), 400
        
    try:
        # Clear ONLY the current simulation state, but KEEP DB parcels for history
        sim_engine.clear()
        
        df = pd.read_csv(file)
        df.columns = [c.strip().lower() for c in df.columns]
        df.to_csv(DAILY_PARCELS_PATH, index=False)
        
        # Generate vehicle details for response
        vehicle_id = f"TRUCK-IN-{random.randint(100, 999)}"
        driver_name = random.choice(["Ramesh", "Suresh", "Vikram", "Anita", "Priya"])
        batch_id = "BATCH-" + str(int(time.time()))
        
        # Unique batch ID ensures history is stored separately
        # We pass vehicle info to thread via closure or just re-generate? 
        # No, we modified process_daily_upload to generate it. 
        # Wait, if we want consistency, we should modify process_daily_upload signature again or accept that the response here is a "Preview".
        # Let's trust the thread to do the heavy lifting and logging.
        # But user wants the response to contain the details.
        
        Thread(target=process_daily_upload, args=(df, g.company_id), daemon=True).start()
        
        # EMIT CRITICAL SYSTEM EVENT
        event_bridge.emit("PARCEL_BATCH_RECEIVED", {
            "batch_id": batch_id,
            "count": len(df),
            "message": f"New manifest with {len(df)} parcels ingested."
        })
        
        # Calculate immediate stats for frontend feedback
        total_weight = df.get("weight_kg", 0).sum() if "weight_kg" in df.columns else 0
        total_volume = df.get("volume", 0).sum() if "volume" in df.columns else 0
        
        # Priority distribution
        priority_counts = df["priority"].value_counts().to_dict() if "priority" in df.columns else {}
        high_prio = priority_counts.get("High", 0)
        total = len(df)
        prio_pct = round((high_prio / total) * 100, 1) if total > 0 else 0

        return jsonify({
            "message": f"Manifest registered with {len(df)} parcels. Ready for processing.",
            "details": {
                "count": len(df),
                "batch_id": batch_id,
                "vehicle_id": vehicle_id,
                "driver_name": driver_name,
                "timestamp": datetime.now().strftime("%H:%M:%S"),
                "stats": {
                    "weight": round(float(total_weight), 1),
                    "volume": round(float(total_volume), 1),
                    "priority_load": prio_pct
                }
            }
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/predict_damage", methods=["POST"])
@require_auth
def predict_damage():
    guard = ensure_role("qos", "admin")
    if guard: return guard
    if "images" not in request.files:
        return jsonify({"error": "No images uploaded"}), 400
    files = request.files.getlist("images")
    if not files:
        return jsonify({"error": "No images uploaded"}), 400

    results_payload = []
    for f in files:
        filename = f.filename or ""
        if not filename: continue
        
        tmp_name = f"scan_{int(time.time()*1000)}_{filename}"
        tmp_path = os.path.join(UPLOAD_DIR, tmp_name)
        f.save(tmp_path)
        
        # Real YOLO detection
        det = detect_damage_image(tmp_path, conf_thr=0.25)
        
        # Determine parcel ID from filename or generate
        parcel_id = os.path.splitext(os.path.basename(filename))[0]
        if not parcel_id or len(parcel_id) < 3:
            parcel_id = f"P-{random.randint(10000, 99999)}"

        # Add to global scan history for persistent UI view
        scan_entry = {
            "timestamp": datetime.utcnow().isoformat(timespec="seconds"),
            "parcel_id": parcel_id,
            "zone": random.choice(["General", "Fragile", "Medical"]), # Mock zone for scan
            "damaged": det["damaged"],
            "severity": det["severity"],
            "confidence": round(det["confidence"], 2),
            "annotated_url": det["annotated_path"],
            "robot_id": f"R-{random.randint(1, 12)}"
        }
        scan_history.append(scan_entry)
        if len(scan_history) > 500: scan_history.pop(0)

        # Notify agents and update registry
        if det["damaged"]:
            damage_registry.add(parcel_id)
        
        agents["detector"].log(f"Manual scan for {parcel_id}: {'DAMAGED' if det['damaged'] else 'SAFE'} (Conf: {det['confidence']:.2f})")

        results_payload.append(scan_entry)

        # Update parcel in DB if it exists
        try:
            p = db.get_parcel_by_id(parcel_id, company_id=g.company_id)
            if p:
                status = "CRITICAL_DAMAGE" if det["severity"] == "severe" else ("MINOR_DAMAGE" if det["damaged"] else "SAFE")
                db.update_parcel_status(parcel_id, quality_status=status, company_id=g.company_id)
        except Exception: pass

    return jsonify({"results": results_payload})

@app.route("/uploads/<path:filename>", methods=["GET"])
def serve_uploads(filename):
    return send_from_directory(UPLOAD_DIR, filename)

@app.route("/api/qos/summary", methods=["GET"])
@require_auth
def qos_summary():
    guard = ensure_role("qos", "admin")
    if guard: return guard
    # Merge real scans with simulated ones for a full dashboard
    all_records = sim_qos_records + scan_history
    summary = summarize_qos(all_records)
    return jsonify(summary)

@app.route("/api/qos/scans", methods=["GET"])
@require_auth
def qos_scans():
    guard = ensure_role("qos", "admin")
    if guard: return guard
    # Return both simulated and real scans, latest first
    all_scans = sorted(sim_qos_records + scan_history, key=lambda x: x["timestamp"], reverse=True)
    return jsonify({"scans": all_scans[:100]})

@app.route("/api/qos/analytics", methods=["GET"])
@require_auth
def qos_analytics():
    guard = ensure_role("qos", "admin")
    if guard: return guard
    summary = summarize_qos(sim_qos_records)
    return jsonify({
        "severity": summary["severity_counts"],
        "zones": summary["zone_counts"],
        "robots": summary["robot_counts"],
    })

@app.route("/forecast", methods=["GET"])
@require_auth
def forecast():
    # Allow forecast to support charts across departments
    data, val, is_high, date_str = run_forecast_logic()
    low_bound = int(val * 0.95)
    upper_bound = int(val * 1.05)
    return jsonify(
        {
            "forecast": data,
            "tomorrow_volume": val,
            "tomorrow_date": date_str,
            "range": f"{low_bound} - {upper_bound}",
            "high_volume": is_high,
            "message": f"Forecast for {date_str}: {val} parcels. {'High Volume' if is_high else 'Normal Volume'}",
            "robot_count": len(sim_engine.robots),
            "prediction": {
                "date": date_str,
                "predicted_volume": val,
                "lower_bound": low_bound,
                "upper_bound": upper_bound,
            },
        }
    )

@app.route("/api/planning/summary", methods=["GET"])
@require_auth
def planning_summary():
    guard = ensure_role("planning", "admin")
    if guard: return guard
    data, val, is_high, date_str = run_forecast_logic()
    peak = random.choice(["11:00 IST", "14:00 IST", "17:00 IST"])
    demand = decision_engine.get_robot_count_for_volume(val)
    accuracy = random.randint(92, 98)
    history = db.get_recent_batches(limit=10)
    return jsonify({
        "tomorrow_volume": val, 
        "peak_hour": peak, 
        "robot_demand": demand, 
        "forecast_accuracy": accuracy,
        "history": history
    })

@app.route("/api/intake/summary", methods=["GET"])
@require_auth
def intake_summary():
    guard = ensure_role("admin", "logistics", "qos") # Allow roles that might need intake info
    if guard: return guard
    parcels = db.get_recent_parcels(limit=1000)
    total = len(parcels)
    zones = {}
    priorities = {}
    weight = 0
    volume = 0
    for p in parcels:
        zones[p["zone"]] = zones.get(p["zone"], 0) + 1
        priorities[p["priority"]] = priorities.get(p["priority"], 0) + 1
        weight += float(p.get("weight_kg") or 0)
        volume += float(p.get("volume") or 0)
    
    return jsonify({
        "total_parcels": total,
        "zone_distribution": zones,
        "priority_distribution": priorities,
        "total_weight": round(weight, 2),
        "total_volume": round(volume, 2)
    })

@app.route("/api/intake/parcels", methods=["GET"])
@require_auth
def intake_parcels():
    batch_id = request.args.get("batch_id")
    parcels = db.get_recent_parcels(limit=100, batch_id=batch_id)
    return jsonify({"parcels": parcels})

@app.route("/api/batches", methods=["GET"])
@require_auth
def get_batches():
    batches = db.get_recent_batches(limit=20)
    return jsonify({"batches": batches})

@app.route("/api/transport/vehicles", methods=["GET"])
@require_auth
def list_vehicles():
    guard = ensure_role("admin", "logistics")
    if guard: return guard
    
    vehicles = db.get_vehicles(company_id=g.company_id)
    
    # Auto-seed if empty (Requirement: DL01-DL20)
    if not vehicles:
        print("Seeding 20 vehicles for demo (DL01-DL20)...")
        for i in range(1, 21):
            vid = f"V-{random.randint(10000,99999)}"
            vnum = f"DL{str(i).zfill(2)}"
            driver = random.choice(["Rajesh", "Suresh", "Amit", "Vijay", "Priya", "Ankit", "Sunil", "Ravi", "Mohan", "Kiran"])
            db.upsert_vehicle(vid, vnum, driver, company_id=g.company_id, status="IDLE", maintenance="OK", vehicle_type="Truck")
        vehicles = db.get_vehicles(company_id=g.company_id)
        
    return jsonify(vehicles)

@app.route("/api/transport/add_vehicle", methods=["POST"])
@require_auth
def add_vehicle():
    guard = ensure_role("admin", "logistics")
    if guard: return guard
    data = request.json or {}
    
    vid = f"V-{random.randint(10000,99999)}"
    vnum = data.get("vehicle_number")
    driver = data.get("driver_name", "Unknown")
    
    if not vnum:
        return jsonify({"error": "Vehicle number required"}), 400
        
    db.upsert_vehicle(vid, vnum, driver, company_id=g.company_id, status="IDLE", maintenance="OK", vehicle_type="Truck")
    return jsonify({"message": "Vehicle added", "vehicle_id": vid})

@app.route("/api/transport/remove_vehicle", methods=["POST"])
@require_auth
def remove_vehicle():
    guard = ensure_role("admin", "logistics")
    if guard: return guard
    data = request.json or {}
    vid = data.get("vehicle_id")
    if not vid:
        return jsonify({"error": "Vehicle ID required"}), 400
        
    db.delete_vehicle(vid, company_id=g.company_id)
    return jsonify({"message": "Vehicle removed"})

@app.route("/api/customer/pay", methods=["POST"])
def customer_pay():
    data = request.json or {}
    parcel_id = data.get("parcel_id")
    amount = data.get("amount")
    mode = data.get("mode", "Credit Card")
    company_id = data.get("company_id") or DEFAULT_COMPANY_ID
    
    # Allow dummy payment even if parcel not found (for demo)
    if not parcel_id or not amount:
        return jsonify({"error": "Missing parcel_id or amount"}), 400
        
    db.insert_financial_record(parcel_id, float(amount), mode, company_id=company_id)
    
    # Try to update parcel status if it exists
    p = db.get_parcel_by_id(parcel_id, company_id=company_id)
    if p:
        db.update_parcel_status(parcel_id, amount_to_pay=0, company_id=company_id)
    
    event_bridge.emit("FINANCE_UPDATE", {"parcel_id": parcel_id, "amount": amount, "type": "PAYMENT"})
    return jsonify({"message": "Payment successful"})

@app.route("/api/delivery/logs", methods=["GET"])
@require_auth
def get_delivery_logs():
    # Fetch both transport logs and financial logs for delivery dept
    financials = db.get_financial_records(company_id=g.company_id, limit=20)
    return jsonify(financials)

@app.route("/api/delivery/summary", methods=["GET"])
@require_auth
def delivery_summary():
    guard = ensure_role("logistics", "admin")
    if guard: return guard
    parcels = db.get_recent_parcels(limit=500)
    cities = {}
    status_counts = {"IN ZONE": 0, "OUT FOR DELIVERY": 0, "DELIVERED": 0}
    for p in parcels:
        city = p.get("destination_city", "Unknown")
        cities[city] = cities.get(city, 0) + 1
        status = p.get("status", "")
        if status in status_counts:
            status_counts[status] += 1
    return jsonify({
        "city_distribution": cities,
        "status_summary": status_counts,
        "total_ready": status_counts["IN ZONE"],
        "total_in_transit": status_counts["OUT FOR DELIVERY"],
        "total_delivered": status_counts["DELIVERED"]
    })

@app.route("/api/admin/notifications", methods=["GET"])
@require_auth
def admin_notifications():
    guard = ensure_role("admin")
    if guard: return guard
    # Get recent logs that look like inter-department notifications
    logs = db.get_recent_logs(limit=20)
    notifs = []
    for l in logs:
        msg = l["message"]
        if "batch" in msg.lower() or "complete" in msg.lower() or "cleared" in msg.lower() or "arrived" in msg.lower() or "delivered" in msg.lower():
            notifs.append(l)
    return jsonify({"notifications": notifs})

@app.route("/api/admin/approve_robots", methods=["POST"])
@require_auth
def approve_robots():
    guard = ensure_role("HUB_MANAGER", "ADMIN")
    if guard: return guard
    
    # Trigger simulation to adjust to forecast volume
    data, val, is_high, date_str = run_forecast_logic()
    sim_engine.update_forecast(val)
    
    # Notify Robotics Role
    target_robots = len(sim_engine.robots)
    msg = f"MANAGER APPROVED: We need {target_robots} robots for tomorrow ({date_str}) to handle {val} parcels."
    db.insert_notification("robotics", msg, company_id=g.company_id)
    
    agents["predictor"].log(f"Hub Manager approved robot assignment for volume: {val}", company_id=g.company_id)
    return jsonify({"message": f"Robot assignment approved. Fleet adjusted to {target_robots} units. Robotics team notified."})

@app.route("/api/admin/initiate_processing", methods=["POST"])
@require_auth
def initiate_processing():
    guard = ensure_role("HUB_MANAGER", "ADMIN")
    if guard: return guard
    
    # Get parcels that were just uploaded
    parcels = db.get_recent_parcels(limit=1000)
    to_process = [p for p in parcels if p["status"] == "RECEIVED_AT_HUB"]
    
    print(f"DEBUG: Found {len(parcels)} total parcels in DB. {len(to_process)} are RECEIVED_AT_HUB.")
    
    if not to_process:
        return jsonify({"error": "No parcels found. Please upload a manifest first."}), 400
        
    def start_background_processing(parcel_list, company_id):
        for p in parcel_list:
            parcel_id = p["id"]
            ptype = p["type"]
            prio = p["priority"]
            
            # Update status to indicate processing has started
            db.update_parcel_status(parcel_id, status="AWAITING_INSPECTION", company_id=company_id)
            
            # 1. Start QOS Inspection (Parallel)
            Thread(target=run_qos_flow, args=(parcel_id, ptype, prio, company_id), daemon=True).start()
            
            # 2. Start Robotics Movement
            run_robotics_flow(parcel_id, ptype, prio, company_id)
            
            agents["predictor"].log(f"Processing started for parcel {parcel_id}", company_id=company_id)
            # Small delay between starting each parcel to prevent robot stampede
            time.sleep(0.2)

    Thread(target=start_background_processing, args=(to_process, g.company_id), daemon=True).start()
    
    agents["predictor"].log(f"Hub Manager initiated processing for {len(to_process)} parcels.", company_id=g.company_id)
    return jsonify({"message": f"Processing started for {len(to_process)} parcels."})

@app.route("/api/finance/summary", methods=["GET"])
@require_auth
def get_finance_summary():
    guard = ensure_role("admin", "HUB_MANAGER", "FINANCE")
    if guard: return guard
    summary = db.get_financial_summary(company_id=g.company_id)
    return jsonify(summary)

@app.route("/api/finance/records", methods=["GET"])
@require_auth
def get_finance_records():
    guard = ensure_role("admin", "HUB_MANAGER", "FINANCE")
    if guard: return guard
    records = db.get_financial_records(company_id=g.company_id)
    
    # Auto-seed dummy finance data if empty (for demo)
    if not records:
        print("Seeding dummy financial records...")
        modes = ["UPI", "Credit Card", "Debit Card", "Net Banking", "Cash"]
        for i in range(15):
            pid = f"P-{random.randint(1000,9999)}"
            amt = random.choice([450, 1200, 890, 2500, 150, 670])
            mode = random.choice(modes)
            db.insert_financial_record(pid, amt, mode, company_id=g.company_id)
        records = db.get_financial_records(company_id=g.company_id)
        
    return jsonify(records)

@app.route("/api/finance/record_payment", methods=["POST"])
@require_auth
def record_payment():
    data = request.json
    parcel_id = data.get("parcel_id")
    amount = data.get("amount")
    mode = data.get("mode", "ONLINE")
    if not parcel_id or not amount:
        return jsonify({"error": "Missing parcel_id or amount"}), 400
    
    db.insert_financial_record(parcel_id, amount, mode, company_id=g.company_id)
    db.update_parcel_status(parcel_id, status="DELIVERED", delivery_status="DELIVERED", company_id=g.company_id)
    
    # Emit system event so dashboards refresh quickly
    event_bridge.emit("FINANCE_PAYMENT", {"parcel_id": parcel_id, "amount": amount, "mode": mode})
    event_bridge.emit("PARCEL_DELIVERED", {"parcel_id": parcel_id, "mode": mode, "city": "N/A"})
    
    # Send email notification to specified email
    target_email = "23i224@psgtech.ac.in"
    print(f"DEBUG: SENDING EMAIL TO {target_email} -> Parcel {parcel_id} has been delivered. Payment of ₹{amount} via {mode} confirmed.")
    
    agents["optimizer"].log(f"Payment recorded for {parcel_id} via {mode}. Status updated to DELIVERED.", company_id=g.company_id)
    return jsonify({"message": "Payment recorded successfully and status updated"})

@app.route("/api/finance/export", methods=["GET"])
@require_auth
def export_finance_csv():
    records = db.get_financial_records(company_id=g.company_id)
    if not records:
        return jsonify({"error": "No records to export"}), 404
        
    df = pd.DataFrame(records)
    csv_data = df.to_csv(index=False)
    return Response(
        csv_data,
        mimetype="text/csv",
        headers={"Content-disposition": "attachment; filename=financial_report.csv"}
    )

@app.route("/api/delivery/export", methods=["GET"])
@require_auth
def export_delivery_csv():
    parcels = db.get_recent_parcels(limit=5000, company_id=g.company_id)
    if not parcels:
        return jsonify({"error": "No records to export"}), 404
        
    df = pd.DataFrame(parcels)
    csv_data = df.to_csv(index=False)
    return Response(
        csv_data,
        mimetype="text/csv",
        headers={"Content-disposition": "attachment; filename=delivery_report.csv"}
    )

@app.route("/api/admin/initiate_delivery", methods=["POST"])
@require_auth
def initiate_delivery():
    guard = ensure_role("admin", "logistics", "HUB_MANAGER")
    if guard: return guard
    
    parcels = db.get_recent_parcels(limit=1000)
    to_deliver = [p for p in parcels if p["status"] == "DELIVERY_READY"]
    
    if not to_deliver:
        return jsonify({"error": "No parcels ready for delivery."}), 400
        
    # Group by city
    groups = {}
    for p in to_deliver:
        city = p.get("destination_city", "Unknown")
        if city not in groups: groups[city] = []
        groups[city].append(p)
        
    for city, city_parcels in groups.items():
        vehicle_id = f"V-{random.randint(1000,9999)}"
        vehicle_num = f"KA-{random.randint(0,99)}-{chr(random.randint(65,90))}{chr(random.randint(65,90))}-{random.randint(1000,9999)}"
        driver = random.choice(["Rajesh", "Suresh", "Amit", "Vijay", "Priya"])
        
        # Record Vehicle in DB with Maintenance Info
        maintenance = random.choice(["OK", "OK", "OK", "NEEDS SERVICE"])
        db.upsert_vehicle(vehicle_id, vehicle_num, driver, company_id=g.company_id, status="OUT_FOR_DELIVERY", maintenance=maintenance)
        
        # Record Transport start with totals
        try:
            total_amount = sum(float(p.get("amount_to_pay", 0.0)) for p in city_parcels)
            parcel_ids = ",".join([p["id"] for p in city_parcels])
            cid = g.company_id
            with db.get_connection() as conn:
                cur = conn.cursor()
                placeholder = "%s" if (db.DB_TYPE == "postgres" and db.HAS_POSTGRES) else "?"
                cur.execute(
                    f"INSERT INTO transport_tracking (company_id, vehicle_id, parcel_count, destination, dispatch_time, status, total_amount, settled_amount, parcel_ids) VALUES ({placeholder}, {placeholder}, {placeholder}, {placeholder}, {placeholder}, 'IN_TRANSIT', {placeholder}, {placeholder}, {placeholder})",
                    (cid, vehicle_id, len(city_parcels), city, datetime.utcnow().isoformat(), total_amount, 0.0, parcel_ids)
                )
            agents["optimizer"].log(f"DISPATCH LOG: {vehicle_id} -> {city}, Parcels: {len(city_parcels)}, Total Amount: ₹{int(total_amount)}", company_id=cid)
            event_bridge.emit("TRANSPORT_LOG", {"vehicle_id": vehicle_id, "city": city, "parcel_count": len(city_parcels), "total_amount": total_amount})
        except Exception as e:
            print(f"Transport record insert failed: {e}")
        
        Thread(target=run_delivery_group_flow, args=(city, city_parcels, g.company_id, vehicle_id), daemon=True).start()
        
    return jsonify({"message": f"Dispatched {len(groups)} vehicles for {len(to_deliver)} parcels."})

@app.route("/api/customer/track", methods=["GET"])
def customer_track():
    parcel_id = request.args.get("parcel_id")
    company_id = request.args.get("company_id") or DEFAULT_COMPANY_ID
    if not parcel_id:
        return jsonify({"error": "Missing parcel_id"}), 400
    
    p = db.get_parcel_by_id(parcel_id, company_id=company_id)
    if not p:
        return jsonify({"error": "Parcel not found"}), 404
        
    return jsonify({
        "id": p["id"],
        "status": p["status"],
        "eta": p.get("eta", "Pending"),
        "route": p.get("route_path", "Pending"),
        "destination": p.get("destination_city", "Unknown"),
        "amount_to_pay": p.get("amount_to_pay", 0.0),
        "last_update": p.get("created_at")
    })

def run_delivery_group_flow(city, parcels, company_id=None, vehicle_id=None):
    # Step 7: Realistic Delivery flow with nodes and TSP logic (mocked nodes)
    if not vehicle_id:
        vehicle_id = f"V-{random.randint(1000,9999)}"
    
    # Register in simulation engine for visualization
    sim_engine.delivery_fleet[vehicle_id] = {
        "id": vehicle_id,
        "city": city,
        "progress": 0.0,
        "status": "DISPATCHED",
        "load": len(parcels),
        "nodes": ["Hub", "Highway", "Toll Plaza", "Regional Depot", "Last Mile"]
    }
    
    route_str = "Hub -> Highway -> Toll -> Depot -> Destination"
    
    for p in parcels:
        db.update_parcel_status(p["id"], status="OUT FOR DELIVERY", route_path=route_str, eta="3-5 hours", company_id=company_id)
    
    agents["optimizer"].log(f"Vehicle {vehicle_id} DISPATCHED to {city} with {len(parcels)} parcels.", company_id=company_id)
    
    # Update Vehicle status in DB
    db.upsert_vehicle(vehicle_id, "", "", company_id=company_id, status="IN_TRANSIT")
    
    # Simulate movement (Total trip time ~30-40s for demo)
    total_steps = 100
    for step in range(total_steps + 1):
        time.sleep(0.4) 
        progress = step / total_steps
        
        # Update sim state
        if vehicle_id in sim_engine.delivery_fleet:
            sim_engine.delivery_fleet[vehicle_id]["progress"] = progress
            
            # Update node status based on progress
            if progress < 0.2: current_node = "Hub Area"
            elif progress < 0.5: current_node = "Highway Transit"
            elif progress < 0.7: current_node = f"{city} Toll Plaza"
            elif progress < 0.9: current_node = "Regional Depot"
            else: current_node = "Last Mile Delivery"
            
            sim_engine.delivery_fleet[vehicle_id]["current_node"] = current_node
            
            # DB Updates at key milestones (reduced frequency)
            if step % 20 == 0:
                eta_h = max(0.5, 4.0 * (1.0 - progress))
                for p in parcels:
                    db.update_parcel_status(p["id"], eta=f"{eta_h:.1f} hours", route_path=f"Current: {current_node}", company_id=company_id)
                # Sync vehicle location in DB
                db.upsert_vehicle(vehicle_id, "", "", company_id=company_id, status="IN_TRANSIT")

    # Completion
    for p in parcels:
        db.update_parcel_status(p["id"], status="DELIVERED", eta="Arrived", company_id=company_id)
        db.insert_log(f"DELIVERY LOG: Vehicle {vehicle_id} delivered parcel {p['id']} to {city}.", source="Delivery", company_id=company_id)
        # Notify customer/event stream
        try:
            event_bridge.emit("PARCEL_DELIVERED", {"parcel_id": p["id"], "city": city, "vehicle_id": vehicle_id})
        except Exception:
            pass
    
    # Update vehicle back to IDLE
    db.upsert_vehicle(vehicle_id, "", "", company_id=company_id, status="IDLE")
    
    agents["optimizer"].log(f"SUCCESS: Vehicle {vehicle_id} completed delivery for {city}.", company_id=company_id)
    try:
        # Mark transport tracking as delivered and compute settlement
        with db.get_connection() as conn:
            cur = conn.cursor()
            placeholder = "%s" if (db.DB_TYPE == "postgres" and db.HAS_POSTGRES) else "?"
            # Sum paid amounts for these parcels
            parcel_ids = [p["id"] for p in parcels]
            if parcel_ids:
                ids_csv = ",".join(parcel_ids)
                # SQLite doesn't support IN with parameterized CSV easily; do simple update by status only
                # Update arrival_time and status
                cur.execute(f"UPDATE transport_tracking SET arrival_time = {placeholder}, status = 'DELIVERED' WHERE vehicle_id = {placeholder} AND company_id = {placeholder}", 
                            (datetime.utcnow().isoformat(), vehicle_id, company_id))
        db.insert_log(f"SETTLEMENT LOG: Vehicle {vehicle_id} marked DELIVERED at {datetime.utcnow().isoformat()}.", source="Delivery", company_id=company_id)
    except Exception as e:
        print(f"Transport settlement update failed: {e}")
    
    # Remove from fleet visualization after delay
    if vehicle_id in sim_engine.delivery_fleet:
        sim_engine.delivery_fleet[vehicle_id]["status"] = "COMPLETED"
        time.sleep(5)
        if vehicle_id in sim_engine.delivery_fleet:
            del sim_engine.delivery_fleet[vehicle_id]
# --- SIMULATION ENDPOINTS ---

def on_parcel_arrived_in_zone(parcel_id, company_id=None):
    p = db.get_parcel_by_id(parcel_id, company_id=company_id)
    if not p: return
    
    db.update_parcel_status(parcel_id, status="IN ZONE", company_id=company_id)
    agents["optimizer"].log(f"Parcel {parcel_id} arrived in {p['zone']} zone.", company_id=company_id)
    
    # Check if we should trigger cluster delivery
    check_and_trigger_clusters(company_id)
    
def check_and_trigger_clusters(company_id):
    # Only consider parcels that are 'IN ZONE' and not yet processed for delivery
    parcels = db.get_recent_parcels(limit=50, company_id=company_id)
    in_zone_parcels = [p for p in parcels if p["status"] == "IN ZONE"]
    
    # If we have enough parcels waiting (e.g., > 5) or some timeout logic (simplified here)
    if len(in_zone_parcels) >= 5:
        # Group by City
        clusters = {}
        for p in in_zone_parcels:
            city = p.get("destination_city", "Unknown")
            if city not in clusters: clusters[city] = []
            clusters[city].append(p)
        
        # Get Vehicles
        available_vehicles = db.get_vehicles(company_id=company_id)
        if not available_vehicles:
            # Seed if missing
            for i in range(1, 21):
                 db.upsert_vehicle(f"V-AUTO-{i}", f"DL{str(i).zfill(2)}", "AutoDriver", company_id=company_id, status="IDLE")
            available_vehicles = db.get_vehicles(company_id=company_id)

        assignments = []
        for city, cluster_parcels in clusters.items():
            # Find IDLE vehicle
            vehicle = next((v for v in available_vehicles if v["current_status"] == "IDLE"), None)
            
            if vehicle:
                vehicle_id = vehicle["vehicle_id"]
                vehicle_num = vehicle["vehicle_number"]
                
                # Mark vehicle as ASSIGNED immediately to prevent double booking in this loop
                vehicle["current_status"] = "ASSIGNED" 
                db.upsert_vehicle(vehicle_id, vehicle_num, vehicle["driver_name"], company_id=company_id, status="ASSIGNED")
                
                assignments.append({"cluster": city, "vehicle": vehicle_num})
                
                # Start Flow in thread
                Thread(target=run_delivery_group_flow, args=(city, cluster_parcels, company_id, vehicle_id), daemon=True).start()
            
        if assignments:
            event_bridge.emit("CLUSTERS_FORMED", {"assignments": assignments, "message": "All parcels sorted into different clusters based on locations"})

    if p.get("quality_status") in ("SAFE", "MINOR_DAMAGE"):
        db.update_parcel_status(parcel_id, status="DELIVERY_READY")
        agents["optimizer"].log(f"Parcel {parcel_id} is DELIVERY_READY.")
        
        # Step 7: Automatic Trigger for Delivery Flow (Simulation)
        Thread(target=run_delivery_flow, args=(parcel_id, company_id), daemon=True).start()
    elif p.get("quality_status") == "CRITICAL_DAMAGE":
        agents["optimizer"].log(f"Parcel {parcel_id} isolated in Quarantine due to Critical Damage.")

def forecast_reality_monitor_loop():
    while True:
        time.sleep(20)
        try:
            state = sim_engine.get_state()
            backlog = state.get("backlog", {})
            for zone, count in backlog.items():
                if count > 15:
                    agents["predictor"].log(f"BOTTLENECK: {zone} zone exceeds forecast capacity ({count} parcels).")
                    # Rebalance: Find idle robots and assign to this zone
                    idle_robots = [r for r in sim_engine.robots if r.status == "idle"]
                    if idle_robots:
                        r = random.choice(idle_robots)
                        r.assign_task(zone)
                        agents["predictor"].log(f"Rebalanced robot {r.id} to {zone} to clear bottleneck.")
        except Exception as e:
            print(f"Monitor error: {e}")

def on_parcel_assigned_to_robot(parcel_id, robot_id, company_id=None):
    db.update_parcel_status(parcel_id, assigned_robot=robot_id, company_id=company_id)
    agents["assigner"].log(f"Robot {robot_id} assigned to parcel {parcel_id}.", company_id=company_id)

def on_robot_low_battery_warning(robot_id, battery_level, company_id=None):
    msg = f"ALERT: Robot {robot_id} battery CRITICAL ({battery_level}%). Routing to charging dock."
    agents["predictor"].log(msg, company_id=company_id)
    event_bridge.emit("ROBOT_BATTERY_LOW", {"robot_id": robot_id, "battery": battery_level, "message": msg})

sim_engine.on_parcel_delivered = on_parcel_arrived_in_zone
sim_engine.on_parcel_assigned = on_parcel_assigned_to_robot
sim_engine.on_robot_low_battery = on_robot_low_battery_warning

@app.route("/digital_twin_state", methods=["GET"])
@require_auth
def digital_twin_state():
    guard = ensure_role("robotics", "admin")
    if guard: return guard
    return jsonify(sim_engine.get_state())

@app.route("/api/robotics/summary", methods=["GET"])
@require_auth
def robotics_summary():
    guard = ensure_role("robotics", "admin", "HUB_MANAGER")
    if guard: return guard
    robots = db.get_robots() or [{"id": f"R-{i+1}", "status":"idle"} for i in range(random.randint(5,12))]
    total_robots = len(robots)
    idle = len([r for r in robots if (r.get("status") or "") == "idle"])
    active = total_robots - idle
    utilization = round((active / total_robots) * 100.0, 1) if total_robots else 0.0
    maint = random.randint(1, max(1, total_robots//3))
    
    # Fetch real CO2 savings from sim engine state
    state = sim_engine.get_state()
    co2 = state.get("stats", {}).get("co2_savings_kg", 0.0)
    
    return jsonify({
        "utilization": utilization, 
        "active": active, 
        "idle": idle, 
        "maintenance_forecast": maint,
        "co2_saved": co2
    })

@app.route("/api/robotics/robots", methods=["GET"])
@require_auth
def robotics_robots():
    guard = ensure_role("robotics", "admin", "HUB_MANAGER")
    if guard: return guard
    state = sim_engine.get_state()
    robots = state.get("robots", [])
    # Add mock temperature for visualization
    for r in robots:
        r["temp"] = round(random.uniform(35.0, 65.0), 1)
    return jsonify({"robots": robots})
@app.route("/parcels", methods=["GET"])
@require_auth
def get_parcels():
    parcels = db.get_recent_parcels(limit=30)
    return jsonify(parcels)

@app.route("/robots", methods=["GET"])
@require_auth
def get_robots():
    robots = db.get_robots()
    if not robots:
        # Seed 30 robots if none present
        try:
            try:
                from .simulation import Robot
            except Exception:
                from simulation import Robot
            types = ["Standard", "Fast", "Heavy"]
            seeded = []
            for i in range(30):
                rid = f"R-{i+1:03d}"
                rtype = random.choice(types)
                rob = Robot(rid, sim_engine.grid, rtype)
                sim_engine.robots.append(rob)
                db.upsert_robot({"id": rid, "company_id": g.company_id, "status": "idle", "current_zone": None, "assigned_parcel": None, "utilization_percentage": 0.0, "type": rtype, "battery": getattr(rob, "battery", 100)}, company_id=g.company_id)
                seeded.append({"id": rid, "type": rtype, "status": "idle", "zone": None, "battery": getattr(rob, "battery", 100)})
            event_bridge.emit("ROBOT_ADDED", {"id": "BULK", "count": len(seeded)})
            return jsonify(seeded)
        except Exception as e:
            # Fallback to any robots in simulation
            try:
                return jsonify([r.to_dict() for r in sim_engine.robots])
            except Exception:
                return jsonify([])
    return jsonify(robots)

@app.route("/api/robots/add", methods=["POST"])
@require_auth
def add_robot():
    guard = ensure_role("admin", "HUB_MANAGER", "ROBOTICS")
    if guard: return guard
    
    # Check if simulation is running (any robot busy)
    is_busy = any(r.status != "idle" for r in sim_engine.robots)
    if is_busy:
        return jsonify({"error": "Cannot add robots while simulation is running. Please wait for idle state."}), 400

    data = request.json or {}
    rid = data.get("id") or f"R-{random.randint(1,999)}"
    rtype = data.get("type") or "Standard"
    try:
        from .simulation import Robot
    except Exception:
        from simulation import Robot
    
    # Check for existing ID
    if any(r.id == rid for r in sim_engine.robots):
        return jsonify({"error": f"Robot ID {rid} already exists"}), 400

    rob = Robot(rid, sim_engine.grid, rtype)
    # Apply initial status/zone from payload if provided
    rob.status = data.get("status", "idle")
    rob.current_zone = data.get("zone", None)
    
    sim_engine.robots.append(rob)
    db.upsert_robot({
        "id": rid, 
        "company_id": g.company_id, 
        "status": rob.status, 
        "current_zone": rob.current_zone, 
        "assigned_parcel": None, 
        "utilization_percentage": 0.0, 
        "type": rtype, 
        "battery": 100
    }, company_id=g.company_id)
    event_bridge.emit("ROBOT_ADDED", {"id": rid, "type": rtype, "status": rob.status, "zone": rob.current_zone})
    return jsonify({"message": "Robot added", "id": rid, "type": rtype})

@app.route("/api/robots/remove", methods=["POST"])
@require_auth
def remove_robot():
    guard = ensure_role("admin", "HUB_MANAGER", "ROBOTICS")
    if guard: return guard
    
    # Check if simulation is running
    is_busy = any(r.status != "idle" for r in sim_engine.robots)
    if is_busy:
        return jsonify({"error": "Cannot remove robots while simulation is running."}), 400

    data = request.json or {}
    rid = data.get("id")
    if not rid:
        return jsonify({"error": "Missing robot id"}), 400
    try:
        # Sync simulation
        sim_engine.robots = [r for r in sim_engine.robots if r.id != rid]
        
        db.delete_robot(rid, company_id=g.company_id)
        event_bridge.emit("ROBOT_REMOVED", {"id": rid})
        return jsonify({"message": f"Robot {rid} removed"})
    except Exception as e:
        return jsonify({"error": f"Failed to remove robot: {e}"}), 500

@app.route("/api/robots/update", methods=["POST"])
@require_auth
def update_robot():
    guard = ensure_role("admin", "HUB_MANAGER", "ROBOTICS")
    if guard: return guard
    
    # Check if simulation is running
    is_busy = any(r.status != "idle" for r in sim_engine.robots)
    if is_busy:
        return jsonify({"error": "Cannot edit robots while simulation is running."}), 400

    data = request.json or {}
    rid = data.get("id")
    new_type = data.get("type")
    if not rid or not new_type:
        return jsonify({"error": "Missing id or type"}), 400
    try:
        # Sync simulation
        for r in sim_engine.robots:
            if r.id == rid:
                r.type = new_type
                break
                
        db.update_robot_type(rid, new_type, company_id=g.company_id)
        event_bridge.emit("ROBOT_UPDATED", {"id": rid, "type": new_type})
        return jsonify({"message": f"Robot {rid} updated", "id": rid, "type": new_type})
    except Exception as e:
        return jsonify({"error": f"Failed to update robot: {e}"}), 500
@app.route("/get_logs", methods=["GET"])
@require_auth
def get_logs():
    logs = db.get_recent_logs(limit=50)
    formatted = [
        f"[{row['timestamp']}] {row['message']}" for row in logs
    ]
    return jsonify(list(reversed(formatted)))

@app.route("/optimize_routes", methods=["GET"])
@require_auth
def optimize_routes():
    guard = ensure_role("logistics", "admin")
    if guard: return guard
    state = sim_engine.get_state()
    zones = state.get("zones", {})
    if not zones:
        return jsonify(route_state)
    names = list(zones.keys())
    origin = names[0]
    targets = names[1:4]
    try:
        from .route_opt import shortest_path_and_co2
    except Exception:
        from route_opt import shortest_path_and_co2
    route, co2_g = shortest_path_and_co2(zones, origin, targets)
    result = {
        "routes": [f"{a} -> {b}" for a, b in zip(route, route[1:])],
        "co2_savings_percent": max(0, 15 - (co2_g / 1000.0)),
    }
    route_state.update(result)
    return jsonify(route_state)

@app.route("/api/logistics/summary", methods=["GET"])
@require_auth
def logistics_summary():
    guard = ensure_role("logistics", "admin")
    if guard: return guard
    route_eff = random.randint(70, 95)
    co2 = round(random.uniform(12.5, 28.0), 2)
    on_time = random.randint(88, 97)
    wx_impact = random.choice(["Low","Moderate","High"])
    return jsonify({"route_efficiency": route_eff, "co2_per_route": co2, "on_time_pct": on_time, "weather_impact": wx_impact})

    total_robots = len(robots)
    avg_util = (
        sum(r.get("utilization_percentage", 0.0) for r in robots) / total_robots
        if total_robots
        else 0.0
    )
    idle = len([r for r in robots if (r.get("status") or "") == "idle"])
    active = total_robots - idle

@app.route("/api/stream/simulation", methods=["GET"])
@require_auth
def stream_simulation():
    with last_stream_lock:
        if last_stream_cache is not None:
            return jsonify(last_stream_cache)
    return jsonify(build_stream_payload())


@app.route("/api/stream/events")
def stream_system_events():
    def gen():
        q = event_bridge.subscribe()
        while True:
            try:
                ev = q.get(timeout=30)
                yield f"data: {json.dumps(ev)}\n\n"
            except queue.Empty:
                yield ": keepalive\n\n"
    return Response(gen(), mimetype="text/event-stream")

@app.route("/api/stream/simulation/sse", methods=["GET"])
@require_auth
def stream_simulation_sse():
    def gen():
        while True:
            with last_stream_lock:
                payload = last_stream_cache or build_stream_payload()
            yield f"data: {json.dumps(payload)}\n\n"
            time.sleep(0.5)
    return Response(gen(), mimetype="text/event-stream")

@app.route("/api/ai/advisor_greeting", methods=["GET"])
@require_auth
def advisor_greeting():
    # Restricted to Hub Manager only
    guard = ensure_role("HUB_MANAGER", "ADMIN")
    if guard: return guard
    
    data, val, is_high, date_str = run_forecast_logic()
    
    # Calculate forecasted distribution (Phase 0)
    # Using 30/30/20/20 split as defined in run_forecast_logic
    med = int(val * 0.3)
    per = int(val * 0.3)
    fra = int(val * 0.2)
    nor = val - med - per - fra
    
    robots = decision_engine.get_robot_count_for_volume(val)
    role_label = g.current_user.get("role", "Manager").replace("_", " ").title()
    
    greeting = (
        f"HubPulse Advisor · {role_label}:<br/>"
        f"Tomorrow expected parcels: {val:,}<br/>"
        f"Medical: {med} | Perishable: {per} | Fragile: {fra} | Normal: {nor}<br/>"
        f"Peak load expected between 10:30 AM – 2:00 PM<br/>"
        f"Recommended robots: {robots} active<br/>"
        f"Medical zone congestion risk: {'HIGH' if is_high else 'NORMAL'}"
    )
    
    history = db.get_recent_batches(limit=10)
    return jsonify({
        "greeting": greeting,
        "forecast": {
            "total": val,
            "distribution": {"Medical": med, "Perishable": per, "Fragile": fra, "Normal": nor},
            "robots": robots,
            "risk": "HIGH" if is_high else "NORMAL"
        },
        "history": history
    })

@app.route("/api/reports/export", methods=["GET"])
@require_auth
def export_csv():
    # Accessible by Manager and Admin
    guard = ensure_role("HUB_MANAGER", "ADMIN")
    if guard: return guard
    
    batch_id = request.args.get("batch_id")
    parcels = db.get_recent_parcels(limit=5000, batch_id=batch_id)
    
    if not parcels:
        return jsonify({"error": "No data found to export."}), 404
        
    df = pd.DataFrame(parcels)
    # Filter to relevant columns for report
    cols = ["id", "batch_id", "zone", "priority", "status", "quality_status", "destination_city", "created_at"]
    df = df[[c for c in cols if c in df.columns]]
    
    output_name = f"report_{batch_id or 'all'}_{int(time.time())}.csv"
    output_path = os.path.join(DATA_DIR, output_name)
    df.to_csv(output_path, index=False)
    
    return send_from_directory(directory=DATA_DIR, path=output_name, as_attachment=True)

# --- THREADS ---
def sim_loop():
    while True:
        sim_engine.step()
        time.sleep(0.05)

def optimize_loop():
    global route_state
    while True:
        state = sim_engine.get_state()
        zones = state.get("zones", {})
        route_state = agents["optimizer"].optimize(zones)
        time.sleep(10)

def build_stream_payload():
    seed_demo_if_needed()
    twin_state = sim_engine.get_state()
    parcels = db.get_recent_parcels(limit=50)
    logs = db.get_recent_logs(limit=50)
    batches = db.get_recent_batches(limit=5)
    logs_payload = [{"message": row["message"], "source": row["source"], "timestamp": row["timestamp"]} for row in logs]
    robots = db.get_robots()
    if not robots:
        robots = [{"id": r.id, "status": r.status, "assigned_parcel": None, "current_zone": r.target_zone, "utilization_percentage": 0.0} for r in sim_engine.robots]
    total_robots = len(robots)
    avg_util = (sum(r.get("utilization_percentage", 0.0) for r in robots) / total_robots) if total_robots else 0.0
    idle = len([r for r in robots if (r.get("status") or "") == "idle"])
    active = total_robots - idle
    prediction = db.get_latest_prediction()
    counts = db.get_parcel_counts_by_status()
    flow_state = {
        "RECEIVED": counts.get("RECEIVED_AT_HUB", 0),
        "INSPECTION": counts.get("AWAITING_INSPECTION", 0),
        "ZONE_ALLOCATION": counts.get("IN ZONE", 0),
        "IN_TRANSIT": counts.get("OUT FOR DELIVERY", 0),
        "DELIVERED": counts.get("DELIVERED", 0),
    }
    payload = {
        "parcels": parcels,
        "robots": robots or [],
        "logs": logs_payload,
        "robot_utilization": {"total": total_robots, "active": active, "idle": idle, "avg_utilization": round(avg_util, 2)},
        "prediction": prediction,
        "twin": twin_state,
        "routes": route_state,
        "batches": batches,
        "flow_state": flow_state,
        "db_type": db.get_active_db_type()
    }
    PARCEL_GAUGE.set(len(parcels))
    ROBOT_GAUGE.set(total_robots)
    return payload

def refresh_stream_cache_loop():
    global last_stream_cache
    while True:
        try:
            payload = build_stream_payload()
            with last_stream_lock:
                last_stream_cache = payload
        except Exception as e:
            print(f"Stream cache refresh error: {e}")
        time.sleep(2)

def robot_sync_loop():
    robot_simulator.sync_robots_to_db(sim_engine, interval_seconds=SIM_TICK_SECONDS)


def parcel_sim_loop():
    if parcel_simulator is None:
        return
    parcel_simulator.run_loop()


def start_background_workers():
    global threads_started, parcel_simulator
    if threads_started:
        return
    threads_started = True

    # Ensure Database Schema is ready
    try:
        db.init_db()
        print(f"Database initialized ({db.get_active_db_type()})")
    except Exception as e:
        print(f"DB init warning: {e}")

    if SIMULATION_ENABLED:
        parcel_simulator = ParcelSimulator(
            sim_engine,
            tick_seconds=SIM_TICK_SECONDS,
        )

    run_forecast_logic()
    # Preload YOLO damage model once at startup to avoid first-request latency
    try:
        get_damage_model()
    except Exception as e:
        print(f"YOLO preload warning: {e}")

    Thread(target=sim_loop, daemon=True).start()
    Thread(target=optimize_loop, daemon=True).start()
    Thread(target=refresh_stream_cache_loop, daemon=True).start()
    try:
        Thread(target=db.pg_pool_refresh_loop, daemon=True).start()
    except Exception:
        pass
    if SIMULATION_ENABLED and parcel_simulator is not None:
        Thread(target=parcel_sim_loop, daemon=True).start()
    Thread(target=robot_sync_loop, daemon=True).start()
    Thread(target=forecast_reality_monitor_loop, daemon=True).start()
    # Auto-start processing once backend is ready for default tenant
    try:
        auto_start_processing(DEFAULT_COMPANY_ID, max_count=100)
    except Exception as e:
        print(f"Auto-start init warning: {e}")

@app.route("/")
def index():
    return jsonify({
        "name": "Smart Logistics Hub API",
        "status": "online",
        "version": "1.0.0",
        "endpoints": ["/api/stream/events", "/api/stream/simulation/sse", "/upload_csv", "/parcels"]
    }), 200

@app.route("/api/admin/debug_db", methods=["GET"])
@require_auth
def debug_db():
    guard = ensure_role("ADMIN", "HUB_MANAGER")
    if guard: return guard
    
    parcels = db.get_recent_parcels(limit=10)
    batches = db.get_recent_batches(limit=5)
    counts = db.get_parcel_counts_by_status()
    
    return jsonify({
        "db_type": db.DB_TYPE,
        "company_id": g.company_id,
        "parcel_count_db": len(parcels),
        "status_counts": counts,
        "recent_parcels": parcels,
        "recent_batches": batches
    })

if __name__ == "__main__":
    try:
        start_background_workers()
        print("Backend starting on http://0.0.0.0:5000")
        app.run(debug=True, host='0.0.0.0', port=5000, use_reloader=False)
    except Exception as e:
        print(f"CRITICAL BACKEND ERROR: {e}")
