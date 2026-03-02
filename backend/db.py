import os
import sqlite3
import threading
import time
import random
from contextlib import contextmanager
from datetime import datetime, timedelta

# Optional PostgreSQL support
try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    from psycopg2 import pool as psycopg2_pool
    HAS_POSTGRES = True
except ImportError:
    HAS_POSTGRES = False

# Global lock to serialize database writes (mainly for SQLite)
_db_lock = threading.Lock()

# Configuration
DB_TYPE = os.environ.get("DB_TYPE", "postgres") # 'sqlite' or 'postgres'
DB_PATH = os.environ.get(
    "WIZTRIC_DB_PATH",
    os.path.join(os.path.dirname(__file__), "wiztric_logistics.db"),
)
# Postgres Config
DATABASE_URL = os.environ.get("DATABASE_URL")
if DATABASE_URL:
    # Parse DATABASE_URL if needed, but psycopg2 can use it directly
    # However, db.py uses individual params below.
    # We will prioritize DATABASE_URL in get_connection
    pass

PG_HOST = os.environ.get("PGHOST", "localhost")
PG_PORT = os.environ.get("PGPORT", "5433")
PG_DB = os.environ.get("PGDATABASE", "wiztric_logistics;")
PG_USER = os.environ.get("PGUSER", "postgres")
PG_PASS = os.environ.get("PGPASSWORD", "oggy")

COMPANY_ID = os.environ.get("COMPANY_ID", "demo_company")
PG_POOL = None

try:
    from flask import g as flask_g
except Exception:
    flask_g = None

def _current_company_id():
    cid = None
    if flask_g is not None:
        try:
            cid = getattr(flask_g, "company_id", None)
        except Exception:
            cid = None
    return cid or os.environ.get("COMPANY_ID", COMPANY_ID)

@contextmanager
def get_connection():
    global PG_POOL
    # Use a local flag to check if we should try postgres
    use_postgres = (DB_TYPE == "postgres" and HAS_POSTGRES)
    
    if use_postgres:
        if PG_POOL is None:
            try:
                if DATABASE_URL:
                    PG_POOL = psycopg2_pool.SimpleConnectionPool(
                        1, 10,
                        dsn=DATABASE_URL,
                        connect_timeout=3
                    )
                    print("Connected to PostgreSQL via DATABASE_URL")
                else:
                    # Try both names in case of semicolon typo
                    for db_name in [PG_DB, f"{PG_DB};"]:
                        try:
                            PG_POOL = psycopg2_pool.SimpleConnectionPool(
                                1, 10,
                                host=PG_HOST,
                                port=PG_PORT,
                                database=db_name,
                                user=PG_USER,
                                password=PG_PASS,
                                connect_timeout=3
                            )
                            print(f"Connected to PostgreSQL: {db_name}")
                            break
                        except Exception as e:
                            print(f"PostgreSQL connection attempt failed for {db_name}: {e}")
                            PG_POOL = None
                            continue
            except Exception as e:
                print(f"PostgreSQL connection failed: {e}")
                PG_POOL = None

        if PG_POOL is not None:
            conn = None
            try:
                conn = PG_POOL.getconn()
                yield conn
                conn.commit()
                return
            except Exception as e:
                print(f"PostgreSQL operation error: {e}")
                if conn: conn.rollback()
                # If it's a connection error, reset pool
                if "connection" in str(e).lower() or "closed" in str(e).lower():
                    PG_POOL = None
            finally:
                if conn and PG_POOL:
                    PG_POOL.putconn(conn)
    
    # SQLite Fallback
    print("Using SQLite database.")
    directory = os.path.dirname(DB_PATH)
    if directory and not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30.0)
    try:
        conn.row_factory = sqlite3.Row
        yield conn
        conn.commit()
    except Exception as e:
        print(f"SQLite operation error: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()

def get_active_db_type():
    global PG_POOL
    return "postgres" if (DB_TYPE == "postgres" and HAS_POSTGRES and PG_POOL is not None) else "sqlite"

def init_db():
    """Initializes both PostgreSQL and SQLite to ensure schema is ready regardless of connection state."""
    _init_sqlite()
    if DB_TYPE == "postgres" and HAS_POSTGRES:
        try:
            _init_postgres()
            print("PostgreSQL schema initialized successfully.")
        except Exception as e:
            print(f"Warning: PostgreSQL initialization failed: {e}. Using SQLite.")

def _init_sqlite():
    with _db_lock:
        directory = os.path.dirname(DB_PATH)
        if directory and not os.path.exists(directory):
            os.makedirs(directory, exist_ok=True)
        conn = sqlite3.connect(DB_PATH)
        try:
            cur = conn.cursor()
            # Core Tables
            cur.execute("CREATE TABLE IF NOT EXISTS parcels (id TEXT, company_id TEXT, batch_id TEXT, type TEXT, priority TEXT, zone TEXT, status TEXT, quality_status TEXT, delivery_status TEXT, volume REAL, weight_kg REAL, destination_city TEXT, damage_flag INTEGER DEFAULT 0, assigned_robot TEXT, eta TEXT, route_path TEXT, created_at TEXT, godown TEXT, amount_to_pay REAL, PRIMARY KEY (id, company_id))")
            cur.execute("CREATE TABLE IF NOT EXISTS batches (id TEXT, company_id TEXT, total_parcels INTEGER, vehicle_id TEXT, driver_name TEXT, created_at TEXT, PRIMARY KEY (id, company_id))")
            cur.execute("CREATE TABLE IF NOT EXISTS vehicles (vehicle_id TEXT PRIMARY KEY, company_id TEXT, vehicle_number TEXT, driver_name TEXT, capacity_kg REAL, current_status TEXT, last_active TEXT, maintenance_status TEXT DEFAULT 'OK', last_service_date TEXT, fuel_level INTEGER DEFAULT 100, vehicle_type TEXT)")
            cur.execute("CREATE TABLE IF NOT EXISTS transport_tracking (id INTEGER PRIMARY KEY AUTOINCREMENT, company_id TEXT, vehicle_id TEXT, parcel_count INTEGER, destination TEXT, dispatch_time TEXT, arrival_time TEXT, status TEXT, total_amount REAL DEFAULT 0, settled_amount REAL DEFAULT 0, parcel_ids TEXT)")
            cur.execute("CREATE TABLE IF NOT EXISTS financial_records (id INTEGER PRIMARY KEY AUTOINCREMENT, company_id TEXT, parcel_id TEXT, amount REAL, payment_mode TEXT, payment_status TEXT, transaction_id TEXT, created_at TEXT)")
            cur.execute("CREATE TABLE IF NOT EXISTS notifications (id INTEGER PRIMARY KEY AUTOINCREMENT, company_id TEXT, role TEXT, message TEXT, read_status INTEGER DEFAULT 0, created_at TEXT)")
            cur.execute("CREATE TABLE IF NOT EXISTS robots (id TEXT, company_id TEXT, status TEXT, assigned_parcel TEXT, current_zone TEXT, utilization_percentage REAL, type TEXT, battery INTEGER DEFAULT 100, PRIMARY KEY (id, company_id))")
            cur.execute("CREATE TABLE IF NOT EXISTS logs (id INTEGER PRIMARY KEY AUTOINCREMENT, company_id TEXT, message TEXT, source TEXT, timestamp TEXT)")
            cur.execute("CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, company_id TEXT, email TEXT UNIQUE, password_hash TEXT, role TEXT, created_at TEXT)")
            cur.execute("CREATE TABLE IF NOT EXISTS predictions (date TEXT, company_id TEXT, predicted_volume INTEGER, lower_bound INTEGER, upper_bound INTEGER, PRIMARY KEY (date, company_id))")
            
            # Migrations
            for col in [("robots", "battery", "INTEGER DEFAULT 100"), ("robots", "type", "TEXT")]:
                try: cur.execute(f"ALTER TABLE {col[0]} ADD COLUMN {col[1]} {col[2]}")
                except Exception: pass
            
            _seed_users(cur, is_pg=False)
            conn.commit()
        finally:
            conn.close()

def _init_postgres():
    conn = None
    if DATABASE_URL:
        try:
            conn = psycopg2.connect(dsn=DATABASE_URL, connect_timeout=3)
        except Exception as e:
            print(f"PostgreSQL init connection failed via DATABASE_URL: {e}")
            raise
    else:
        target_db = PG_DB
        try:
            conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, database=target_db, user=PG_USER, password=PG_PASS, connect_timeout=3)
        except Exception:
            target_db = f"{PG_DB};"
            conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, database=target_db, user=PG_USER, password=PG_PASS, connect_timeout=3)
    try:
        cur = conn.cursor()
        # Same tables as SQLite but with Postgres types
        cur.execute("CREATE TABLE IF NOT EXISTS parcels (id TEXT, company_id TEXT, batch_id TEXT, type TEXT, priority TEXT, zone TEXT, status TEXT, quality_status TEXT, delivery_status TEXT, volume REAL, weight_kg REAL, destination_city TEXT, damage_flag INTEGER DEFAULT 0, assigned_robot TEXT, eta TEXT, route_path TEXT, created_at TEXT, godown TEXT, amount_to_pay REAL, PRIMARY KEY (id, company_id))")
        cur.execute("CREATE TABLE IF NOT EXISTS batches (id TEXT, company_id TEXT, total_parcels INTEGER, vehicle_id TEXT, driver_name TEXT, created_at TEXT, PRIMARY KEY (id, company_id))")
        cur.execute("CREATE TABLE IF NOT EXISTS vehicles (vehicle_id TEXT PRIMARY KEY, company_id TEXT, vehicle_number TEXT, driver_name TEXT, capacity_kg REAL, current_status TEXT, last_active TEXT, maintenance_status TEXT DEFAULT 'OK', last_service_date TEXT, fuel_level INTEGER DEFAULT 100, vehicle_type TEXT)")
        cur.execute("CREATE TABLE IF NOT EXISTS transport_tracking (id SERIAL PRIMARY KEY, company_id TEXT, vehicle_id TEXT, parcel_count INTEGER, destination TEXT, dispatch_time TEXT, arrival_time TEXT, status TEXT, total_amount REAL DEFAULT 0, settled_amount REAL DEFAULT 0, parcel_ids TEXT)")
        cur.execute("CREATE TABLE IF NOT EXISTS financial_records (id SERIAL PRIMARY KEY, company_id TEXT, parcel_id TEXT, amount REAL, payment_mode TEXT, payment_status TEXT, transaction_id TEXT, created_at TEXT)")
        cur.execute("CREATE TABLE IF NOT EXISTS notifications (id SERIAL PRIMARY KEY, company_id TEXT, role TEXT, message TEXT, read_status INTEGER DEFAULT 0, created_at TEXT)")
        cur.execute("CREATE TABLE IF NOT EXISTS robots (id TEXT, company_id TEXT, status TEXT, assigned_parcel TEXT, current_zone TEXT, utilization_percentage REAL, type TEXT, battery INTEGER DEFAULT 100, PRIMARY KEY (id, company_id))")
        cur.execute("CREATE TABLE IF NOT EXISTS logs (id SERIAL PRIMARY KEY, company_id TEXT, message TEXT, source TEXT, timestamp TEXT)")
        cur.execute("CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, company_id TEXT, email TEXT UNIQUE, password_hash TEXT, role TEXT, created_at TEXT)")
        cur.execute("CREATE TABLE IF NOT EXISTS predictions (date TEXT, company_id TEXT, predicted_volume INTEGER, lower_bound INTEGER, upper_bound INTEGER, PRIMARY KEY (date, company_id))")
        
        # Postgres Migrations
        try:
            cur.execute("ALTER TABLE robots ADD COLUMN IF NOT EXISTS type TEXT")
            cur.execute("ALTER TABLE robots ADD COLUMN IF NOT EXISTS battery INTEGER DEFAULT 100")
        except Exception: conn.rollback()

        _seed_users(cur, is_pg=True)
        conn.commit()
    finally:
        conn.close()

def _seed_users(cur, is_pg=False):
    pass # Users are seeded in backend.py ensure_role_users

# --- ROBOT HELPERS ---

def upsert_robot(robot, company_id=None):
    with _db_lock:
        with get_connection() as conn:
            cur = conn.cursor()
            cid = company_id or robot.get("company_id") or _current_company_id()
            active_db = get_active_db_type()
            vals = (robot.get("id"), cid, robot.get("status"), robot.get("assigned_parcel"), robot.get("current_zone"),
                    float(robot.get("utilization_percentage", 0.0)), robot.get("type"), int(robot.get("battery", 100)))
            if active_db == "postgres":
                cur.execute("""
                    INSERT INTO robots (id, company_id, status, assigned_parcel, current_zone, utilization_percentage, type, battery)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT(id, company_id) DO UPDATE SET
                        status=EXCLUDED.status, assigned_parcel=EXCLUDED.assigned_parcel,
                        current_zone=EXCLUDED.current_zone, utilization_percentage=EXCLUDED.utilization_percentage,
                        type=COALESCE(EXCLUDED.type, robots.type), battery=EXCLUDED.battery
                """, vals)
            else:
                cur.execute("""
                    INSERT INTO robots (id, company_id, status, assigned_parcel, current_zone, utilization_percentage, type, battery)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(id, company_id) DO UPDATE SET
                        status=excluded.status, assigned_parcel=excluded.assigned_parcel,
                        current_zone=excluded.current_zone, utilization_percentage=excluded.utilization_percentage,
                        type=COALESCE(excluded.type, robots.type), battery=excluded.battery
                """, vals)

def get_robots(company_id=None):
    with get_connection() as conn:
        active_db = get_active_db_type()
        placeholder = "%s" if active_db == "postgres" else "?"
        cid = company_id or _current_company_id()
        query = f"SELECT id, status, assigned_parcel, current_zone as zone, utilization_percentage, COALESCE(type, 'Standard') AS type, COALESCE(battery, 100) as battery FROM robots WHERE company_id = {placeholder} ORDER BY id"
        if active_db == "postgres":
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute(query, (cid,))
        else:
            cur = conn.execute(query, (cid,))
        return [dict(row) for row in cur.fetchall()]

def delete_robot(robot_id, company_id=None):
    cid = company_id or _current_company_id()
    with _db_lock:
        with get_connection() as conn:
            cur = conn.cursor()
            active_db = get_active_db_type()
            placeholder = "%s" if active_db == "postgres" else "?"
            cur.execute(f"DELETE FROM robots WHERE id = {placeholder} AND company_id = {placeholder}", (robot_id, cid))

def update_robot_type(robot_id, new_type, company_id=None):
    cid = company_id or _current_company_id()
    with _db_lock:
        with get_connection() as conn:
            cur = conn.cursor()
            active_db = get_active_db_type()
            placeholder = "%s" if active_db == "postgres" else "?"
            cur.execute(f"UPDATE robots SET type = {placeholder} WHERE id = {placeholder} AND company_id = {placeholder}", (new_type, robot_id, cid))

# --- LOG HELPERS ---

def insert_log(message, source=None, company_id=None):
    ts = datetime.utcnow().isoformat(timespec="seconds")
    cid = company_id or _current_company_id()
    with _db_lock:
        with get_connection() as conn:
            cur = conn.cursor()
            active_db = get_active_db_type()
            placeholder = "%s" if active_db == "postgres" else "?"
            cur.execute(f"INSERT INTO logs (company_id, message, source, timestamp) VALUES ({placeholder}, {placeholder}, {placeholder}, {placeholder})", (cid, message, source, ts))

def get_recent_logs(limit=50, company_id=None):
    with get_connection() as conn:
        active_db = get_active_db_type()
        placeholder = "%s" if active_db == "postgres" else "?"
        cid = company_id or _current_company_id()
        query = f"SELECT message, source, timestamp FROM logs WHERE company_id = {placeholder} ORDER BY timestamp DESC LIMIT {placeholder}"
        if active_db == "postgres":
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute(query, (cid, limit))
        else:
            cur = conn.execute(query, (cid, limit))
        return [dict(row) for row in cur.fetchall()]

# --- USER HELPERS ---

def upsert_user(user):
    with _db_lock:
        with get_connection() as conn:
            cur = conn.cursor()
            active_db = get_active_db_type()
            ca = user.get("created_at") or datetime.utcnow().isoformat(timespec="seconds")
            vals = (user.get("id"), user.get("company_id") or _current_company_id(), user.get("email"), user.get("password_hash"), user.get("role", "manager"), ca)
            if active_db == "postgres":
                cur.execute("INSERT INTO users (id, company_id, email, password_hash, role, created_at) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO UPDATE SET email = EXCLUDED.email, password_hash = EXCLUDED.password_hash, role = EXCLUDED.role", vals)
            else:
                cur.execute("INSERT INTO users (id, company_id, email, password_hash, role, created_at) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT (id) DO UPDATE SET email = excluded.email, password_hash = excluded.password_hash, role = excluded.role", vals)

def get_user_by_email(email):
    with get_connection() as conn:
        active_db = get_active_db_type()
        placeholder = "%s" if active_db == "postgres" else "?"
        query = f"SELECT * FROM users WHERE email = {placeholder} LIMIT 1"
        if active_db == "postgres":
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute(query, (email,))
        else:
            cur = conn.execute(query, (email,))
        row = cur.fetchone()
        return dict(row) if row else None

# --- PARCEL HELPERS ---

def get_parcel_by_id(parcel_id, company_id=None):
    with get_connection() as conn:
        cid = company_id or _current_company_id()
        active_db = get_active_db_type()
        placeholder = "%s" if active_db == "postgres" else "?"
        query = f"SELECT * FROM parcels WHERE id = {placeholder} AND company_id = {placeholder} LIMIT 1"
        if active_db == "postgres":
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute(query, (parcel_id, cid))
        else:
            cur = conn.execute(query, (parcel_id, cid))
        row = cur.fetchone()
        return dict(row) if row else None

def update_parcel_status(parcel_id, status=None, quality_status=None, delivery_status=None, eta=None, route_path=None, company_id=None):
    with get_connection() as conn:
        active_db = get_active_db_type()
        placeholder = "%s" if active_db == "postgres" else "?"
        fields = []
        params = []
        if status: fields.append(f"status = {placeholder}"); params.append(status)
        if quality_status: fields.append(f"quality_status = {placeholder}"); params.append(quality_status)
        if delivery_status: fields.append(f"delivery_status = {placeholder}"); params.append(delivery_status)
        if eta: fields.append(f"eta = {placeholder}"); params.append(eta)
        if route_path: fields.append(f"route_path = {placeholder}"); params.append(route_path)
        if not fields: return
        cid = company_id or _current_company_id()
        query = f"UPDATE parcels SET {', '.join(fields)} WHERE id = {placeholder} AND company_id = {placeholder}"
        params.extend([parcel_id, cid])
        cur = conn.cursor()
        cur.execute(query, params)

def insert_parcel(parcel):
    with _db_lock:
        with get_connection() as conn:
            cur = conn.cursor()
            cid = parcel.get("company_id") or _current_company_id()
            active_db = get_active_db_type()
            vals = (parcel.get("id"), cid, parcel.get("batch_id"), parcel.get("type"), parcel.get("priority"), parcel.get("zone"), parcel.get("status"), parcel.get("quality_status"), parcel.get("delivery_status"), float(parcel.get("volume", 0.0)), float(parcel.get("weight_kg", 0.0)), parcel.get("destination_city"), int(parcel.get("damage_flag", 0)), parcel.get("assigned_robot"), parcel.get("eta"), parcel.get("route_path"), parcel.get("created_at"), parcel.get("godown"), float(parcel.get("amount_to_pay", 0.0)))
            if active_db == "postgres":
                cur.execute("""
                    INSERT INTO parcels (id, company_id, batch_id, type, priority, zone, status, quality_status, delivery_status, volume, weight_kg, destination_city, damage_flag, assigned_robot, eta, route_path, created_at, godown, amount_to_pay)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id, company_id) DO UPDATE SET status = EXCLUDED.status
                """, vals)
            else:
                cur.execute("""
                    INSERT INTO parcels (id, company_id, batch_id, type, priority, zone, status, quality_status, delivery_status, volume, weight_kg, destination_city, damage_flag, assigned_robot, eta, route_path, created_at, godown, amount_to_pay)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (id, company_id) DO UPDATE SET status = excluded.status
                """, vals)

def bulk_insert_parcels(parcels, company_id=None):
    with _db_lock:
        with get_connection() as conn:
            cur = conn.cursor()
            active_db = get_active_db_type()
            for p in parcels:
                cid = p.get("company_id") or company_id or _current_company_id()
                vals = (p["id"], cid, p["batch_id"], p["type"], p["priority"], p["zone"], p["status"], p["quality_status"], p["delivery_status"], float(p.get("volume", 0.0)), float(p.get("weight_kg", 0.0)), p.get("destination_city"), int(p.get("damage_flag", 0)), p.get("assigned_robot"), p.get("eta"), p.get("route_path"), p["created_at"], p.get("godown"), float(p.get("amount_to_pay", 0.0)))
                if active_db == "postgres":
                    cur.execute("""
                        INSERT INTO parcels (id, company_id, batch_id, type, priority, zone, status, quality_status, delivery_status, volume, weight_kg, destination_city, damage_flag, assigned_robot, eta, route_path, created_at, godown, amount_to_pay)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (id, company_id) DO UPDATE SET status = EXCLUDED.status
                    """, vals)
                else:
                    cur.execute("""
                        INSERT INTO parcels (id, company_id, batch_id, type, priority, zone, status, quality_status, delivery_status, volume, weight_kg, destination_city, damage_flag, assigned_robot, eta, route_path, created_at, godown, amount_to_pay)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT (id, company_id) DO UPDATE SET status = excluded.status
                    """, vals)

# --- VEHICLE & TRANSPORT ---

def upsert_vehicle(vehicle_id, vehicle_number, driver_name, capacity=500, company_id=None, status="IDLE", maintenance="OK", vehicle_type=None):
    cid = company_id or _current_company_id()
    ts = datetime.utcnow().isoformat()
    with _db_lock:
        with get_connection() as conn:
            cur = conn.cursor()
            active_db = get_active_db_type()
            vals = (vehicle_id, cid, vehicle_number, driver_name, capacity, status, ts, maintenance, vehicle_type)
            if active_db == "postgres":
                cur.execute("INSERT INTO vehicles (vehicle_id, company_id, vehicle_number, driver_name, capacity_kg, current_status, last_active, maintenance_status, vehicle_type) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (vehicle_id) DO UPDATE SET current_status = EXCLUDED.current_status, last_active = EXCLUDED.last_active", vals)
            else:
                cur.execute("INSERT INTO vehicles (vehicle_id, company_id, vehicle_number, driver_name, capacity_kg, current_status, last_active, maintenance_status, vehicle_type) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (vehicle_id) DO UPDATE SET current_status = excluded.current_status, last_active = excluded.last_active", vals)

def delete_vehicle(vehicle_id, company_id=None):
    cid = company_id or _current_company_id()
    with _db_lock:
        with get_connection() as conn:
            cur = conn.cursor()
            active_db = get_active_db_type()
            placeholder = "%s" if active_db == "postgres" else "?"
            cur.execute(f"DELETE FROM vehicles WHERE vehicle_id = {placeholder} AND company_id = {placeholder}", (vehicle_id, cid))

def get_vehicles(company_id=None):
    with get_connection() as conn:
        cid = company_id or _current_company_id()
        active_db = get_active_db_type()
        placeholder = "%s" if active_db == "postgres" else "?"
        query = f"SELECT * FROM vehicles WHERE company_id = {placeholder}"
        if active_db == "postgres":
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute(query, (cid,))
        else:
            cur = conn.execute(query, (cid,))
        return [dict(row) for row in cur.fetchall()]

# --- FINANCE & BATCHES ---

def insert_batch(batch_id, total_parcels, company_id=None, vehicle_id=None, driver_name=None):
    ts = datetime.utcnow().isoformat()
    cid = company_id or _current_company_id()
    with _db_lock:
        with get_connection() as conn:
            cur = conn.cursor()
            active_db = get_active_db_type()
            vals = (batch_id, cid, total_parcels, vehicle_id, driver_name, ts)
            if active_db == "postgres":
                cur.execute("INSERT INTO batches (id, company_id, total_parcels, vehicle_id, driver_name, created_at) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (id, company_id) DO UPDATE SET total_parcels = EXCLUDED.total_parcels", vals)
            else:
                cur.execute("INSERT INTO batches (id, company_id, total_parcels, vehicle_id, driver_name, created_at) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT (id, company_id) DO UPDATE SET total_parcels = excluded.total_parcels", vals)

def get_recent_batches(limit=10, company_id=None):
    with get_connection() as conn:
        cid = company_id or _current_company_id()
        active_db = get_active_db_type()
        placeholder = "%s" if active_db == "postgres" else "?"
        query = f"SELECT * FROM batches WHERE company_id = {placeholder} ORDER BY created_at DESC LIMIT {placeholder}"
        if active_db == "postgres":
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute(query, (cid, limit))
        else:
            cur = conn.execute(query, (cid, limit))
        return [dict(row) for row in cur.fetchall()]

def insert_financial_record(parcel_id, amount, mode, company_id=None):
    ts = datetime.utcnow().isoformat()
    cid = company_id or _current_company_id()
    with _db_lock:
        with get_connection() as conn:
            cur = conn.cursor()
            active_db = get_active_db_type()
            placeholder = "%s" if active_db == "postgres" else "?"
            cur.execute(f"INSERT INTO financial_records (company_id, parcel_id, amount, payment_mode, payment_status, created_at) VALUES ({placeholder}, {placeholder}, {placeholder}, {placeholder}, 'PAID', {placeholder})", (cid, parcel_id, amount, mode, ts))

# --- NOTIFICATIONS ---

def insert_notification(role, message, company_id=None):
    cid = company_id or _current_company_id()
    ts = datetime.utcnow().isoformat()
    with _db_lock:
        with get_connection() as conn:
            cur = conn.cursor()
            active_db = get_active_db_type()
            placeholder = "%s" if active_db == "postgres" else "?"
            cur.execute(f"INSERT INTO notifications (company_id, role, message, created_at) VALUES ({placeholder}, {placeholder}, {placeholder}, {placeholder})", (cid, role.lower(), message, ts))

def get_notifications(role, company_id=None):
    cid = company_id or _current_company_id()
    with get_connection() as conn:
        active_db = get_active_db_type()
        placeholder = "%s" if active_db == "postgres" else "?"
        query = f"SELECT * FROM notifications WHERE company_id = {placeholder} AND (role = {placeholder} OR role = 'all') ORDER BY created_at DESC LIMIT 20"
        if active_db == "postgres":
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute(query, (cid, role.lower()))
        else:
            cur = conn.execute(query, (cid, role.lower()))
        return [dict(row) for row in cur.fetchall()]

# --- PREDICTION HELPERS ---

def save_prediction(date, predicted_volume, lower_bound, upper_bound, company_id=None):
    cid = company_id or _current_company_id()
    with _db_lock:
        with get_connection() as conn:
            cur = conn.cursor()
            active_db = get_active_db_type()
            vals = (date, cid, int(predicted_volume), int(lower_bound), int(upper_bound))
            if active_db == "postgres":
                cur.execute("""
                    INSERT INTO predictions (date, company_id, predicted_volume, lower_bound, upper_bound)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (date, company_id) DO UPDATE SET
                        predicted_volume = EXCLUDED.predicted_volume,
                        lower_bound = EXCLUDED.lower_bound,
                        upper_bound = EXCLUDED.upper_bound
                """, vals)
            else:
                cur.execute("""
                    INSERT INTO predictions (date, company_id, predicted_volume, lower_bound, upper_bound)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT (date, company_id) DO UPDATE SET
                        predicted_volume = excluded.predicted_volume,
                        lower_bound = excluded.lower_bound,
                        upper_bound = excluded.upper_bound
                """, vals)

def get_latest_prediction(company_id=None):
    cid = company_id or _current_company_id()
    with get_connection() as conn:
        active_db = get_active_db_type()
        placeholder = "%s" if active_db == "postgres" else "?"
        query = f"SELECT * FROM predictions WHERE company_id = {placeholder} ORDER BY date DESC LIMIT 1"
        if active_db == "postgres":
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute(query, (cid,))
        else:
            cur = conn.execute(query, (cid,))
        row = cur.fetchone()
        return dict(row) if row else None

# --- ADDITIONAL PARCEL HELPERS ---

def get_recent_parcels(limit=50, company_id=None, batch_id=None):
    cid = company_id or _current_company_id()
    with get_connection() as conn:
        active_db = get_active_db_type()
        placeholder = "%s" if active_db == "postgres" else "?"
        params = [cid]
        query = f"SELECT * FROM parcels WHERE company_id = {placeholder}"
        if batch_id:
            query += f" AND batch_id = {placeholder}"
            params.append(batch_id)
        query += f" ORDER BY created_at DESC LIMIT {placeholder}"
        params.append(limit)
        
        if active_db == "postgres":
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute(query, tuple(params))
        else:
            cur = conn.execute(query, tuple(params))
        return [dict(row) for row in cur.fetchall()]

def get_parcel_counts_by_status(company_id=None):
    cid = company_id or _current_company_id()
    with get_connection() as conn:
        active_db = get_active_db_type()
        placeholder = "%s" if active_db == "postgres" else "?"
        query = f"SELECT status, COUNT(*) as count FROM parcels WHERE company_id = {placeholder} GROUP BY status"
        if active_db == "postgres":
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute(query, (cid,))
        else:
            cur = conn.execute(query, (cid,))
        rows = cur.fetchall()
        return {row["status"]: row["count"] for row in rows}

# --- ADDITIONAL FINANCE HELPERS ---

def get_financial_records(company_id=None, limit=50):
    cid = company_id or _current_company_id()
    with get_connection() as conn:
        active_db = get_active_db_type()
        placeholder = "%s" if active_db == "postgres" else "?"
        query = f"SELECT * FROM financial_records WHERE company_id = {placeholder} ORDER BY created_at DESC LIMIT {placeholder}"
        if active_db == "postgres":
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute(query, (cid, limit))
        else:
            cur = conn.execute(query, (cid, limit))
        return [dict(row) for row in cur.fetchall()]

def get_financial_summary(company_id=None):
    cid = company_id or _current_company_id()
    with get_connection() as conn:
        active_db = get_active_db_type()
        placeholder = "%s" if active_db == "postgres" else "?"
        query = f"SELECT SUM(amount) as total_revenue, COUNT(*) as total_transactions FROM financial_records WHERE company_id = {placeholder}"
        if active_db == "postgres":
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute(query, (cid,))
        else:
            cur = conn.execute(query, (cid,))
        row = cur.fetchone()
        return dict(row) if row else {"total_revenue": 0, "total_transactions": 0}

# --- ADDITIONAL USER HELPERS ---

def get_user_by_id(user_id):
    with get_connection() as conn:
        active_db = get_active_db_type()
        placeholder = "%s" if active_db == "postgres" else "?"
        query = f"SELECT * FROM users WHERE id = {placeholder} LIMIT 1"
        if active_db == "postgres":
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute(query, (user_id,))
        else:
            cur = conn.execute(query, (user_id,))
        row = cur.fetchone()
        return dict(row) if row else None

# --- SYSTEM HELPERS ---

def pg_pool_refresh_loop():
    """Dummy loop to satisfy backend.py if needed."""
    while True:
        time.sleep(600)
