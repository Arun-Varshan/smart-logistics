from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from . import auth
from .database import init_db
from .routes import hubs
from .routes import robots as robots_routes
from .routes import analytics as analytics_routes
from .routes import stream as stream_routes
from .routes import debug as debug_routes
from .simulator.run import start_simulator_thread


def create_app() -> FastAPI:
    app = FastAPI(title="Wiztric Technologies – Smart Logistics Hub API")

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    init_db()
    
    # Auto-seed users if using FastAPI backend
    try:
        from .database import SessionLocal
        from .auth import get_password_hash
        from .models import User
        
        db = SessionLocal()
        if db.query(User).count() == 0:
            print("Seeding default users (FastAPI)...")
            users = [
                ("admin@wiztric.com", "admin123", "ADMIN"),
                ("manager@wiztric.com", "manager123", "HUB_MANAGER"),
                ("qos@wiztric.com", "qos123", "QOS"),
                ("robots@wiztric.com", "robots123", "ROBOTICS"),
                ("delivery@wiztric.com", "delivery123", "DELIVERY"),
                ("finance@wiztric.com", "finance123", "FINANCE")
            ]
            for email, pwd, role in users:
                u = User(email=email, hashed_password=get_password_hash(pwd), role=role, tenant_id=1)
                db.add(u)
            db.commit()
            print("Seeded users.")
        db.close()
    except Exception as e:
        print(f"FastAPI seeding error: {e}")

    app.include_router(auth.router)
    app.include_router(hubs.router)
    app.include_router(robots_routes.router)
    app.include_router(analytics_routes.router)
    app.include_router(stream_routes.router)
    app.include_router(debug_routes.router)

    @app.get("/")   # ✅ MOVE THIS HERE
    def root():
        return {"status": "ok", "service": "smart-backend"}

    @app.on_event("startup")
    def _startup() -> None:
        start_simulator_thread()

    return app


app = create_app()