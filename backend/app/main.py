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
                ("logistics@gmail.com", "admin123", "ADMIN"),
                ("logistics@gmail.com", "admin123", "HUB_MANAGER"),
                ("logistics@gmail.com", "admin123", "QOS"),
                ("logistics@gmail.com", "admin123", "ROBOTICS"),
                ("logistics@gmail.com", "admin123", "DELIVERY"),
                ("logistics@gmail.com", "admin123", "FINANCE")
            ]
            for email, pwd, role in users:
                # Use email+role as unique check or just add multiple entries for same email?
                # Typically email is unique. 
                # If we want one login for everything, we just need one user with SUPER ADMIN role or handle it.
                # BUT user requested: "for all departments set the mail... as logistics@gmail.com"
                # This implies one user account that can access everything OR multiple accounts with same email (which DB constraint prevents).
                # Strategy: Create ONE Super Admin user "logistics@gmail.com" with role "ADMIN" (which usually access all) 
                # OR update the existing single user to have a role that satisfies all.
                # However, your system checks `role` from token.
                # Let's seed ONE user "logistics@gmail.com" with role "ADMIN".
                # But wait, `ensure_role` checks specific strings.
                # If I log in as ADMIN, can I access Delivery? 
                # In backend.py: `ensure_role("admin", "logistics")` -> Yes if I have "admin" role.
                pass
            
            # Implementation: Just one super user since email must be unique in `models.py` (usually)
            u = User(email="logistics@gmail.com", hashed_password=get_password_hash("admin123"), role="ADMIN", tenant_id=1)
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