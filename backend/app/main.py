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