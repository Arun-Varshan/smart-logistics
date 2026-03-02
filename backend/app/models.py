from datetime import datetime

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
)
from sqlalchemy.orm import relationship

from .database import Base


class Tenant(Base):
    __tablename__ = "tenants"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), nullable=False)
    company_name = Column(String(200), nullable=False)
    api_key = Column(String(255), unique=True, index=True, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    is_active = Column(Boolean, default=True, nullable=False)

    users = relationship("User", back_populates="tenant")
    hubs = relationship("Hub", back_populates="tenant")


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    email = Column(String(255), unique=True, index=True, nullable=False)
    password_hash = Column(String(255), nullable=False)
    role = Column(String(50), default="user", nullable=False)
    tenant_id = Column(Integer, ForeignKey("tenants.id"), nullable=False, index=True)

    tenant = relationship("Tenant", back_populates="users")


class Hub(Base):
    __tablename__ = "hubs"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(200), nullable=False)
    location = Column(String(200), nullable=True)
    tenant_id = Column(Integer, ForeignKey("tenants.id"), nullable=False, index=True)

    tenant = relationship("Tenant", back_populates="hubs")
    timeseries = relationship("HubTimeseries", back_populates="hub")
    robots = relationship("Robot", back_populates="hub")
    parcels = relationship("Parcel", back_populates="hub")
    alerts = relationship("Alert", back_populates="hub")


class HubTimeseries(Base):
    __tablename__ = "hub_timeseries"

    id = Column(Integer, primary_key=True, index=True)
    hub_id = Column(Integer, ForeignKey("hubs.id"), nullable=False, index=True)
    tenant_id = Column(Integer, ForeignKey("tenants.id"), nullable=False, index=True)
    timestamp = Column(DateTime, default=datetime.utcnow, index=True)
    total_parcels = Column(Integer, default=0)
    parcels_processed = Column(Integer, default=0)
    active_robots = Column(Integer, default=0)
    avg_processing_time = Column(Float, default=0.0)
    zone_medical_load = Column(Integer, default=0)
    zone_fragile_load = Column(Integer, default=0)
    zone_general_load = Column(Integer, default=0)
    utilization_pct = Column(Float, default=0.0)

    hub = relationship("Hub", back_populates="timeseries")


class Robot(Base):
    __tablename__ = "robots"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(50), nullable=False)
    hub_id = Column(Integer, ForeignKey("hubs.id"), nullable=False, index=True)
    tenant_id = Column(Integer, ForeignKey("tenants.id"), nullable=False, index=True)
    status = Column(String(50), default="idle")
    x = Column(Float, default=0.0)
    y = Column(Float, default=0.0)
    utilization_pct = Column(Float, default=0.0)
    last_update = Column(DateTime, default=datetime.utcnow)

    hub = relationship("Hub", back_populates="robots")


class Parcel(Base):
    __tablename__ = "parcels"

    id = Column(Integer, primary_key=True, index=True)
    tracking_id = Column(String(100), index=True, nullable=False)
    hub_id = Column(Integer, ForeignKey("hubs.id"), nullable=False, index=True)
    tenant_id = Column(Integer, ForeignKey("tenants.id"), nullable=False, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    zone = Column(String(100), nullable=False)
    status = Column(String(50), default="intake")
    assigned_robot = Column(String(50), nullable=True)

    hub = relationship("Hub", back_populates="parcels")


class Alert(Base):
    __tablename__ = "alerts"

    id = Column(Integer, primary_key=True, index=True)
    hub_id = Column(Integer, ForeignKey("hubs.id"), nullable=False, index=True)
    tenant_id = Column(Integer, ForeignKey("tenants.id"), nullable=False, index=True)
    timestamp = Column(DateTime, default=datetime.utcnow, index=True)
    type = Column(String(100), nullable=False)
    message = Column(Text, nullable=False)
    severity = Column(String(50), default="info")

    hub = relationship("Hub", back_populates="alerts")


class Prediction(Base):
    __tablename__ = "predictions"

    id = Column(Integer, primary_key=True, index=True)
    hub_id = Column(Integer, ForeignKey("hubs.id"), nullable=False, index=True)
    tenant_id = Column(Integer, ForeignKey("tenants.id"), nullable=False, index=True)
    ds = Column(DateTime, index=True, nullable=False)
    yhat = Column(Float, nullable=False)
    yhat_lower = Column(Float, nullable=True)
    yhat_upper = Column(Float, nullable=True)

