from fastapi import Depends, HTTPException, status
from sqlalchemy.orm import Session

from .auth import get_current_token_data
from .database import get_db
from .models import Hub, Tenant, User


def get_current_user(
    token_data=Depends(get_current_token_data),
    db: Session = Depends(get_db),
) -> User:
    user = db.query(User).filter(
        User.id == token_data.user_id,
        User.tenant_id == token_data.tenant_id,
    ).first()
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found",
        )
    return user


def get_current_tenant(
    token_data=Depends(get_current_token_data),
    db: Session = Depends(get_db),
) -> Tenant:
    tenant = db.query(Tenant).filter(
        Tenant.id == token_data.tenant_id,
        Tenant.is_active.is_(True),
    ).first()
    if not tenant:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Tenant inactive or not found",
        )
    return tenant


def ensure_hub_belongs_to_tenant(
    hub_id: int,
    token_data=Depends(get_current_token_data),
    db: Session = Depends(get_db),
) -> Hub:
    hub = db.query(Hub).filter(
        Hub.id == hub_id,
        Hub.tenant_id == token_data.tenant_id,
    ).first()
    if not hub:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Hub not found for this tenant",
        )
    return hub

