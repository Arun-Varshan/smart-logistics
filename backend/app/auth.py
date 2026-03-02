import os
from datetime import datetime, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError, jwt
from passlib.context import CryptContext
from pydantic import BaseModel
from sqlalchemy.orm import Session

from .database import get_db
from .models import Tenant, User


SECRET_KEY = os.environ.get("WIZTRIC_JWT_SECRET", "change-this-secret")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 12


pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login")

router = APIRouter(prefix="/auth", tags=["auth"])


class Token(BaseModel):
    access_token: str
    token_type: str


class TokenData(BaseModel):
    user_id: int
    tenant_id: int
    role: str


class UserCreate(BaseModel):
    email: str
    password: str
    tenant_name: Optional[str] = None
    company_name: Optional[str] = None


def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password: str) -> str:
    return pwd_context.hash(password)


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


def authenticate_user(db: Session, email: str, password: str) -> Optional[User]:
    user = db.query(User).filter(User.email == email).first()
    if not user:
        return None
    if not verify_password(password, user.password_hash):
        return None
    return user


@router.post("/register", response_model=Token)
def register_user(payload: UserCreate, db: Session = Depends(get_db)):
    existing = db.query(User).filter(User.email == payload.email).first()
    if existing:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Email already registered")

    tenant = db.query(Tenant).filter(Tenant.name == (payload.tenant_name or "Wiztric Demo")).first()
    if not tenant:
        tenant = Tenant(
            name=payload.tenant_name or "Wiztric Demo",
            company_name=payload.company_name or "Wiztric Technologies – Smart Logistics Hub",
            api_key=os.urandom(16).hex(),
        )
        db.add(tenant)
        db.flush()

    user = User(
        email=payload.email,
        password_hash=get_password_hash(payload.password),
        role="admin",
        tenant_id=tenant.id,
    )
    db.add(user)
    db.commit()
    db.refresh(user)

    token_data = {
        "user_id": user.id,
        "tenant_id": tenant.id,
        "role": user.role,
    }
    access_token = create_access_token(token_data)
    return {"access_token": access_token, "token_type": "bearer"}


@router.post("/login", response_model=Token)
def login_for_access_token(
    form_data: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(get_db)
):
    user = authenticate_user(db, form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    token_data = {
        "user_id": user.id,
        "tenant_id": user.tenant_id,
        "role": user.role,
    }
    access_token = create_access_token(token_data)
    return {"access_token": access_token, "token_type": "bearer"}


def get_current_token_data(token: str = Depends(oauth2_scheme)) -> TokenData:
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id = payload.get("user_id")
        tenant_id = payload.get("tenant_id")
        role = payload.get("role") or "user"
        if user_id is None or tenant_id is None:
            raise credentials_exception
        return TokenData(user_id=user_id, tenant_id=tenant_id, role=role)
    except JWTError:
        raise credentials_exception

