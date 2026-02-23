import hmac
import re
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.core.auth import get_current_user
from app.core.config import settings
from app.core.security import create_access_token, hash_password, verify_password
from app.db.session import get_db
from app.models import User
from app.schemas import AuthTokenResponse, LoginRequest, RegisterRequest, UserInfoResponse

router = APIRouter()
USERNAME_PATTERN = re.compile(r"^[A-Za-z0-9_]{3,50}$")


def _validate_username(username: str) -> str:
    value = str(username or "").strip()
    if not USERNAME_PATTERN.match(value):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="用户名仅支持字母、数字、下划线，长度 3-50",
        )
    return value


def _build_auth_response(user: User) -> AuthTokenResponse:
    token = create_access_token(user.username)
    return AuthTokenResponse(access_token=token, user=UserInfoResponse.model_validate(user))


@router.post("/auth/register", response_model=AuthTokenResponse, summary="用户注册")
def register_user(payload: RegisterRequest, db: Session = Depends(get_db)):
    if not hmac.compare_digest(str(payload.register_key or "").strip(), str(settings.REGISTER_SECRET or "").strip()):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="注册密钥错误")

    username = _validate_username(payload.username)
    existing = db.query(User).filter(User.username == username).first()
    if existing:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="用户名已存在")

    user = User(
        username=username,
        password_hash=hash_password(payload.password),
        is_active=True,
        created_at=datetime.utcnow(),
        last_login_at=datetime.utcnow(),
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    return _build_auth_response(user)


@router.post("/auth/login", response_model=AuthTokenResponse, summary="用户登录")
def login_user(payload: LoginRequest, db: Session = Depends(get_db)):
    username = _validate_username(payload.username)
    user = db.query(User).filter(User.username == username).first()
    if not user or not verify_password(payload.password, user.password_hash):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="用户名或密码错误")
    if not user.is_active:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="用户已禁用")

    user.last_login_at = datetime.utcnow()
    db.commit()
    db.refresh(user)
    return _build_auth_response(user)


@router.get("/auth/me", response_model=UserInfoResponse, summary="当前登录用户")
def get_me(current_user: User = Depends(get_current_user)):
    return UserInfoResponse.model_validate(current_user)
