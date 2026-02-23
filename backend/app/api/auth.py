# app/api/auth.py
import logging
import secrets

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import Session

from app.core.config import settings
from app.core.security import hash_password, verify_password, create_access_token, get_current_user
from app.db.session import get_db
from app.models import User
from app.schemas import RegisterRequest, LoginRequest, TokenResponse, UserInfoResponse

router = APIRouter(prefix="/auth", tags=["认证"])
logger = logging.getLogger(__name__)


@router.post("/register", response_model=TokenResponse, summary="用户注册")
def register(req: RegisterRequest, db: Session = Depends(get_db)):
    provided_register_key = str(req.register_key or "").strip()
    expected_register_key = str(settings.REGISTER_SECRET_KEY or "").strip()

    if not expected_register_key:
        raise HTTPException(status_code=500, detail="服务端未配置注册密钥")

    # 校验注册密钥
    if not secrets.compare_digest(provided_register_key, expected_register_key):
        raise HTTPException(status_code=403, detail="注册密钥错误")

    username = str(req.username or "").strip()
    if not username:
        raise HTTPException(status_code=400, detail="用户名不能为空")

    # 检查用户名是否已存在
    try:
        existing = db.query(User.id).filter(User.username == username).first()
    except SQLAlchemyError:
        logger.exception("Failed to read users table while registering")
        raise HTTPException(status_code=500, detail="数据库读取失败，请检查数据库结构")

    if existing:
        raise HTTPException(status_code=400, detail="用户名已存在")

    try:
        user = User(
            username=username,
            hashed_password=hash_password(req.password),
        )
        db.add(user)
        db.commit()
        db.refresh(user)
    except IntegrityError:
        db.rollback()
        raise HTTPException(status_code=400, detail="用户名已存在")
    except SQLAlchemyError:
        db.rollback()
        logger.exception("Failed to create user during registration")
        raise HTTPException(status_code=500, detail="注册失败，请稍后重试")

    token = create_access_token(data={"sub": user.username})
    return TokenResponse(access_token=token, username=user.username)


@router.post("/login", response_model=TokenResponse, summary="用户登录")
def login(req: LoginRequest, db: Session = Depends(get_db)):
    username = str(req.username or "").strip()
    try:
        user = db.query(User).filter(User.username == username).first()
    except SQLAlchemyError:
        logger.exception("Failed to read users table while logging in")
        raise HTTPException(status_code=500, detail="数据库读取失败，请稍后重试")

    if not user or not verify_password(req.password, user.hashed_password):
        raise HTTPException(status_code=401, detail="用户名或密码错误")
    if not user.is_active:
        raise HTTPException(status_code=403, detail="账号已被禁用")

    token = create_access_token(data={"sub": user.username})
    return TokenResponse(access_token=token, username=user.username)


@router.get("/me", response_model=UserInfoResponse, summary="获取当前用户信息")
def get_me(current_user: User = Depends(get_current_user)):
    return current_user
