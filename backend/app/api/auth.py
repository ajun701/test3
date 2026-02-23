# app/api/auth.py
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.core.config import settings
from app.core.security import hash_password, verify_password, create_access_token, get_current_user
from app.db.session import get_db
from app.models import User
from app.schemas import RegisterRequest, LoginRequest, TokenResponse, UserInfoResponse

router = APIRouter(prefix="/auth", tags=["认证"])


@router.post("/register", response_model=TokenResponse, summary="用户注册")
def register(req: RegisterRequest, db: Session = Depends(get_db)):
    # 校验注册密钥
    if req.register_key != settings.REGISTER_SECRET_KEY:
        raise HTTPException(status_code=403, detail="注册密钥错误")

    # 检查用户名是否已存在
    existing = db.query(User).filter(User.username == req.username).first()
    if existing:
        raise HTTPException(status_code=400, detail="用户名已存在")

    user = User(
        username=req.username,
        hashed_password=hash_password(req.password),
    )
    db.add(user)
    db.commit()
    db.refresh(user)

    token = create_access_token(data={"sub": user.username})
    return TokenResponse(access_token=token, username=user.username)


@router.post("/login", response_model=TokenResponse, summary="用户登录")
def login(req: LoginRequest, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.username == req.username).first()
    if not user or not verify_password(req.password, user.hashed_password):
        raise HTTPException(status_code=401, detail="用户名或密码错误")
    if not user.is_active:
        raise HTTPException(status_code=403, detail="账号已被禁用")

    token = create_access_token(data={"sub": user.username})
    return TokenResponse(access_token=token, username=user.username)


@router.get("/me", response_model=UserInfoResponse, summary="获取当前用户信息")
def get_me(current_user: User = Depends(get_current_user)):
    return current_user
