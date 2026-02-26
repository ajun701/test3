from __future__ import annotations

import hmac
import re
from datetime import datetime
from typing import Optional

from sqlalchemy.orm import Session

from app.core.config import settings
from app.core.security import hash_password, verify_password
from app.db.models import User

USERNAME_PATTERN = re.compile(r"^[A-Za-z0-9_]{3,50}$")


def _normalize_register_key(value: str) -> str:
    key = str(value or "").strip()
    wrappers = {("【", "】"), ("[", "]"), ("(", ")"), ("（", "）"), ('"', '"'), ("'", "'")}
    if len(key) >= 2 and (key[0], key[-1]) in wrappers:
        key = key[1:-1].strip()
    return key


def validate_username(username: str) -> str:
    value = str(username or "").strip()
    if not USERNAME_PATTERN.match(value):
        raise ValueError("用户名仅支持字母、数字、下划线，长度 3-50")
    return value


def register_user(db: Session, username: str, password: str, register_key: str) -> User:
    provided = _normalize_register_key(register_key)
    expected = _normalize_register_key(settings.register_secret)
    if not hmac.compare_digest(provided.encode("utf-8"), expected.encode("utf-8")):
        raise ValueError("注册密钥错误")

    valid_name = validate_username(username)
    if not str(password or ""):
        raise ValueError("密码不能为空")

    existing = db.query(User).filter(User.username == valid_name).first()
    if existing:
        raise ValueError("用户名已存在")

    user = User(
        username=valid_name,
        password_hash=hash_password(password),
        is_active=True,
        created_at=datetime.utcnow(),
        last_login_at=datetime.utcnow(),
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


def login_user(db: Session, username: str, password: str) -> User:
    valid_name = validate_username(username)
    user: Optional[User] = db.query(User).filter(User.username == valid_name).first()
    if not user or not verify_password(password, user.password_hash):
        raise ValueError("用户名或密码错误")
    if not user.is_active:
        raise ValueError("用户已禁用")

    user.last_login_at = datetime.utcnow()
    db.commit()
    db.refresh(user)
    return user
