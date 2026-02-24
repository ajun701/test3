import base64
import hashlib
import hmac
import json
import secrets
import time
import uuid
from typing import Any, Dict, Optional

from app.core.config import settings

PBKDF2_NAME = "pbkdf2_sha256"
PBKDF2_ITERATIONS = 240_000


def _b64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("utf-8").rstrip("=")


def _b64url_decode(raw: str) -> bytes:
    padding = "=" * (-len(raw) % 4)
    return base64.urlsafe_b64decode((raw + padding).encode("utf-8"))


def hash_password(password: str) -> str:
    if not isinstance(password, str) or not password:
        raise ValueError("password is required")
    salt = secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, PBKDF2_ITERATIONS)
    return f"{PBKDF2_NAME}${PBKDF2_ITERATIONS}${salt.hex()}${digest.hex()}"


def verify_password(password: str, password_hash: str) -> bool:
    try:
        algo, rounds, salt_hex, hash_hex = str(password_hash).split("$", 3)
        if algo != PBKDF2_NAME:
            return False
        iterations = int(rounds)
        salt = bytes.fromhex(salt_hex)
        expected = bytes.fromhex(hash_hex)
        actual = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
        return hmac.compare_digest(actual, expected)
    except Exception:
        return False


def _sign(data: bytes) -> str:
    mac = hmac.new(settings.SECRET_KEY.encode("utf-8"), data, hashlib.sha256).digest()
    return _b64url_encode(mac)


def create_access_token(subject: str, expires_minutes: Optional[int] = None) -> str:
    now = int(time.time())
    ttl_minutes = int(expires_minutes or settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    payload: Dict[str, Any] = {
        "sub": str(subject),
        "iat": now,
        "exp": now + max(1, ttl_minutes) * 60,
        "jti": uuid.uuid4().hex,
    }
    header = {"alg": "HS256", "typ": "JWT"}
    header_part = _b64url_encode(json.dumps(header, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))
    payload_part = _b64url_encode(json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))
    signing_input = f"{header_part}.{payload_part}".encode("utf-8")
    signature = _sign(signing_input)
    return f"{header_part}.{payload_part}.{signature}"


def decode_access_token(token: str) -> Dict[str, Any]:
    try:
        header_part, payload_part, signature = str(token).split(".", 2)
    except ValueError as e:
        raise ValueError("invalid token format") from e

    signing_input = f"{header_part}.{payload_part}".encode("utf-8")
    expected_sig = _sign(signing_input)
    if not hmac.compare_digest(signature, expected_sig):
        raise ValueError("invalid token signature")

    try:
        payload = json.loads(_b64url_decode(payload_part).decode("utf-8"))
    except Exception as e:
        raise ValueError("invalid token payload") from e

    exp = int(payload.get("exp", 0))
    if exp <= int(time.time()):
        raise ValueError("token expired")
    return payload
