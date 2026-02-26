from __future__ import annotations

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from app.core.config import settings
from app.db.models import Base

engine = create_engine(
    settings.database_url,
    connect_args={"check_same_thread": False} if settings.database_url.startswith("sqlite") else {},
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def init_db() -> None:
    Base.metadata.create_all(bind=engine)
    if not settings.database_url.startswith("sqlite"):
        return
    with engine.begin() as conn:
        cols = conn.execute(text("PRAGMA table_info(ai_tasks)")).fetchall()
        names = {str(c[1]) for c in cols}
        if "operator" not in names:
            conn.execute(text("ALTER TABLE ai_tasks ADD COLUMN operator VARCHAR(50) DEFAULT 'system'"))


def get_session():
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()
