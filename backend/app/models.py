from datetime import datetime

from sqlalchemy import Boolean, Column, DateTime, Float, Integer, JSON, String, Text
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class User(Base):
    """Application user account."""

    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    username = Column(String(50), unique=True, nullable=False, index=True)
    password_hash = Column(String(255), nullable=False)
    is_active = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    last_login_at = Column(DateTime, nullable=True)


class OperationHistory(Base):
    """Operation audit trail."""

    __tablename__ = "operation_history"

    id = Column(Integer, primary_key=True, index=True)
    timestamp = Column(DateTime, default=datetime.utcnow, index=True)
    stage = Column(String(50), nullable=False, index=True)
    action = Column(String(50), nullable=False)
    operator = Column(String(50), default="system", index=True)

    input_rows = Column(Integer, default=0)
    output_rows = Column(Integer, default=0)
    detail = Column(JSON, default=dict)


class ArtifactRecord(Base):
    """Persisted downloadable file metadata."""

    __tablename__ = "artifact_records"

    id = Column(Integer, primary_key=True, index=True)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    stage = Column(String(50), default="", index=True)
    action = Column(String(50), default="", index=True)
    operator = Column(String(50), default="system", index=True)
    source_file = Column(String(255), default="")
    task_id = Column(String(100), default="", index=True)
    input_rows = Column(Integer, default=0)
    output_rows = Column(Integer, default=0)
    file_url = Column(String(500), nullable=False)
    file_name = Column(String(255), nullable=False)
    payload = Column(JSON, default=dict)


class AITask(Base):
    """AI task state table."""

    __tablename__ = "ai_tasks"

    id = Column(Integer, primary_key=True, index=True)
    task_id = Column(String(100), unique=True, index=True, nullable=False)
    operator = Column(String(50), default="system", index=True)

    status = Column(String(20), default="pending", index=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    finished_at = Column(DateTime, nullable=True)

    source_file = Column(String(255), nullable=True)
    input_rows = Column(Integer, default=0)
    total = Column(Integer, default=0)
    next_idx = Column(Integer, default=0)

    col_amount = Column(String(100), nullable=False)
    col_shot = Column(String(100), nullable=False)
    model_name = Column(String(50), default="qwen-vl-plus")
    max_images = Column(Integer, default=4)
    min_interval_sec = Column(Float, default=0.8)
    max_retries = Column(Integer, default=4)
    backoff_base_sec = Column(Float, default=1.0)

    error_message = Column(Text, nullable=True)
    df_work_path = Column(String(255), nullable=False)
    source_df_path = Column(String(255), nullable=False)
    artifacts = Column(JSON, default=list)
