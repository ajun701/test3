# app/core/config.py
import os
from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    # 项目基础信息
    PROJECT_NAME: str = "Refund Audit System API"
    API_V1_STR: str = "/api/v1"
    
    # 数据库配置 (默认使用 SQLite 方便迁移，生产换成 MySQL/PostgreSQL 链接)
    DATABASE_URL: str = "sqlite:///./refund_audit.db"
    
    # Celery & Redis 队列配置
    REDIS_URL: str = "redis://localhost:6379/0"
    
    # AI 模型配置
    DASHSCOPE_API_KEY: str = ""
    
    # 本地文件挂载卷配置（生产环境中可替换为 OSS 的路径）
    DATA_DIR: Path = Path.cwd() / "data"
    ARTIFACT_DIR: Path = DATA_DIR / "artifacts"
    TASK_DIR: Path = DATA_DIR / "tasks"

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

settings = Settings()

# 确保挂载目录存在
settings.ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
settings.TASK_DIR.mkdir(parents=True, exist_ok=True)