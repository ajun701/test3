from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    PROJECT_NAME: str = "Refund Audit System API"
    API_V1_STR: str = "/api/v1"

    DATABASE_URL: str = "sqlite:///./refund_audit.db"
    REDIS_URL: str = "redis://localhost:6379/0"
    DASHSCOPE_API_KEY: str = ""

    # Auth
    SECRET_KEY: str = "replace-this-secret-in-production"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 720
    REGISTER_SECRET: str = "16d8udP4JkJEdCUzZcQdd3Pa0P1ws6A52oPiWRPf6nUvKGL"

    # File storage
    DATA_DIR: Path = Path.cwd() / "data"
    ARTIFACT_DIR: Path = DATA_DIR / "artifacts"
    TASK_DIR: Path = DATA_DIR / "tasks"

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = Settings()

settings.ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
settings.TASK_DIR.mkdir(parents=True, exist_ok=True)
