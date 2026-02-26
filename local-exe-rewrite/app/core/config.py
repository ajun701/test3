from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from pathlib import Path



def _runtime_root() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parents[2]


@dataclass(slots=True)
class AppSettings:
    project_name: str
    secret_key: str
    dashscope_api_key: str
    database_url: str
    data_dir: Path
    artifact_dir: Path
    task_dir: Path

    @classmethod
    def from_env(cls) -> "AppSettings":
        root = _runtime_root()
        data_dir = root / "data"
        artifact_dir = data_dir / "artifacts"
        task_dir = data_dir / "tasks"

        db_url = os.getenv("DATABASE_URL", "").strip()
        if not db_url:
            db_path = data_dir / "refund_audit_local.db"
            db_url = f"sqlite:///{db_path.as_posix()}"

        settings = cls(
            project_name="退运费智能审核中台（本地版）",
            secret_key=os.getenv("SECRET_KEY", "replace-this-secret-in-production"),
            dashscope_api_key=os.getenv("DASHSCOPE_API_KEY", "").strip(),
            database_url=db_url,
            data_dir=data_dir,
            artifact_dir=artifact_dir,
            task_dir=task_dir,
        )
        settings.ensure_dirs()
        return settings

    def ensure_dirs(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.artifact_dir.mkdir(parents=True, exist_ok=True)
        self.task_dir.mkdir(parents=True, exist_ok=True)


settings = AppSettings.from_env()
