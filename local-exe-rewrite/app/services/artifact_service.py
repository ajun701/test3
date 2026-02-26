from __future__ import annotations

import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from sqlalchemy.orm import Session

from app.core.config import settings
from app.db.models import ArtifactRecord



def _safe_prefix(prefix: str) -> str:
    return str(prefix or "artifact").replace("/", "_").replace("\\", "_").strip("_") or "artifact"


def save_artifact(
    file_bytes: bytes,
    prefix: str,
    suffix: str = ".xlsx",
    *,
    db: Optional[Session] = None,
    stage: str = "",
    action: str = "",
    operator: str = "system",
    source_file: str = "",
    task_id: str = "",
    input_rows: int = 0,
    output_rows: int = 0,
    payload: Optional[Dict[str, Any]] = None,
) -> str:
    filename = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{_safe_prefix(prefix)}_{uuid.uuid4().hex[:6]}{suffix}"
    filepath = settings.artifact_dir / filename
    filepath.write_bytes(file_bytes)
    file_url = str(filepath)

    if db is not None:
        db.add(
            ArtifactRecord(
                stage=str(stage or ""),
                action=str(action or ""),
                operator=str(operator or "system"),
                source_file=str(source_file or ""),
                task_id=str(task_id or ""),
                input_rows=max(int(input_rows or 0), 0),
                output_rows=max(int(output_rows or 0), 0),
                file_url=file_url,
                file_name=filename,
                payload=payload or {},
            )
        )

    return file_url


def normalize_artifact_file_name(file_url: str) -> str:
    raw = str(file_url or "").strip()
    if not raw:
        return ""
    return Path(raw).name
