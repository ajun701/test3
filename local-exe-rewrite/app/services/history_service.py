from __future__ import annotations

import csv
import io
from datetime import datetime
from typing import Optional

from sqlalchemy.orm import Query

from app.db.models import ArtifactRecord, OperationHistory


def apply_history_filters(
    query: Query,
    stage: str = "",
    action: str = "",
    operator: str = "",
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
) -> Query:
    q = query
    if stage.strip():
        q = q.filter(OperationHistory.stage == stage.strip())
    if action.strip():
        q = q.filter(OperationHistory.action.contains(action.strip()))
    if operator.strip():
        q = q.filter(OperationHistory.operator == operator.strip())
    if start_time is not None:
        q = q.filter(OperationHistory.timestamp >= start_time)
    if end_time is not None:
        q = q.filter(OperationHistory.timestamp <= end_time)
    return q


def apply_artifact_filters(
    query: Query,
    stage: str = "",
    action: str = "",
    operator: str = "",
    task_id: str = "",
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
) -> Query:
    q = query
    if stage.strip():
        q = q.filter(ArtifactRecord.stage == stage.strip())
    if action.strip():
        q = q.filter(ArtifactRecord.action.contains(action.strip()))
    if operator.strip():
        q = q.filter(ArtifactRecord.operator == operator.strip())
    if task_id.strip():
        q = q.filter(ArtifactRecord.task_id == task_id.strip())
    if start_time is not None:
        q = q.filter(ArtifactRecord.created_at >= start_time)
    if end_time is not None:
        q = q.filter(ArtifactRecord.created_at <= end_time)
    return q


def export_history_csv(rows: list[OperationHistory]) -> str:
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["timestamp", "stage", "action", "operator", "input_rows", "output_rows", "detail"])

    for row in rows:
        writer.writerow(
            [
                row.timestamp.strftime("%Y-%m-%d %H:%M:%S") if row.timestamp else "",
                row.stage,
                row.action,
                row.operator,
                row.input_rows,
                row.output_rows,
                row.detail,
            ]
        )

    return "\ufeff" + output.getvalue()
