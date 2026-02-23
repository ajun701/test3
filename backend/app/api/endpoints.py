# app/api/endpoints.py
import math
import csv
import io
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import pandas as pd
from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, UploadFile
from fastapi.encoders import jsonable_encoder
from fastapi.responses import Response
from sqlalchemy.orm import Session

from app.core.auth import get_current_user
from app.core.config import settings
from app.core.constants import (
    COL_AI_EXTRACTED_AMOUNT,
    COL_AI_MATCH,
    COL_AI_NOTE,
    COL_AMOUNT_CANDIDATES,
    COL_SCREENSHOT_CANDIDATES,
    HYPERLINK_SUFFIX,
)
from app.db.session import get_db
from app.models import AITask, ArtifactRecord, OperationHistory, User
from app.schemas import (
    AITaskResponse,
    AITaskRowsResponse,
    AITaskSnapshotResponse,
    AITaskStatusResponse,
    ArtifactRecordListResponse,
    CleanResponse,
    MatchResponse,
    OperationHistoryListResponse,
    TablePreview,
    TablePreviewResponse,
)
from app.services.artifact_service import save_artifact
from app.services.cleaning_service import ensure_required_columns, process_cleaning
from app.services.cleaning_service import compare_source_and_processed
from app.services.matching_service import process_matching
from app.utils.excel_utils import (
    attach_hyperlink_helper_column,
    df_to_excel_bytes,
    read_table,
)

router = APIRouter()


def _is_sub_path(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _strip_internal_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    keep_cols = [c for c in df.columns if not str(c).endswith(HYPERLINK_SUFFIX)]
    return df[keep_cols].copy()


def _df_to_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    if df.empty:
        return []
    safe_df = df.astype(object).where(pd.notna(df), None)
    records = safe_df.to_dict(orient="records")
    return jsonable_encoder(records)


def _df_to_preview(df: pd.DataFrame, sample_rows: int) -> TablePreview:
    view_df = _strip_internal_columns(df)
    if view_df.empty:
        return TablePreview(total_rows=0, shown_rows=0, columns=[], rows=[])

    total_rows = len(view_df)
    shown_rows = min(max(int(sample_rows), 0), total_rows)
    slice_df = view_df.head(shown_rows)
    return TablePreview(
        total_rows=total_rows,
        shown_rows=shown_rows,
        columns=[str(c) for c in view_df.columns],
        rows=_df_to_records(slice_df),
    )


def _resolve_artifact_path(file_url: str) -> Path:
    raw = str(file_url or "").strip().split("?", 1)[0]
    if not raw:
        raise HTTPException(status_code=400, detail="文件地址不能为空")

    if raw.startswith("/artifacts/"):
        rel_path = raw[len("/artifacts/") :]
    elif raw.startswith("artifacts/"):
        rel_path = raw[len("artifacts/") :]
    else:
        raise HTTPException(status_code=400, detail="file_url must start with /artifacts/")

    candidate = (settings.ARTIFACT_DIR / rel_path).resolve()
    root = settings.ARTIFACT_DIR.resolve()
    if not _is_sub_path(candidate, root):
        raise HTTPException(status_code=400, detail="非法文件路径")
    if not candidate.exists() or not candidate.is_file():
        raise HTTPException(status_code=404, detail="artifact file not found")
    return candidate


def _read_artifact_bytes(file_url: str) -> Tuple[bytes, str]:
    artifact_path = _resolve_artifact_path(file_url)
    return artifact_path.read_bytes(), artifact_path.name


def _build_ai_task_frames(
    task: AITask, df_work: pd.DataFrame, source_df: Optional[pd.DataFrame] = None
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if not isinstance(df_work, pd.DataFrame):
        df_work = pd.DataFrame()
    if not isinstance(source_df, pd.DataFrame):
        source_df = pd.DataFrame()

    total = min(max(int(task.total or 0), 0), len(df_work))
    in_scope = df_work.iloc[:total].copy()

    if COL_AI_MATCH in in_scope.columns:
        processed_mask = in_scope[COL_AI_MATCH].notna()
    else:
        processed_mask = pd.Series(False, index=in_scope.index)

    df_processed = in_scope[processed_mask].copy()
    df_unprocessed_in_scope = in_scope[~processed_mask].copy()
    df_unprocessed_extra = pd.DataFrame()
    if isinstance(source_df, pd.DataFrame) and len(source_df) > total:
        df_unprocessed_extra = source_df.iloc[total:].copy()

    if not df_unprocessed_in_scope.empty and not df_unprocessed_extra.empty:
        df_unprocessed = pd.concat([df_unprocessed_in_scope, df_unprocessed_extra], ignore_index=True)
    elif not df_unprocessed_in_scope.empty:
        df_unprocessed = df_unprocessed_in_scope
    else:
        df_unprocessed = df_unprocessed_extra

    if COL_AI_MATCH in df_processed.columns:
        df_ok = df_processed[df_processed[COL_AI_MATCH] == True].copy()
        df_bad = df_processed[df_processed[COL_AI_MATCH] != True].copy()
    else:
        df_ok = pd.DataFrame()
        df_bad = pd.DataFrame()

    return df_processed, df_unprocessed, df_ok, df_bad


def _apply_history_filters(
    query,
    stage: str = "",
    action: str = "",
    operator: str = "",
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
):
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


def _apply_artifact_filters(
    query,
    stage: str = "",
    action: str = "",
    operator: str = "",
    task_id: str = "",
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
):
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


def _ensure_ai_task_access(task: AITask, current_user: User) -> None:
    owner = str(task.operator or "").strip() or "system"
    if owner != current_user.username:
        raise HTTPException(status_code=403, detail="无权访问其他用户任务")


async def _resolve_upload_or_artifact(
    upload_file: Optional[UploadFile], file_url: str, default_filename: str
) -> Tuple[bytes, str]:
    if upload_file is not None:
        file_bytes = await upload_file.read()
        filename = upload_file.filename or default_filename
        if file_bytes:
            return file_bytes, filename

    if str(file_url or "").strip():
        return _read_artifact_bytes(file_url)

    return b"", default_filename


def enqueue_ai_task(task_id: str, api_key: str = "") -> None:
    # 延迟导入，避免非 AI 接口受到可选依赖初始化影响
    from app.tasks.ai_tasks import run_ai_task

    run_ai_task.delay(task_id, api_key or "")


@router.post("/preview-table", response_model=TablePreviewResponse, summary="上传表格预览")
async def preview_table(
    file: UploadFile = File(...),
    sample_rows: int = Form(200, ge=1, le=2000),
):
    filename = file.filename or "upload.xlsx"
    file_bytes = await file.read()
    df = read_table(file_bytes, filename)
    if df.empty:
        raise HTTPException(status_code=400, detail="上传表格为空")
    return TablePreviewResponse(**_df_to_preview(df, sample_rows).model_dump())


@router.get("/artifact/preview", response_model=TablePreviewResponse, summary="产物文件预览")
def preview_artifact(
    file_url: str = Query(..., description="产物文件 URL，例如 /artifacts/xxx.xlsx"),
    sample_rows: int = Query(200, ge=1, le=2000),
):
    file_bytes, filename = _read_artifact_bytes(file_url)
    df = read_table(file_bytes, filename)
    if df.empty:
        raise HTTPException(status_code=400, detail="产物文件为空")
    return TablePreviewResponse(**_df_to_preview(df, sample_rows).model_dump())


@router.post("/clean", response_model=CleanResponse, summary="步骤一：数据清洗与规则初筛")
async def clean_data(
    file: UploadFile = File(...),
    preview_rows: int = Form(100, ge=1, le=1000),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    try:
        filename = file.filename or "upload.xlsx"
        file_bytes = await file.read()
        res = process_cleaning(file_bytes, filename)

        df_raw = res["df_raw"]
        df_normal = res["df_normal"]
        df_abnormal = res["df_abnormal"]
        shot_col = res["shot_col"]

        # 导出带超链接的 Excel
        hyperlink_cols_n = [shot_col] if shot_col and shot_col in df_normal.columns else None
        hyperlink_cols_ab = [shot_col] if shot_col and shot_col in df_abnormal.columns else None

        b_normal = df_to_excel_bytes(df_normal, sheet_name="正常", hyperlink_cols=hyperlink_cols_n)
        b_abnormal = df_to_excel_bytes(df_abnormal, sheet_name="异常", hyperlink_cols=hyperlink_cols_ab)

        url_normal = save_artifact(
            b_normal,
            "clean_normal",
            db=db,
            stage="步骤一清洗",
            action="执行清洗",
            operator=current_user.username,
            source_file=filename,
            input_rows=len(df_raw),
            output_rows=len(df_normal),
            payload={"kind": "normal"},
        )
        url_abnormal = save_artifact(
            b_abnormal,
            "clean_abnormal_need_callback",
            db=db,
            stage="步骤一清洗",
            action="执行清洗",
            operator=current_user.username,
            source_file=filename,
            input_rows=len(df_raw),
            output_rows=len(df_abnormal),
            payload={"kind": "abnormal"},
        )

        # 记录历史
        hist = OperationHistory(
            stage="步骤一清洗",
            action="执行清洗",
            operator=current_user.username,
            input_rows=len(df_raw),
            output_rows=len(df_normal) + len(df_abnormal),
            detail={
                "source_file": filename,
                "normal_rows": len(df_normal),
                "abnormal_rows": len(df_abnormal),
                "artifacts": [url_normal, url_abnormal],
            },
        )
        db.add(hist)
        db.commit()

        return CleanResponse(
            total_rows=len(df_raw),
            normal_rows=len(df_normal),
            abnormal_rows=len(df_abnormal),
            normal_file_url=url_normal,
            abnormal_file_url=url_abnormal,
            report=res["report"],
            normal_preview=_df_to_preview(df_normal, preview_rows),
            abnormal_preview=_df_to_preview(df_abnormal, preview_rows),
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"内部服务器错误: {e}")


@router.post("/match", response_model=MatchResponse, summary="步骤二：入库单号匹配")
async def match_data(
    source_file: Optional[UploadFile] = File(None, description="步骤一正常表"),
    inbound_file: Optional[UploadFile] = File(None, description="已入库物流单号表"),
    source_file_url: str = Form("", description="可选：步骤一正常表产物 URL"),
    inbound_file_url: str = Form("", description="可选：已入库物流单号表产物 URL"),
    preview_rows: int = Form(100, ge=1, le=1000),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    try:
        source_bytes, source_filename = await _resolve_upload_or_artifact(
            source_file, source_file_url, "source.xlsx"
        )
        inbound_bytes, inbound_filename = await _resolve_upload_or_artifact(
            inbound_file, inbound_file_url, "inbound.xlsx"
        )
        if not source_bytes:
            raise HTTPException(status_code=400, detail="请上传步骤一正常表，或提供 source_file_url")
        if not inbound_bytes:
            raise HTTPException(status_code=400, detail="请上传已入库物流单号表，或提供 inbound_file_url")

        res = process_matching(source_bytes, source_filename, inbound_bytes, inbound_filename)

        df_source = res["df_source"]
        df_inbound = res["df_inbound"]
        df_pending = res["df_pending"]
        shot_col = res["shot_col"]

        hyperlink_cols_inb = [shot_col] if shot_col and shot_col in df_inbound.columns else None
        hyperlink_cols_pen = [shot_col] if shot_col and shot_col in df_pending.columns else None

        b_inbound = df_to_excel_bytes(df_inbound, sheet_name="已入库", hyperlink_cols=hyperlink_cols_inb)
        b_pending = df_to_excel_bytes(df_pending, sheet_name="未入库", hyperlink_cols=hyperlink_cols_pen)

        url_inbound = save_artifact(
            b_inbound,
            "matched_inbound_for_ai",
            db=db,
            stage="步骤二入库匹配",
            action="执行匹配",
            operator=current_user.username,
            source_file=source_filename,
            input_rows=len(df_source),
            output_rows=len(df_inbound),
            payload={"kind": "inbound", "inbound_file": inbound_filename},
        )
        url_pending = save_artifact(
            b_pending,
            "not_inbound_followup",
            db=db,
            stage="步骤二入库匹配",
            action="执行匹配",
            operator=current_user.username,
            source_file=source_filename,
            input_rows=len(df_source),
            output_rows=len(df_pending),
            payload={"kind": "pending", "inbound_file": inbound_filename},
        )

        hist = OperationHistory(
            stage="步骤二入库匹配",
            action="执行匹配",
            operator=current_user.username,
            input_rows=len(df_source),
            output_rows=len(df_inbound) + len(df_pending),
            detail={
                "inbound_rows": len(df_inbound),
                "pending_rows": len(df_pending),
                "artifacts": [url_inbound, url_pending],
            },
        )
        db.add(hist)
        db.commit()

        return MatchResponse(
            total_rows=len(df_source),
            inbound_rows=len(df_inbound),
            pending_rows=len(df_pending),
            inbound_file_url=url_inbound,
            pending_file_url=url_pending,
            report=res["report"],
            inbound_preview=_df_to_preview(df_inbound, preview_rows),
            pending_preview=_df_to_preview(df_pending, preview_rows),
        )
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"内部服务器错误: {e}")


@router.post("/ai-task/start", response_model=AITaskResponse, summary="步骤三：启动 AI 多图复核异步任务")
async def start_ai_task(
    file: Optional[UploadFile] = File(None),
    file_url: str = Form("", description="可选：步骤二已入库表产物 URL"),
    api_key: str = Form("", description="可选：本次任务使用的 DashScope API Key"),
    model_name: str = Form("qwen3-vl-flash"),
    max_images: int = Form(4, ge=1, le=10),
    min_interval_sec: float = Form(0.8, ge=0.0),
    max_retries: int = Form(4, ge=0, le=10),
    backoff_base_sec: float = Form(1.0, ge=0.1),
    max_ai_rows: int = Form(300, ge=1, le=10000),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    filename = "upload.xlsx"
    file_bytes, filename = await _resolve_upload_or_artifact(file, file_url, filename)
    if not file_bytes:
        raise HTTPException(status_code=400, detail="请上传待复核表格，或提供 file_url")

    effective_api_key = str(api_key or "").strip() or str(settings.DASHSCOPE_API_KEY or "").strip()
    if not effective_api_key:
        raise HTTPException(status_code=400, detail="缺少 DashScope API Key，请在页面填写或配置后端环境变量")

    df_in = read_table(file_bytes, filename)
    if df_in.empty:
        raise HTTPException(status_code=400, detail="上传表格为空")

    if not model_name.strip():
        raise HTTPException(status_code=400, detail="模型名称不能为空")

    try:
        req = {"amount": COL_AMOUNT_CANDIDATES, "screenshot": COL_SCREENSHOT_CANDIDATES}
        matched = ensure_required_columns(df_in, req)
        col_amount = matched["amount"]
        col_shot = matched["screenshot"]

        if filename.lower().endswith((".xlsx", ".xls")):
            df_in = attach_hyperlink_helper_column(df_in, file_bytes, col_shot)

        total_rows = min(len(df_in), max_ai_rows)
        df_work = df_in.iloc[:total_rows].copy()

        # 初始化 AI 结果列
        df_work[COL_AI_EXTRACTED_AMOUNT] = None
        df_work[COL_AI_MATCH] = None
        df_work[COL_AI_NOTE] = ""

        task_id = f"ai_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
        df_path = settings.TASK_DIR / f"{task_id}_work.pkl"
        src_path = settings.TASK_DIR / f"{task_id}_source.pkl"

        df_work.to_pickle(df_path)
        df_in.to_pickle(src_path)

        new_task = AITask(
            task_id=task_id,
            operator=current_user.username,
            status="pending",
            source_file=filename,
            input_rows=len(df_in),
            total=total_rows,
            col_amount=col_amount,
            col_shot=col_shot,
            model_name=model_name,
            max_images=max_images,
            min_interval_sec=min_interval_sec,
            max_retries=max_retries,
            backoff_base_sec=backoff_base_sec,
            df_work_path=str(df_path),
            source_df_path=str(src_path),
        )
        db.add(new_task)

        # 记录历史
        hist = OperationHistory(
            stage="步骤三AI复核",
            action="创建AI任务",
            operator=current_user.username,
            input_rows=len(df_in),
            output_rows=0,
            detail={
                "task_id": task_id,
                "model": model_name,
                "max_rows": total_rows,
                "operator": current_user.username,
            },
        )
        db.add(hist)
        db.commit()

        try:
            # 投递任务到 Celery 队列
            enqueue_ai_task(task_id, effective_api_key)
        except Exception as e:
            new_task.status = "error"
            new_task.error_message = f"任务投递失败: {e}"
            new_task.finished_at = datetime.utcnow()
            db.commit()
            raise HTTPException(status_code=503, detail="任务投递失败，请检查 Redis/Celery 服务")

        new_task.status = "running"
        db.commit()

        return AITaskResponse(task_id=task_id, status="running", message="任务已成功投递到队列后台运行")
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/ai-task/latest", response_model=AITaskResponse, summary="查询最近 AI 任务")
def get_latest_ai_task(
    active_only: bool = Query(True, description="true 时仅返回 pending/running/paused/error 任务"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    q = db.query(AITask).filter(AITask.operator == current_user.username)
    if active_only:
        q = q.filter(AITask.status.in_(["pending", "running", "paused", "error"]))

    task = q.order_by(AITask.created_at.desc()).first()
    if not task:
        raise HTTPException(status_code=404, detail="暂无可恢复的 AI 任务")

    return AITaskResponse(task_id=task.task_id, status=task.status, message="ok")

@router.get("/ai-task/{task_id}/status", response_model=AITaskStatusResponse, summary="轮询 AI 任务进度")
def get_ai_task_status(
    task_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    task = db.query(AITask).filter(AITask.task_id == task_id).first()
    if not task:
        raise HTTPException(status_code=404, detail="任务不存在")
    _ensure_ai_task_access(task, current_user)

    ok_rows = 0
    bad_rows = 0
    alignment_report: Dict[str, Any] = {}
    # 为保证接口响应速度，这里仅做轻量统计
    try:
        df = pd.read_pickle(task.df_work_path)
        processed = df[COL_AI_MATCH].notna()
        ok_rows = int(df[processed & (df[COL_AI_MATCH] == True)].shape[0])
        bad_rows = int(df[processed & (df[COL_AI_MATCH] != True)].shape[0])

        try:
            src_df = pd.read_pickle(task.source_df_path)
            src_scope = src_df.iloc[: min(max(task.total, 0), len(src_df))].copy() if isinstance(src_df, pd.DataFrame) else pd.DataFrame()
            alignment_report = compare_source_and_processed(src_scope, df, stage_name="步骤三AI复核")
        except Exception:
            alignment_report = {}
    except Exception:
        pass

    processed_rows = min(max(task.next_idx, 0), task.total)
    pending_rows = max(task.total - processed_rows, 0)
    progress_ratio = round(processed_rows / max(task.total, 1), 2)

    return AITaskStatusResponse(
        task_id=task.task_id,
        status=task.status,
        created_at=task.created_at,
        updated_at=task.updated_at,
        finished_at=task.finished_at,
        total=task.total,
        processed=processed_rows,
        pending=pending_rows,
        ok_rows=ok_rows,
        bad_rows=bad_rows,
        min_interval_sec=task.min_interval_sec,
        error_message=task.error_message,
        artifacts=task.artifacts if isinstance(task.artifacts, list) else [],
        progress_ratio=progress_ratio,
        alignment_report=alignment_report,
    )


@router.get("/ai-task/{task_id}/rows", response_model=AITaskRowsResponse, summary="查看 AI 任务行级进度")
def get_ai_task_rows(
    task_id: str,
    scope: str = Query("all", description="all | processed | pending"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    task = db.query(AITask).filter(AITask.task_id == task_id).first()
    if not task:
        raise HTTPException(status_code=404, detail="任务不存在")
    _ensure_ai_task_access(task, current_user)

    try:
        df = pd.read_pickle(task.df_work_path)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"读取任务数据失败: {e}")

    if not isinstance(df, pd.DataFrame):
        df = pd.DataFrame()

    view_df = _strip_internal_columns(df).copy()
    if not view_df.empty:
        view_df.insert(0, "_row_no", range(1, len(view_df) + 1))
        if COL_AI_MATCH in view_df.columns:
            processed_mask = view_df[COL_AI_MATCH].notna()
        else:
            processed_mask = pd.Series(False, index=view_df.index)

        scope_key = str(scope or "").strip().lower()
        if scope_key == "processed":
            view_df = view_df[processed_mask].copy()
        elif scope_key == "pending":
            view_df = view_df[~processed_mask].copy()

    total_rows = len(view_df)
    total_pages = max(1, math.ceil(total_rows / page_size)) if total_rows > 0 else 1
    page = min(page, total_pages)
    start = (page - 1) * page_size
    end = start + page_size
    page_df = view_df.iloc[start:end].copy() if total_rows > 0 else pd.DataFrame()

    return AITaskRowsResponse(
        task_id=task_id,
        total_rows=total_rows,
        page=page,
        page_size=page_size,
        total_pages=total_pages,
        columns=[str(c) for c in page_df.columns],
        rows=_df_to_records(page_df),
    )


@router.post("/ai-task/{task_id}/snapshot", response_model=AITaskSnapshotResponse, summary="导出当前任务快照")
def export_ai_task_snapshot(
    task_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    task = db.query(AITask).filter(AITask.task_id == task_id).first()
    if not task:
        raise HTTPException(status_code=404, detail="任务不存在")
    _ensure_ai_task_access(task, current_user)

    try:
        df_work = pd.read_pickle(task.df_work_path)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"读取任务数据失败: {e}")

    try:
        source_df = pd.read_pickle(task.source_df_path)
    except Exception:
        source_df = pd.DataFrame()

    df_processed, df_unprocessed, df_ok, df_bad = _build_ai_task_frames(task, df_work, source_df)
    shot_col = task.col_shot

    hyperlink_processed = [shot_col] if shot_col in df_processed.columns else None
    hyperlink_unprocessed = [shot_col] if shot_col in df_unprocessed.columns else None
    b_processed = df_to_excel_bytes(df_processed, sheet_name="AI已处理", hyperlink_cols=hyperlink_processed)
    b_unprocessed = df_to_excel_bytes(df_unprocessed, sheet_name="AI未处理", hyperlink_cols=hyperlink_unprocessed)

    url_processed = save_artifact(
        b_processed,
        "ai_processed_snapshot",
        db=db,
        stage="步骤三AI复核",
        action="导出任务快照",
        operator=current_user.username,
        source_file=task.source_file or "",
        task_id=task.task_id,
        input_rows=task.total,
        output_rows=len(df_processed),
        payload={"kind": "processed"},
    )
    url_unprocessed = save_artifact(
        b_unprocessed,
        "ai_unprocessed_snapshot",
        db=db,
        stage="步骤三AI复核",
        action="导出任务快照",
        operator=current_user.username,
        source_file=task.source_file or "",
        task_id=task.task_id,
        input_rows=task.total,
        output_rows=len(df_unprocessed),
        payload={"kind": "unprocessed"},
    )

    url_ok = None
    url_bad = None
    if not df_ok.empty:
        hyperlink_ok = [shot_col] if shot_col in df_ok.columns else None
        b_ok = df_to_excel_bytes(df_ok, sheet_name="AI可打款", hyperlink_cols=hyperlink_ok)
        url_ok = save_artifact(
            b_ok,
            "ai_ok_snapshot",
            db=db,
            stage="步骤三AI复核",
            action="导出任务快照",
            operator=current_user.username,
            source_file=task.source_file or "",
            task_id=task.task_id,
            input_rows=task.total,
            output_rows=len(df_ok),
            payload={"kind": "ok"},
        )
    if not df_bad.empty:
        hyperlink_bad = [shot_col] if shot_col in df_bad.columns else None
        b_bad = df_to_excel_bytes(df_bad, sheet_name="AI需回访", hyperlink_cols=hyperlink_bad)
        url_bad = save_artifact(
            b_bad,
            "ai_bad_snapshot",
            db=db,
            stage="步骤三AI复核",
            action="导出任务快照",
            operator=current_user.username,
            source_file=task.source_file or "",
            task_id=task.task_id,
            input_rows=task.total,
            output_rows=len(df_bad),
            payload={"kind": "bad"},
        )

    db.add(
        OperationHistory(
            stage="步骤三AI复核",
            action="导出任务快照",
            operator=current_user.username,
            input_rows=task.total,
            output_rows=task.next_idx,
            detail={
                "task_id": task.task_id,
                "processed_rows": len(df_processed),
                "unprocessed_rows": len(df_unprocessed),
                "artifacts": [u for u in [url_processed, url_unprocessed, url_ok, url_bad] if u],
            },
        )
    )
    db.commit()

    return AITaskSnapshotResponse(
        task_id=task.task_id,
        processed_rows=len(df_processed),
        unprocessed_rows=len(df_unprocessed),
        processed_file_url=url_processed,
        unprocessed_file_url=url_unprocessed,
        ok_file_url=url_ok,
        bad_file_url=url_bad,
    )


@router.post("/ai-task/{task_id}/pause", response_model=AITaskResponse, summary="暂停 AI 任务")
def pause_ai_task(
    task_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    task = db.query(AITask).filter(AITask.task_id == task_id).first()
    if not task:
        raise HTTPException(status_code=404, detail="任务不存在")
    _ensure_ai_task_access(task, current_user)
    if task.status == "running":
        task.status = "paused"
        db.add(
            OperationHistory(
                stage="步骤三AI复核",
                action="暂停AI任务",
                operator=current_user.username,
                input_rows=task.total,
                output_rows=task.next_idx,
                detail={"task_id": task.task_id, "status": "paused"},
            )
        )
        db.commit()
    return AITaskResponse(task_id=task.task_id, status=task.status, message="已下发暂停指令")


@router.post("/ai-task/{task_id}/resume", response_model=AITaskResponse, summary="恢复 AI 任务")
def resume_ai_task(
    task_id: str,
    api_key: str = Form("", description="可选：本次恢复使用的 DashScope API Key"),
    min_interval_sec: Optional[float] = Form(None, ge=0.0, description="可选：恢复后更新 AI 最小请求间隔"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    task = db.query(AITask).filter(AITask.task_id == task_id).first()
    if not task:
        raise HTTPException(status_code=404, detail="任务不存在")
    _ensure_ai_task_access(task, current_user)
    if task.status == "completed":
        raise HTTPException(status_code=400, detail="已完成任务不能恢复")

    effective_api_key = str(api_key or "").strip() or str(settings.DASHSCOPE_API_KEY or "").strip()
    if task.status in ["paused", "error", "pending"]:
        if not effective_api_key:
            raise HTTPException(status_code=400, detail="继续任务缺少 DashScope API Key")
        if min_interval_sec is not None:
            task.min_interval_sec = float(min_interval_sec)
        task.status = "running"
        task.error_message = None
        db.add(
            OperationHistory(
                stage="步骤三AI复核",
                action="恢复AI任务",
                operator=current_user.username,
                input_rows=task.total,
                output_rows=task.next_idx,
                detail={"task_id": task.task_id, "status": "running", "min_interval_sec": task.min_interval_sec},
            )
        )
        db.commit()
        try:
            # 重新下发 Celery 任务恢复执行
            enqueue_ai_task(task_id, effective_api_key)
        except Exception as e:
            task.status = "error"
            task.error_message = f"任务恢复失败: {e}"
            db.commit()
            raise HTTPException(status_code=503, detail="任务恢复失败，请检查 Redis/Celery 服务")
    return AITaskResponse(task_id=task.task_id, status=task.status, message="任务已恢复并在后台运行")


@router.get("/history", response_model=OperationHistoryListResponse, summary="查询历史操作记录")
def list_operation_history(
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    stage: str = Query("", description="按阶段筛选"),
    action: str = Query("", description="按动作模糊筛选"),
    operator: str = Query("", description="按操作人筛选"),
    start_time: Optional[datetime] = Query(None, description="开始时间，ISO 或 YYYY-MM-DD HH:MM:SS"),
    end_time: Optional[datetime] = Query(None, description="结束时间，ISO 或 YYYY-MM-DD HH:MM:SS"),
    db: Session = Depends(get_db),
):
    if start_time and end_time and start_time > end_time:
        raise HTTPException(status_code=400, detail="开始时间不能晚于结束时间")

    q = _apply_history_filters(
        db.query(OperationHistory),
        stage=stage,
        action=action,
        operator=operator,
        start_time=start_time,
        end_time=end_time,
    )

    total = q.count()
    items = q.order_by(OperationHistory.timestamp.desc()).offset(offset).limit(limit).all()
    return OperationHistoryListResponse(total=total, items=items)


@router.get("/history/files", response_model=ArtifactRecordListResponse, summary="查询历史处理文件")
def list_artifact_history(
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    stage: str = Query("", description="按阶段筛选"),
    action: str = Query("", description="按动作模糊筛选"),
    operator: str = Query("", description="按操作人筛选"),
    task_id: str = Query("", description="按任务ID筛选"),
    start_time: Optional[datetime] = Query(None, description="开始时间，ISO 或 YYYY-MM-DD HH:MM:SS"),
    end_time: Optional[datetime] = Query(None, description="结束时间，ISO 或 YYYY-MM-DD HH:MM:SS"),
    db: Session = Depends(get_db),
):
    if start_time and end_time and start_time > end_time:
        raise HTTPException(status_code=400, detail="开始时间不能晚于结束时间")

    q = _apply_artifact_filters(
        db.query(ArtifactRecord),
        stage=stage,
        action=action,
        operator=operator,
        task_id=task_id,
        start_time=start_time,
        end_time=end_time,
    )
    total = q.count()
    items = q.order_by(ArtifactRecord.created_at.desc()).offset(offset).limit(limit).all()
    return ArtifactRecordListResponse(total=total, items=items)


@router.get("/history/export", summary="导出历史记录 CSV")
def export_operation_history_csv(
    stage: str = Query("", description="按阶段筛选"),
    action: str = Query("", description="按动作模糊筛选"),
    operator: str = Query("", description="按操作人筛选"),
    start_time: Optional[datetime] = Query(None, description="开始时间，ISO 或 YYYY-MM-DD HH:MM:SS"),
    end_time: Optional[datetime] = Query(None, description="结束时间，ISO 或 YYYY-MM-DD HH:MM:SS"),
    db: Session = Depends(get_db),
):
    if start_time and end_time and start_time > end_time:
        raise HTTPException(status_code=400, detail="开始时间不能晚于结束时间")

    q = _apply_history_filters(
        db.query(OperationHistory),
        stage=stage,
        action=action,
        operator=operator,
        start_time=start_time,
        end_time=end_time,
    )
    rows = q.order_by(OperationHistory.timestamp.desc()).all()

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
                jsonable_encoder(row.detail),
            ]
        )

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"operation_history_{ts}.csv"
    csv_text = "\ufeff" + output.getvalue()
    return Response(
        content=csv_text,
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


