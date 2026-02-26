from __future__ import annotations

import math
import threading
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import pandas as pd

from app.core.config import settings
from app.core.constants import (
    AI_ACTIVE_STATUS,
    COL_AI_EXTRACTED_AMOUNT,
    COL_AI_MATCH,
    COL_AI_NOTE,
    COL_AMOUNT_CANDIDATES,
    COL_SCREENSHOT_CANDIDATES,
    HYPERLINK_SUFFIX,
    IMAGE_EXTENSIONS,
)
from app.db.models import AITask, OperationHistory, User
from app.services.ai_service import call_qwen_vl_multi_with_retry
from app.services.artifact_service import save_artifact
from app.services.cleaning_service import compare_source_and_processed, ensure_required_columns, parse_money
from app.utils.excel_utils import (
    attach_hyperlink_helper_column,
    df_to_excel_bytes,
    df_to_records,
    extract_image_urls_from_cell_value,
    normalize_preview_url,
    read_table,
)



def _strip_internal_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    keep_cols = [col for col in df.columns if not str(col).endswith(HYPERLINK_SUFFIX)]
    return df[keep_cols].copy()



def _build_ai_task_frames(
    task: AITask,
    df_work: pd.DataFrame,
    source_df: Optional[pd.DataFrame] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
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
        df_ok = df_processed[df_processed[COL_AI_MATCH] == True].copy()  # noqa: E712
        df_bad = df_processed[df_processed[COL_AI_MATCH] != True].copy()  # noqa: E712
    else:
        df_ok = pd.DataFrame()
        df_bad = pd.DataFrame()

    return df_processed, df_unprocessed, df_ok, df_bad


class AITaskRunner:
    def __init__(self, session_factory):
        self._session_factory = session_factory
        self._threads: dict[str, threading.Thread] = {}
        self._lock = threading.Lock()

    @staticmethod
    def _atomic_pickle_dump(df: pd.DataFrame, path_text: str) -> None:
        path = Path(str(path_text))
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(path.name + ".tmp")
        df.to_pickle(tmp)
        tmp.replace(path)

    @staticmethod
    def _read_pickle_with_retry(path_text: str, retries: int = 3, wait_sec: float = 0.05) -> pd.DataFrame:
        last_exc: Exception | None = None
        for idx in range(max(int(retries), 1)):
            try:
                return pd.read_pickle(path_text)
            except Exception as exc:
                last_exc = exc
                if idx < retries - 1:
                    time.sleep(wait_sec * (idx + 1))
        raise ValueError(f"读取任务数据失败: {last_exc}") from last_exc

    def _log_path(self, task_id: str) -> Path:
        return settings.task_dir / f"{str(task_id or '').strip()}.log"

    def _reset_runtime_log(self, task_id: str) -> None:
        try:
            self._log_path(task_id).write_text("", encoding="utf-8")
        except Exception:
            pass

    def _append_runtime_log(self, task_id: str, message: str) -> None:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"[{ts}] {str(message or '').strip()}".rstrip() + "\n"
        try:
            path = self._log_path(task_id)
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as fh:
                fh.write(line)
        except Exception:
            pass

    def get_runtime_log_path(self, task_id: str) -> str:
        return str(self._log_path(task_id))

    def get_runtime_logs(self, task_id: str, max_lines: int = 300) -> list[str]:
        path = self._log_path(task_id)
        if not path.exists() or not path.is_file():
            return []
        try:
            lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
            n = max(int(max_lines or 0), 1)
            return lines[-n:]
        except Exception:
            return []

    def _calc_alignment_report(self, task: AITask, df_work: pd.DataFrame) -> Dict[str, Any]:
        try:
            src_df = pd.read_pickle(task.source_df_path)
        except Exception:
            return {}

        if not isinstance(src_df, pd.DataFrame):
            src_df = pd.DataFrame()

        source_scope = src_df.iloc[: min(max(int(task.total or 0), 0), len(src_df))].copy()
        try:
            return compare_source_and_processed(source_scope, df_work, stage_name="步骤三AI复核")
        except Exception:
            return {}

    def _ensure_access(self, task: AITask, user: User) -> None:
        owner = str(task.operator or "").strip() or "system"
        if owner != user.username:
            raise ValueError("无权访问其他用户任务")

    def _effective_api_key(self, value: str) -> str:
        return str(value or "").strip() or str(settings.dashscope_api_key or "").strip()

    def start_task(
        self,
        db,
        current_user: User,
        *,
        file_bytes: bytes,
        filename: str,
        api_key: str,
        model_name: str = "qwen3-vl-flash",
        max_images: int = 4,
        min_interval_sec: float = 0.8,
        max_retries: int = 4,
        backoff_base_sec: float = 1.0,
        max_ai_rows: int = 300,
    ) -> AITask:
        effective_api_key = self._effective_api_key(api_key)
        if not effective_api_key:
            raise ValueError("缺少 DashScope API Key")

        df_in = read_table(file_bytes, filename)
        if df_in.empty:
            raise ValueError("上传表格为空")

        if not str(model_name or "").strip():
            raise ValueError("模型名称不能为空")

        required = {"amount": COL_AMOUNT_CANDIDATES, "screenshot": COL_SCREENSHOT_CANDIDATES}
        matched = ensure_required_columns(df_in, required)
        col_amount = matched["amount"]
        col_shot = matched["screenshot"]

        if filename.lower().endswith((".xlsx", ".xls")):
            df_in = attach_hyperlink_helper_column(df_in, file_bytes, col_shot)

        total_rows = min(len(df_in), max_ai_rows)
        df_work = df_in.iloc[:total_rows].copy()

        df_work[COL_AI_EXTRACTED_AMOUNT] = None
        df_work[COL_AI_MATCH] = None
        df_work[COL_AI_NOTE] = ""

        task_id = f"ai_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
        df_path = settings.task_dir / f"{task_id}_work.pkl"
        src_path = settings.task_dir / f"{task_id}_source.pkl"

        self._atomic_pickle_dump(df_work, str(df_path))
        self._atomic_pickle_dump(df_in, str(src_path))

        task = AITask(
            task_id=task_id,
            operator=current_user.username,
            status="running",
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
            next_idx=0,
        )
        db.add(task)
        db.add(
            OperationHistory(
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
        )
        db.commit()
        db.refresh(task)
        self._reset_runtime_log(task_id)
        self._append_runtime_log(
            task_id,
            f"任务创建成功，模型={model_name}，计划处理 {total_rows} 行，最小间隔 {min_interval_sec:.3f} 秒/条",
        )

        self._start_thread(task_id, effective_api_key)
        return task

    def _start_thread(self, task_id: str, api_key: str) -> None:
        with self._lock:
            old = self._threads.get(task_id)
            if old and old.is_alive():
                self._append_runtime_log(task_id, "检测到任务线程已在运行，跳过重复启动。")
                return
            t = threading.Thread(target=self._run_task, args=(task_id, api_key), daemon=True)
            self._threads[task_id] = t
            t.start()

    def _run_task(self, task_id: str, api_key: str) -> None:
        db = self._session_factory()
        task = None
        try:
            task = db.query(AITask).filter(AITask.task_id == task_id).first()
            if not task or task.status != "running":
                self._append_runtime_log(task_id, "任务不存在或状态非 running，线程退出。")
                return
            self._append_runtime_log(task_id, "任务线程启动，开始执行。")

            effective_api_key = self._effective_api_key(api_key)
            if not effective_api_key:
                task.status = "error"
                task.error_message = "缺少 DashScope API Key，无法执行 AI 任务"
                task.finished_at = datetime.utcnow()
                self._append_runtime_log(task_id, "缺少 DashScope API Key，任务失败。")
                db.add(
                    OperationHistory(
                        stage="步骤三AI复核",
                        action="AI任务异常",
                        operator=task.operator,
                        input_rows=task.total,
                        output_rows=task.next_idx,
                        detail={"task_id": task.task_id, "error": task.error_message},
                    )
                )
                db.commit()
                return

            df_work = self._read_pickle_with_retry(task.df_work_path, retries=4, wait_sec=0.03)
            if task.total > len(df_work):
                task.total = len(df_work)
                db.commit()
                self._append_runtime_log(task_id, f"任务总行数修正为 {task.total}。")

            last_call_ts = 0.0

            while task.next_idx < task.total:
                db.refresh(task)
                if task.status != "running":
                    self._atomic_pickle_dump(df_work, task.df_work_path)
                    self._append_runtime_log(task_id, f"任务状态变更为 {task.status}，线程安全退出。")
                    return

                idx = task.next_idx
                row = df_work.iloc[idx]
                human_idx = idx + 1
                self._append_runtime_log(task_id, f"[{human_idx}/{task.total}] 开始处理。")

                now_m = time.monotonic()
                wait = max(0.0, task.min_interval_sec - (now_m - last_call_ts))
                if wait > 0:
                    time.sleep(wait)
                last_call_ts = time.monotonic()

                expected = parse_money(row.get(task.col_amount))
                raw_cell = row.get(task.col_shot + HYPERLINK_SUFFIX) or row.get(task.col_shot)
                img_urls = extract_image_urls_from_cell_value(raw_cell, max_images=task.max_images)

                if not img_urls and isinstance(raw_cell, str) and raw_cell.strip().startswith("http"):
                    expanded = normalize_preview_url(raw_cell.strip())
                    img_urls = [u for u in expanded if u.lower().endswith(IMAGE_EXTENSIONS)][: task.max_images]

                if expected is None:
                    res = {"paid_amount": None, "is_match": False, "reason": "金额字段无法解析为数字", "attempts": 0}
                elif not img_urls:
                    res = {"paid_amount": None, "is_match": None, "reason": "未找到可用图片URL", "attempts": 0}
                else:
                    res = call_qwen_vl_multi_with_retry(
                        img_urls,
                        float(expected),
                        task.model_name,
                        task.max_retries,
                        task.backoff_base_sec,
                        effective_api_key,
                    )

                df_work.at[idx, COL_AI_EXTRACTED_AMOUNT] = res.get("paid_amount")
                df_work.at[idx, COL_AI_MATCH] = bool(res.get("is_match") is True)
                df_work.at[idx, COL_AI_NOTE] = "" if res.get("is_match") else (res.get("reason") or "AI判定异常")
                attempts = int(res.get("attempts") or 0)
                paid_amount = res.get("paid_amount")
                reason = str(res.get("reason") or "").strip()
                is_match = res.get("is_match")
                self._append_runtime_log(
                    task_id,
                    f"[{human_idx}/{task.total}] 完成，attempts={attempts}，paid={paid_amount}，is_match={is_match}，reason={reason}",
                )

                task.next_idx += 1
                task.updated_at = datetime.utcnow()

                if task.next_idx % 10 == 0 or task.next_idx >= task.total:
                    self._atomic_pickle_dump(df_work, task.df_work_path)
                    self._append_runtime_log(task_id, f"已持久化进度：{task.next_idx}/{task.total}。")

                db.commit()

            task.status = "completed"
            task.finished_at = datetime.utcnow()
            self._append_runtime_log(task_id, "任务处理结束，开始生成结果产物。")

            processed_mask = df_work[COL_AI_MATCH].notna()
            df_processed = df_work[processed_mask]
            df_ok = df_processed[df_processed[COL_AI_MATCH] == True]  # noqa: E712
            df_bad = df_processed[df_processed[COL_AI_MATCH] != True]  # noqa: E712

            df_pending = pd.DataFrame()
            report_step3 = {}
            try:
                df_source = self._read_pickle_with_retry(task.source_df_path, retries=4, wait_sec=0.03)
                if len(df_source) > task.total:
                    df_pending = df_source.iloc[task.total:].copy()
                src_scope = df_source.iloc[: min(max(task.total, 0), len(df_source))].copy()
                report_step3 = compare_source_and_processed(src_scope, df_work, stage_name="步骤三AI复核")
            except Exception:
                df_pending = pd.DataFrame()
                report_step3 = {}

            artifacts = []
            hyperlink_ok = [task.col_shot] if task.col_shot in df_ok.columns else None
            b_ok = df_to_excel_bytes(df_ok, sheet_name="AI可打款", hyperlink_cols=hyperlink_ok)
            url_ok = save_artifact(
                b_ok,
                "ai_ok",
                db=db,
                stage="步骤三AI复核",
                action="完成AI任务",
                operator=task.operator,
                source_file=task.source_file or "",
                task_id=task.task_id,
                input_rows=task.total,
                output_rows=len(df_ok),
                payload={"kind": "ok"},
            )
            artifacts.append(url_ok)

            hyperlink_bad = [task.col_shot] if task.col_shot in df_bad.columns else None
            b_bad = df_to_excel_bytes(df_bad, sheet_name="AI需回访", hyperlink_cols=hyperlink_bad)
            url_bad = save_artifact(
                b_bad,
                "ai_bad",
                db=db,
                stage="步骤三AI复核",
                action="完成AI任务",
                operator=task.operator,
                source_file=task.source_file or "",
                task_id=task.task_id,
                input_rows=task.total,
                output_rows=len(df_bad),
                payload={"kind": "bad"},
            )
            artifacts.append(url_bad)

            if not df_pending.empty:
                hyperlink_pending = [task.col_shot] if task.col_shot in df_pending.columns else None
                b_pending = df_to_excel_bytes(df_pending, sheet_name="AI未处理", hyperlink_cols=hyperlink_pending)
                url_pending = save_artifact(
                    b_pending,
                    "ai_pending",
                    db=db,
                    stage="步骤三AI复核",
                    action="完成AI任务",
                    operator=task.operator,
                    source_file=task.source_file or "",
                    task_id=task.task_id,
                    input_rows=task.total,
                    output_rows=len(df_pending),
                    payload={"kind": "pending"},
                )
                artifacts.append(url_pending)

            task.artifacts = artifacts
            self._append_runtime_log(task_id, f"产物生成完成，共 {len(artifacts)} 个文件。")
            db.add(
                OperationHistory(
                    stage="步骤三AI复核",
                    action="完成AI任务",
                    operator=task.operator,
                    input_rows=task.total,
                    output_rows=task.next_idx,
                    detail={
                        "task_id": task.task_id,
                        "artifacts": artifacts,
                        "alignment_report": report_step3,
                    },
                )
            )
            db.commit()
            self._append_runtime_log(task_id, "任务已完成。")
        except Exception as exc:
            db.rollback()
            self._append_runtime_log(task_id, f"任务执行异常：{exc}")
            if task is not None:
                task.status = "error"
                task.error_message = str(exc)
                task.finished_at = datetime.utcnow()
                db.add(
                    OperationHistory(
                        stage="步骤三AI复核",
                        action="AI任务异常",
                        operator=task.operator,
                        input_rows=task.total,
                        output_rows=task.next_idx,
                        detail={"task_id": task.task_id, "error": str(exc)},
                    )
                )
                db.commit()
        finally:
            db.close()

    def get_latest_task(self, db, current_user: User, active_only: bool = True) -> Optional[AITask]:
        q = db.query(AITask).filter(AITask.operator == current_user.username)
        if active_only:
            q = q.filter(AITask.status.in_(AI_ACTIVE_STATUS))
        return q.order_by(AITask.created_at.desc()).first()

    def get_task_status(self, db, current_user: User, task_id: str) -> Dict[str, Any]:
        task = db.query(AITask).filter(AITask.task_id == task_id).first()
        if not task:
            raise ValueError("任务不存在")
        self._ensure_access(task, current_user)

        ok_rows = 0
        bad_rows = 0
        alignment_report: Dict[str, Any] = {}

        try:
            df = self._read_pickle_with_retry(task.df_work_path, retries=4, wait_sec=0.03)
            if not isinstance(df, pd.DataFrame):
                df = pd.DataFrame()
            if COL_AI_MATCH in df.columns:
                processed = df[COL_AI_MATCH].notna()
                ok_rows = int(df[processed & (df[COL_AI_MATCH] == True)].shape[0])  # noqa: E712
                bad_rows = int(df[processed & (df[COL_AI_MATCH] != True)].shape[0])  # noqa: E712
            alignment_report = self._calc_alignment_report(task, df)
        except Exception:
            pass

        processed_rows = min(max(task.next_idx, 0), task.total)
        pending_rows = max(task.total - processed_rows, 0)
        progress_ratio = round(processed_rows / max(task.total, 1), 2)

        return {
            "task_id": task.task_id,
            "status": task.status,
            "created_at": task.created_at,
            "updated_at": task.updated_at,
            "finished_at": task.finished_at,
            "total": task.total,
            "processed": processed_rows,
            "pending": pending_rows,
            "ok_rows": ok_rows,
            "bad_rows": bad_rows,
            "min_interval_sec": task.min_interval_sec,
            "error_message": task.error_message,
            "artifacts": task.artifacts if isinstance(task.artifacts, list) else [],
            "runtime_log_path": self.get_runtime_log_path(task.task_id),
            "progress_ratio": progress_ratio,
            "alignment_report": alignment_report,
        }

    def get_task_rows(
        self,
        db,
        current_user: User,
        task_id: str,
        scope: str = "all",
        page: int = 1,
        page_size: int = 50,
    ) -> Dict[str, Any]:
        task = db.query(AITask).filter(AITask.task_id == task_id).first()
        if not task:
            raise ValueError("任务不存在")
        self._ensure_access(task, current_user)

        df = self._read_pickle_with_retry(task.df_work_path, retries=5, wait_sec=0.04)

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
        page = min(max(page, 1), total_pages)
        start = (page - 1) * page_size
        end = start + page_size
        page_df = view_df.iloc[start:end].copy() if total_rows > 0 else pd.DataFrame()

        return {
            "task_id": task_id,
            "total_rows": total_rows,
            "page": page,
            "page_size": page_size,
            "total_pages": total_pages,
            "columns": [str(c) for c in page_df.columns],
            "rows": df_to_records(page_df),
        }

    def pause_task(self, db, current_user: User, task_id: str) -> AITask:
        task = db.query(AITask).filter(AITask.task_id == task_id).first()
        if not task:
            raise ValueError("任务不存在")
        self._ensure_access(task, current_user)

        if task.status == "running":
            task.status = "paused"
            self._append_runtime_log(task_id, f"用户 {current_user.username} 下发暂停指令。")
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
            db.refresh(task)

        return task

    def resume_task(
        self,
        db,
        current_user: User,
        task_id: str,
        api_key: str,
        min_interval_sec: Optional[float] = None,
    ) -> AITask:
        task = db.query(AITask).filter(AITask.task_id == task_id).first()
        if not task:
            raise ValueError("任务不存在")
        self._ensure_access(task, current_user)

        if task.status == "completed":
            raise ValueError("已完成任务不能恢复")

        effective_api_key = self._effective_api_key(api_key)
        if task.status in ("paused", "error", "pending"):
            if not effective_api_key:
                raise ValueError("继续任务缺少 DashScope API Key")

            if min_interval_sec is not None:
                task.min_interval_sec = float(min_interval_sec)
            task.status = "running"
            task.error_message = None
            self._append_runtime_log(task_id, f"用户 {current_user.username} 恢复任务，最小间隔={task.min_interval_sec:.3f}s。")
            db.add(
                OperationHistory(
                    stage="步骤三AI复核",
                    action="恢复AI任务",
                    operator=current_user.username,
                    input_rows=task.total,
                    output_rows=task.next_idx,
                    detail={
                        "task_id": task.task_id,
                        "status": "running",
                        "min_interval_sec": task.min_interval_sec,
                    },
                )
            )
            db.commit()
            db.refresh(task)
            self._start_thread(task_id, effective_api_key)

        return task

    def alignment_check(self, db, current_user: User, task_id: str) -> Dict[str, Any]:
        task = db.query(AITask).filter(AITask.task_id == task_id).first()
        if not task:
            raise ValueError("任务不存在")
        self._ensure_access(task, current_user)

        try:
            df_work = self._read_pickle_with_retry(task.df_work_path, retries=5, wait_sec=0.04)
        except Exception as exc:
            raise ValueError(str(exc)) from exc

        if not isinstance(df_work, pd.DataFrame):
            df_work = pd.DataFrame()

        alignment_report = self._calc_alignment_report(task, df_work)
        self._append_runtime_log(
            task_id,
            f"执行手动一致性校验：ok={bool(alignment_report.get('ok'))}，"
            f"missing={int(alignment_report.get('missing_rows') or 0)}，"
            f"extra={int(alignment_report.get('extra_rows') or 0)}。",
        )

        db.add(
            OperationHistory(
                stage="步骤三AI复核",
                action="手动一致性校验",
                operator=current_user.username,
                input_rows=task.total,
                output_rows=task.next_idx,
                detail={
                    "task_id": task.task_id,
                    "ok": bool(alignment_report.get("ok")),
                    "missing_rows": int(alignment_report.get("missing_rows") or 0),
                    "extra_rows": int(alignment_report.get("extra_rows") or 0),
                },
            )
        )
        db.commit()
        return alignment_report

    def export_snapshot(self, db, current_user: User, task_id: str) -> Dict[str, Any]:
        task = db.query(AITask).filter(AITask.task_id == task_id).first()
        if not task:
            raise ValueError("任务不存在")
        self._ensure_access(task, current_user)

        try:
            df_work = self._read_pickle_with_retry(task.df_work_path, retries=5, wait_sec=0.04)
        except Exception as exc:
            raise ValueError(str(exc)) from exc

        try:
            source_df = self._read_pickle_with_retry(task.source_df_path, retries=4, wait_sec=0.03)
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

        url_bad = None
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
        self._append_runtime_log(
            task_id,
            f"导出快照完成：processed={len(df_processed)}，unprocessed={len(df_unprocessed)}，"
            f"ok={len(df_ok)}，bad={len(df_bad)}。",
        )

        return {
            "task_id": task.task_id,
            "processed_rows": len(df_processed),
            "unprocessed_rows": len(df_unprocessed),
            "processed_file_url": url_processed,
            "unprocessed_file_url": url_unprocessed,
            "ok_file_url": url_ok,
            "bad_file_url": url_bad,
        }
