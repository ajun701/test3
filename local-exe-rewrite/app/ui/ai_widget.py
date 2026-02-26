from __future__ import annotations

import json
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable

from PySide6.QtCore import Qt, QTimer
from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QFileDialog,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QPushButton,
    QProgressBar,
    QScrollArea,
    QSpinBox,
    QDoubleSpinBox,
    QTableWidget,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from app.services.task_runner import AITaskRunner
from app.ui.helpers import (
    StatCard,
    init_table,
    open_path,
    read_file_bytes,
    set_dataframe,
    set_records_table,
    show_error,
    show_info,
    show_warn,
)
from app.utils.excel_utils import read_table


class AIWidget(QWidget):
    def __init__(
        self,
        session_factory,
        get_current_user: Callable[[], Any],
        get_match_result: Callable[[], dict[str, Any]],
        task_runner: AITaskRunner,
    ) -> None:
        super().__init__()
        self._session_factory = session_factory
        self._get_current_user = get_current_user
        self._get_match_result = get_match_result
        self._task_runner = task_runner

        self.task_id = ""
        self.rows_page = 1
        self._last_log_text = ""

        self.timer = QTimer(self)
        self.timer.setInterval(1500)
        self.timer.timeout.connect(self._poll)

        self._build_ui()

    @staticmethod
    def _status_cn(status: str) -> str:
        mapping = {
            "pending": "排队中",
            "running": "运行中",
            "paused": "已暂停",
            "completed": "已完成",
            "error": "异常",
        }
        return mapping.get(str(status or "").strip().lower(), str(status or "未知"))

    @staticmethod
    def _fmt_dt(value: Any) -> str:
        if value is None:
            return "-"
        try:
            return value.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return str(value)

    @staticmethod
    def _ratio_text(part: int, total: int, prefix: str = "占比") -> str:
        if total <= 0:
            return f"{prefix} 0.0%"
        return f"{prefix} {part * 100.0 / total:.1f}%"

    @staticmethod
    def _fmt_duration(seconds: float) -> str:
        total = max(int(round(float(seconds))), 0)
        hours, rem = divmod(total, 3600)
        minutes, sec = divmod(rem, 60)
        if hours > 0:
            return f"{hours}小时{minutes}分{sec}秒"
        if minutes > 0:
            return f"{minutes}分{sec}秒"
        return f"{sec}秒"

    @contextmanager
    def _db(self):
        db = self._session_factory()
        try:
            yield db
        finally:
            db.close()

    def _build_ui(self) -> None:
        root_layout = QVBoxLayout(self)
        root_layout.setContentsMargins(8, 8, 8, 8)
        root_layout.setSpacing(8)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        root_layout.addWidget(scroll)

        container = QWidget()
        layout = QVBoxLayout(container)
        layout.setContentsMargins(4, 4, 4, 4)
        layout.setSpacing(10)
        scroll.setWidget(container)

        form_box = QGroupBox("步骤三：AI复核")
        form = QGridLayout(form_box)
        self.use_step2 = QCheckBox("使用步骤二已入库表")
        self.use_step2.stateChanged.connect(self.sync_source_preview)
        self.file_edit = QLineEdit()
        self.file_btn = QPushButton("选择待复核文件")
        self.file_btn.clicked.connect(self._select_file)
        self.api_key = QLineEdit()
        self.api_key.setEchoMode(QLineEdit.Password)
        self.model = QLineEdit("qwen3-vl-flash")
        self.max_images = QSpinBox()
        self.max_images.setRange(1, 10)
        self.max_images.setValue(4)
        self.max_rows = QSpinBox()
        self.max_rows.setRange(1, 10000)
        self.max_rows.setValue(300)
        self.rate_sec = QDoubleSpinBox()
        self.rate_sec.setRange(0.1, 60.0)
        self.rate_sec.setSingleStep(0.1)
        self.rate_sec.setValue(2.0)
        self.rate_rows = QSpinBox()
        self.rate_rows.setRange(1, 100)
        self.rate_rows.setValue(1)
        self.rate_sec.valueChanged.connect(self._update_speed_label)
        self.rate_rows.valueChanged.connect(self._update_speed_label)
        self.speed_label = QLabel()
        self._update_speed_label()

        form.addWidget(self.use_step2, 0, 0, 1, 3)
        form.addWidget(QLabel("待复核文件"), 1, 0)
        form.addWidget(self.file_edit, 1, 1)
        form.addWidget(self.file_btn, 1, 2)
        form.addWidget(QLabel("DashScope API Key"), 2, 0)
        form.addWidget(self.api_key, 2, 1, 1, 2)
        form.addWidget(QLabel("模型"), 3, 0)
        form.addWidget(self.model, 3, 1, 1, 2)
        form.addWidget(QLabel("最大图片数"), 4, 0)
        form.addWidget(self.max_images, 4, 1)
        form.addWidget(QLabel("最大处理行数"), 4, 2)
        form.addWidget(self.max_rows, 4, 3)
        form.addWidget(QLabel("速度(秒/条数)"), 5, 0)
        form.addWidget(self.rate_sec, 5, 1)
        form.addWidget(self.rate_rows, 5, 2)
        form.addWidget(self.speed_label, 5, 3)
        layout.addWidget(form_box)

        btn_row = QHBoxLayout()
        btn_start = QPushButton("启动任务")
        btn_pause = QPushButton("暂停")
        btn_resume = QPushButton("继续")
        btn_refresh = QPushButton("刷新")
        btn_align = QPushButton("主动校验一致性")
        btn_snap = QPushButton("生成快照")
        btn_start.clicked.connect(self._start_task)
        btn_pause.clicked.connect(self._pause_task)
        btn_resume.clicked.connect(self._resume_task)
        btn_refresh.clicked.connect(self.refresh_panels)
        btn_align.clicked.connect(self._alignment_check)
        btn_snap.clicked.connect(self._snapshot)
        btn_row.addWidget(btn_start)
        btn_row.addWidget(btn_pause)
        btn_row.addWidget(btn_resume)
        btn_row.addWidget(btn_refresh)
        btn_row.addWidget(btn_align)
        btn_row.addWidget(btn_snap)
        btn_row.addStretch(1)
        layout.addLayout(btn_row)

        self.source_table = QTableWidget()
        init_table(self.source_table)
        src_box = QGroupBox("待复核预览")
        src_layout = QVBoxLayout(src_box)
        src_box.setMinimumHeight(210)
        src_layout.addWidget(self.source_table)
        layout.addWidget(src_box, 1)

        status_box = QGroupBox("任务状态")
        status_layout = QVBoxLayout(status_box)
        card_row = QHBoxLayout()
        self.card_total = StatCard("计划", "0", accent="primary", meta="计划处理行数")
        self.card_processed = StatCard("已处理", "0", accent="info", meta="完成率 0.0%")
        self.card_ok = StatCard("正常", "0", accent="success", meta="占已处理 0.0%")
        self.card_bad = StatCard("异常", "0", accent="warning", meta="占已处理 0.0%")
        card_row.addWidget(self.card_total)
        card_row.addWidget(self.card_processed)
        card_row.addWidget(self.card_ok)
        card_row.addWidget(self.card_bad)
        status_layout.addLayout(card_row)

        top_row = QHBoxLayout()
        self.status_label = QLabel("任务: -")
        self.status_badge = QLabel("未知")
        self.status_badge.setObjectName("statusBadge")
        self.updated_label = QLabel("更新时间：-")
        top_row.addWidget(self.status_label)
        top_row.addWidget(self.status_badge)
        top_row.addStretch(1)
        top_row.addWidget(self.updated_label)

        self.progress = QProgressBar()
        self.progress.setRange(0, 100)
        self.progress.setTextVisible(True)
        self.progress.setFormat("总进度 %p%%")
        self.progress_desc = QLabel("已处理 0 / 0（0.0%），待处理 0")
        self.status_detail_label = QLabel("状态：-")
        self.runtime_speed_label = QLabel("当前速度：-")
        self.error_label = QLabel("")
        self.error_label.setObjectName("errorInline")
        self.align_text = QTextEdit()
        self.align_text.setReadOnly(True)
        self.artifact_list = QListWidget()
        self.artifact_list.itemDoubleClicked.connect(self._open_item)
        status_layout.addLayout(top_row)
        status_layout.addWidget(self.progress)
        status_layout.addWidget(self.progress_desc)
        status_layout.addWidget(self.status_detail_label)
        status_layout.addWidget(self.runtime_speed_label)
        status_layout.addWidget(self.error_label)
        status_layout.addWidget(QLabel("一致性报告"))
        status_layout.addWidget(self.align_text)
        status_layout.addWidget(QLabel("任务产物（双击打开）"))
        status_layout.addWidget(self.artifact_list)
        self.align_text.setMinimumHeight(140)
        self.artifact_list.setMinimumHeight(90)
        layout.addWidget(status_box, 1)

        rows_box = QGroupBox("行级进度")
        rows_layout = QVBoxLayout(rows_box)
        rows_ctrl = QHBoxLayout()
        self.scope = QComboBox()
        self.scope.addItem("全部", "all")
        self.scope.addItem("已处理", "processed")
        self.scope.addItem("未处理", "pending")
        self.scope.currentIndexChanged.connect(self._reset_rows)
        self.page_size = QComboBox()
        self.page_size.addItem("20", 20)
        self.page_size.addItem("50", 50)
        self.page_size.addItem("100", 100)
        self.page_size.setCurrentIndex(1)
        self.page_size.currentIndexChanged.connect(self._reset_rows)
        self.page_label = QLabel("第 1 页")
        btn_prev = QPushButton("上一页")
        btn_next = QPushButton("下一页")
        btn_prev.clicked.connect(self._prev_page)
        btn_next.clicked.connect(self._next_page)
        rows_ctrl.addWidget(QLabel("范围"))
        rows_ctrl.addWidget(self.scope)
        rows_ctrl.addWidget(QLabel("每页"))
        rows_ctrl.addWidget(self.page_size)
        rows_ctrl.addWidget(btn_prev)
        rows_ctrl.addWidget(btn_next)
        rows_ctrl.addWidget(self.page_label)
        rows_ctrl.addStretch(1)
        rows_layout.addLayout(rows_ctrl)

        self.rows_table = QTableWidget()
        init_table(self.rows_table)
        rows_box.setMinimumHeight(280)
        rows_layout.addWidget(self.rows_table)
        layout.addWidget(rows_box, 2)

        self.snapshot_list = QListWidget()
        self.snapshot_list.itemDoubleClicked.connect(self._open_item)
        snap_box = QGroupBox("快照产物")
        snap_layout = QVBoxLayout(snap_box)
        snap_box.setMinimumHeight(120)
        snap_layout.addWidget(self.snapshot_list)
        layout.addWidget(snap_box, 0)

        log_box = QGroupBox("实时处理日志")
        log_layout = QVBoxLayout(log_box)
        log_ctrl = QHBoxLayout()
        self.log_lines = QComboBox()
        self.log_lines.addItem("最近100行", 100)
        self.log_lines.addItem("最近300行", 300)
        self.log_lines.addItem("最近800行", 800)
        self.log_lines.setCurrentIndex(1)
        self.log_lines.currentIndexChanged.connect(self._refresh_runtime_logs)
        self.log_auto_scroll = QCheckBox("自动滚动")
        self.log_auto_scroll.setChecked(True)
        btn_open_log = QPushButton("打开日志文件")
        btn_open_log.clicked.connect(self._open_runtime_log_file)
        log_ctrl.addWidget(self.log_lines)
        log_ctrl.addWidget(self.log_auto_scroll)
        log_ctrl.addWidget(btn_open_log)
        log_ctrl.addStretch(1)
        log_layout.addLayout(log_ctrl)
        self.log_view = QTextEdit()
        self.log_view.setObjectName("runtimeLog")
        self.log_view.setReadOnly(True)
        self.log_view.setPlaceholderText("任务运行后，这里会显示逐行处理日志。")
        self.log_view.setMinimumHeight(200)
        log_layout.addWidget(self.log_view)
        layout.addWidget(log_box, 1)

        layout.addStretch(1)

    def _select_file(self) -> None:
        path, _ = QFileDialog.getOpenFileName(self, "选择待复核文件", "", "表格文件 (*.xlsx *.xls *.csv)")
        if not path:
            return
        self.file_edit.setText(path)
        self.sync_source_preview()

    def _update_speed_label(self) -> None:
        self.speed_label.setText(f"≈ {self._min_interval():.3f} 秒/条")

    def _min_interval(self) -> float:
        sec = float(self.rate_sec.value())
        rows = float(self.rate_rows.value())
        if sec <= 0 or rows <= 0:
            return 0.8
        return max(0.0, sec / rows)

    def sync_source_preview(self) -> None:
        use_step2 = self.use_step2.isChecked()
        self.file_edit.setEnabled(not use_step2)
        self.file_btn.setEnabled(not use_step2)

        try:
            if use_step2:
                match_res = self._get_match_result()
                src = str(match_res.get("inbound_file_url") or "").strip()
                if not src:
                    show_warn(self, "未检测到步骤二结果，请关闭开关后上传文件")
                    return
                file_bytes, filename = read_file_bytes(src)
            else:
                src = self.file_edit.text().strip()
                if not src:
                    return
                file_bytes, filename = read_file_bytes(src)
            set_dataframe(self.source_table, read_table(file_bytes, filename))
        except Exception as exc:
            show_error(self, f"预览失败: {exc}")

    def _start_task(self) -> None:
        user = self._get_current_user()
        if user is None:
            show_warn(self, "本地用户未就绪，请重启程序")
            return

        try:
            if self.use_step2.isChecked():
                src = str(self._get_match_result().get("inbound_file_url") or "").strip()
                if not src:
                    raise ValueError("请先完成步骤二")
            else:
                src = self.file_edit.text().strip()
                if not src:
                    raise ValueError("请上传待复核表")

            file_bytes, filename = read_file_bytes(src)
            with self._db() as db:
                task = self._task_runner.start_task(
                    db,
                    user,
                    file_bytes=file_bytes,
                    filename=filename,
                    api_key=self.api_key.text().strip(),
                    model_name=self.model.text().strip() or "qwen3-vl-flash",
                    max_images=int(self.max_images.value()),
                    min_interval_sec=self._min_interval(),
                    max_retries=4,
                    backoff_base_sec=1.0,
                    max_ai_rows=int(self.max_rows.value()),
                )
            self.task_id = task.task_id
            self.rows_page = 1
            self.refresh_panels()
            self.timer.start()
            show_info(self, "AI任务已启动")
        except Exception as exc:
            show_error(self, f"启动失败: {exc}")

    def _pause_task(self) -> None:
        user = self._get_current_user()
        if user is None or not self.task_id:
            show_warn(self, "暂无任务")
            return
        try:
            with self._db() as db:
                self._task_runner.pause_task(db, user, self.task_id)
            self.timer.stop()
            self.refresh_panels()
            show_info(self, "任务已暂停")
        except Exception as exc:
            show_error(self, f"暂停失败: {exc}")

    def _resume_task(self) -> None:
        user = self._get_current_user()
        if user is None or not self.task_id:
            show_warn(self, "暂无任务")
            return
        try:
            with self._db() as db:
                self._task_runner.resume_task(
                    db,
                    user,
                    self.task_id,
                    api_key=self.api_key.text().strip(),
                    min_interval_sec=self._min_interval(),
                )
            self.refresh_panels()
            self.timer.start()
            show_info(self, "任务已恢复")
        except Exception as exc:
            show_error(self, f"恢复失败: {exc}")

    def _alignment_check(self) -> None:
        user = self._get_current_user()
        if user is None or not self.task_id:
            show_warn(self, "暂无任务")
            return
        try:
            with self._db() as db:
                report = self._task_runner.alignment_check(db, user, self.task_id)
            self.align_text.setPlainText(json.dumps(report, ensure_ascii=False, indent=2))
            self.refresh_panels()
            show_info(self, "一致性校验完成")
        except Exception as exc:
            show_error(self, f"一致性校验失败: {exc}")

    def _snapshot(self) -> None:
        user = self._get_current_user()
        if user is None or not self.task_id:
            show_warn(self, "暂无任务")
            return
        try:
            with self._db() as db:
                snap = self._task_runner.export_snapshot(db, user, self.task_id)
            self.snapshot_list.clear()
            for label, key in (("已处理", "processed_file_url"), ("未处理", "unprocessed_file_url"), ("可打款", "ok_file_url"), ("需回访", "bad_file_url")):
                path = str(snap.get(key) or "").strip()
                if not path:
                    continue
                item = QListWidgetItem(f"{label}: {Path(path).name}")
                item.setData(Qt.UserRole, path)
                self.snapshot_list.addItem(item)
            show_info(self, "快照已生成")
        except Exception as exc:
            show_error(self, f"快照生成失败: {exc}")

    def refresh_panels(self) -> None:
        self._refresh_status()
        self._refresh_rows()
        self._refresh_runtime_logs()

    def _refresh_status(self) -> None:
        user = self._get_current_user()
        if user is None or not self.task_id:
            return

        with self._db() as db:
            status = self._task_runner.get_task_status(db, user, self.task_id)

        total = int(status.get("total") or 0)
        processed = int(status.get("processed") or 0)
        pending = int(status.get("pending") or max(total - processed, 0))
        ok_rows = int(status.get("ok_rows") or 0)
        bad_rows = int(status.get("bad_rows") or 0)
        progress_pct = processed * 100.0 / total if total > 0 else 0.0
        status_cn = self._status_cn(str(status.get("status") or ""))
        status_raw = str(status.get("status") or "").strip().lower()

        self.status_label.setText(
            f"任务: {status['task_id']}"
        )
        self.status_badge.setText(f"{status_cn} ({status.get('status')})")
        self.status_badge.setProperty("state", status_raw)
        self.status_badge.style().unpolish(self.status_badge)
        self.status_badge.style().polish(self.status_badge)
        self.updated_label.setText(f"更新时间：{self._fmt_dt(status.get('updated_at'))}")
        self.card_total.set_value(total)
        self.card_total.set_meta("计划处理行数")
        self.card_processed.set_value(processed)
        self.card_processed.set_meta(self._ratio_text(processed, total, prefix="完成率"))
        self.card_ok.set_value(ok_rows)
        self.card_ok.set_meta(self._ratio_text(ok_rows, processed, prefix="占已处理"))
        self.card_bad.set_value(bad_rows)
        self.card_bad.set_meta(self._ratio_text(bad_rows, processed, prefix="占已处理"))
        self.progress.setValue(int(round(float(status.get("progress_ratio", 0.0)) * 100)))
        self.progress_desc.setText(f"已处理 {processed} / {total}（{progress_pct:.1f}%），待处理 {pending}")
        self.status_detail_label.setText(f"状态：{status_cn}（{status.get('status')}）")
        interval = status.get("min_interval_sec")
        if interval is None:
            self.runtime_speed_label.setText("当前速度：-")
        else:
            interval_val = float(interval)
            self.runtime_speed_label.setText(f"当前速度：{interval_val:.3f} 秒/条")
            if pending > 0:
                self.progress_desc.setText(
                    f"已处理 {processed} / {total}（{progress_pct:.1f}%），待处理 {pending}，预计剩余 {self._fmt_duration(pending * interval_val)}"
                )
        err = str(status.get("error_message") or "").strip()
        self.error_label.setText(f"异常：{err}" if err else "")
        self.align_text.setPlainText(json.dumps(status.get("alignment_report") or {}, ensure_ascii=False, indent=2))

        self.artifact_list.clear()
        for path in status.get("artifacts") or []:
            item = QListWidgetItem(Path(str(path)).name)
            item.setData(Qt.UserRole, str(path))
            self.artifact_list.addItem(item)

        if status["status"] in ("completed", "paused", "error"):
            self.timer.stop()

    def _refresh_rows(self) -> None:
        user = self._get_current_user()
        if user is None or not self.task_id:
            return

        with self._db() as db:
            data = self._task_runner.get_task_rows(
                db,
                user,
                self.task_id,
                scope=self.scope.currentData(),
                page=self.rows_page,
                page_size=int(self.page_size.currentData()),
            )

        self.rows_page = int(data["page"])
        total_pages = int(data["total_pages"])
        self.page_label.setText(f"第 {self.rows_page}/{total_pages} 页，总 {data['total_rows']} 行")
        set_records_table(self.rows_table, data.get("columns") or [], data.get("rows") or [])

    def _reset_rows(self) -> None:
        self.rows_page = 1
        self._refresh_rows()

    def _prev_page(self) -> None:
        if self.rows_page <= 1:
            return
        self.rows_page -= 1
        self._refresh_rows()

    def _next_page(self) -> None:
        self.rows_page += 1
        self._refresh_rows()

    def _open_item(self, item: QListWidgetItem) -> None:
        path = str(item.data(Qt.UserRole) or "")
        if not path:
            return
        try:
            open_path(path)
        except Exception as exc:
            show_error(self, f"无法打开文件: {exc}")

    def _refresh_runtime_logs(self, *_args) -> None:
        user = self._get_current_user()
        if user is None or not self.task_id:
            self.log_view.clear()
            self._last_log_text = ""
            return

        lines = self._task_runner.get_runtime_logs(self.task_id, max_lines=int(self.log_lines.currentData() or 300))
        text = "\n".join(lines)
        if text == self._last_log_text:
            return
        self._last_log_text = text
        self.log_view.setPlainText(text)
        if self.log_auto_scroll.isChecked():
            sb = self.log_view.verticalScrollBar()
            sb.setValue(sb.maximum())

    def _open_runtime_log_file(self) -> None:
        if not self.task_id:
            show_warn(self, "暂无任务日志")
            return
        path = self._task_runner.get_runtime_log_path(self.task_id)
        try:
            open_path(path)
        except Exception as exc:
            show_error(self, f"无法打开日志文件: {exc}")

    def _poll(self) -> None:
        try:
            self.refresh_panels()
        except Exception as exc:
            self.timer.stop()
            show_error(self, f"任务轮询失败: {exc}")

    def restore_latest_task(self, task_id: str) -> None:
        self.task_id = str(task_id or "").strip()
        self.rows_page = 1
        self._last_log_text = ""
        if self.task_id:
            self.refresh_panels()

    def stop_polling(self) -> None:
        self.timer.stop()

