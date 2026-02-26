from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Optional

from PySide6.QtCore import QDateTime
from PySide6.QtWidgets import (
    QAbstractItemView,
    QCheckBox,
    QComboBox,
    QDateTimeEdit,
    QFileDialog,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QTableWidget,
    QVBoxLayout,
    QWidget,
)

from app.core.config import settings
from app.db.models import ArtifactRecord, OperationHistory
from app.services.history_service import apply_artifact_filters, apply_history_filters, export_history_csv
from app.ui.helpers import init_table, open_path, set_records_table, show_error, show_info, show_warn


class HistoryWidget(QWidget):
    def __init__(self, session_factory, get_current_user: Callable[[], Any]) -> None:
        super().__init__()
        self._session_factory = session_factory
        self._get_current_user = get_current_user

        self.history_page = 1
        self.history_size = 50
        self.history_total = 0
        self.artifact_page = 1
        self.artifact_size = 50
        self.artifact_total = 0
        self._history_rows_cache: list[dict[str, Any]] = []
        self._artifact_rows_cache: list[dict[str, Any]] = []

        self._build_ui()

    @contextmanager
    def _db(self):
        db = self._session_factory()
        try:
            yield db
        finally:
            db.close()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)

        filter_box = QGroupBox("筛选")
        filter_grid = QGridLayout(filter_box)
        self.stage = QLineEdit()
        self.action = QLineEdit()
        self.operator = QLineEdit()
        self.task_id = QLineEdit()
        self.stage.setPlaceholderText("按阶段过滤")
        self.action.setPlaceholderText("按动作关键字过滤")
        self.operator.setPlaceholderText("按操作人过滤")
        self.task_id.setPlaceholderText("仅文件记录：按任务ID过滤")
        self.start_enable = QCheckBox("启用开始时间")
        self.end_enable = QCheckBox("启用结束时间")
        self.start_time = QDateTimeEdit(QDateTime.currentDateTime().addDays(-7))
        self.end_time = QDateTimeEdit(QDateTime.currentDateTime())
        self.start_time.setCalendarPopup(True)
        self.end_time.setCalendarPopup(True)
        self.quick_range = QComboBox()
        self.quick_range.addItem("自定义", "custom")
        self.quick_range.addItem("最近24小时", "24h")
        self.quick_range.addItem("最近3天", "3d")
        self.quick_range.addItem("最近7天", "7d")
        self.quick_range.addItem("最近30天", "30d")
        self.quick_range.addItem("全部时间", "all")
        self.quick_range.setCurrentIndex(3)
        self.quick_range.currentIndexChanged.connect(self._apply_quick_range)
        self.stage.returnPressed.connect(self.query_all)
        self.action.returnPressed.connect(self.query_all)
        self.operator.returnPressed.connect(self.query_all)
        self.task_id.returnPressed.connect(self.query_all)

        filter_grid.addWidget(QLabel("阶段"), 0, 0)
        filter_grid.addWidget(self.stage, 0, 1)
        filter_grid.addWidget(QLabel("动作"), 0, 2)
        filter_grid.addWidget(self.action, 0, 3)
        filter_grid.addWidget(QLabel("操作人"), 0, 4)
        filter_grid.addWidget(self.operator, 0, 5)
        filter_grid.addWidget(QLabel("任务ID"), 1, 0)
        filter_grid.addWidget(self.task_id, 1, 1, 1, 2)
        filter_grid.addWidget(QLabel("时间快捷"), 1, 3)
        filter_grid.addWidget(self.quick_range, 1, 4, 1, 2)
        filter_grid.addWidget(self.start_enable, 2, 0)
        filter_grid.addWidget(self.start_time, 2, 1, 1, 2)
        filter_grid.addWidget(self.end_enable, 2, 3)
        filter_grid.addWidget(self.end_time, 2, 4, 1, 2)

        btn_query = QPushButton("查询")
        btn_refresh = QPushButton("刷新")
        btn_reset = QPushButton("重置筛选")
        btn_export = QPushButton("导出CSV")
        btn_query.clicked.connect(self.query_all)
        btn_refresh.clicked.connect(self.refresh_all)
        btn_reset.clicked.connect(self.reset_filters)
        btn_export.clicked.connect(self.export_csv)
        filter_grid.addWidget(btn_query, 2, 6)
        filter_grid.addWidget(btn_refresh, 0, 6)
        filter_grid.addWidget(btn_reset, 1, 6)
        filter_grid.addWidget(btn_export, 3, 6)
        self.filter_summary = QLabel("当前筛选：最近7天")
        self.filter_summary.setObjectName("filterSummary")
        self.filter_hint = QLabel("提示：主筛选输入后按 Enter 查询；表格“当前页筛选”只过滤当前页数据。")
        self.filter_hint.setObjectName("subtleHint")
        filter_grid.addWidget(self.filter_summary, 4, 0, 1, 7)
        filter_grid.addWidget(self.filter_hint, 5, 0, 1, 7)

        layout.addWidget(filter_box)

        hist_box = QGroupBox("操作记录")
        hist_layout = QVBoxLayout(hist_box)
        hist_ctrl = QHBoxLayout()
        self.history_columns = ["timestamp", "operator", "stage", "action", "input_rows", "output_rows", "detail"]
        self.history_headers = ["时间", "操作人", "阶段", "动作", "输入", "输出", "详情"]
        self.history_page_label = QLabel("第 1 页")
        self.history_local_keyword = QLineEdit()
        self.history_local_keyword.setPlaceholderText("当前页快速筛选（关键词）")
        self.history_local_keyword.textChanged.connect(self._apply_history_local_filter)
        self.history_filtered_label = QLabel("当前页显示 0 / 0")
        self.history_size_combo = QComboBox()
        self.history_size_combo.addItem("20条/页", 20)
        self.history_size_combo.addItem("50条/页", 50)
        self.history_size_combo.addItem("100条/页", 100)
        self.history_size_combo.setCurrentIndex(1)
        self.history_size_combo.currentIndexChanged.connect(self._on_history_page_size_change)
        btn_prev = QPushButton("上一页")
        btn_next = QPushButton("下一页")
        btn_prev.clicked.connect(self._history_prev)
        btn_next.clicked.connect(self._history_next)
        hist_ctrl.addWidget(self.history_local_keyword)
        hist_ctrl.addWidget(self.history_size_combo)
        hist_ctrl.addWidget(btn_prev)
        hist_ctrl.addWidget(btn_next)
        hist_ctrl.addWidget(self.history_page_label)
        hist_ctrl.addWidget(self.history_filtered_label)
        hist_ctrl.addStretch(1)
        hist_layout.addLayout(hist_ctrl)

        history_col_row = QHBoxLayout()
        history_col_row.addWidget(QLabel("列筛选:"))
        self.hf_operator = QLineEdit()
        self.hf_stage = QLineEdit()
        self.hf_action = QLineEdit()
        self.hf_operator.setPlaceholderText("操作人")
        self.hf_stage.setPlaceholderText("阶段")
        self.hf_action.setPlaceholderText("动作")
        self.hf_operator.textChanged.connect(self._apply_history_local_filter)
        self.hf_stage.textChanged.connect(self._apply_history_local_filter)
        self.hf_action.textChanged.connect(self._apply_history_local_filter)
        btn_clear_hist_local = QPushButton("清空当前页筛选")
        btn_clear_hist_local.clicked.connect(self._clear_history_local_filters)
        history_col_row.addWidget(self.hf_operator)
        history_col_row.addWidget(self.hf_stage)
        history_col_row.addWidget(self.hf_action)
        history_col_row.addWidget(btn_clear_hist_local)
        hist_layout.addLayout(history_col_row)

        self.history_table = QTableWidget()
        init_table(self.history_table)
        hist_layout.addWidget(self.history_table)
        layout.addWidget(hist_box)

        art_box = QGroupBox("历史处理文件")
        art_layout = QVBoxLayout(art_box)
        art_ctrl = QHBoxLayout()
        self.artifact_columns = ["created_at", "operator", "stage", "action", "file_name", "task_id", "file_url"]
        self.artifact_headers = ["时间", "操作人", "阶段", "动作", "文件名", "任务ID", "文件路径"]
        self.artifact_page_label = QLabel("第 1 页")
        self.artifact_local_keyword = QLineEdit()
        self.artifact_local_keyword.setPlaceholderText("当前页快速筛选（关键词）")
        self.artifact_local_keyword.textChanged.connect(self._apply_artifact_local_filter)
        self.artifact_filtered_label = QLabel("当前页显示 0 / 0")
        self.artifact_size_combo = QComboBox()
        self.artifact_size_combo.addItem("20条/页", 20)
        self.artifact_size_combo.addItem("50条/页", 50)
        self.artifact_size_combo.addItem("100条/页", 100)
        self.artifact_size_combo.setCurrentIndex(1)
        self.artifact_size_combo.currentIndexChanged.connect(self._on_artifact_page_size_change)
        btn_art_prev = QPushButton("上一页")
        btn_art_next = QPushButton("下一页")
        btn_open = QPushButton("打开选中文件")
        btn_art_prev.clicked.connect(self._artifact_prev)
        btn_art_next.clicked.connect(self._artifact_next)
        btn_open.clicked.connect(self._open_selected_artifact)
        art_ctrl.addWidget(self.artifact_local_keyword)
        art_ctrl.addWidget(self.artifact_size_combo)
        art_ctrl.addWidget(btn_art_prev)
        art_ctrl.addWidget(btn_art_next)
        art_ctrl.addWidget(self.artifact_page_label)
        art_ctrl.addWidget(self.artifact_filtered_label)
        art_ctrl.addWidget(btn_open)
        art_ctrl.addStretch(1)
        art_layout.addLayout(art_ctrl)

        artifact_col_row = QHBoxLayout()
        artifact_col_row.addWidget(QLabel("列筛选:"))
        self.af_operator = QLineEdit()
        self.af_stage = QLineEdit()
        self.af_action = QLineEdit()
        self.af_file = QLineEdit()
        self.af_task = QLineEdit()
        self.af_operator.setPlaceholderText("操作人")
        self.af_stage.setPlaceholderText("阶段")
        self.af_action.setPlaceholderText("动作")
        self.af_file.setPlaceholderText("文件名")
        self.af_task.setPlaceholderText("任务ID")
        self.af_operator.textChanged.connect(self._apply_artifact_local_filter)
        self.af_stage.textChanged.connect(self._apply_artifact_local_filter)
        self.af_action.textChanged.connect(self._apply_artifact_local_filter)
        self.af_file.textChanged.connect(self._apply_artifact_local_filter)
        self.af_task.textChanged.connect(self._apply_artifact_local_filter)
        btn_clear_art_local = QPushButton("清空当前页筛选")
        btn_clear_art_local.clicked.connect(self._clear_artifact_local_filters)
        artifact_col_row.addWidget(self.af_operator)
        artifact_col_row.addWidget(self.af_stage)
        artifact_col_row.addWidget(self.af_action)
        artifact_col_row.addWidget(self.af_file)
        artifact_col_row.addWidget(self.af_task)
        artifact_col_row.addWidget(btn_clear_art_local)
        art_layout.addLayout(artifact_col_row)

        self.artifact_table = QTableWidget()
        init_table(self.artifact_table)
        self.artifact_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.artifact_table.setSelectionMode(QAbstractItemView.SingleSelection)
        art_layout.addWidget(self.artifact_table)
        layout.addWidget(art_box)

        self._apply_quick_range()
        self._update_filter_summary()

    def _apply_quick_range(self, *_args) -> None:
        mode = str(self.quick_range.currentData() or "custom")
        now = QDateTime.currentDateTime()
        if mode == "custom":
            self._update_filter_summary()
            return
        if mode == "all":
            self.start_enable.setChecked(False)
            self.end_enable.setChecked(False)
            self._update_filter_summary()
            return

        delta_days = {"24h": 1, "3d": 3, "7d": 7, "30d": 30}.get(mode, 7)
        self.start_enable.setChecked(True)
        self.end_enable.setChecked(True)
        self.end_time.setDateTime(now)
        self.start_time.setDateTime(now.addDays(-delta_days))
        self._update_filter_summary()

    def reset_filters(self) -> None:
        self.stage.clear()
        self.action.clear()
        self.operator.clear()
        self.task_id.clear()
        self._clear_history_local_filters()
        self._clear_artifact_local_filters()
        self.quick_range.setCurrentIndex(3)
        self._apply_quick_range()
        self.query_all()

    def _time_filters(self) -> tuple[Optional[datetime], Optional[datetime]]:
        start = self.start_time.dateTime().toPython() if self.start_enable.isChecked() else None
        end = self.end_time.dateTime().toPython() if self.end_enable.isChecked() else None
        if start and end and start > end:
            raise ValueError("开始时间不能晚于结束时间")
        return start, end

    def query_all(self) -> None:
        self.history_page = 1
        self.artifact_page = 1
        self._update_filter_summary()
        self._load_history()
        self._load_artifacts()

    def refresh_all(self) -> None:
        self._update_filter_summary()
        self._load_history()
        self._load_artifacts()

    def _load_history(self) -> None:
        if self._get_current_user() is None:
            show_warn(self, "本地用户未就绪，请重启程序")
            return

        try:
            start, end = self._time_filters()
        except ValueError as exc:
            show_warn(self, str(exc))
            return
        with self._db() as db:
            q = apply_history_filters(
                db.query(OperationHistory),
                stage=self.stage.text().strip(),
                action=self.action.text().strip(),
                operator=self.operator.text().strip(),
                start_time=start,
                end_time=end,
            )
            self.history_total = q.count()
            items = (
                q.order_by(OperationHistory.timestamp.desc())
                .offset((self.history_page - 1) * self.history_size)
                .limit(self.history_size)
                .all()
            )

        total_pages = max(1, (self.history_total + self.history_size - 1) // self.history_size)
        self.history_page = min(max(self.history_page, 1), total_pages)
        self.history_page_label.setText(f"第 {self.history_page}/{total_pages} 页，总 {self.history_total} 条")

        rows = []
        for item in items:
            rows.append(
                {
                    "timestamp": item.timestamp.strftime("%Y-%m-%d %H:%M:%S") if item.timestamp else "",
                    "operator": item.operator,
                    "stage": item.stage,
                    "action": item.action,
                    "input_rows": item.input_rows,
                    "output_rows": item.output_rows,
                    "detail": json.dumps(item.detail, ensure_ascii=False),
                }
            )
        self._history_rows_cache = rows
        self._apply_history_local_filter()

    def _load_artifacts(self) -> None:
        if self._get_current_user() is None:
            show_warn(self, "本地用户未就绪，请重启程序")
            return

        try:
            start, end = self._time_filters()
        except ValueError as exc:
            show_warn(self, str(exc))
            return
        with self._db() as db:
            q = apply_artifact_filters(
                db.query(ArtifactRecord),
                stage=self.stage.text().strip(),
                action=self.action.text().strip(),
                operator=self.operator.text().strip(),
                task_id=self.task_id.text().strip(),
                start_time=start,
                end_time=end,
            )
            self.artifact_total = q.count()
            items = (
                q.order_by(ArtifactRecord.created_at.desc())
                .offset((self.artifact_page - 1) * self.artifact_size)
                .limit(self.artifact_size)
                .all()
            )

        total_pages = max(1, (self.artifact_total + self.artifact_size - 1) // self.artifact_size)
        self.artifact_page = min(max(self.artifact_page, 1), total_pages)
        self.artifact_page_label.setText(f"第 {self.artifact_page}/{total_pages} 页，总 {self.artifact_total} 条")

        rows = []
        for item in items:
            rows.append(
                {
                    "created_at": item.created_at.strftime("%Y-%m-%d %H:%M:%S") if item.created_at else "",
                    "operator": item.operator,
                    "stage": item.stage,
                    "action": item.action,
                    "file_name": item.file_name,
                    "task_id": item.task_id,
                    "file_url": item.file_url,
                }
            )
        self._artifact_rows_cache = rows
        self._apply_artifact_local_filter()

    def export_csv(self) -> None:
        if self._get_current_user() is None:
            show_warn(self, "本地用户未就绪，请重启程序")
            return

        try:
            start, end = self._time_filters()
        except ValueError as exc:
            show_warn(self, str(exc))
            return
        with self._db() as db:
            q = apply_history_filters(
                db.query(OperationHistory),
                stage=self.stage.text().strip(),
                action=self.action.text().strip(),
                operator=self.operator.text().strip(),
                start_time=start,
                end_time=end,
            )
            rows = q.order_by(OperationHistory.timestamp.desc()).all()

        csv_text = export_history_csv(rows)
        default_name = f"operation_history_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        default_path = str(settings.data_dir / default_name)
        path, _ = QFileDialog.getSaveFileName(self, "保存历史CSV", default_path, "CSV Files (*.csv)")
        if not path:
            return
        Path(path).write_text(csv_text, encoding="utf-8")
        show_info(self, "历史记录已导出")

    def _history_prev(self) -> None:
        if self.history_page <= 1:
            return
        self.history_page -= 1
        self._load_history()

    def _history_next(self) -> None:
        self.history_page += 1
        self._load_history()

    def _artifact_prev(self) -> None:
        if self.artifact_page <= 1:
            return
        self.artifact_page -= 1
        self._load_artifacts()

    def _artifact_next(self) -> None:
        self.artifact_page += 1
        self._load_artifacts()

    def _open_selected_artifact(self) -> None:
        row = self.artifact_table.currentRow()
        if row < 0:
            show_warn(self, "请先选择一条文件记录")
            return

        path_item = self.artifact_table.item(row, 6)
        path = path_item.text().strip() if path_item else ""
        if not path:
            show_warn(self, "文件路径为空")
            return

        try:
            open_path(path)
        except Exception as exc:
            show_error(self, f"无法打开文件: {exc}")

    def _apply_history_local_filter(self, *_args) -> None:
        keyword = self.history_local_keyword.text().strip().lower()
        op_kw = self.hf_operator.text().strip().lower()
        stage_kw = self.hf_stage.text().strip().lower()
        action_kw = self.hf_action.text().strip().lower()
        rows = self._history_rows_cache
        filtered = []
        for row in rows:
            op_v = str(row.get("operator") or "").lower()
            stage_v = str(row.get("stage") or "").lower()
            action_v = str(row.get("action") or "").lower()
            if op_kw and op_kw not in op_v:
                continue
            if stage_kw and stage_kw not in stage_v:
                continue
            if action_kw and action_kw not in action_v:
                continue
            if keyword:
                joined = " | ".join(str(row.get(col) or "") for col in self.history_columns).lower()
                if keyword not in joined:
                    continue
            filtered.append(row)
        set_records_table(self.history_table, self.history_columns, filtered, headers=self.history_headers)
        self.history_filtered_label.setText(f"当前页显示 {len(filtered)} / {len(rows)}")

    def _apply_artifact_local_filter(self, *_args) -> None:
        keyword = self.artifact_local_keyword.text().strip().lower()
        op_kw = self.af_operator.text().strip().lower()
        stage_kw = self.af_stage.text().strip().lower()
        action_kw = self.af_action.text().strip().lower()
        file_kw = self.af_file.text().strip().lower()
        task_kw = self.af_task.text().strip().lower()
        rows = self._artifact_rows_cache
        filtered = []
        for row in rows:
            op_v = str(row.get("operator") or "").lower()
            stage_v = str(row.get("stage") or "").lower()
            action_v = str(row.get("action") or "").lower()
            file_v = str(row.get("file_name") or "").lower()
            task_v = str(row.get("task_id") or "").lower()
            if op_kw and op_kw not in op_v:
                continue
            if stage_kw and stage_kw not in stage_v:
                continue
            if action_kw and action_kw not in action_v:
                continue
            if file_kw and file_kw not in file_v:
                continue
            if task_kw and task_kw not in task_v:
                continue
            if keyword:
                joined = " | ".join(str(row.get(col) or "") for col in self.artifact_columns).lower()
                if keyword not in joined:
                    continue
            filtered.append(row)
        set_records_table(self.artifact_table, self.artifact_columns, filtered, headers=self.artifact_headers)
        self.artifact_filtered_label.setText(f"当前页显示 {len(filtered)} / {len(rows)}")

    def _clear_history_local_columns(self) -> None:
        self.hf_operator.clear()
        self.hf_stage.clear()
        self.hf_action.clear()

    def _clear_artifact_local_columns(self) -> None:
        self.af_operator.clear()
        self.af_stage.clear()
        self.af_action.clear()
        self.af_file.clear()
        self.af_task.clear()

    def _clear_history_local_filters(self) -> None:
        self.history_local_keyword.clear()
        self._clear_history_local_columns()

    def _clear_artifact_local_filters(self) -> None:
        self.artifact_local_keyword.clear()
        self._clear_artifact_local_columns()

    def _update_filter_summary(self) -> None:
        if not hasattr(self, "filter_summary"):
            return
        tags: list[str] = []
        if self.start_enable.isChecked():
            tags.append(f"开始 {self.start_time.dateTime().toString('yyyy-MM-dd HH:mm')}")
        if self.end_enable.isChecked():
            tags.append(f"结束 {self.end_time.dateTime().toString('yyyy-MM-dd HH:mm')}")
        if not self.start_enable.isChecked() and not self.end_enable.isChecked():
            tags.append("全部时间")

        stage = self.stage.text().strip()
        action = self.action.text().strip()
        operator = self.operator.text().strip()
        task_id = self.task_id.text().strip()
        if stage:
            tags.append(f"阶段={stage}")
        if action:
            tags.append(f"动作={action}")
        if operator:
            tags.append(f"操作人={operator}")
        if task_id:
            tags.append(f"任务ID={task_id}")
        text = "；".join(tags) if tags else "无"
        self.filter_summary.setText(f"当前筛选：{text}")

    def _on_history_page_size_change(self, *_args) -> None:
        self.history_size = int(self.history_size_combo.currentData() or 50)
        self.history_page = 1
        self._load_history()

    def _on_artifact_page_size_change(self, *_args) -> None:
        self.artifact_size = int(self.artifact_size_combo.currentData() or 50)
        self.artifact_page = 1
        self._load_artifacts()

