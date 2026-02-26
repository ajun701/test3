from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Callable

import pandas as pd
from PySide6.QtWidgets import (
    QCheckBox,
    QFileDialog,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QSpinBox,
    QTabWidget,
    QTableWidget,
    QVBoxLayout,
    QWidget,
)

from app.db.models import OperationHistory
from app.services.artifact_service import save_artifact
from app.services.matching_service import process_matching
from app.ui.helpers import StatCard, init_table, open_path, read_file_bytes, set_dataframe, show_error, show_info, show_warn
from app.utils.excel_utils import df_to_excel_bytes, read_table


class MatchWidget(QWidget):
    def __init__(
        self,
        session_factory,
        get_current_user: Callable[[], Any],
        get_clean_result: Callable[[], dict[str, Any]],
        on_match_done: Callable[[dict[str, Any]], None],
    ) -> None:
        super().__init__()
        self._session_factory = session_factory
        self._get_current_user = get_current_user
        self._get_clean_result = get_clean_result
        self._on_match_done = on_match_done
        self.result: dict[str, Any] = {}
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

        form_box = QGroupBox("步骤二：入库单号匹配")
        form = QGridLayout(form_box)

        self.use_step1 = QCheckBox("使用步骤一正常表")
        self.use_step1.stateChanged.connect(self.sync_source_preview)
        self.source_edit = QLineEdit()
        self.source_btn = QPushButton("选择源表")
        self.source_btn.clicked.connect(self._select_source)
        self.inbound_edit = QLineEdit()
        self.inbound_btn = QPushButton("选择入库表")
        self.inbound_btn.clicked.connect(self._select_inbound)
        self.preview_rows = QSpinBox()
        self.preview_rows.setRange(20, 1000)
        self.preview_rows.setValue(200)
        btn_run = QPushButton("执行匹配")
        btn_run.clicked.connect(self._run_match)

        form.addWidget(self.use_step1, 0, 0, 1, 3)
        form.addWidget(QLabel("源表"), 1, 0)
        form.addWidget(self.source_edit, 1, 1)
        form.addWidget(self.source_btn, 1, 2)
        form.addWidget(QLabel("已入库表"), 2, 0)
        form.addWidget(self.inbound_edit, 2, 1)
        form.addWidget(self.inbound_btn, 2, 2)
        form.addWidget(QLabel("预览行数"), 3, 0)
        form.addWidget(self.preview_rows, 3, 1)
        form.addWidget(btn_run, 3, 2)
        layout.addWidget(form_box)

        prev_tabs = QTabWidget()
        self.source_table = QTableWidget()
        self.inbound_table = QTableWidget()
        init_table(self.source_table)
        init_table(self.inbound_table)
        prev_tabs.addTab(self.source_table, "源表预览")
        prev_tabs.addTab(self.inbound_table, "入库表预览")
        layout.addWidget(prev_tabs)

        result_box = QGroupBox("匹配结果")
        result_layout = QVBoxLayout(result_box)
        card_row = QHBoxLayout()
        self.card_total = StatCard("总行", "0", accent="primary")
        self.card_inbound = StatCard("已入库", "0", accent="success")
        self.card_pending = StatCard("未入库", "0", accent="warning")
        card_row.addWidget(self.card_total)
        card_row.addWidget(self.card_inbound)
        card_row.addWidget(self.card_pending)
        result_layout.addLayout(card_row)

        btn_row = QHBoxLayout()
        self.btn_inbound = QPushButton("打开已入库结果")
        self.btn_pending = QPushButton("打开未入库结果")
        self.btn_inbound.clicked.connect(lambda: self._open_result("inbound_file_url"))
        self.btn_pending.clicked.connect(lambda: self._open_result("pending_file_url"))
        btn_row.addWidget(self.btn_inbound)
        btn_row.addWidget(self.btn_pending)
        btn_row.addStretch(1)
        result_layout.addLayout(btn_row)

        self.res_tabs = QTabWidget()
        self.inbound_res_table = QTableWidget()
        self.pending_res_table = QTableWidget()
        init_table(self.inbound_res_table)
        init_table(self.pending_res_table)
        self.res_tabs.addTab(self.inbound_res_table, "已入库")
        self.res_tabs.addTab(self.pending_res_table, "未入库")
        result_layout.addWidget(self.res_tabs)

        layout.addWidget(result_box)

    def _select_source(self) -> None:
        path, _ = QFileDialog.getOpenFileName(self, "选择源表", "", "表格文件 (*.xlsx *.xls *.csv)")
        if not path:
            return
        self.source_edit.setText(path)
        self.sync_source_preview()

    def _select_inbound(self) -> None:
        path, _ = QFileDialog.getOpenFileName(self, "选择入库表", "", "表格文件 (*.xlsx *.xls *.csv)")
        if not path:
            return
        self.inbound_edit.setText(path)
        try:
            file_bytes, filename = read_file_bytes(path)
            set_dataframe(self.inbound_table, read_table(file_bytes, filename))
        except Exception as exc:
            show_error(self, f"预览失败: {exc}")

    def sync_source_preview(self) -> None:
        use_step1 = self.use_step1.isChecked()
        self.source_edit.setEnabled(not use_step1)
        self.source_btn.setEnabled(not use_step1)

        if use_step1:
            clean_res = self._get_clean_result()
            src = str(clean_res.get("normal_file_url") or "").strip()
            if not src:
                show_warn(self, "未检测到步骤一正常结果，请关闭开关后手动上传源表")
                set_dataframe(self.source_table, pd.DataFrame())
                return
            try:
                file_bytes, filename = read_file_bytes(src)
                set_dataframe(self.source_table, read_table(file_bytes, filename))
            except Exception as exc:
                show_error(self, f"预览失败: {exc}")
            return

        path = self.source_edit.text().strip()
        if not path:
            return
        try:
            file_bytes, filename = read_file_bytes(path)
            set_dataframe(self.source_table, read_table(file_bytes, filename))
        except Exception as exc:
            show_error(self, f"预览失败: {exc}")

    def _run_match(self) -> None:
        user = self._get_current_user()
        if user is None:
            show_warn(self, "请先登录")
            return

        inbound_path = self.inbound_edit.text().strip()
        if not inbound_path:
            show_warn(self, "请上传已入库表")
            return

        try:
            if self.use_step1.isChecked():
                clean_res = self._get_clean_result()
                source_path = str(clean_res.get("normal_file_url") or "").strip()
                if not source_path:
                    raise ValueError("请先完成步骤一")
            else:
                source_path = self.source_edit.text().strip()
                if not source_path:
                    raise ValueError("请上传待匹配源表")

            source_bytes, source_filename = read_file_bytes(source_path)
            inbound_bytes, inbound_filename = read_file_bytes(inbound_path)

            res = process_matching(source_bytes, source_filename, inbound_bytes, inbound_filename)
            df_source = res["df_source"]
            df_inbound = res["df_inbound"]
            df_pending = res["df_pending"]
            shot_col = res["shot_col"]

            hyperlink_cols_inb = [shot_col] if shot_col and shot_col in df_inbound.columns else None
            hyperlink_cols_pen = [shot_col] if shot_col and shot_col in df_pending.columns else None

            with self._db() as db:
                b_inbound = df_to_excel_bytes(df_inbound, sheet_name="已入库", hyperlink_cols=hyperlink_cols_inb)
                b_pending = df_to_excel_bytes(df_pending, sheet_name="未入库", hyperlink_cols=hyperlink_cols_pen)
                url_inbound = save_artifact(
                    b_inbound,
                    "matched_inbound_for_ai",
                    db=db,
                    stage="步骤二入库匹配",
                    action="执行匹配",
                    operator=user.username,
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
                    operator=user.username,
                    source_file=source_filename,
                    input_rows=len(df_source),
                    output_rows=len(df_pending),
                    payload={"kind": "pending", "inbound_file": inbound_filename},
                )
                db.add(
                    OperationHistory(
                        stage="步骤二入库匹配",
                        action="执行匹配",
                        operator=user.username,
                        input_rows=len(df_source),
                        output_rows=len(df_inbound) + len(df_pending),
                        detail={
                            "inbound_rows": len(df_inbound),
                            "pending_rows": len(df_pending),
                            "artifacts": [url_inbound, url_pending],
                            "report": res["report"],
                        },
                    )
                )
                db.commit()

            self.result = {
                "inbound_file_url": url_inbound,
                "pending_file_url": url_pending,
                "df_inbound": df_inbound,
                "df_pending": df_pending,
                "inbound_rows": len(df_inbound),
                "pending_rows": len(df_pending),
                "total_rows": len(df_source),
            }
            self.card_total.set_value(len(df_source))
            self.card_inbound.set_value(len(df_inbound))
            self.card_pending.set_value(len(df_pending))
            set_dataframe(self.inbound_res_table, df_inbound)
            set_dataframe(self.pending_res_table, df_pending)
            self._on_match_done(self.result)
            show_info(self, "步骤二完成")
        except Exception as exc:
            show_error(self, f"匹配失败: {exc}")

    def _open_result(self, key: str) -> None:
        path = str(self.result.get(key) or "").strip()
        if not path:
            show_warn(self, "暂无可打开文件")
            return
        try:
            open_path(path)
        except Exception as exc:
            show_error(self, f"无法打开文件: {exc}")
