from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Callable

from PySide6.QtWidgets import (
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
from app.services.cleaning_service import process_cleaning
from app.ui.helpers import StatCard, init_table, open_path, read_file_bytes, set_dataframe, show_error, show_info, show_warn
from app.utils.excel_utils import df_to_excel_bytes, read_table


class CleanWidget(QWidget):
    def __init__(
        self,
        session_factory,
        get_current_user: Callable[[], Any],
        on_clean_done: Callable[[dict[str, Any]], None],
    ) -> None:
        super().__init__()
        self._session_factory = session_factory
        self._get_current_user = get_current_user
        self._on_clean_done = on_clean_done
        self.result: dict[str, Any] = {}
        self._build_ui()

    @contextmanager
    def _db(self):
        db = self._session_factory()
        try:
            yield db
        finally:
            db.close()

    @staticmethod
    def _ratio_text(part: int, total: int) -> str:
        if total <= 0:
            return "占比 0.0%"
        return f"占比 {part * 100.0 / total:.1f}%"

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)

        form_box = QGroupBox("步骤一：上传并清洗")
        form = QGridLayout(form_box)
        self.file_edit = QLineEdit()
        btn_file = QPushButton("选择文件")
        btn_file.clicked.connect(self._select_file)
        self.preview_rows = QSpinBox()
        self.preview_rows.setRange(20, 1000)
        self.preview_rows.setValue(200)
        btn_run = QPushButton("执行清洗")
        btn_run.clicked.connect(self._run_clean)

        form.addWidget(QLabel("源表"), 0, 0)
        form.addWidget(self.file_edit, 0, 1)
        form.addWidget(btn_file, 0, 2)
        form.addWidget(QLabel("预览行数"), 1, 0)
        form.addWidget(self.preview_rows, 1, 1)
        form.addWidget(btn_run, 1, 2)
        layout.addWidget(form_box)

        self.source_table = QTableWidget()
        init_table(self.source_table)
        src_box = QGroupBox("上传预览")
        src_layout = QVBoxLayout(src_box)
        src_layout.addWidget(self.source_table)
        layout.addWidget(src_box)

        result_box = QGroupBox("清洗结果")
        result_layout = QVBoxLayout(result_box)
        card_row = QHBoxLayout()
        self.card_total = StatCard("总行", "0", accent="primary", meta="上传行数")
        self.card_normal = StatCard("正常", "0", accent="success", meta="占比 0.0%")
        self.card_abnormal = StatCard("异常", "0", accent="warning", meta="占比 0.0%")
        self.card_over = StatCard("超12其余正常", "0", accent="info", meta="占比 0.0%")
        card_row.addWidget(self.card_total)
        card_row.addWidget(self.card_normal)
        card_row.addWidget(self.card_abnormal)
        card_row.addWidget(self.card_over)
        result_layout.addLayout(card_row)

        btn_row = QHBoxLayout()
        self.btn_normal = QPushButton("下载正常表")
        self.btn_abnormal = QPushButton("下载异常表")
        self.btn_over = QPushButton("下载超12单独表")
        self.btn_normal.clicked.connect(lambda: self._open_result("normal_file_url"))
        self.btn_abnormal.clicked.connect(lambda: self._open_result("abnormal_file_url"))
        self.btn_over.clicked.connect(lambda: self._open_result("over_limit_file_url"))
        btn_row.addWidget(self.btn_normal)
        btn_row.addWidget(self.btn_abnormal)
        btn_row.addWidget(self.btn_over)
        btn_row.addStretch(1)
        result_layout.addLayout(btn_row)

        self.tabs = QTabWidget()
        self.normal_table = QTableWidget()
        self.abnormal_table = QTableWidget()
        self.over_table = QTableWidget()
        init_table(self.normal_table)
        init_table(self.abnormal_table)
        init_table(self.over_table)
        self.tabs.addTab(self.normal_table, "正常预览")
        self.tabs.addTab(self.abnormal_table, "异常预览")
        self.tabs.addTab(self.over_table, "超12其余正常预览")
        result_layout.addWidget(self.tabs)
        layout.addWidget(result_box)

    def _select_file(self) -> None:
        path, _ = QFileDialog.getOpenFileName(self, "选择文件", "", "表格文件 (*.xlsx *.xls *.csv)")
        if not path:
            return
        self.file_edit.setText(path)
        try:
            file_bytes, filename = read_file_bytes(path)
            df = read_table(file_bytes, filename)
            set_dataframe(self.source_table, df)
        except Exception as exc:
            show_error(self, f"预览失败: {exc}")

    def _run_clean(self) -> None:
        user = self._get_current_user()
        if user is None:
            show_warn(self, "本地用户未就绪，请重启程序")
            return

        path = self.file_edit.text().strip()
        if not path:
            show_warn(self, "请先选择文件")
            return

        try:
            file_bytes, filename = read_file_bytes(path)
            res = process_cleaning(file_bytes, filename)

            df_raw = res["df_raw"]
            df_normal = res["df_normal"]
            df_abnormal = res["df_abnormal"]
            df_over_limit = res["df_over_limit"]
            shot_col = res["shot_col"]

            hyperlink_cols_n = [shot_col] if shot_col and shot_col in df_normal.columns else None
            hyperlink_cols_ab = [shot_col] if shot_col and shot_col in df_abnormal.columns else None
            hyperlink_cols_ol = [shot_col] if shot_col and shot_col in df_over_limit.columns else None

            with self._db() as db:
                b_normal = df_to_excel_bytes(df_normal, sheet_name="正常", hyperlink_cols=hyperlink_cols_n)
                b_abnormal = df_to_excel_bytes(df_abnormal, sheet_name="异常", hyperlink_cols=hyperlink_cols_ab)
                url_normal = save_artifact(
                    b_normal,
                    "clean_normal",
                    db=db,
                    stage="步骤一清洗",
                    action="执行清洗",
                    operator=user.username,
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
                    operator=user.username,
                    source_file=filename,
                    input_rows=len(df_raw),
                    output_rows=len(df_abnormal),
                    payload={"kind": "abnormal"},
                )
                url_over = None
                if not df_over_limit.empty:
                    b_over = df_to_excel_bytes(df_over_limit, sheet_name="金额超12_其余正常", hyperlink_cols=hyperlink_cols_ol)
                    url_over = save_artifact(
                        b_over,
                        "clean_over_12_followup",
                        db=db,
                        stage="步骤一清洗",
                        action="执行清洗",
                        operator=user.username,
                        source_file=filename,
                        input_rows=len(df_raw),
                        output_rows=len(df_over_limit),
                        payload={"kind": "over_limit_only"},
                    )

                db.add(
                    OperationHistory(
                        stage="步骤一清洗",
                        action="执行清洗",
                        operator=user.username,
                        input_rows=len(df_raw),
                        output_rows=len(df_normal) + len(df_abnormal) + len(df_over_limit),
                        detail={
                            "source_file": filename,
                            "normal_rows": len(df_normal),
                            "abnormal_rows": len(df_abnormal),
                            "over_limit_rows": len(df_over_limit),
                            "artifacts": [u for u in [url_normal, url_abnormal, url_over] if u],
                            "report": res["report"],
                        },
                    )
                )
                db.commit()

            self.result = {
                "normal_file_url": url_normal,
                "abnormal_file_url": url_abnormal,
                "over_limit_file_url": url_over,
                "df_normal": df_normal,
                "df_abnormal": df_abnormal,
                "df_over_limit": df_over_limit,
                "total_rows": len(df_raw),
                "normal_rows": len(df_normal),
                "abnormal_rows": len(df_abnormal),
                "over_limit_rows": len(df_over_limit),
            }

            total_rows = len(df_raw)
            normal_rows = len(df_normal)
            abnormal_rows = len(df_abnormal)
            over_rows = len(df_over_limit)

            self.card_total.set_value(total_rows)
            self.card_total.set_meta("上传行数")
            self.card_normal.set_value(normal_rows)
            self.card_normal.set_meta(self._ratio_text(normal_rows, total_rows))
            self.card_abnormal.set_value(abnormal_rows)
            self.card_abnormal.set_meta(self._ratio_text(abnormal_rows, total_rows))
            self.card_over.set_value(over_rows)
            self.card_over.set_meta(self._ratio_text(over_rows, total_rows))
            set_dataframe(self.normal_table, df_normal)
            set_dataframe(self.abnormal_table, df_abnormal)
            set_dataframe(self.over_table, df_over_limit)
            self._on_clean_done(self.result)
            show_info(self, "步骤一完成")
        except Exception as exc:
            show_error(self, f"清洗失败: {exc}")

    def _open_result(self, key: str) -> None:
        path = str(self.result.get(key) or "").strip()
        if not path:
            show_warn(self, "暂无可打开文件")
            return
        try:
            open_path(path)
        except Exception as exc:
            show_error(self, f"无法打开文件: {exc}")

