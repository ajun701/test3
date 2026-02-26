from __future__ import annotations

from pathlib import Path

from PySide6.QtWidgets import (
    QHBoxLayout,
    QLabel,
    QMainWindow,
    QPushButton,
    QStackedWidget,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)

from app.core.config import settings
from app.db.session import SessionLocal
from app.services.task_runner import AITaskRunner
from app.ui.ai_widget import AIWidget
from app.ui.auth_widget import AuthWidget
from app.ui.clean_widget import CleanWidget
from app.ui.helpers import open_path, show_error, show_info
from app.ui.history_widget import HistoryWidget
from app.ui.match_widget import MatchWidget


class MainWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle(settings.project_name)
        self.resize(1500, 980)

        self.current_user = None
        self.clean_result: dict = {}
        self.match_result: dict = {}

        self.task_runner = AITaskRunner(SessionLocal)

        self._build_ui()
        self._apply_theme()

    def _build_ui(self) -> None:
        root = QWidget()
        self.setCentralWidget(root)
        layout = QVBoxLayout(root)

        self.stack = QStackedWidget()
        layout.addWidget(self.stack)

        self.auth_widget = AuthWidget(SessionLocal, self._on_auth_success)
        self.stack.addWidget(self.auth_widget)

        self.main_widget = QWidget()
        self.stack.addWidget(self.main_widget)
        self.stack.setCurrentWidget(self.auth_widget)

        main_layout = QVBoxLayout(self.main_widget)
        top = QHBoxLayout()
        self.user_label = QLabel("当前用户：-")
        top.addWidget(self.user_label)
        top.addStretch(1)

        btn_data_dir = QPushButton("打开数据目录")
        btn_data_dir.clicked.connect(self._open_data_dir)
        btn_logout = QPushButton("退出登录")
        btn_logout.clicked.connect(self._logout)
        top.addWidget(btn_data_dir)
        top.addWidget(btn_logout)
        main_layout.addLayout(top)

        self.tabs = QTabWidget()
        main_layout.addWidget(self.tabs)

        self.clean_widget = CleanWidget(SessionLocal, self._get_current_user, self._on_clean_done)
        self.match_widget = MatchWidget(
            SessionLocal,
            self._get_current_user,
            self._get_clean_result,
            self._on_match_done,
        )
        self.ai_widget = AIWidget(
            SessionLocal,
            self._get_current_user,
            self._get_match_result,
            self.task_runner,
        )
        self.history_widget = HistoryWidget(SessionLocal, self._get_current_user)

        self.tabs.addTab(self.clean_widget, "1. 清洗")
        self.tabs.addTab(self.match_widget, "2. 入库匹配")
        self.tabs.addTab(self.ai_widget, "3. AI复核")
        self.tabs.addTab(self.history_widget, "4. 历史记录")

    def _apply_theme(self) -> None:
        self.setStyleSheet(
            """
            QMainWindow {
                background: #edf4ff;
            }
            QWidget {
                font-family: "Microsoft YaHei UI";
                font-size: 13px;
            }
            QGroupBox {
                border: 1px solid #d6e3f7;
                border-radius: 12px;
                margin-top: 10px;
                background: #ffffff;
                font-weight: 600;
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                left: 12px;
                top: 0px;
                padding: 0 6px;
                color: #2a4f88;
            }
            QPushButton {
                border: 1px solid #a9c4ec;
                border-radius: 8px;
                background: #f7fbff;
                color: #1d477f;
                padding: 6px 12px;
                min-height: 18px;
            }
            QPushButton:hover {
                background: #eaf3ff;
            }
            QPushButton:pressed {
                background: #deecff;
            }
            QTabWidget::pane {
                border: 1px solid #d6e3f7;
                border-radius: 10px;
                background: #ffffff;
                top: -1px;
            }
            QTabBar::tab {
                border: 1px solid #d6e3f7;
                border-bottom-color: #d6e3f7;
                border-top-left-radius: 8px;
                border-top-right-radius: 8px;
                padding: 6px 12px;
                background: #f2f7ff;
                color: #47658f;
                margin-right: 2px;
            }
            QTabBar::tab:selected {
                background: #ffffff;
                color: #1d477f;
                border-bottom-color: #ffffff;
            }
            QLineEdit, QDateTimeEdit, QComboBox, QSpinBox, QDoubleSpinBox, QTextEdit {
                border: 1px solid #c5d8f3;
                border-radius: 8px;
                background: #fbfdff;
                padding: 4px 6px;
            }
            QTableWidget {
                border: 1px solid #d7e4f8;
                background: #ffffff;
                gridline-color: #e6eefb;
                alternate-background-color: #f8fbff;
            }
            QHeaderView::section {
                background: #f0f6ff;
                color: #365d94;
                border: 1px solid #d6e3f7;
                padding: 4px;
                font-weight: 600;
            }
            QProgressBar {
                border: 1px solid #b7d0ef;
                border-radius: 8px;
                background: #f3f8ff;
                text-align: center;
                color: #1d477f;
                min-height: 20px;
            }
            QProgressBar::chunk {
                border-radius: 7px;
                background: qlineargradient(x1:0, y1:0, x2:1, y2:0, stop:0 #3b8cff, stop:1 #3dc2ff);
            }
            QFrame#stat-card-primary, QFrame#stat-card-success, QFrame#stat-card-warning, QFrame#stat-card-info {
                border-radius: 10px;
                border: 1px solid #d6e3f7;
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #f9fcff, stop:1 #f3f8ff);
            }
            QFrame#stat-card-primary {
                border-left: 4px solid #287dff;
            }
            QFrame#stat-card-success {
                border-left: 4px solid #2fa760;
            }
            QFrame#stat-card-warning {
                border-left: 4px solid #f08a2b;
            }
            QFrame#stat-card-info {
                border-left: 4px solid #30a5c9;
            }
            QLabel#stat-title {
                color: #5d789d;
                font-size: 12px;
                font-weight: 500;
            }
            QLabel#stat-value {
                color: #173d75;
                font-size: 22px;
                font-weight: 700;
            }
            QLabel#stat-meta {
                color: #6b87ae;
                font-size: 11px;
                font-weight: 500;
            }
            QLabel#statusBadge {
                border-radius: 10px;
                border: 1px solid #c9d8ef;
                padding: 2px 10px;
                font-weight: 600;
                color: #365d94;
                background: #f2f7ff;
            }
            QLabel#statusBadge[state="running"] {
                border-color: #59a3ff;
                color: #0f5dc0;
                background: #eaf3ff;
            }
            QLabel#statusBadge[state="pending"] {
                border-color: #88a5c6;
                color: #486587;
                background: #f1f6fb;
            }
            QLabel#statusBadge[state="paused"] {
                border-color: #f0c26d;
                color: #9d6209;
                background: #fff6e8;
            }
            QLabel#statusBadge[state="completed"] {
                border-color: #7fdaa8;
                color: #1f8a4f;
                background: #ebfaf1;
            }
            QLabel#statusBadge[state="error"] {
                border-color: #f09da1;
                color: #b13a42;
                background: #fff0f1;
            }
            QTextEdit#runtimeLog {
                font-family: "Consolas", "Courier New", monospace;
                font-size: 12px;
                background: #f7fbff;
                border: 1px solid #c5d8f3;
                border-radius: 8px;
            }
            QLabel#filterSummary {
                border: 1px solid #cfe0f8;
                border-radius: 8px;
                background: #f3f8ff;
                color: #2f588f;
                padding: 5px 8px;
                font-size: 12px;
                font-weight: 600;
            }
            QLabel#subtleHint {
                color: #6a86ac;
                font-size: 12px;
            }
            QLabel#errorInline {
                color: #b13a42;
                background: #fff3f4;
                border: 1px solid #f0c7cb;
                border-radius: 8px;
                padding: 4px 8px;
            }
            """
        )

    def _get_current_user(self):
        return self.current_user

    def _get_clean_result(self):
        return self.clean_result

    def _get_match_result(self):
        return self.match_result

    def _on_auth_success(self, user) -> None:
        self.current_user = user
        self.user_label.setText(f"当前用户：{user.username}")
        self.stack.setCurrentWidget(self.main_widget)
        self.history_widget.query_all()
        self._restore_latest_task()

    def _on_clean_done(self, result: dict) -> None:
        self.clean_result = result
        if self.match_widget.use_step1.isChecked():
            self.match_widget.sync_source_preview()
        self.history_widget.refresh_all()

    def _on_match_done(self, result: dict) -> None:
        self.match_result = result
        if self.ai_widget.use_step2.isChecked():
            self.ai_widget.sync_source_preview()
        self.history_widget.refresh_all()

    def _restore_latest_task(self) -> None:
        if self.current_user is None:
            return
        try:
            db = SessionLocal()
            try:
                latest = self.task_runner.get_latest_task(db, self.current_user, active_only=True)
            finally:
                db.close()
            if latest is None:
                return
            self.ai_widget.restore_latest_task(latest.task_id)
            if latest.status in ("running", "pending"):
                self.ai_widget.timer.start()
            self.tabs.setCurrentIndex(2)
        except Exception:
            pass

    def _open_data_dir(self) -> None:
        try:
            open_path(str(Path(settings.data_dir)))
        except Exception as exc:
            show_error(self, f"无法打开数据目录: {exc}")

    def _logout(self) -> None:
        self.current_user = None
        self.clean_result = {}
        self.match_result = {}
        self.ai_widget.restore_latest_task("")
        self.ai_widget.stop_polling()
        self.user_label.setText("当前用户：-")
        self.stack.setCurrentWidget(self.auth_widget)
        show_info(self, "已退出登录")
