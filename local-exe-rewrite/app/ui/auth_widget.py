from __future__ import annotations

from contextlib import contextmanager
from typing import Callable

from PySide6.QtWidgets import (
    QFormLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)

from app.services.auth_service import login_user, register_user
from app.ui.helpers import show_error, show_info, show_warn


class AuthWidget(QWidget):
    def __init__(self, session_factory, on_auth_success: Callable[[object], None]) -> None:
        super().__init__()
        self._session_factory = session_factory
        self._on_auth_success = on_auth_success
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

        title = QLabel("退运费智能审核中台（本地版）")
        title.setStyleSheet("font-size: 24px; font-weight: 600;")
        layout.addWidget(title)

        subtitle = QLabel("单机运行，无需 Redis/FastAPI，仅本地 SQLite 与本地文件")
        subtitle.setStyleSheet("color: #666;")
        layout.addWidget(subtitle)

        tabs = QTabWidget()
        layout.addWidget(tabs)

        login_tab = QWidget()
        login_form = QFormLayout(login_tab)
        self.login_username = QLineEdit()
        self.login_password = QLineEdit()
        self.login_password.setEchoMode(QLineEdit.Password)
        login_form.addRow("用户名", self.login_username)
        login_form.addRow("密码", self.login_password)
        btn_login = QPushButton("登录")
        btn_login.clicked.connect(self._run_login)
        login_form.addRow(btn_login)
        tabs.addTab(login_tab, "登录")

        register_tab = QWidget()
        register_form = QFormLayout(register_tab)
        self.register_username = QLineEdit()
        self.register_password = QLineEdit()
        self.register_password.setEchoMode(QLineEdit.Password)
        self.register_key = QLineEdit()
        self.register_key.setEchoMode(QLineEdit.Password)
        register_form.addRow("用户名", self.register_username)
        register_form.addRow("密码", self.register_password)
        register_form.addRow("注册密钥", self.register_key)
        btn_register = QPushButton("注册并登录")
        btn_register.clicked.connect(self._run_register)
        register_form.addRow(btn_register)
        tabs.addTab(register_tab, "注册")

    def _run_login(self) -> None:
        username = self.login_username.text().strip()
        password = self.login_password.text()
        if not username or not password:
            show_warn(self, "请输入用户名和密码")
            return

        try:
            with self._db() as db:
                user = login_user(db, username, password)
            self._on_auth_success(user)
            show_info(self, "登录成功")
        except Exception as exc:
            show_error(self, str(exc))

    def _run_register(self) -> None:
        username = self.register_username.text().strip()
        password = self.register_password.text()
        register_key = self.register_key.text().strip()
        if not username or not password or not register_key:
            show_warn(self, "请完整填写注册信息")
            return

        try:
            with self._db() as db:
                user = register_user(db, username, password, register_key)
            self._on_auth_success(user)
            show_info(self, "注册成功")
        except Exception as exc:
            show_error(self, str(exc))
