from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pandas as pd
from PySide6.QtWidgets import (
    QFrame,
    QHeaderView,
    QLabel,
    QMessageBox,
    QSizePolicy,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)



def open_path(path: str) -> None:
    raw = str(path or "").strip()
    if not raw:
        return

    p = Path(raw)
    if not p.exists():
        raise FileNotFoundError(raw)

    if os.name == "nt":
        os.startfile(str(p))  # type: ignore[attr-defined]
        return

    if sys.platform == "darwin":
        subprocess.Popen(["open", str(p)])
    else:
        subprocess.Popen(["xdg-open", str(p)])



def safe_text(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return str(value)



def show_error(parent: QWidget, message: str) -> None:
    QMessageBox.critical(parent, "错误", str(message))



def show_info(parent: QWidget, message: str) -> None:
    QMessageBox.information(parent, "提示", str(message))



def show_warn(parent: QWidget, message: str) -> None:
    QMessageBox.warning(parent, "提示", str(message))



def set_dataframe(table: QTableWidget, df: pd.DataFrame, limit: int = 300) -> None:
    table.clear()
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        table.setRowCount(0)
        table.setColumnCount(0)
        return

    view = df.head(max(int(limit), 1)).copy()
    view.columns = [str(c) for c in view.columns]

    table.setRowCount(len(view))
    table.setColumnCount(len(view.columns))
    table.setHorizontalHeaderLabels([str(c) for c in view.columns])

    for row_idx, row in enumerate(view.itertuples(index=False, name=None)):
        for col_idx, value in enumerate(row):
            table.setItem(row_idx, col_idx, QTableWidgetItem(safe_text(value)))

    header = table.horizontalHeader()
    header.setSectionResizeMode(QHeaderView.ResizeToContents)
    header.setStretchLastSection(True)



def set_records_table(table: QTableWidget, columns: list[str], rows: list[dict[str, Any]]) -> None:
    table.clear()
    table.setRowCount(len(rows))
    table.setColumnCount(len(columns))
    table.setHorizontalHeaderLabels(columns)

    for row_idx, record in enumerate(rows):
        for col_idx, col in enumerate(columns):
            table.setItem(row_idx, col_idx, QTableWidgetItem(safe_text(record.get(col))))

    header = table.horizontalHeader()
    header.setSectionResizeMode(QHeaderView.ResizeToContents)
    header.setStretchLastSection(True)



def init_table(table: QTableWidget) -> None:
    from PySide6.QtWidgets import QAbstractItemView

    table.setAlternatingRowColors(True)
    table.setSelectionBehavior(QAbstractItemView.SelectRows)
    table.setSelectionMode(QAbstractItemView.SingleSelection)
    table.setEditTriggers(QAbstractItemView.NoEditTriggers)



def read_file_bytes(path: str) -> tuple[bytes, str]:
    p = Path(str(path or "").strip())
    if not p.exists() or not p.is_file():
        raise ValueError("文件不存在")
    return p.read_bytes(), p.name


class StatCard(QFrame):
    def __init__(self, title: str, value: str = "0", accent: str = "primary") -> None:
        super().__init__()
        self.setObjectName(f"stat-card-{accent}")
        self.setFrameShape(QFrame.StyledPanel)
        self.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.setMinimumHeight(74)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(12, 10, 12, 10)
        layout.setSpacing(4)

        self.title_label = QLabel(str(title))
        self.title_label.setObjectName("stat-title")
        self.value_label = QLabel(str(value))
        self.value_label.setObjectName("stat-value")

        layout.addWidget(self.title_label)
        layout.addWidget(self.value_label)

    def set_value(self, value: Any) -> None:
        self.value_label.setText(str(value))
