from __future__ import annotations

import sys

from PySide6.QtWidgets import QApplication

from app.db.session import init_db
from app.ui.main_window import MainWindow


def main() -> int:
    init_db()
    app = QApplication(sys.argv)
    win = MainWindow()
    win.show()
    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())
