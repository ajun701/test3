import logging

from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine

from app.core.security import hash_password
from app.db.session import engine
from app.models import Base

logger = logging.getLogger(__name__)


def _get_table_columns(db_engine: Engine, table_name: str) -> set[str]:
    inspector = inspect(db_engine)
    if table_name not in inspector.get_table_names():
        return set()
    return {str(col["name"]) for col in inspector.get_columns(table_name)}


def _build_alter_statements(dialect: str, columns: set[str]) -> list[str]:
    statements: list[str] = []

    if "hashed_password" not in columns:
        statements.append("ALTER TABLE users ADD COLUMN hashed_password VARCHAR(255) NOT NULL DEFAULT ''")

    if "is_active" not in columns:
        if dialect in {"mysql", "mariadb"}:
            statements.append("ALTER TABLE users ADD COLUMN is_active TINYINT(1) NOT NULL DEFAULT 1")
        else:
            statements.append("ALTER TABLE users ADD COLUMN is_active BOOLEAN NOT NULL DEFAULT 1")

    if "created_at" not in columns:
        if dialect == "postgresql":
            statements.append("ALTER TABLE users ADD COLUMN created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP")
        else:
            statements.append("ALTER TABLE users ADD COLUMN created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP")

    return statements


def _migrate_legacy_users_table(db_engine: Engine) -> None:
    columns = _get_table_columns(db_engine, "users")
    if not columns:
        return

    dialect = db_engine.dialect.name
    alter_statements = _build_alter_statements(dialect, columns)
    if not alter_statements:
        return

    logger.warning("Detected legacy users table schema, applying compatibility migration.")
    with db_engine.begin() as conn:
        for statement in alter_statements:
            try:
                conn.execute(text(statement))
            except Exception:
                logger.exception("Failed executing users table migration SQL: %s", statement)

        updated_columns = _get_table_columns(db_engine, "users")
        if "password" not in updated_columns or "hashed_password" not in updated_columns:
            return

        rows = conn.execute(
            text(
                "SELECT id, password FROM users "
                "WHERE (hashed_password IS NULL OR hashed_password = '') AND password IS NOT NULL"
            )
        ).mappings().all()

        migrated_count = 0
        for row in rows:
            raw_password = str(row.get("password") or "").strip()
            if not raw_password:
                continue

            try:
                hashed_password = raw_password if raw_password.startswith("$2") else hash_password(raw_password)
            except Exception:
                logger.exception("Failed to hash legacy password for user id=%s", row.get("id"))
                continue

            conn.execute(
                text("UPDATE users SET hashed_password = :hashed_password WHERE id = :user_id"),
                {"hashed_password": hashed_password, "user_id": row["id"]},
            )
            migrated_count += 1

        if migrated_count:
            logger.warning("Migrated %s legacy password record(s) to hashed_password.", migrated_count)


def init_database() -> None:
    Base.metadata.create_all(bind=engine)
    _migrate_legacy_users_table(engine)
