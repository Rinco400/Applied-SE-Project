from __future__ import annotations

import shutil
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

STUDENT_ID = "23071082"

SOURCE_DB = Path(f"{STUDENT_ID}-seeding.db")
OUTPUT_DB = Path(f"{STUDENT_ID}-sq26-classification.db")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    columns = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return any(row[1] == column for row in columns)


def add_column_if_missing(
    conn: sqlite3.Connection,
    table: str,
    column: str,
    definition: str,
) -> None:
    if not column_exists(conn, table, column):
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


def main() -> None:
    if not SOURCE_DB.exists():
        raise FileNotFoundError(
            f"Source database not found: {SOURCE_DB.resolve()}"
        )

    if OUTPUT_DB.exists():
        OUTPUT_DB.unlink()

    shutil.copy2(SOURCE_DB, OUTPUT_DB)

    conn = sqlite3.connect(OUTPUT_DB)

    try:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }

        required = {"PROJECTS", "FILES"}
        missing = required - tables
        if missing:
            raise RuntimeError(
                f"Source DB misses required tables: {', '.join(sorted(missing))}"
            )

        # Required Part 2 project-level fields.
        add_column_if_missing(
            conn,
            "PROJECTS",
            "type",
            "TEXT CHECK(type IN "
            "('QDA_PROJECT', 'QD_PROJECT', 'OTHER_PROJECT', 'NOT_A_PROJECT'))",
        )
        add_column_if_missing(conn, "PROJECTS", "primary_class", "TEXT")
        add_column_if_missing(conn, "PROJECTS", "secondary_class", "TEXT")
        add_column_if_missing(conn, "PROJECTS", "no_project_files", "INTEGER")
        add_column_if_missing(conn, "PROJECTS", "classification_evidence", "TEXT")
        add_column_if_missing(conn, "PROJECTS", "classification_method", "TEXT")
        add_column_if_missing(conn, "PROJECTS", "is_canonical", "INTEGER DEFAULT 1")

        # File-level classification for primary data files.
        add_column_if_missing(conn, "FILES", "is_primary_data", "INTEGER DEFAULT 0")
        add_column_if_missing(conn, "FILES", "primary_class", "TEXT")
        add_column_if_missing(conn, "FILES", "secondary_class", "TEXT")
        add_column_if_missing(conn, "FILES", "classification_evidence", "TEXT")
        add_column_if_missing(conn, "FILES", "classification_method", "TEXT")

        # Keeps deduplication transparent instead of deleting evidence.
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS PROJECT_DEDUPLICATION (
                source_project_id INTEGER PRIMARY KEY,
                canonical_project_id INTEGER NOT NULL,
                duplicate_reason TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )

        # Stores project tags for later search and analysis.
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS PROJECT_TAGS (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                project_id INTEGER NOT NULL,
                tag TEXT NOT NULL,
                source TEXT NOT NULL
            )
            """
        )

        # Stores detailed classification evidence.
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS CLASSIFICATION_RUNS (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                project_id INTEGER NOT NULL,
                file_id INTEGER,
                project_type TEXT,
                primary_class TEXT,
                secondary_class TEXT,
                evidence TEXT,
                method TEXT NOT NULL,
                classified_at TEXT NOT NULL
            )
            """
        )

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_files_project_id ON FILES(project_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_projects_repository_id "
            "ON PROJECTS(repository_id)"
        )

        project_count = conn.execute(
            "SELECT COUNT(*) FROM PROJECTS"
        ).fetchone()[0]

        file_count = conn.execute(
            "SELECT COUNT(*) FROM FILES"
        ).fetchone()[0]

        conn.commit()

        print(f"[OK] Created classification database: {OUTPUT_DB}")
        print(f"[INFO] Projects available: {project_count}")
        print(f"[INFO] Files available: {file_count}")
        print("[INFO] Part 1 source DB was not modified.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()