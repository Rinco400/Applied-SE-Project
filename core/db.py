import sqlite3
from pathlib import Path
from typing import Any, Dict

CREATE_ACQUISITIONS_SQL = """
CREATE TABLE IF NOT EXISTS acquisitions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    file_url TEXT NOT NULL,
    downloaded_at TEXT NOT NULL,
    local_dir TEXT NOT NULL,
    local_filename TEXT NOT NULL,
    repository TEXT,
    license TEXT,
    uploader_name TEXT,
    uploader_email TEXT,
    title TEXT,
    persistent_id TEXT,
    query_text TEXT,
    file_type TEXT,
    file_category TEXT,
    UNIQUE(file_url)
);
"""

CREATE_FAILURES_SQL = """
CREATE TABLE IF NOT EXISTS failures (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    repository TEXT NOT NULL,
    query_text TEXT,
    dataset_url TEXT,
    file_url TEXT,
    reason TEXT NOT NULL,
    created_at TEXT NOT NULL
);
"""


def init_db(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(CREATE_ACQUISITIONS_SQL)
        conn.execute(CREATE_FAILURES_SQL)
        conn.commit()
    finally:
        conn.close()


def exists_file_url(db_path: Path, file_url: str) -> bool:
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT 1 FROM acquisitions WHERE file_url = ? LIMIT 1",
            (file_url,),
        ).fetchone()
        return row is not None
    finally:
        conn.close()


def insert_acquisition(db_path: Path, row: Dict[str, Any]) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            INSERT OR IGNORE INTO acquisitions (
                file_url,
                downloaded_at,
                local_dir,
                local_filename,
                repository,
                license,
                uploader_name,
                uploader_email,
                title,
                persistent_id,
                query_text,
                file_type,
                file_category
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row["file_url"],
                row["downloaded_at"],
                row["local_dir"],
                row["local_filename"],
                row.get("repository"),
                row.get("license"),
                row.get("uploader_name"),
                row.get("uploader_email"),
                row.get("title"),
                row.get("persistent_id"),
                row.get("query_text"),
                row.get("file_type"),
                row.get("file_category"),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def insert_failure(db_path: Path, row: Dict[str, Any]) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            INSERT INTO failures (
                repository,
                query_text,
                dataset_url,
                file_url,
                reason,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                row.get("repository"),
                row.get("query_text"),
                row.get("dataset_url"),
                row.get("file_url"),
                row["reason"],
                row["created_at"],
            ),
        )
        conn.commit()
    finally:
        conn.close()