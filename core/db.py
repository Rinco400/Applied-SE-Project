import sqlite3
from pathlib import Path
from typing import Any, Dict

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS acquisitions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  qda_url TEXT NOT NULL,
  downloaded_at TEXT NOT NULL,
  local_dir TEXT NOT NULL,
  qda_filename TEXT NOT NULL,
  repository TEXT,
  license TEXT,
  uploader_name TEXT,
  uploader_email TEXT
);
"""

def init_db(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(CREATE_TABLE_SQL)
        conn.commit()
    finally:
        conn.close()

def insert_acquisition(db_path: Path, row: Dict[str, Any]) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            INSERT INTO acquisitions
            (qda_url, downloaded_at, local_dir, qda_filename, repository, license, uploader_name, uploader_email)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row["qda_url"],
                row["downloaded_at"],
                row["local_dir"],
                row["qda_filename"],
                row.get("repository"),
                row.get("license"),
                row.get("uploader_name"),
                row.get("uploader_email"),
            ),
        )
        conn.commit()
    finally:
        conn.close()