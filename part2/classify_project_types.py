from __future__ import annotations

import json
import re
import sqlite3
import zipfile
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

STUDENT_ID = "23071082"
DB_PATH = Path(f"{STUDENT_ID}-sq26-classification.db")

# QDA software project formats.
QDA_EXTENSIONS = {
    ".qdpx", ".qde", ".qdas", ".qda", ".qdp",
    ".nvp", ".nvpx",
    ".atlproj", ".hpr7", ".hpr6", ".hpr5",
    ".mx", ".mx22", ".mx23", ".mx24", ".mx25",
    ".maxqda", ".maxqdaproject",
}

# Likely primary qualitative data formats.
PRIMARY_DATA_EXTENSIONS = {
    ".txt", ".rtf", ".doc", ".docx", ".odt",
    ".pdf", ".md", ".html", ".htm",
    ".mp3", ".wav", ".m4a", ".ogg", ".flac",
    ".mp4", ".mov", ".avi", ".mkv", ".webm",
    ".jpg", ".jpeg", ".png", ".tif", ".tiff", ".webp",
    ".vtt", ".srt",
}

# Structured or otherwise valid data formats.
VALID_DATA_EXTENSIONS = {
    ".csv", ".tsv", ".tab",
    ".xlsx", ".xls",
    ".sav", ".dta", ".rdata", ".rds",
    ".json", ".xml", ".yaml", ".yml",
    ".parquet", ".feather",
    ".sql", ".geojson", ".shp", ".gpkg",
    ".npy", ".npz",
}

ARCHIVE_EXTENSIONS = {".zip"}

METHOD = "extension_and_zip_rules_v1"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def normalise_text(value: str | None) -> str:
    text = (value or "").strip().lower()
    text = re.sub(r"\s+", " ", text)
    return text


def normalise_url(value: str | None) -> str:
    url = normalise_text(value)
    return url.split("?")[0].rstrip("/")


def get_extension(file_name: str | None, file_type: str | None) -> str:
    candidate = normalise_text(file_type)

    if candidate.startswith("."):
        return candidate

    return Path(file_name or "").suffix.lower()


def extension_category(extension: str) -> str:
    if extension in QDA_EXTENSIONS:
        return "qda"
    if extension in PRIMARY_DATA_EXTENSIONS:
        return "primary"
    if extension in VALID_DATA_EXTENSIONS:
        return "valid"
    return "other"


def safe_local_path(project_folder: str | None, file_name: str | None) -> Path | None:
    if not project_folder or not file_name:
        return None

    # Keep only the final filename to avoid accidental nested/remote paths.
    return Path(project_folder) / Path(file_name).name


def inspect_zip(path: Path) -> tuple[list[tuple[str, str]], str | None]:
    """
    Returns recognised archive members as:
    [(member_name, category), ...]

    ZIP central-directory inspection does not extract file contents.
    """
    recognised: list[tuple[str, str]] = []

    try:
        with zipfile.ZipFile(path) as archive:
            for info in archive.infolist():
                if info.is_dir():
                    continue

                ext = Path(info.filename).suffix.lower()
                category = extension_category(ext)

                if category != "other":
                    recognised.append((info.filename, category))

        return recognised, None

    except (zipfile.BadZipFile, OSError, PermissionError) as exc:
        return [], str(exc)


def build_duplicate_map(conn: sqlite3.Connection) -> int:
    """
    Marks a canonical project without deleting rows.

    Strong duplicate key:
      repository_id + project_url

    Fallback duplicate key:
      repository_id + normalized title
    """
    conn.execute("DELETE FROM PROJECT_DEDUPLICATION")
    conn.execute("UPDATE PROJECTS SET is_canonical = 1")

    rows = conn.execute(
        """
        SELECT id, repository_id, title, project_url
        FROM PROJECTS
        ORDER BY id
        """
    ).fetchall()

    canonical_by_key: dict[tuple[str, str, str], int] = {}
    duplicate_count = 0

    for project_id, repository_id, title, project_url in rows:
        url_key = normalise_url(project_url)

        if url_key:
            key = ("url", str(repository_id or ""), url_key)
            reason = "same repository_id and normalized project_url"
        else:
            title_key = normalise_text(title)
            key = ("title", str(repository_id or ""), title_key)
            reason = "same repository_id and normalized title"

        if key not in canonical_by_key:
            canonical_by_key[key] = project_id
            continue

        canonical_project_id = canonical_by_key[key]

        if canonical_project_id == project_id:
            continue

        conn.execute(
            """
            INSERT INTO PROJECT_DEDUPLICATION (
                source_project_id,
                canonical_project_id,
                duplicate_reason,
                created_at
            )
            VALUES (?, ?, ?, ?)
            """,
            (project_id, canonical_project_id, reason, utc_now()),
        )

        conn.execute(
            "UPDATE PROJECTS SET is_canonical = 0 WHERE id = ?",
            (project_id,),
        )

        duplicate_count += 1

    return duplicate_count


def main() -> None:
    if not DB_PATH.exists():
        raise FileNotFoundError(
            f"Classification DB not found: {DB_PATH.resolve()}"
        )

    conn = sqlite3.connect(DB_PATH)

    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ARCHIVE_MEMBERS (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_file_id INTEGER NOT NULL,
                member_name TEXT NOT NULL,
                file_type TEXT,
                category TEXT NOT NULL
            )
            """
        )

        # Safe re-run behaviour.
        conn.execute("DELETE FROM ARCHIVE_MEMBERS")
        conn.execute(
            "DELETE FROM PROJECT_TAGS WHERE source = ?",
            (METHOD,),
        )
        conn.execute(
            "DELETE FROM CLASSIFICATION_RUNS WHERE method = ?",
            (METHOD,),
        )
        conn.execute("UPDATE FILES SET is_primary_data = 0")

        duplicates = build_duplicate_map(conn)

        project_rows = conn.execute(
            """
            SELECT
                id,
                repository_id,
                title,
                download_project_folder
            FROM PROJECTS
            ORDER BY id
            """
        ).fetchall()

        all_file_rows = conn.execute(
            """
            SELECT
                id,
                project_id,
                file_name,
                file_type,
                status
            FROM FILES
            ORDER BY project_id, id
            """
        ).fetchall()

        files_by_project: dict[int, list[tuple]] = defaultdict(list)
        for row in all_file_rows:
            files_by_project[row[1]].append(row)

        project_type_counts: Counter[str] = Counter()
        archive_scanned = 0
        archive_missing = 0
        archive_errors = 0

        for index, (
            project_id,
            repository_id,
            title,
            project_folder,
        ) in enumerate(project_rows, start=1):
            files = files_by_project.get(project_id, [])
            successful_files = [
                row for row in files
                if normalise_text(row[4]) == "succeeded"
            ]

            direct_categories: Counter[str] = Counter()
            archive_categories: Counter[str] = Counter()
            direct_extensions: Counter[str] = Counter()
            evidence: list[str] = []
            tags: set[str] = set()

            for file_id, _, file_name, file_type, _status in successful_files:
                extension = get_extension(file_name, file_type)
                category = extension_category(extension)

                if extension:
                    direct_extensions[extension] += 1
                    tags.add(f"filetype:{extension}")

                direct_categories[category] += 1

                is_primary = 1 if category == "primary" else 0
                conn.execute(
                    "UPDATE FILES SET is_primary_data = ? WHERE id = ?",
                    (is_primary, file_id),
                )

                if extension not in ARCHIVE_EXTENSIONS:
                    continue

                local_path = safe_local_path(project_folder, file_name)

                if local_path is None or not local_path.exists():
                    archive_missing += 1
                    evidence.append(
                        f"ZIP not inspected because local file was unavailable: {file_name}"
                    )
                    continue

                archive_scanned += 1
                members, error = inspect_zip(local_path)

                if error:
                    archive_errors += 1
                    evidence.append(f"ZIP inspection failed for {file_name}: {error}")
                    continue

                member_counter: Counter[str] = Counter()

                for member_name, member_category in members:
                    member_ext = Path(member_name).suffix.lower()
                    archive_categories[member_category] += 1
                    member_counter[member_ext] += 1

                    conn.execute(
                        """
                        INSERT INTO ARCHIVE_MEMBERS (
                            source_file_id,
                            member_name,
                            file_type,
                            category
                        )
                        VALUES (?, ?, ?, ?)
                        """,
                        (file_id, member_name, member_ext, member_category),
                    )

                if member_counter:
                    compact = ", ".join(
                        f"{ext}:{count}"
                        for ext, count in member_counter.most_common(8)
                    )
                    evidence.append(
                        f"ZIP {file_name} contains recognised members ({compact})"
                    )

                    # Treat archive as primary when it contains primary material.
                    if archive_categories["primary"] > 0:
                        conn.execute(
                            "UPDATE FILES SET is_primary_data = 1 WHERE id = ?",
                            (file_id,),
                        )

            # Required cascade from the slides.
            has_qda = (
                direct_categories["qda"] > 0
                or archive_categories["qda"] > 0
            )
            has_primary = (
                direct_categories["primary"] > 0
                or archive_categories["primary"] > 0
            )
            has_valid = (
                direct_categories["valid"] > 0
                or archive_categories["valid"] > 0
            )

            if has_qda:
                project_type = "QDA_PROJECT"
            elif has_primary:
                project_type = "QD_PROJECT"
            elif has_valid:
                project_type = "OTHER_PROJECT"
            else:
                project_type = "NOT_A_PROJECT"

            project_type_counts[project_type] += 1
            tags.add(f"project_type:{project_type.lower()}")

            direct_summary = ", ".join(
                f"{ext}:{count}"
                for ext, count in direct_extensions.most_common(12)
            )

            if direct_summary:
                evidence.insert(0, f"Direct successful file types: {direct_summary}")

            if not successful_files:
                evidence.append(
                    "No successfully downloaded files were available for type derivation"
                )

            evidence_text = " | ".join(evidence)

            conn.execute(
                """
                UPDATE PROJECTS
                SET
                    type = ?,
                    no_project_files = ?,
                    classification_evidence = ?,
                    classification_method = ?
                WHERE id = ?
                """,
                (
                    project_type,
                    len(files),
                    evidence_text,
                    METHOD,
                    project_id,
                ),
            )

            for tag in sorted(tags):
                conn.execute(
                    """
                    INSERT INTO PROJECT_TAGS (project_id, tag, source)
                    VALUES (?, ?, ?)
                    """,
                    (project_id, tag, METHOD),
                )

            conn.execute(
                """
                INSERT INTO CLASSIFICATION_RUNS (
                    project_id,
                    file_id,
                    project_type,
                    primary_class,
                    secondary_class,
                    evidence,
                    method,
                    classified_at
                )
                VALUES (?, NULL, ?, NULL, NULL, ?, ?, ?)
                """,
                (
                    project_id,
                    project_type,
                    evidence_text,
                    METHOD,
                    utc_now(),
                ),
            )

            if index % 10 == 0 or index == len(project_rows):
                print(
                    f"[PROGRESS] Classified {index}/{len(project_rows)} projects"
                )

        conn.commit()

        canonical_count = conn.execute(
            "SELECT COUNT(*) FROM PROJECTS WHERE is_canonical = 1"
        ).fetchone()[0]

        print("\n[PROJECT TYPE SUMMARY]")
        for project_type in (
            "QDA_PROJECT",
            "QD_PROJECT",
            "OTHER_PROJECT",
            "NOT_A_PROJECT",
        ):
            print(f"{project_type}: {project_type_counts[project_type]}")

        print("\n[DEDUPLICATION SUMMARY]")
        print(f"Projects total: {len(project_rows)}")
        print(f"Duplicate mappings recorded: {duplicates}")
        print(f"Canonical projects: {canonical_count}")

        print("\n[ZIP INSPECTION SUMMARY]")
        print(f"ZIP archives scanned: {archive_scanned}")
        print(f"ZIP archives missing locally: {archive_missing}")
        print(f"ZIP inspection errors: {archive_errors}")

        print(f"\n[OK] Updated classification DB: {DB_PATH}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()