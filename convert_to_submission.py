import sqlite3
from pathlib import Path
import json

SRC_DB = Path("metadata.db")
DST_DB = Path("23071082-seeding.db")

if DST_DB.exists():
    DST_DB.unlink()

src = sqlite3.connect(SRC_DB)
dst = sqlite3.connect(DST_DB)


def normalize_license(value):
    if value is None:
        return None

    if isinstance(value, (dict, list)):
        try:
            if isinstance(value, dict):
                rid = value.get("rightsIdentifier") or value.get("name") or ""
                rid = str(rid).strip().upper()
                if "CC0" in rid:
                    return "CC0"
                if "CC BY 4.0" in rid or "CC-BY-4.0" in rid or "CC BY" in rid:
                    return "CC BY 4.0"
            value = json.dumps(value, ensure_ascii=False)
        except Exception:
            value = str(value)

    text = str(value).strip()
    upper = text.upper()

    if "CC0" in upper:
        return "CC0"
    if "CC BY 4.0" in upper or "CC-BY-4.0" in upper or "CREATIVE COMMONS ATTRIBUTION 4.0" in upper:
        return "CC BY 4.0"

    return None


def normalize_status(reason_text):
    if not reason_text:
        return "SUCCEEDED"
    upper = str(reason_text).upper()
    if "LOGIN" in upper or "AUTH" in upper:
        return "FAILED_LOGIN_REQUIRED"
    if "TOO LARGE" in upper:
        return "FAILED_TOO_LARGE"
    return "FAILED_SERVER_UNRESPONSIVE"


def repo_info(repository: str):
    if repository == "dataverse_no":
        return 1, "https://dataverse.no", "API", "dataverse_no"
    if repository == "icpsr":
        return 2, "https://www.icpsr.umich.edu", "SCRAPING", "icpsr"
    return None, "", "", repository or ""


# -------------------------------------------------
# Required SQ26 tables only
# -------------------------------------------------
dst.executescript("""
CREATE TABLE PROJECTS (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    repository_id INTEGER,
    title TEXT,
    description TEXT,
    repository_url TEXT,
    project_url TEXT,
    download_date TEXT,
    download_method TEXT,
    download_project_folder TEXT,
    download_repository_folder TEXT
);

CREATE TABLE FILES (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id INTEGER,
    file_name TEXT,
    file_type TEXT,
    status TEXT
);

CREATE TABLE KEYWORDS (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id INTEGER,
    keyword TEXT
);

CREATE TABLE PERSON_ROLE (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id INTEGER,
    name TEXT,
    role TEXT
);

CREATE TABLE LICENSES (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id INTEGER,
    license TEXT
);
""")

# -------------------------------------------------
# Build projects from acquisitions
# one project per (repository, title, persistent_id, local_dir)
# -------------------------------------------------
project_map = {}

src_rows = src.execute("""
    SELECT
        repository,
        title,
        persistent_id,
        local_dir,
        query_text,
        file_url,
        local_filename,
        file_type,
        downloaded_at,
        license,
        uploader_name,
        uploader_email,
        file_category
    FROM acquisitions
""").fetchall()

for row in src_rows:
    (
        repository, title, persistent_id, local_dir, query_text,
        file_url, local_filename, file_type, downloaded_at,
        license_text, uploader_name, uploader_email, file_category
    ) = row

    repository = repository or ""
    title = title or ""
    persistent_id = persistent_id or ""
    local_dir = local_dir or ""
    query_text = query_text or ""
    file_url = file_url or ""
    local_filename = local_filename or ""
    file_type = file_type or ""
    downloaded_at = downloaded_at or ""
    uploader_name = uploader_name or ""

    project_key = (repository, title, persistent_id, local_dir)

    if project_key not in project_map:
        repository_id, repository_url, download_method, download_repository_folder = repo_info(repository)
        project_url = persistent_id if persistent_id else ""
        description = f"Persistent ID: {persistent_id}; Query: {query_text}" if persistent_id or query_text else ""

        dst.execute("""
            INSERT INTO PROJECTS (
                repository_id, title, description, repository_url, project_url,
                download_date, download_method, download_project_folder, download_repository_folder
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            repository_id,
            title,
            description,
            repository_url,
            project_url,
            downloaded_at,
            download_method,
            local_dir,
            download_repository_folder,
        ))
        project_id = dst.execute("SELECT last_insert_rowid()").fetchone()[0]
        project_map[project_key] = project_id

        norm_license = normalize_license(license_text)
        if norm_license:
            dst.execute("""
                INSERT INTO LICENSES (project_id, license)
                VALUES (?, ?)
            """, (project_id, norm_license))

        if uploader_name:
            dst.execute("""
                INSERT INTO PERSON_ROLE (project_id, name, role)
                VALUES (?, ?, ?)
            """, (
                project_id,
                uploader_name,
                "UPLOADER",
            ))

        if query_text:
            for kw in [x.strip() for x in query_text.split() if x.strip()]:
                dst.execute("""
                    INSERT INTO KEYWORDS (project_id, keyword)
                    VALUES (?, ?)
                """, (project_id, kw))

    project_id = project_map[project_key]

    dst.execute("""
        INSERT INTO FILES (
            project_id, file_name, file_type, status
        ) VALUES (?, ?, ?, ?)
    """, (
        project_id,
        local_filename,
        file_type,
        "SUCCEEDED",
    ))

# -------------------------------------------------
# Failures -> FILES failed rows
# -------------------------------------------------
fail_rows = src.execute("""
    SELECT repository, query_text, dataset_url, file_url, reason, created_at
    FROM failures
""").fetchall()

for repository, query_text, dataset_url, file_url, reason, created_at in fail_rows:
    repository = repository or ""
    query_text = query_text or ""
    dataset_url = dataset_url or ""
    file_url = file_url or ""
    reason = reason or ""
    created_at = created_at or ""

    project_key = (repository, dataset_url, "", dataset_url)

    if project_key not in project_map:
        repository_id, repository_url, download_method, download_repository_folder = repo_info(repository)
        project_url = dataset_url or ""
        description = f"Failure-derived project; Query: {query_text}" if query_text else "Failure-derived project"

        dst.execute("""
            INSERT INTO PROJECTS (
                repository_id, title, description, repository_url, project_url,
                download_date, download_method, download_project_folder, download_repository_folder
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            repository_id,
            dataset_url or "unknown_project",
            description,
            repository_url,
            project_url,
            created_at,
            download_method,
            dataset_url or "",
            download_repository_folder,
        ))
        project_id = dst.execute("SELECT last_insert_rowid()").fetchone()[0]
        project_map[project_key] = project_id

        if query_text:
            for kw in [x.strip() for x in query_text.split() if x.strip()]:
                dst.execute("""
                    INSERT INTO KEYWORDS (project_id, keyword)
                    VALUES (?, ?)
                """, (project_id, kw))

    project_id = project_map[project_key]

    filename = file_url.split("/")[-1] if file_url else "unknown"
    file_type = Path(filename).suffix.lower()
    status = normalize_status(reason)

    dst.execute("""
        INSERT INTO FILES (
            project_id, file_name, file_type, status
        ) VALUES (?, ?, ?, ?)
    """, (
        project_id,
        filename,
        file_type,
        status,
    ))

dst.commit()
src.close()
dst.close()

print("Created:", DST_DB)