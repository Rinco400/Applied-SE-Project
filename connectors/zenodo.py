from __future__ import annotations
from typing import Any, Dict, Iterable, List, Optional, Tuple
import requests
import time
from requests import Response
from core.config import QDA_EXTENSIONS

ZENODO_API = "https://zenodo.org/api/records"

def _find_qda_file(files: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    for f in files:
        key = (f.get("key") or "").lower()
        if any(key.endswith(ext) for ext in QDA_EXTENSIONS):
            return f
    return None

def _file_url(f: Dict[str, Any]) -> str:
    links = f.get("links", {}) or {}
    return links.get("self") or links.get("download") or ""

def search_records_with_qda(
    user_agent: str,
    query: str,
    size: int = 25,
    max_pages: int = 3
) -> Iterable[Dict[str, Any]]:
    headers = {"User-Agent": user_agent}
    params = {"q": query, "size": size, "page": 1}

    for _ in range(max_pages):
        resp = _get_with_backoff(ZENODO_API, headers=headers, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        hits = data.get("hits", {}).get("hits", [])
        if not hits:
            break

        for rec in hits:
            files = rec.get("files", []) or []
            if _find_qda_file(files):
                yield rec

        params["page"] += 1

def extract_job(record: Dict[str, Any]) -> Dict[str, Any]:
    recid = record.get("id")
    metadata = record.get("metadata", {}) or {}
    files = record.get("files", []) or []

    qda_f = _find_qda_file(files)

    all_files: List[Tuple[str, str]] = []
    for f in files:
        url = _file_url(f)
        name = f.get("key") or "file.bin"
        if url:
            all_files.append((url, name))

    qda_url = _file_url(qda_f) if qda_f else (all_files[0][0] if all_files else "")
    qda_filename = (qda_f.get("key") if qda_f else (all_files[0][1] if all_files else "unknown.qda"))

    # License info best-effort
    lic = metadata.get("license", {})
    if isinstance(lic, dict):
        license_str = lic.get("id") or lic.get("title")
    else:
        license_str = str(lic) if lic else None

    # Uploader info is often not present; use first creator as best-effort
    creators = metadata.get("creators") or []
    uploader_name = creators[0].get("name") if creators and isinstance(creators[0], dict) else None
    uploader_email = None

    dataset_url = record.get("links", {}).get("html") or (f"https://zenodo.org/records/{recid}")

    dataset_slug = f"zenodo-{recid}"

    return {
        "dataset_slug": dataset_slug,
        "dataset_url": dataset_url,
        "license": license_str,
        "uploader_name": uploader_name,
        "uploader_email": uploader_email,
        "qda_url": qda_url,
        "qda_filename": qda_filename,
        "all_files": all_files,
    }

def _get_with_backoff(url: str, headers: dict, params: dict, timeout: int = 30, max_retries: int = 8) -> Response:
    """
    Handles Zenodo 429 rate limits using exponential backoff + Retry-After if provided.
    """
    sleep_s = 1.0
    for attempt in range(1, max_retries + 1):
        resp = requests.get(url, headers=headers, params=params, timeout=timeout)

        if resp.status_code != 429:
            return resp

        # 429: respect Retry-After header if present, otherwise exponential backoff
        retry_after = resp.headers.get("Retry-After")
        if retry_after:
            try:
                wait = float(retry_after)
            except ValueError:
                wait = sleep_s
        else:
            wait = sleep_s

        print(f"[RATE] 429 Too Many Requests. Sleeping {wait:.1f}s (attempt {attempt}/{max_retries})...")
        time.sleep(wait)
        sleep_s = min(sleep_s * 2, 60)  # cap at 60s

    # If still failing, raise for status
    resp.raise_for_status()
    return resp