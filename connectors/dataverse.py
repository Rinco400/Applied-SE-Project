from __future__ import annotations
from typing import Any, Dict, Iterable, List, Tuple, Optional
import time
import requests

from core.config import QDA_EXTENSIONS


def _get(url: str, headers: dict, params: dict | None = None, timeout: int = 30) -> requests.Response:
    r = requests.get(url, headers=headers, params=params, timeout=timeout)
    r.raise_for_status()
    return r


def _find_qda_file(files: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    for f in files:
        name = (f.get("label") or f.get("dataFile", {}).get("filename") or "").lower()
        if any(name.endswith(ext) for ext in QDA_EXTENSIONS):
            return f
    return None


def search_datasets_with_qda(
    base_url: str,
    user_agent: str,
    query: str,
    max_pages: int = 5,
    per_page: int = 25,
) -> Iterable[Dict[str, Any]]:
    """
    Dataverse Search API:
    /api/search?q=...&type=dataset
    Then Native API:
    /api/datasets/:persistentId/?persistentId=doi:...
    """
    headers = {"User-Agent": user_agent}

    for page in range(start := 0, max_pages * per_page, per_page):
        params = {
            "q": query,
            "type": "dataset",
            "start": page,
            "per_page": per_page,
        }
        search_url = f"{base_url.rstrip('/')}/api/search"
        resp = _get(search_url, headers, params=params)
        data = resp.json().get("data", {})
        items = data.get("items", [])
        if not items:
            break

        for item in items:
            pid = item.get("global_id")
            if not pid:
                continue

            meta_url = f"{base_url.rstrip('/')}/api/datasets/:persistentId/"
            meta_resp = _get(meta_url, headers, params={"persistentId": pid})
            meta = meta_resp.json().get("data", {})
            latest = meta.get("latestVersion", {})
            files = latest.get("files", [])

            if _find_qda_file(files):
                item["_dataset_meta"] = meta
                yield item

        time.sleep(0.4)


def extract_job(base_url: str, item: Dict[str, Any]) -> Dict[str, Any]:
    meta = item["_dataset_meta"]
    latest = meta.get("latestVersion", {})
    files = latest.get("files", [])

    qda_f = _find_qda_file(files)

    all_files: List[Tuple[str, str]] = []
    for f in files:
        label = f.get("label") or f.get("dataFile", {}).get("filename") or "file.bin"
        file_id = f.get("dataFile", {}).get("id")
        if file_id:
            download_url = f"{base_url.rstrip('/')}/api/access/datafile/{file_id}"
            all_files.append((download_url, label))

    qda_filename = (
        qda_f.get("label")
        if qda_f else
        (all_files[0][1] if all_files else "unknown.qda")
    )

    qda_url = (
        f"{base_url.rstrip('/')}/api/access/datafile/{qda_f['dataFile']['id']}"
        if qda_f and qda_f.get("dataFile", {}).get("id")
        else (all_files[0][0] if all_files else "")
    )

    pid = item.get("global_id")
    dataset_url = f"{base_url.rstrip('/')}/dataset.xhtml?persistentId={pid}" if pid else base_url

    license_str = None
    uploader_name = None
    uploader_email = None

    return {
        "dataset_slug": pid.replace(":", "_").replace("/", "_") if pid else "dataverse-dataset",
        "dataset_url": dataset_url,
        "license": license_str,
        "uploader_name": uploader_name,
        "uploader_email": uploader_email,
        "qda_url": qda_url,
        "qda_filename": qda_filename,
        "all_files": all_files,
    }