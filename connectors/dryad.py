from __future__ import annotations
from typing import Any, Dict, Iterable, List, Tuple, Optional
import time
import time
import requests

from core.config import QDA_EXTENSIONS

DRYAD_API = "https://datadryad.org/api/v2"


def _get(url: str, headers: dict, params: dict | None = None, timeout: int = 30, max_retries: int = 6) -> requests.Response:
    """
    Retries on transient server errors (5xx) with exponential backoff.
    """
    sleep_s = 1.0
    last = None
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(url, headers=headers, params=params, timeout=timeout)
            # Retry on 5xx
            if 500 <= r.status_code <= 599:
                last = r
                print(f"[WARN] Dryad {r.status_code} on {url}. Backoff {sleep_s:.1f}s (attempt {attempt}/{max_retries})")
                time.sleep(sleep_s)
                sleep_s = min(sleep_s * 2, 30)
                continue

            r.raise_for_status()
            return r

        except requests.RequestException as e:
            print(f"[WARN] Dryad request error on {url}: {e}. Backoff {sleep_s:.1f}s (attempt {attempt}/{max_retries})")
            time.sleep(sleep_s)
            sleep_s = min(sleep_s * 2, 30)

    # If still failing, raise last response or a generic error
    if last is not None:
        last.raise_for_status()
    raise RuntimeError(f"Dryad API failed after retries: {url}")


def _files_for_dataset(headers: dict, dataset_id: str) -> List[Dict[str, Any]]:
    try:
        versions = _get(f"{DRYAD_API}/datasets/{dataset_id}/versions", headers).json()
    except Exception as e:
        print(f"[SKIP] Dryad dataset {dataset_id}: cannot fetch versions ({e})")
        return []

    items = versions.get("_embedded", {}).get("stash:versions", [])
    if not items:
        return []

    latest_version = items[0]
    files_href = latest_version.get("_links", {}).get("stash:files", {}).get("href")
    if not files_href:
        return []

    try:
        files_json = _get(files_href, headers).json()
    except Exception as e:
        print(f"[SKIP] Dryad dataset {dataset_id}: cannot fetch files ({e})")
        return []

    return files_json.get("_embedded", {}).get("stash:files", [])


def _find_qda(files: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    for f in files:
        path = (f.get("path") or "").lower()
        if any(path.endswith(ext) for ext in QDA_EXTENSIONS):
            return f
    return None


def search_datasets_with_qda(user_agent: str, query: str, max_pages: int = 5, per_page: int = 20) -> Iterable[Dict[str, Any]]:
    """
    Search datasets via Dryad API v2. Query syntax mirrors the website search.
    Docs: https://datadryad.org/api  (v2) :contentReference[oaicite:3]{index=3}
    """
    headers = {"User-Agent": user_agent}

    for page in range(1, max_pages + 1):
        params = {"q": query, "page": page, "per_page": per_page}
        j = _get(f"{DRYAD_API}/datasets", headers, params=params).json()

        datasets = j.get("_embedded", {}).get("stash:datasets", [])
        if not datasets:
            break

        for ds in datasets:
            ds_id = ds.get("id")
            if not ds_id:
                continue
            try:
                if int(ds_id) < 1000:
                    continue
            except Exception:
                continue

            files = _files_for_dataset(headers, ds_id)
            if _find_qda(files):
                # attach files for downstream processing
                ds["_qda_files"] = files
                yield ds

        time.sleep(0.3)  # polite pacing


def extract_job(dataset: Dict[str, Any]) -> Dict[str, Any]:
    ds_id = dataset.get("id")
    title = dataset.get("title") or f"dryad-{ds_id}"
    files = dataset.get("_qda_files") or []

    qda_f = _find_qda(files)
    qda_filename = (qda_f.get("path") if qda_f else "unknown.qda").split("/")[-1]

    # File download links: each file has _links with 'stash:download'
    all_files: List[Tuple[str, str]] = []
    for f in files:
        fname = (f.get("path") or "file.bin").split("/")[-1]
        durl = f.get("_links", {}).get("stash:download", {}).get("href")
        if durl:
            all_files.append((durl, fname))

    dataset_url = dataset.get("_links", {}).get("stash:html", {}).get("href") or f"https://datadryad.org/stash/dataset/{ds_id}"

    # Dryad is generally CC0 for data; still record what API returns if present :contentReference[oaicite:4]{index=4}
    license_str = dataset.get("license")

    # Uploader name/email may not be present in public API; keep best-effort
    uploader_name = None
    uploader_email = None

    return {
        "dataset_slug": f"dryad-{ds_id}",
        "dataset_url": dataset_url,
        "license": license_str,
        "uploader_name": uploader_name,
        "uploader_email": uploader_email,
        "qda_url": all_files[0][0] if all_files else dataset_url,
        "qda_filename": qda_filename,
        "all_files": all_files,
    }