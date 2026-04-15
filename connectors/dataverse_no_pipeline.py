from __future__ import annotations

import json
import re
import time
import datetime as dt
from pathlib import Path
from typing import Any, Iterable, Sequence

import requests

from core.db import init_db, insert_acquisition, exists_file_url, insert_failure


class DataverseNOPipeline:
    BASE_URL = "https://dataverse.no"
    SEARCH_API = f"{BASE_URL}/api/search"
    DATASET_BY_PID_API = f"{BASE_URL}/api/datasets/:persistentId/"
    FILE_ACCESS_API = f"{BASE_URL}/api/access/datafile"


    def _to_db_text(self, value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, str):
            return value
        if isinstance(value, (int, float, bool)):
            return str(value)
        if isinstance(value, (dict, list)):
            return json.dumps(value, ensure_ascii=False)
        return str(value)

    def __init__(
        self,
        out_dir: Path,
        db_path: Path,
        queries: Sequence[str],
        qda_extensions: set[str],
        associated_extensions: set[str],
        per_page: int = 100,
        delay: float = 0.2,
        timeout: int = 60,
    ) -> None:
        self.out_dir = Path(out_dir)
        self.db_path = Path(db_path)
        self.queries = list(queries)

        self.qda_extensions = {self._normalize_ext(e) for e in qda_extensions}
        self.associated_extensions = {self._normalize_ext(e) for e in associated_extensions}

        self.per_page = min(max(per_page, 1), 1000)
        self.delay = delay
        self.timeout = timeout

        self.session = requests.Session()
        self.session.headers.update(
            {"User-Agent": "QDArchiveSeeder/1.0 (+educational use)"}
        )

        self.out_dir.mkdir(parents=True, exist_ok=True)
        init_db(self.db_path)

        self.total_queries = len(self.queries)
        self.total_datasets_seen = 0
        self.total_datasets_processed = 0
        self.total_qda_downloads = 0
        self.total_associated_downloads = 0
        self.total_failures = 0

    def _now(self) -> str:
        return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")

    def _normalize_ext(self, ext: str) -> str:
        ext = ext.strip().lower()
        if not ext:
            return ext
        return ext if ext.startswith(".") else f".{ext}"

    def _slugify(self, value: str) -> str:
        value = value.strip().lower()
        value = re.sub(r"[^\w\s\-\.]", "", value)
        value = re.sub(r"[\s/\\]+", "-", value)
        value = re.sub(r"-{2,}", "-", value)
        return value[:180].strip("-") or "dataset"

    def _safe_filename(self, name: str) -> str:
        name = name.strip()
        name = re.sub(r'[<>:"/\\|?*]+', "_", name)
        return name[:220] or "file.bin"

    def _request_json(self, url: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        resp = self.session.get(url, params=params, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    def _log_failure(
        self,
        repository: str,
        query_text: str | None,
        dataset_url: str | None,
        file_url: str | None,
        reason: str,
    ) -> None:
        self.total_failures += 1
        insert_failure(
            self.db_path,
            {
                "repository": repository,
                "query_text": query_text,
                "dataset_url": dataset_url,
                "file_url": file_url,
                "reason": reason,
                "created_at": self._now(),
            },
        )

    def _download_file(self, url: str, out_path: Path) -> None:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with self.session.get(url, stream=True, timeout=self.timeout) as resp:
            resp.raise_for_status()
            with open(out_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)

    def _get_extension_from_name(self, name: str | None) -> str:
        if not name:
            return ""
        return Path(name).suffix.lower()

    def _iter_search_results(self, query: str) -> Iterable[dict[str, Any]]:
        start = 0
        while True:
            payload = self._request_json(
                self.SEARCH_API,
                params={
                    "q": query,
                    "type": "dataset",
                    "per_page": self.per_page,
                    "start": start,
                },
            )
            data = payload.get("data", {}) or {}
            items = data.get("items", []) or []
            if not items:
                break

            for item in items:
                yield item

            count_in_response = len(items)
            total_count = data.get("total_count", 0)
            start += count_in_response

            if start >= total_count or count_in_response == 0:
                break

            time.sleep(self.delay)

    def _extract_simple_value(self, value: Any) -> str | None:
        if isinstance(value, str):
            return value.strip() or None

        if isinstance(value, list):
            for item in value:
                if isinstance(item, str) and item.strip():
                    return item.strip()
                if isinstance(item, dict):
                    if isinstance(item.get("value"), str) and item.get("value", "").strip():
                        return item["value"].strip()

        if isinstance(value, dict):
            inner = value.get("value")
            if isinstance(inner, str) and inner.strip():
                return inner.strip()

        return None

    def _extract_author_name(self, author_entry: Any) -> str | None:
        if isinstance(author_entry, str):
            return author_entry.strip() or None

        if isinstance(author_entry, dict):
            author_name_raw = author_entry.get("authorName")

            if isinstance(author_name_raw, str):
                return author_name_raw.strip() or None

            if isinstance(author_name_raw, dict):
                return self._extract_simple_value(author_name_raw.get("value"))

            if isinstance(author_name_raw, list):
                for item in author_name_raw:
                    if isinstance(item, str) and item.strip():
                        return item.strip()
                    if isinstance(item, dict):
                        if item.get("typeName") == "authorName":
                            return self._extract_simple_value(item.get("value"))
                        if isinstance(item.get("value"), str) and item.get("value", "").strip():
                            return item["value"].strip()

        return None

    def extract_metadata(self, version_payload: dict[str, Any]) -> dict[str, Any]:
        data = version_payload.get("data", {}) or {}

        if isinstance(data.get("latestVersion"), dict):
            version_obj = data.get("latestVersion", {}) or {}
        else:
            version_obj = data

        metadata_blocks = version_obj.get("metadataBlocks", {}) or {}
        citation = metadata_blocks.get("citation", {}) or {}
        fields = citation.get("fields", []) or []

        title = None
        license_name = None
        uploader_name = None
        uploader_email = None

        for field in fields:
            if not isinstance(field, dict):
                continue

            type_name = field.get("typeName")
            value = field.get("value")

            if type_name == "title":
                title = self._extract_simple_value(value)

            elif type_name == "author":
                if isinstance(value, list):
                    author_names: list[str] = []
                    for author in value:
                        name = self._extract_author_name(author)
                        if name:
                            author_names.append(name)
                    if author_names:
                        uploader_name = "; ".join(author_names)

            elif type_name == "datasetContact":
                if isinstance(value, list):
                    for entry in value:
                        if isinstance(entry, dict):
                            maybe_name = self._extract_simple_value(entry.get("datasetContactName"))
                            maybe_email = self._extract_simple_value(entry.get("datasetContactEmail"))
                            if maybe_name and not uploader_name:
                                uploader_name = maybe_name
                            if maybe_email and not uploader_email:
                                uploader_email = maybe_email

            elif type_name == "license":
                license_name = self._extract_simple_value(value)

        if not license_name:
            license_name = (
                version_obj.get("license")
                or version_obj.get("termsOfUse")
                or data.get("license")
            )

        return {
            "title": title,
            "license": license_name,
            "uploader_name": uploader_name,
            "uploader_email": uploader_email,
        }

    def _get_dataset_version(self, persistent_id: str) -> dict[str, Any]:
        return self._request_json(
            f"{self.DATASET_BY_PID_API}versions/:latest",
            params={"persistentId": persistent_id},
        )

    def _get_dataset_files(self, version_payload: dict[str, Any]) -> list[dict[str, Any]]:
        data = version_payload.get("data", {}) or {}

        if isinstance(data.get("latestVersion"), dict):
            latest_version = data.get("latestVersion", {}) or {}
            files = latest_version.get("files", []) or []
        else:
            files = data.get("files", []) or []

        clean_files: list[dict[str, Any]] = []
        for entry in files:
            if not isinstance(entry, dict):
                continue

            data_file = entry.get("dataFile", {}) or {}
            if not isinstance(data_file, dict):
                continue

            clean_files.append(entry)

        return clean_files

    def _pick_files(
        self, files: list[dict[str, Any]]
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        qda_files: list[dict[str, Any]] = []
        associated_files: list[dict[str, Any]] = []

        for entry in files:
            data_file = entry.get("dataFile", {}) or {}
            filename = data_file.get("filename") or entry.get("label") or "unknown"
            ext = self._get_extension_from_name(filename)

            if ext in self.qda_extensions:
                qda_files.append(entry)
            elif ext in self.associated_extensions:
                associated_files.append(entry)

        return qda_files, associated_files

    def _file_download_url(self, file_id: int | str) -> str:
        return f"{self.FILE_ACCESS_API}/{file_id}"

    def process_dataset(self, item: dict[str, Any], query: str) -> None:
        persistent_id = item.get("global_id")
        dataset_url = item.get("url") or item.get("identifier") or item.get("persistentUrl")
        dataset_name = item.get("name") or persistent_id or "unknown-dataset"

        self.total_datasets_processed += 1
        print(f"[DATASET] {self.total_datasets_processed}: {dataset_name}")

        if not persistent_id:
            self._log_failure("dataverse_no", query, dataset_url, None, "missing persistent_id")
            print("[SKIP] Missing persistent_id")
            return

        try:
            version_payload = self._get_dataset_version(persistent_id)
        except Exception as e:
            self._log_failure("dataverse_no", query, dataset_url, None, f"version fetch failed: {e}")
            print(f"[FAIL] Version fetch failed: {e}")
            return

        try:
            meta = self.extract_metadata(version_payload)
        except Exception as e:
            self._log_failure("dataverse_no", query, dataset_url, None, f"metadata extraction failed: {e}")
            print(f"[FAIL] Metadata extraction failed: {e}")
            return

        files = self._get_dataset_files(version_payload)
        qda_files, associated_files = self._pick_files(files)

        print(f"[FILES] total={len(files)} | qda={len(qda_files)} | associated={len(associated_files)}")

        if not qda_files and not associated_files:
            self._log_failure("dataverse_no", query, dataset_url, None, "no downloadable file found")
            print("[SKIP] No downloadable file found")
            return

        if not qda_files and associated_files:
            print("[INFO] No QDA file found, downloading associated files only")

        dataset_title = meta.get("title") or item.get("name") or persistent_id
        dataset_slug = self._slugify(dataset_title)
        dataset_dir = self.out_dir / dataset_slug
        dataset_dir.mkdir(parents=True, exist_ok=True)

        downloaded_any = False
        downloaded_any_qda = False

        for entry in qda_files + associated_files:
            data_file = entry.get("dataFile", {}) or {}
            file_id = data_file.get("id")
            filename = self._safe_filename(
                data_file.get("filename") or entry.get("label") or f"file_{file_id}"
            )
            file_url = self._file_download_url(file_id)

            try:
                self._download_file(file_url, dataset_dir / filename)
                ext = self._get_extension_from_name(filename)
                file_category = "qda" if ext in self.qda_extensions else "associated"

                downloaded_any = True

                if not exists_file_url(self.db_path, file_url):
                    insert_acquisition(
                        self.db_path,
                        {
                            "file_url": file_url,
                            "downloaded_at": self._now(),
                            "local_dir": str(dataset_dir),
                            "local_filename": filename,
                            "repository": "dataverse_no",
                            "license": self._to_db_text(meta.get("license")),
                            "uploader_name": self._to_db_text(meta.get("uploader_name")),
                            "uploader_email": self._to_db_text(meta.get("uploader_email")),
                            "title": self._to_db_text(dataset_title),
                            "persistent_id": self._to_db_text(persistent_id),
                            "query_text": self._to_db_text(query),
                            "file_type": self._to_db_text(ext),
                            "file_category": self._to_db_text(file_category),
                        },
                    )

                if ext in self.qda_extensions:
                    downloaded_any_qda = True
                    self.total_qda_downloads += 1
                    print(f"[DOWNLOADED QDA] {filename}")
                else:
                    self.total_associated_downloads += 1
                    print(f"[DOWNLOADED ASSOCIATED] {filename}")

            except Exception as e:
                self._log_failure(
                    "dataverse_no",
                    query,
                    dataset_url,
                    file_url,
                    f"download failed: {e}",
                )
                print(f"[FAIL DOWNLOAD] {filename} -> {e}")
                continue

        if downloaded_any:
            if downloaded_any_qda:
                print(f"[OK] {dataset_slug} -> {dataset_dir} (with QDA)")
            else:
                print(f"[OK] {dataset_slug} -> {dataset_dir} (associated files only)")
        else:
            self._log_failure(
                "dataverse_no",
                query,
                dataset_url,
                None,
                "files existed but none downloaded successfully",
            )
            print("[FAIL] Files existed but none downloaded successfully")

    def run(self) -> None:
        seen_persistent_ids: set[str] = set()

        for idx, query in enumerate(self.queries, start=1):
            print("\n" + "=" * 70)
            print(f"[INFO] Query {idx}/{self.total_queries}: {query}")
            print("=" * 70)

            query_processed_before = self.total_datasets_processed
            query_qda_before = self.total_qda_downloads
            query_assoc_before = self.total_associated_downloads
            query_fail_before = self.total_failures

            try:
                for item in self._iter_search_results(query):
                    persistent_id = item.get("global_id")
                    if not persistent_id:
                        continue

                    self.total_datasets_seen += 1

                    if persistent_id in seen_persistent_ids:
                        continue

                    seen_persistent_ids.add(persistent_id)
                    self.process_dataset(item, query)
                    time.sleep(self.delay)

            except Exception as e:
                self._log_failure("dataverse_no", query, None, None, f"query failed: {e}")
                print(f"[WARN] Query failed: {query} ({e})")

            print("\n[QUERY SUMMARY]")
            print(f"Query: {query}")
            print(f"New unique datasets processed: {self.total_datasets_processed - query_processed_before}")
            print(f"QDA files downloaded: {self.total_qda_downloads - query_qda_before}")
            print(f"Associated files downloaded: {self.total_associated_downloads - query_assoc_before}")
            print(f"Failures logged: {self.total_failures - query_fail_before}")
            print(f"Unique datasets total so far: {len(seen_persistent_ids)}")
            print("-" * 70)

        print("\n" + "#" * 70)
        print("[FINAL SUMMARY]")
        print(f"Total queries run: {self.total_queries}")
        print(f"Total dataset hits seen: {self.total_datasets_seen}")
        print(f"Total unique datasets processed: {self.total_datasets_processed}")
        print(f"Total QDA files downloaded: {self.total_qda_downloads}")
        print(f"Total associated files downloaded: {self.total_associated_downloads}")
        print(f"Total failures logged: {self.total_failures}")
        print("#" * 70)