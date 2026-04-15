from __future__ import annotations

import datetime as dt
import json
import re
import time
from pathlib import Path
from typing import Any, Optional, Sequence
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from core.db import init_db, insert_acquisition, exists_file_url, insert_failure

BASE_URL = "https://www.icpsr.umich.edu"
SEARCH_URL = f"{BASE_URL}/web/ICPSR/search/studies"


def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


def slugify(value: str, max_len: int = 80) -> str:
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    value = re.sub(r"-{2,}", "-", value).strip("-")
    return value[:max_len] or "study"


class ICPSRPipeline:
    def __init__(
        self,
        out_dir: Path,
        db_path: Path,
        queries: Sequence[str],
        qda_extensions: Optional[set[str]] = None,
        associated_extensions: Optional[set[str]] = None,
        delay: float = 0.5,
        timeout: int = 60,
        seed_file: Optional[Path] = None,
    ) -> None:
        self.out_dir = Path(out_dir)
        self.db_path = Path(db_path)
        self.queries = list(queries)
        self.qda_extensions = {
            self._normalize_ext(e)
            for e in (
                qda_extensions
                or {
                    ".qdpx",
                    ".qde",
                    ".qdas",
                    ".nvpx",
                    ".atlproj",
                    ".maxqdaproject",
                    ".mx",
                    ".qdp",
                    ".qda",
                }
            )
        }
        self.associated_extensions = {
            self._normalize_ext(e)
            for e in (
                associated_extensions
                or {
                    ".pdf",
                    ".doc",
                    ".docx",
                    ".txt",
                    ".rtf",
                    ".odt",
                    ".csv",
                    ".xlsx",
                    ".xls",
                    ".mp3",
                    ".wav",
                    ".m4a",
                    ".mp4",
                    ".mov",
                    ".avi",
                    ".jpg",
                    ".jpeg",
                    ".png",
                    ".webp",
                    ".json",
                    ".xml",
                    ".zip",
                }
            )
        }
        self.delay = delay
        self.timeout = timeout
        self.seed_file = Path(seed_file) if seed_file else Path("icpsr_seed_urls.txt")

        self.session = requests.Session()
        self.session.headers.update(
            {"User-Agent": "QDArchiveSeeder/1.0 (+educational use)"}
        )

        self.out_dir.mkdir(parents=True, exist_ok=True)
        init_db(self.db_path)

        self.total_queries = len(self.queries)
        self.total_studies_seen = 0
        self.total_studies_processed = 0
        self.total_qda_downloads = 0
        self.total_associated_downloads = 0
        self.total_failures = 0

    def _normalize_ext(self, ext: str) -> str:
        ext = ext.strip().lower()
        if not ext:
            return ext
        return ext if ext.startswith(".") else f".{ext}"

    def _normalize_url(self, url: str) -> str:
        return url.split("?")[0].strip()

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

    def _log_failure(
        self,
        code: str,
        details: str,
        item_url: str = "",
        item_title: str = "",
        query_text: str = "",
        file_url: str = "",
    ) -> None:
        self.total_failures += 1
        insert_failure(
            self.db_path,
            {
                "repository": "icpsr",
                "query_text": self._to_db_text(query_text),
                "dataset_url": self._to_db_text(item_url),
                "file_url": self._to_db_text(file_url),
                "reason": self._to_db_text(f"{code}: {details}"),
                "created_at": utc_now_iso(),
            },
        )

    def get_soup(self, url: str, params: Optional[dict] = None) -> BeautifulSoup:
        resp = self.session.get(url, params=params, timeout=self.timeout)
        resp.raise_for_status()
        return BeautifulSoup(resp.text, "html.parser")

    def _load_seed_urls(self) -> list[tuple[str, str]]:
        if not self.seed_file.exists():
            return []

        studies: list[tuple[str, str]] = []
        for line in self.seed_file.read_text(encoding="utf-8").splitlines():
            url = line.strip()
            if not url or url.startswith("#"):
                continue
            title_hint = url.rstrip("/").split("/")[-1]
            studies.append((url, title_hint))
        return studies

    def search_studies(
        self,
        query: str,
        rows: int = 50,
        start: int = 0,
    ) -> list[tuple[str, str]]:
        params = {"q": query, "rows": rows, "start": start}
        soup = self.get_soup(SEARCH_URL, params=params)

        links: list[tuple[str, str]] = []
        for a in soup.select('a[href*="/web/ICPSR/studies/"]'):
            href = a.get("href") or ""
            title = a.get_text(" ", strip=True)
            if not href or not title:
                continue
            full = urljoin(BASE_URL, href)
            links.append((full, title))

        dedup: list[tuple[str, str]] = []
        seen: set[str] = set()
        for url, title in links:
            if url not in seen:
                dedup.append((url, title))
                seen.add(url)

        return dedup

    def extract_study_metadata(self, study_url: str) -> dict[str, Any]:
        soup = self.get_soup(study_url)
        title = (
            soup.find("h1").get_text(" ", strip=True)
            if soup.find("h1")
            else study_url.rstrip("/").split("/")[-1]
        )
        text = soup.get_text(" ", strip=True)

        study_id_match = re.search(
            r"\bICPSR\s*(?:Study\s*)?#?\s*(\d{3,})\b",
            text,
            flags=re.I,
        )
        study_id = (
            study_id_match.group(1)
            if study_id_match
            else study_url.rstrip("/").split("/")[-1]
        )

        license_text: Any = ""
        license_patterns = [
            r"Creative Commons[^.]*",
            r"CC BY[^.]*",
            r"Terms of Use[^.]*",
        ]
        for pattern in license_patterns:
            match = re.search(pattern, text, flags=re.I)
            if match:
                license_text = match.group(0).strip()
                break

        uploader_name: Any = ""
        for label in [
            "Principal Investigator",
            "Investigators",
            "Producer",
            "Depositor",
        ]:
            match = re.search(label + r"\s*:?\s*(.+?)\s{2,}", text, flags=re.I)
            if match:
                uploader_name = match.group(1).strip()
                break

        files: list[dict[str, str]] = []
        for a in soup.select("a[href]"):
            href = a.get("href") or ""
            full = urljoin(BASE_URL, href)
            label = a.get_text(" ", strip=True)
            lower = href.lower()
            ext = Path(lower).suffix.lower()

            if ext in self.qda_extensions or ext in self.associated_extensions:
                files.append(
                    {
                        "url": full,
                        "label": label or Path(href).name,
                        "ext": ext,
                    }
                )
            elif any(token in lower for token in ["download", "documentation", "codebook", "pdf", "export"]):
                files.append(
                    {
                        "url": full,
                        "label": label or Path(href).name,
                        "ext": ext or ".bin",
                    }
                )

        export_links: list[str] = []
        for a in soup.select('a[href*="metadata"], a[href*="export"]'):
            href = a.get("href") or ""
            export_links.append(urljoin(BASE_URL, href))

        return {
            "title": title,
            "study_id": study_id,
            "license": license_text,
            "uploader_name": uploader_name,
            "files": files,
            "export_links": list(dict.fromkeys(export_links)),
            "raw_text_excerpt": text[:5000],
        }

    def download_file(self, url: str, dest: Path) -> tuple[str, str]:
        try:
            with self.session.get(
                url,
                timeout=self.timeout,
                stream=True,
                allow_redirects=True,
            ) as resp:
                ctype = resp.headers.get("content-type", "")
                final_url = str(resp.url)

                if resp.status_code != 200:
                    return "failed", f"http_{resp.status_code}"

                if "text/html" in ctype and any(
                    x in final_url.lower() for x in ["login", "signin", "passport"]
                ):
                    return "skipped", "authentication_required"

                dest.parent.mkdir(parents=True, exist_ok=True)
                with dest.open("wb") as fh:
                    for chunk in resp.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            fh.write(chunk)

            return "downloaded", ""
        except Exception as exc:
            return "failed", str(exc)

    def process_study(self, study_url: str, title_hint: str, query_text: str) -> None:
        self.total_studies_processed += 1
        print(f"[STUDY] {self.total_studies_processed}: {title_hint}")

        try:
            meta = self.extract_study_metadata(study_url)
        except Exception as exc:
            self._log_failure("study_fetch_failed", str(exc), study_url, title_hint, query_text)
            print(f"[FAIL] Study fetch failed: {exc}")
            return

        title = meta["title"]
        study_id = str(meta["study_id"])
        local_dir_name = f"{slugify(title)}-icpsr-{slugify(study_id, 30)}"
        study_dir = self.out_dir / local_dir_name
        study_dir.mkdir(parents=True, exist_ok=True)

        (study_dir / "study_metadata.json").write_text(
            json.dumps(meta, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

        if not meta["files"] and not meta["export_links"]:
            self._log_failure(
                "no_public_file_links_found",
                "No file or export links found on study page",
                study_url,
                title,
                query_text,
            )
            print("[SKIP] No public file links found")
            return

        targets = list(meta["files"])
        for idx, url in enumerate(meta["export_links"], start=1):
            targets.append(
                {
                    "url": url,
                    "label": f"metadata_export_{idx}.xml",
                    "ext": Path(url).suffix.lower() or ".xml",
                }
            )

        qda_count = 0
        assoc_count = 0
        for item in targets:
            ext = Path(item["label"]).suffix.lower() or item.get("ext", "").lower()
            if ext in self.qda_extensions:
                qda_count += 1
            else:
                assoc_count += 1

        print(f"[FILES] total={len(targets)} | qda={qda_count} | associated={assoc_count}")

        downloaded_any = False
        downloaded_any_qda = False

        for item in targets:
            file_url = self._normalize_url(item["url"])
            filename = Path(file_url).name or item["label"] or f"file-{int(time.time())}"
            if "." not in filename and item.get("ext"):
                filename = f"{filename}{item['ext']}"

            if exists_file_url(self.db_path, file_url):
                print(f"[SKIP DUPLICATE FILE] {filename}")
                continue

            status, reason = self.download_file(file_url, study_dir / filename)
            ext = Path(filename).suffix.lower()
            file_category = "qda" if ext in self.qda_extensions else "associated"

            if status == "downloaded":
                downloaded_any = True

                insert_acquisition(
                    self.db_path,
                    {
                        "file_url": self._to_db_text(file_url),
                        "downloaded_at": utc_now_iso(),
                        "local_dir": self._to_db_text(str(study_dir)),
                        "local_filename": self._to_db_text(filename),
                        "repository": "icpsr",
                        "license": self._to_db_text(meta.get("license")),
                        "uploader_name": self._to_db_text(meta.get("uploader_name")),
                        "uploader_email": "",
                        "title": self._to_db_text(title),
                        "persistent_id": self._to_db_text(study_id),
                        "query_text": self._to_db_text(query_text),
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
            else:
                self._log_failure(
                    "download_not_completed",
                    f"{filename}: {reason}",
                    item_url=study_url,
                    item_title=title,
                    query_text=query_text,
                    file_url=file_url,
                )
                print(f"[FAIL DOWNLOAD] {filename} -> {reason}")

            time.sleep(self.delay)

        if downloaded_any:
            if downloaded_any_qda:
                print(f"[OK] {local_dir_name} -> {study_dir} (with QDA)")
            else:
                print(f"[OK] {local_dir_name} -> {study_dir} (associated files only)")
        else:
            print("[WARN] Study processed but nothing downloadable was saved")

    def run(self) -> None:
        self.out_dir.mkdir(parents=True, exist_ok=True)
        seen_urls: set[str] = set()

        seed_urls = self._load_seed_urls()
        if seed_urls:
            print(f"[INFO] Loaded {len(seed_urls)} seed study URLs from {self.seed_file}")

        for idx, query in enumerate(self.queries, start=1):
            print("\n" + "=" * 70)
            print(f"[INFO] Query {idx}/{self.total_queries}: {query}")
            print("=" * 70)

            query_processed_before = self.total_studies_processed
            query_qda_before = self.total_qda_downloads
            query_assoc_before = self.total_associated_downloads
            query_fail_before = self.total_failures

            try:
                studies = self.search_studies(query)
            except Exception as exc:
                self._log_failure("search_failed", str(exc), query_text=query)
                print(f"[FAIL] Search failed: {exc}")
                studies = []

            if not studies:
                self._log_failure(
                    "search_returned_no_links",
                    "ICPSR search page returned no parsable study links; likely JS-dependent page structure",
                    query_text=query,
                )
                print("[WARN] No parsable study links found from search page")
                studies = seed_urls

            if not studies:
                print("[WARN] No studies available for this query even after seed fallback")
                print("\n[QUERY SUMMARY]")
                print(f"Query: {query}")
                print(f"New unique studies processed: {self.total_studies_processed - query_processed_before}")
                print(f"QDA files downloaded: {self.total_qda_downloads - query_qda_before}")
                print(f"Associated files downloaded: {self.total_associated_downloads - query_assoc_before}")
                print(f"Failures logged: {self.total_failures - query_fail_before}")
                print(f"Unique studies total so far: {len(seen_urls)}")
                print("-" * 70)
                continue

            print(f"[INFO] Found {len(studies)} study links for query: {query}")

            for study_url, title in studies:
                self.total_studies_seen += 1
                if study_url in seen_urls:
                    continue
                seen_urls.add(study_url)
                self.process_study(study_url, title, query)
                time.sleep(self.delay)

            print("\n[QUERY SUMMARY]")
            print(f"Query: {query}")
            print(f"New unique studies processed: {self.total_studies_processed - query_processed_before}")
            print(f"QDA files downloaded: {self.total_qda_downloads - query_qda_before}")
            print(f"Associated files downloaded: {self.total_associated_downloads - query_assoc_before}")
            print(f"Failures logged: {self.total_failures - query_fail_before}")
            print(f"Unique studies total so far: {len(seen_urls)}")
            print("-" * 70)

        print("\n" + "#" * 70)
        print("[FINAL SUMMARY]")
        print(f"Total queries run: {self.total_queries}")
        print(f"Total study hits seen: {self.total_studies_seen}")
        print(f"Total unique studies processed: {self.total_studies_processed}")
        print(f"Total QDA files downloaded: {self.total_qda_downloads}")
        print(f"Total associated files downloaded: {self.total_associated_downloads}")
        print(f"Total failures logged: {self.total_failures}")
        print("#" * 70)