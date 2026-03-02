from __future__ import annotations
import argparse
import time
import datetime as dt

from core.config import SETTINGS
from core.db import init_db, insert_acquisition, exists_qda_url
from core.folder_manager import ensure_dataset_dir
from core.downloader import download_file

from connectors.zenodo import search_records_with_qda, extract_job

DEFAULT_QUERY = "qdpx OR nvivo OR atlas.ti OR maxqda OR QDA OR REFI"

def run_zenodo(max_pages: int, query: str) -> None:
    init_db(SETTINGS.db_path)
    SETTINGS.downloads_root.mkdir(parents=True, exist_ok=True)

    for rec in search_records_with_qda(SETTINGS.user_agent, query=query, max_pages=max_pages):
        job = extract_job(rec)

        if not job["qda_url"]:
            continue

        if exists_qda_url(SETTINGS.db_path, job["qda_url"]):
            print(f"[SKIP] Already have: {job['dataset_slug']}")
            continue
    

        dataset_dir = ensure_dataset_dir(SETTINGS.downloads_root, "zenodo", job["dataset_slug"])

        # Download all files (QDA + associated)
        for url, filename in job["all_files"]:
            out_path = dataset_dir / filename
            try:
                download_file(url, out_path, SETTINGS.user_agent)
            except Exception as e:
                print(f"[WARN] Failed: {url} -> {filename} ({e})")

        # Record SQLite row (required fields + optional metadata)
        row = {
            "qda_url": job["qda_url"],
            "downloaded_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
            "local_dir": str(dataset_dir),
            "qda_filename": job["qda_filename"],
            "repository": "zenodo",
            "license": job.get("license"),
            "uploader_name": job.get("uploader_name"),
            "uploader_email": job.get("uploader_email"),
        }
        insert_acquisition(SETTINGS.db_path, row)

        print(f"[OK] {job['dataset_slug']} -> {dataset_dir}")

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--repo", choices=["zenodo"], default="zenodo")
    p.add_argument("--max-pages", type=int, default=1)
    p.add_argument("--query", type=str, default=DEFAULT_QUERY)
    args = p.parse_args()

    if args.repo == "zenodo":
        run_zenodo(args.max_pages, args.query)

if __name__ == "__main__":
    main()