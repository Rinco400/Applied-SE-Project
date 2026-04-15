from __future__ import annotations

import argparse

from core.config import SETTINGS
from connectors.dataverse_no_pipeline import DataverseNOPipeline
from connectors.icpsr_pipeline import ICPSRPipeline

DEFAULT_QUERIES = [
    "qdpx",
    "nvivo",
    "nvpx",
    "maxqda",
    "atlas.ti",
    "atlproj",
    "qualitative data",
    "qualitative research",
    "interview study",
    "interview transcript",
    "focus group",
    "focus group data",
    "oral history",
    "transcript",
    "coded interview",
    "thematic analysis",
    "ethnography",
    "case study",
    "field notes",
    "qualitative dataset",
]

QDA_EXTENSIONS = {
    ".qdpx",
    ".qda",
    ".qde",
    ".nvpx",
    ".nvp",
    ".atlproj",
    ".hpr7",
    ".hpr6",
    ".maxqda",
    ".mx",
}

ASSOCIATED_EXTENSIONS = {
    ".txt",
    ".rtf",
    ".doc",
    ".docx",
    ".pdf",
    ".odt",
    ".csv",
    ".xlsx",
    ".xls",
    ".jpg",
    ".jpeg",
    ".png",
    ".mp3",
    ".wav",
    ".mp4",
    ".mov",
    ".zip",
}

def main() -> None:
    parser = argparse.ArgumentParser(description="QDArchive acquisition runner")
    parser.add_argument(
        "--repo",
        choices=["dataverse_no", "icpsr", "all"],
        default="all",
        help="Repository to run",
    )
    parser.add_argument(
        "--per-page",
        type=int,
        default=100,
        help="Results per page where supported",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.2,
        help="Delay between requests",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=60,
        help="HTTP timeout in seconds",
    )
    args = parser.parse_args()

    if args.repo in ("dataverse_no", "all"):
        print("[INFO] Running DataverseNO pipeline...")
        dataverse_pipeline = DataverseNOPipeline(
            out_dir=SETTINGS.downloads_root / "dataverse_no",
            db_path=SETTINGS.db_path,
            queries=DEFAULT_QUERIES,
            qda_extensions=QDA_EXTENSIONS,
            associated_extensions=ASSOCIATED_EXTENSIONS,
            per_page=args.per_page,
            delay=args.delay,
            timeout=args.timeout,
        )
        dataverse_pipeline.run()

    if args.repo in ("icpsr", "all"):
        print("[INFO] Running ICPSR pipeline...")
        icpsr_pipeline = ICPSRPipeline(
            out_dir=SETTINGS.downloads_root / "icpsr",
            db_path=SETTINGS.db_path,
            queries=DEFAULT_QUERIES,
            qda_extensions=QDA_EXTENSIONS,
            associated_extensions=ASSOCIATED_EXTENSIONS,
        )
        icpsr_pipeline.run()

def main() -> None:
    parser = argparse.ArgumentParser(
        description="QDArchive acquisition runner"
    )
    parser.add_argument(
        "--repo",
        choices=["dataverse_no", "icpsr", "all"],
        default="all",
        help="Repository to run",
    )
    parser.add_argument(
        "--per-page",
        type=int,
        default=100,
        help="Results per page where supported",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.2,
        help="Delay between requests",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=60,
        help="HTTP timeout in seconds",
    )
    args = parser.parse_args()

    queries = DEFAULT_QUERIES

    if args.repo in ("dataverse_no", "all"):
        print("[INFO] Running DataverseNO pipeline...")
        dataverse_pipeline = DataverseNOPipeline(
            out_dir=SETTINGS.downloads_root / "dataverse_no",
            db_path=SETTINGS.db_path,
            queries=queries,
            qda_extensions=QDA_EXTENSIONS,
            associated_extensions=ASSOCIATED_EXTENSIONS,
            per_page=args.per_page,
            delay=args.delay,
            timeout=args.timeout,
        )
        dataverse_pipeline.run()

    if args.repo in ("icpsr", "all"):
        print("[INFO] Running ICPSR pipeline...")
        icpsr_pipeline = ICPSRPipeline(
            out_dir=SETTINGS.downloads_root / "icpsr",
            db_path=SETTINGS.db_path,
            queries=DEFAULT_QUERIES,
            qda_extensions=QDA_EXTENSIONS,
            associated_extensions=ASSOCIATED_EXTENSIONS,
            delay=args.delay,
            timeout=args.timeout,
        )
        icpsr_pipeline.run()


if __name__ == "__main__":
    main()