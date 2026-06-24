from __future__ import annotations

import csv
import json
import re
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from docx import Document
from pypdf import PdfReader


STUDENT_ID = "23071082"

DB_PATH = Path(f"{STUDENT_ID}-sq26-classification.db")
ISIC_CSV = Path("part2/isic_rev5_structure.csv")

METHOD = "rules_metadata_sampled_content_isic_rev5_v1"

# Reads only a small, controlled sample from the 144 GB archive.
MAX_CONTENT_FILES_PER_PROJECT = 4
MAX_TEXT_FILE_BYTES = 2_000_000
MAX_TEXT_CHARS_PER_FILE = 60_000
MAX_PDF_PAGES = 5

PARSABLE_EXTENSIONS = {
    ".txt",
    ".md",
    ".csv",
    ".tsv",
    ".tab",
    ".json",
    ".xml",
    ".yaml",
    ".yml",
    ".html",
    ".htm",
    ".rtf",
    ".pdf",
    ".docx",
}

PARSER_PRIORITY = {
    ".txt": 1,
    ".md": 1,
    ".csv": 2,
    ".tsv": 2,
    ".tab": 2,
    ".docx": 3,
    ".pdf": 4,
    ".rtf": 5,
    ".json": 6,
    ".xml": 6,
    ".html": 7,
    ".htm": 7,
    ".yaml": 8,
    ".yml": 8,
}

# Division-level rules. The full class names come from the official ISIC CSV.
# The classifier is transparent: every match is retained as evidence.
DIVISION_KEYWORDS: dict[str, dict[str, int]] = {
    "01": {
        "agricultur": 4, "crop": 4, "plant": 3, "soil": 3,
        "farm": 4, "livestock": 4, "grazing": 4, "fertilizer": 4,
        "clover": 3, "wheat": 3, "maize": 3, "pasture": 3,
    },
    "02": {
        "forest": 4, "forestry": 5, "tree": 3, "timber": 4,
        "woodland": 4, "logging": 4,
    },
    "03": {
        "fish": 4, "fisher": 4, "aquaculture": 5, "marine": 3,
        "seafood": 4, "fjord": 3,
    },
    "05": {
        "coal": 5, "mining": 5, "mine": 4, "ore": 4,
    },
    "07": {
        "iron ore": 5, "copper ore": 5, "metal ore": 5,
    },
    "10": {
        "food processing": 5, "dairy": 4, "meat production": 4,
        "beverage": 4, "food product": 4,
    },
    "20": {
        "chemical": 4, "chemistry": 4, "polymer": 4,
        "compound": 3, "coating": 3,
    },
    "21": {
        "pharmaceutical": 5, "drug formulation": 5, "medicine production": 4,
    },
    "26": {
        "semiconductor": 5, "electronic device": 4, "sensor hardware": 4,
    },
    "28": {
        "machinery": 4, "turbine": 4, "industrial equipment": 4,
    },
    "35": {
        "electricity": 4, "energy": 3, "heat pump": 5,
        "solar power": 5, "wind power": 5, "power generation": 5,
    },
    "36": {
        "water supply": 5, "drinking water": 4, "water treatment": 5,
    },
    "37": {
        "wastewater": 5, "sewerage": 5, "sewage": 5,
    },
    "38": {
        "waste management": 5, "recycling": 5, "solid waste": 4,
    },
    "41": {
        "construction": 5, "building": 4, "infrastructure construction": 5,
    },
    "49": {
        "traffic": 4, "road transport": 5, "vehicle": 3,
        "railway": 4, "transportation": 3,
    },
    "50": {
        "shipping": 5, "maritime transport": 5, "vessel": 4,
    },
    "51": {
        "aviation": 5, "air transport": 5, "aircraft": 4,
    },
    "55": {
        "tourism": 5, "hotel": 5, "accommodation": 4,
    },
    "58": {
        "publishing": 5, "publication": 3, "journalism": 4,
    },
    "59": {
        "film": 4, "video production": 5, "audio production": 5,
    },
    "61": {
        "telecommunication": 5, "telecom": 5, "mobile network": 4,
    },
    "62": {
        "software": 4, "programming": 5, "machine learning": 4,
        "artificial intelligence": 4, "computer vision": 4,
        "neural network": 4,
    },
    "63": {
        "database service": 5, "web platform": 5, "cloud service": 5,
        "data processing service": 5,
    },
    "64": {
        "banking": 5, "finance": 4, "financial market": 5,
        "investment": 4,
    },
    "65": {
        "insurance": 5, "risk insurance": 5,
    },
    "68": {
        "real estate": 5, "housing market": 4, "property market": 4,
    },
    "69": {
        "legal": 4, "law firm": 5, "accounting": 5, "audit": 4,
    },
    "70": {
        "management consulting": 5, "business strategy": 4,
        "organisational management": 4,
    },
    "71": {
        "engineering": 4, "technical testing": 5, "geological": 4,
        "architecture": 4,
    },
    "72": {
        "research": 3, "replication": 4, "experiment": 3,
        "laboratory": 3, "scientific study": 4, "study data": 3,
        "research data": 4, "methodology": 2, "dataset": 1,
    },
    "73": {
        "market research": 5, "consumer survey": 5, "advertising": 5,
    },
    "74": {
        "design service": 4, "photography": 5, "translation": 5,
    },
    "75": {
        "veterinary": 5, "animal clinic": 5,
    },
    "78": {
        "employment": 4, "recruitment": 5, "labour placement": 5,
    },
    "80": {
        "security service": 5, "surveillance service": 4,
    },
    "81": {
        "landscaping": 5, "cleaning service": 5,
    },
    "84": {
        "government": 4, "public administration": 5, "public policy": 5,
        "census": 4, "election": 4, "municipality": 4,
    },
    "85": {
        "education": 5, "school": 4, "university": 4,
        "student": 3, "teaching": 4, "learning": 4,
    },
    "86": {
        "health": 4, "hospital": 5, "patient": 4, "clinical": 5,
        "medical": 4, "nursing": 5, "disease": 3,
    },
    "87": {
        "residential care": 5, "elderly care": 5, "care home": 5,
    },
    "88": {
        "social work": 5, "social services": 5, "welfare": 4,
    },
    "90": {
        "art": 4, "music": 4, "cultural production": 5,
        "performing arts": 5,
    },
    "91": {
        "museum": 5, "library": 5, "archive": 4, "heritage": 4,
    },
    "93": {
        "sport": 4, "recreation": 5, "leisure": 4,
    },
    "94": {
        "association": 4, "religion": 4, "trade union": 5,
        "nonprofit": 4,
    },
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def normalize_text(value: str | None) -> str:
    text = (value or "").lower()
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def safe_file_path(folder: str | None, file_name: str | None) -> Path | None:
    if not folder or not file_name:
        return None
    return Path(folder) / Path(file_name).name


def get_extension(file_name: str | None, file_type: str | None) -> str:
    candidate = normalize_text(file_type)
    if candidate.startswith("."):
        return candidate
    return Path(file_name or "").suffix.lower()


def load_isic() -> tuple[dict[str, str], dict[str, str], dict[str, str]]:
    if not ISIC_CSV.exists():
        raise FileNotFoundError(
            f"ISIC CSV not found: {ISIC_CSV.resolve()}"
        )

    sections: dict[str, str] = {}
    divisions: dict[str, str] = {}
    division_section: dict[str, str] = {}

    current_section = ""

    with ISIC_CSV.open("r", encoding="latin1", newline="") as handle:
        reader = csv.DictReader(handle)

        for row in reader:
            code = (row.get("ISIC Rev 5 Code") or "").strip()
            title = (row.get("ISIC Rev 5 Title") or "").strip()

            if len(code) == 1 and code.isalpha():
                current_section = code
                sections[code] = title

            elif len(code) == 2 and code.isdigit():
                divisions[code] = title
                division_section[code] = current_section

    return sections, divisions, division_section


def phrase_present(text: str, phrase: str) -> bool:
    escaped = re.escape(phrase.lower())
    pattern = rf"(?<![a-z0-9]){escaped}(?![a-z0-9])"
    return re.search(pattern, text) is not None


def score_text(text: str) -> tuple[Counter[str], dict[str, list[str]]]:
    normalized = normalize_text(text)
    scores: Counter[str] = Counter()
    hits: dict[str, list[str]] = defaultdict(list)

    for division, rules in DIVISION_KEYWORDS.items():
        for phrase, weight in rules.items():
            if phrase_present(normalized, phrase):
                scores[division] += weight
                hits[division].append(phrase)

    return scores, hits


def combine_scores(
    metadata_text: str,
    file_name: str = "",
    content_text: str = "",
) -> tuple[Counter[str], dict[str, list[str]]]:
    combined: Counter[str] = Counter()
    evidence: dict[str, list[str]] = defaultdict(list)

    sources = [
        ("metadata", metadata_text, 1.0),
        ("filename", file_name, 1.2),
        ("content", content_text, 1.4),
    ]

    for source_name, text, multiplier in sources:
        if not text:
            continue

        scores, hits = score_text(text)

        for division, score in scores.items():
            combined[division] += score * multiplier

        for division, phrases in hits.items():
            for phrase in phrases:
                evidence[division].append(f"{source_name}:{phrase}")

    return combined, evidence


def choose_division(
    scores: Counter[str],
    evidence: dict[str, list[str]],
) -> tuple[str, list[str], float]:
    if not scores:
        return "72", ["fallback:research-archive-context"], 0.0

    # Prefer a specific non-M72 division when its score is tied with M72.
    ordered = sorted(
        scores.items(),
        key=lambda item: (
            -item[1],
            item[0] == "72",
            item[0],
        ),
    )

    division, score = ordered[0]
    return division, evidence.get(division, []), float(score)


def format_class(
    division: str,
    sections: dict[str, str],
    divisions: dict[str, str],
    division_section: dict[str, str],
) -> tuple[str, str]:
    section_code = division_section.get(division, "")
    section_name = sections.get(section_code, "Unknown section")
    division_name = divisions.get(division, "Unknown division")

    primary = f"{section_code} — {section_name}"
    secondary = f"{section_code}{division} — {division_name}"

    return primary, secondary


def decode_text(raw: bytes) -> str:
    for encoding in ("utf-8", "utf-8-sig", "cp1252", "latin1"):
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="ignore")


def extract_text(path: Path, extension: str) -> tuple[str, str]:
    if not path.exists():
        return "", "missing_local_file"

    try:
        size = path.stat().st_size
    except OSError:
        return "", "unreadable_file"

    if size > MAX_TEXT_FILE_BYTES:
        return "", "skipped_too_large"

    try:
        if extension in {
            ".txt", ".md", ".csv", ".tsv", ".tab",
            ".json", ".xml", ".yaml", ".yml",
            ".html", ".htm",
        }:
            with path.open("rb") as handle:
                raw = handle.read(MAX_TEXT_FILE_BYTES)

            return decode_text(raw)[:MAX_TEXT_CHARS_PER_FILE], "extracted_text"

        if extension == ".rtf":
            with path.open("rb") as handle:
                raw = handle.read(MAX_TEXT_FILE_BYTES)

            text = decode_text(raw)
            text = re.sub(r"\\[a-zA-Z]+\d* ?", " ", text)
            text = re.sub(r"[{}]", " ", text)

            return text[:MAX_TEXT_CHARS_PER_FILE], "extracted_rtf"

        if extension == ".docx":
            document = Document(path)
            text = "\n".join(
                paragraph.text
                for paragraph in document.paragraphs
                if paragraph.text.strip()
            )
            return text[:MAX_TEXT_CHARS_PER_FILE], "extracted_docx"

        if extension == ".pdf":
            reader = PdfReader(str(path))
            pages = reader.pages[:MAX_PDF_PAGES]
            text = "\n".join(
                page.extract_text() or ""
                for page in pages
            )
            return text[:MAX_TEXT_CHARS_PER_FILE], "extracted_pdf"

        return "", "unsupported_extension"

    except Exception as exc:
        return "", f"parser_failed:{type(exc).__name__}"


def select_content_files(
    primary_files: list[tuple[int, str, str]],
    project_folder: str | None,
) -> list[tuple[int, str, str, Path]]:
    candidates: list[tuple[int, int, str, str, Path]] = []

    for file_id, file_name, file_type in primary_files:
        extension = get_extension(file_name, file_type)

        if extension not in PARSABLE_EXTENSIONS:
            continue

        path = safe_file_path(project_folder, file_name)

        if path is None or not path.exists():
            continue

        try:
            size = path.stat().st_size
        except OSError:
            continue

        if size > MAX_TEXT_FILE_BYTES:
            continue

        priority = PARSER_PRIORITY.get(extension, 99)
        candidates.append((priority, size, file_id, extension, path))

    candidates.sort(key=lambda row: (row[0], row[1], row[2]))

    selected: list[tuple[int, str, str, Path]] = []

    for _, _, file_id, extension, path in candidates[:MAX_CONTENT_FILES_PER_PROJECT]:
        selected.append((file_id, extension, path.name, path))

    return selected


def add_tags(
    conn: sqlite3.Connection,
    project_id: int,
    primary_class: str,
    secondary_class: str,
) -> None:
    tags = {
        f"isic_section:{primary_class.split(' — ')[0]}",
        f"isic_division:{secondary_class.split(' — ')[0]}",
    }

    for tag in tags:
        conn.execute(
            """
            INSERT INTO PROJECT_TAGS (project_id, tag, source)
            VALUES (?, ?, ?)
            """,
            (project_id, tag, METHOD),
        )


def main() -> None:
    if not DB_PATH.exists():
        raise FileNotFoundError(
            f"Classification DB not found: {DB_PATH.resolve()}"
        )

    sections, divisions, division_section = load_isic()

    conn = sqlite3.connect(DB_PATH)

    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS CONTENT_EXTRACTION_LOG (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                project_id INTEGER NOT NULL,
                file_id INTEGER NOT NULL,
                file_name TEXT,
                parser_status TEXT NOT NULL,
                chars_extracted INTEGER NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )

        # Safe re-run behaviour.
        conn.execute(
            "DELETE FROM CONTENT_EXTRACTION_LOG"
        )
        conn.execute(
            "DELETE FROM CLASSIFICATION_RUNS WHERE method = ?",
            (METHOD,),
        )
        conn.execute(
            "DELETE FROM PROJECT_TAGS WHERE source = ?",
            (METHOD,),
        )

        conn.execute(
            """
            UPDATE PROJECTS
            SET
                primary_class = NULL,
                secondary_class = NULL,
                classification_evidence = NULL,
                classification_method = NULL
            """
        )

        conn.execute(
            """
            UPDATE FILES
            SET
                primary_class = NULL,
                secondary_class = NULL,
                classification_evidence = NULL,
                classification_method = NULL
            """
        )

        keyword_rows = conn.execute(
            """
            SELECT project_id, GROUP_CONCAT(keyword, ' ')
            FROM KEYWORDS
            GROUP BY project_id
            """
        ).fetchall()

        keywords_by_project = {
            project_id: keywords or ""
            for project_id, keywords in keyword_rows
        }

        project_rows = conn.execute(
            """
            SELECT
                id,
                repository_id,
                title,
                description,
                download_project_folder,
                type,
                is_canonical
            FROM PROJECTS
            ORDER BY id
            """
        ).fetchall()

        primary_file_rows = conn.execute(
            """
            SELECT
                id,
                project_id,
                file_name,
                file_type,
                status
            FROM FILES
            WHERE is_primary_data = 1
            ORDER BY project_id, id
            """
        ).fetchall()

        primary_files_by_project: dict[int, list[tuple[int, str, str]]] = defaultdict(list)

        for file_id, project_id, file_name, file_type, status in primary_file_rows:
            if normalize_text(status) != "succeeded":
                continue

            primary_files_by_project[project_id].append(
                (file_id, file_name or "", file_type or "")
            )

        project_counts: Counter[str] = Counter()
        file_classifications = 0
        extracted_files = 0
        parser_status_counts: Counter[str] = Counter()

        eligible_projects = [
            row for row in project_rows
            if row[5] in {"QDA_PROJECT", "QD_PROJECT"} and row[6] == 1
        ]

        for index, (
            project_id,
            repository_id,
            title,
            description,
            project_folder,
            project_type,
            is_canonical,
        ) in enumerate(eligible_projects, start=1):
            keywords = keywords_by_project.get(project_id, "")

            metadata_text = " ".join(
                part for part in [
                    title or "",
                    description or "",
                    keywords,
                ]
                if part
            )

            primary_files = primary_files_by_project.get(project_id, [])
            selected_files = select_content_files(primary_files, project_folder)

            extracted_by_file_id: dict[int, str] = {}
            project_content_parts: list[str] = []

            for file_id, extension, file_name, path in selected_files:
                text, parser_status = extract_text(path, extension)

                parser_status_counts[parser_status] += 1

                conn.execute(
                    """
                    INSERT INTO CONTENT_EXTRACTION_LOG (
                        project_id,
                        file_id,
                        file_name,
                        parser_status,
                        chars_extracted,
                        created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        project_id,
                        file_id,
                        file_name,
                        parser_status,
                        len(text),
                        utc_now(),
                    ),
                )

                if text:
                    extracted_files += 1
                    extracted_by_file_id[file_id] = text
                    project_content_parts.append(text)

            project_content = "\n".join(project_content_parts)

            project_scores, project_hits = combine_scores(
                metadata_text=metadata_text,
                content_text=project_content,
            )

            division, matched_terms, score = choose_division(
                project_scores,
                project_hits,
            )

            primary_class, secondary_class = format_class(
                division,
                sections,
                divisions,
                division_section,
            )

            evidence = {
                "project_type": project_type,
                "metadata_chars": len(metadata_text),
                "primary_files_total": len(primary_files),
                "content_files_sampled": len(selected_files),
                "content_files_extracted": len(extracted_by_file_id),
                "selected_division": division,
                "score": score,
                "matched_terms": matched_terms[:20],
            }

            evidence_text = json.dumps(
                evidence,
                ensure_ascii=False,
            )

            conn.execute(
                """
                UPDATE PROJECTS
                SET
                    primary_class = ?,
                    secondary_class = ?,
                    classification_evidence = ?,
                    classification_method = ?
                WHERE id = ?
                """,
                (
                    primary_class,
                    secondary_class,
                    evidence_text,
                    METHOD,
                    project_id,
                ),
            )

            add_tags(
                conn,
                project_id,
                primary_class,
                secondary_class,
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
                VALUES (?, NULL, ?, ?, ?, ?, ?, ?)
                """,
                (
                    project_id,
                    project_type,
                    primary_class,
                    secondary_class,
                    evidence_text,
                    METHOD,
                    utc_now(),
                ),
            )

            # Classify every primary data file.
            for file_id, file_name, file_type in primary_files:
                file_content = extracted_by_file_id.get(file_id, "")

                file_scores, file_hits = combine_scores(
                    metadata_text=metadata_text,
                    file_name=file_name,
                    content_text=file_content,
                )

                file_division, file_terms, file_score = choose_division(
                    file_scores,
                    file_hits,
                )

                file_primary, file_secondary = format_class(
                    file_division,
                    sections,
                    divisions,
                    division_section,
                )

                file_evidence = {
                    "project_id": project_id,
                    "file_name": file_name,
                    "content_used": bool(file_content),
                    "selected_division": file_division,
                    "score": file_score,
                    "matched_terms": file_terms[:20],
                }

                file_evidence_text = json.dumps(
                    file_evidence,
                    ensure_ascii=False,
                )

                conn.execute(
                    """
                    UPDATE FILES
                    SET
                        primary_class = ?,
                        secondary_class = ?,
                        classification_evidence = ?,
                        classification_method = ?
                    WHERE id = ?
                    """,
                    (
                        file_primary,
                        file_secondary,
                        file_evidence_text,
                        METHOD,
                        file_id,
                    ),
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
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        project_id,
                        file_id,
                        project_type,
                        file_primary,
                        file_secondary,
                        file_evidence_text,
                        METHOD,
                        utc_now(),
                    ),
                )

                file_classifications += 1

            project_counts[project_type] += 1

            if index % 10 == 0 or index == len(eligible_projects):
                print(
                    f"[PROGRESS] Classified {index}/{len(eligible_projects)} "
                    f"eligible projects"
                )

        conn.commit()

        print("\n[ISIC CLASSIFICATION SUMMARY]")
        print(f"Eligible projects classified: {len(eligible_projects)}")
        print(f"QDA_PROJECT classified: {project_counts['QDA_PROJECT']}")
        print(f"QD_PROJECT classified: {project_counts['QD_PROJECT']}")
        print(f"Primary data files classified: {file_classifications}")
        print(f"Content files successfully parsed: {extracted_files}")

        print("\n[CONTENT EXTRACTION STATUS]")
        for status, count in parser_status_counts.most_common():
            print(f"{status}: {count}")

        print(f"\n[OK] ISIC classification stored in: {DB_PATH}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()