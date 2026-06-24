from __future__ import annotations

import sqlite3
from pathlib import Path
from collections import Counter

STUDENT_ID = "23071082"
DB_PATH = Path(f"{STUDENT_ID}-sq26-classification.db")
OUTPUT_DIR = Path("part2/outputs")
OUTPUT_FILE = OUTPUT_DIR / "classification_audit.txt"

REPOSITORY_NAMES = {
    1: "DataverseNO",
    2: "ICPSR",
}


def repository_name(repository_id: int | None) -> str:
    return REPOSITORY_NAMES.get(repository_id, f"Repository {repository_id}")


def write_line(lines: list[str], text: str = "") -> None:
    print(text)
    lines.append(text)


def main() -> None:
    if not DB_PATH.exists():
        raise FileNotFoundError(f"Missing database: {DB_PATH.resolve()}")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    lines: list[str] = []

    try:
        write_line(lines, "=" * 78)
        write_line(lines, "PART 2 CLASSIFICATION AUDIT")
        write_line(lines, "=" * 78)

        total_projects = conn.execute(
            "SELECT COUNT(*) FROM PROJECTS"
        ).fetchone()[0]

        total_files = conn.execute(
            "SELECT COUNT(*) FROM FILES"
        ).fetchone()[0]

        classified_projects = conn.execute(
            """
            SELECT COUNT(*)
            FROM PROJECTS
            WHERE primary_class IS NOT NULL
            """
        ).fetchone()[0]

        classified_primary_files = conn.execute(
            """
            SELECT COUNT(*)
            FROM FILES
            WHERE is_primary_data = 1
              AND primary_class IS NOT NULL
            """
        ).fetchone()[0]

        parsed_files = conn.execute(
            """
            SELECT COUNT(*)
            FROM CONTENT_EXTRACTION_LOG
            WHERE chars_extracted > 0
            """
        ).fetchone()[0]

        write_line(lines, f"Total projects: {total_projects}")
        write_line(lines, f"Total files: {total_files}")
        write_line(lines, f"Projects with ISIC classification: {classified_projects}")
        write_line(lines, f"Primary files with ISIC classification: {classified_primary_files}")
        write_line(lines, f"Files with extracted content: {parsed_files}")

        write_line(lines, "\n" + "=" * 78)
        write_line(lines, "PROJECT TYPE DISTRIBUTION BY REPOSITORY")
        write_line(lines, "=" * 78)

        rows = conn.execute(
            """
            SELECT repository_id, type, COUNT(*) AS count
            FROM PROJECTS
            GROUP BY repository_id, type
            ORDER BY repository_id, type
            """
        ).fetchall()

        for repository_id, project_type, count in rows:
            write_line(
                lines,
                f"{repository_name(repository_id)} | {project_type}: {count}",
            )

        write_line(lines, "\n" + "=" * 78)
        write_line(lines, "PROJECT-LEVEL PRIMARY CLASSES BY REPOSITORY")
        write_line(lines, "=" * 78)

        repositories = conn.execute(
            """
            SELECT DISTINCT repository_id
            FROM PROJECTS
            ORDER BY repository_id
            """
        ).fetchall()

        for (repository_id,) in repositories:
            write_line(lines, f"\n{repository_name(repository_id)}")

            class_rows = conn.execute(
                """
                SELECT primary_class, COUNT(*) AS count
                FROM PROJECTS
                WHERE repository_id = ?
                  AND is_canonical = 1
                  AND primary_class IS NOT NULL
                GROUP BY primary_class
                ORDER BY count DESC, primary_class
                """,
                (repository_id,),
            ).fetchall()

            if not class_rows:
                write_line(lines, "  No classified QD/QDA projects.")
                continue

            for primary_class, count in class_rows:
                write_line(lines, f"  {count:>4} | {primary_class}")

        write_line(lines, "\n" + "=" * 78)
        write_line(lines, "PROJECT-LEVEL DIVISIONS BY REPOSITORY — TOP 20")
        write_line(lines, "=" * 78)

        for (repository_id,) in repositories:
            write_line(lines, f"\n{repository_name(repository_id)}")

            division_rows = conn.execute(
                """
                SELECT secondary_class, COUNT(*) AS count
                FROM PROJECTS
                WHERE repository_id = ?
                  AND is_canonical = 1
                  AND secondary_class IS NOT NULL
                GROUP BY secondary_class
                ORDER BY count DESC, secondary_class
                LIMIT 20
                """,
                (repository_id,),
            ).fetchall()

            if not division_rows:
                write_line(lines, "  No classified QD/QDA projects.")
                continue

            for secondary_class, count in division_rows:
                write_line(lines, f"  {count:>4} | {secondary_class}")

        write_line(lines, "\n" + "=" * 78)
        write_line(lines, "TOP PRIMARY FILE-LEVEL DIVISIONS BY REPOSITORY")
        write_line(lines, "=" * 78)

        for (repository_id,) in repositories:
            write_line(lines, f"\n{repository_name(repository_id)}")

            file_rows = conn.execute(
                """
                SELECT f.secondary_class, COUNT(*) AS count
                FROM FILES f
                JOIN PROJECTS p ON p.id = f.project_id
                WHERE p.repository_id = ?
                  AND p.is_canonical = 1
                  AND f.is_primary_data = 1
                  AND f.secondary_class IS NOT NULL
                GROUP BY f.secondary_class
                ORDER BY count DESC, f.secondary_class
                LIMIT 10
                """,
                (repository_id,),
            ).fetchall()

            if not file_rows:
                write_line(lines, "  No classified primary files.")
                continue

            for secondary_class, count in file_rows:
                write_line(lines, f"  {count:>6} | {secondary_class}")

        write_line(lines, "\n" + "=" * 78)
        write_line(lines, "UNCLASSIFIED ELIGIBLE PROJECT CHECK")
        write_line(lines, "=" * 78)

        missing_rows = conn.execute(
            """
            SELECT id, repository_id, title, type
            FROM PROJECTS
            WHERE type IN ('QDA_PROJECT', 'QD_PROJECT')
              AND primary_class IS NULL
            ORDER BY repository_id, id
            """
        ).fetchall()

        write_line(
            lines,
            f"Eligible projects without ISIC class: {len(missing_rows)}",
        )

        for project_id, repository_id, title, project_type in missing_rows[:20]:
            write_line(
                lines,
                f"  id={project_id} | {repository_name(repository_id)} | "
                f"{project_type} | {title}",
            )

        write_line(lines, "\n" + "=" * 78)
        write_line(lines, "CONTENT EXTRACTION STATUS")
        write_line(lines, "=" * 78)

        extraction_rows = conn.execute(
            """
            SELECT parser_status, COUNT(*) AS count
            FROM CONTENT_EXTRACTION_LOG
            GROUP BY parser_status
            ORDER BY count DESC, parser_status
            """
        ).fetchall()

        for parser_status, count in extraction_rows:
            write_line(lines, f"{count:>4} | {parser_status}")

        OUTPUT_FILE.write_text("\n".join(lines), encoding="utf-8")

        print(f"\n[OK] Audit written to: {OUTPUT_FILE}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()