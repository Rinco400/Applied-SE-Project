from __future__ import annotations

import sqlite3
import textwrap
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.backends.backend_pdf import PdfPages


STUDENT_ID = "23071082"

DB_PATH = Path(f"{STUDENT_ID}-sq26-classification.db")
OUTPUT_DIR = Path("part2/outputs")

XLSX_PATH = OUTPUT_DIR / f"{STUDENT_ID}-classification-results.xlsx"
PDF_PATH = OUTPUT_DIR / f"{STUDENT_ID}-classification-report.pdf"

REPOSITORIES = {
    1: "DataverseNO",
    2: "ICPSR",
}


def sanitize(value: object) -> str:
    """Avoid problematic long dash glyphs and normalize blank values."""
    if pd.isna(value):
        return ""
    return str(value).replace("—", "-").strip()


def wrap_label(value: object, width: int = 42) -> str:
    return "\n".join(textwrap.wrap(sanitize(value), width=width))


def repo_name(repository_id: int) -> str:
    return REPOSITORIES.get(repository_id, f"Repository {repository_id}")


def scalar(conn: sqlite3.Connection, query: str) -> int:
    return int(conn.execute(query).fetchone()[0])


def get_data(conn: sqlite3.Connection) -> tuple[pd.DataFrame, pd.DataFrame]:
    results = pd.read_sql_query(
        """
        SELECT
            repository_id,
            type AS project_type,
            title AS project_title,
            primary_class,
            secondary_class,
            COALESCE(no_project_files, 0) AS no_project_files
        FROM PROJECTS
        WHERE COALESCE(is_canonical, 1) = 1
        ORDER BY repository_id, project_title
        """,
        conn,
    )

    classified = results[
        results["primary_class"].notna()
        & (results["primary_class"].astype(str).str.strip() != "")
    ].copy()

    return results, classified


def write_excel(
    results: pd.DataFrame,
    classified: pd.DataFrame,
    project_type_summary: pd.DataFrame,
    top_classes: pd.DataFrame,
    metrics: pd.DataFrame,
) -> None:
    required_columns = [
        "repository_id",
        "project_type",
        "project_title",
        "primary_class",
        "secondary_class",
        "no_project_files",
    ]

    results = results[required_columns].copy()

    with pd.ExcelWriter(XLSX_PATH, engine="xlsxwriter") as writer:
        results.to_excel(
            writer,
            sheet_name="Classification Results",
            index=False,
        )
        metrics.to_excel(writer, sheet_name="Summary", index=False)
        project_type_summary.to_excel(
            writer,
            sheet_name="Project Types",
            index=False,
        )
        top_classes.to_excel(
            writer,
            sheet_name="Top Primary Classes",
            index=False,
        )

        workbook = writer.book
        header_format = workbook.add_format(
            {
                "bold": True,
                "text_wrap": True,
                "valign": "top",
            }
        )
        text_format = workbook.add_format(
            {
                "text_wrap": True,
                "valign": "top",
            }
        )

        for sheet_name, dataframe in {
            "Classification Results": results,
            "Summary": metrics,
            "Project Types": project_type_summary,
            "Top Primary Classes": top_classes,
        }.items():
            worksheet = writer.sheets[sheet_name]
            worksheet.freeze_panes(1, 0)

            for col_index, column_name in enumerate(dataframe.columns):
                worksheet.write(0, col_index, column_name, header_format)

                values = dataframe[column_name].fillna("").astype(str)
                max_length = max(
                    len(str(column_name)),
                    values.map(len).max() if not values.empty else 0,
                )

                width = min(max(max_length + 2, 14), 55)
                worksheet.set_column(col_index, col_index, width, text_format)

            worksheet.set_row(0, 32)


def add_title_page(pdf: PdfPages, metrics: pd.DataFrame) -> None:
    fig = plt.figure(figsize=(11.69, 8.27))
    fig.text(
        0.5,
        0.82,
        "QDArchive - Part 2: Data Classification",
        ha="center",
        va="center",
        fontsize=22,
        fontweight="bold",
    )
    fig.text(
        0.5,
        0.74,
        "Seeding QDArchive - FAU Erlangen",
        ha="center",
        va="center",
        fontsize=14,
    )
    fig.text(
        0.5,
        0.69,
        f"Student ID: {STUDENT_ID}",
        ha="center",
        va="center",
        fontsize=12,
    )

    metric_lines = [
        f"{row['Metric']}: {row['Value']}"
        for _, row in metrics.iterrows()
    ]

    fig.text(
        0.15,
        0.55,
        "Classification Summary",
        fontsize=15,
        fontweight="bold",
    )
    fig.text(
        0.15,
        0.50,
        "\n".join(metric_lines),
        fontsize=11,
        va="top",
        linespacing=1.6,
    )

    fig.text(
        0.15,
        0.22,
        (
            "Method: transparent rules based on project metadata, file names, "
            "file extensions, archive-member inspection, and bounded content "
            "sampling for parsable files. ISIC Rev. 5 was assigned at section "
            "and division level."
        ),
        fontsize=10,
        va="top",
        wrap=True,
    )

    pdf.savefig(fig)
    plt.close(fig)


def add_method_page(pdf: PdfPages) -> None:
    fig = plt.figure(figsize=(11.69, 8.27))
    fig.text(
        0.08,
        0.92,
        "Methodology",
        fontsize=18,
        fontweight="bold",
    )

    content = [
        "1. Project-type classification used the required cascade:",
        "   QDA_PROJECT -> QD_PROJECT -> OTHER_PROJECT -> NOT_A_PROJECT.",
        "",
        "2. QDA formats included REFI/QDPX, NVivo, ATLAS.ti, and MAXQDA project extensions.",
        "",
        "3. QD_PROJECT classification used likely primary qualitative data formats, such as TXT, PDF, DOCX, RTF, audio, video, and image formats.",
        "",
        "4. ZIP archives were inspected through their central directory without extracting their content.",
        "",
        "5. ISIC Rev. 5 classification used project title, description, keywords, file names, and a bounded sample of parsable file content.",
        "",
        "6. Statistics in this report use canonical project-level results. File-level totals are retained in the database but are not used for the main charts because a few projects contain many thousands of files.",
    ]

    fig.text(
        0.10,
        0.84,
        "\n".join(content),
        fontsize=11,
        va="top",
        linespacing=1.7,
    )

    pdf.savefig(fig)
    plt.close(fig)


def add_project_type_page(
    pdf: PdfPages,
    project_type_summary: pd.DataFrame,
) -> None:
    fig, ax = plt.subplots(figsize=(11.69, 8.27))

    display = project_type_summary.copy()
    display["label"] = (
        display["repository_name"]
        + " - "
        + display["project_type"]
    )

    bars = ax.barh(display["label"], display["count"])

    ax.set_title("Project Type Distribution by Repository", fontsize=16)
    ax.set_xlabel("Number of canonical projects")

    for bar, count in zip(bars, display["count"]):
        ax.text(
            bar.get_width() + 0.3,
            bar.get_y() + bar.get_height() / 2,
            str(int(count)),
            va="center",
            fontsize=10,
        )

    ax.set_xlim(0, max(display["count"].max() * 1.15, 5))
    fig.tight_layout()

    pdf.savefig(fig)
    plt.close(fig)


def add_histogram(
    pdf: PdfPages,
    repository_id: int,
    class_counts: pd.DataFrame,
) -> None:
    name = repo_name(repository_id)

    fig, ax = plt.subplots(figsize=(11.69, 8.27))

    if class_counts.empty:
        ax.text(
            0.5,
            0.5,
            "No QDA/QD projects received an ISIC class for this repository.",
            ha="center",
            va="center",
            fontsize=14,
        )
        ax.axis("off")
    else:
        labels = [
            wrap_label(value, 48)
            for value in class_counts["primary_class"]
        ]

        bars = ax.barh(labels, class_counts["count"])
        ax.invert_yaxis()

        ax.set_title(
            f"{name} - Primary ISIC Classes (Project Level)",
            fontsize=16,
        )
        ax.set_xlabel("Number of canonical projects")

        for bar, count in zip(bars, class_counts["count"]):
            ax.text(
                bar.get_width() + 0.25,
                bar.get_y() + bar.get_height() / 2,
                str(int(count)),
                va="center",
                fontsize=10,
            )

        ax.set_xlim(0, max(class_counts["count"].max() * 1.18, 3))

    fig.tight_layout()
    pdf.savefig(fig)
    plt.close(fig)


def repository_comments(
    repository_id: int,
    results: pd.DataFrame,
    class_counts: pd.DataFrame,
) -> list[str]:
    name = repo_name(repository_id)
    repo_results = results[results["repository_id"] == repository_id]

    total = len(repo_results)
    qda = int((repo_results["project_type"] == "QDA_PROJECT").sum())
    qd = int((repo_results["project_type"] == "QD_PROJECT").sum())
    other = int((repo_results["project_type"] == "OTHER_PROJECT").sum())
    not_project = int((repo_results["project_type"] == "NOT_A_PROJECT").sum())

    comments = [
        f"{name} contains {total} canonical projects.",
        f"Project types: {qda} QDA_PROJECT, {qd} QD_PROJECT, "
        f"{other} OTHER_PROJECT, and {not_project} NOT_A_PROJECT.",
    ]

    if not class_counts.empty:
        top = class_counts.iloc[0]
        comments.append(
            f"The dominant primary class is {sanitize(top['primary_class'])} "
            f"with {int(top['count'])} classified projects."
        )

    if qda == 0:
        comments.append(
            "No confirmed QDA software-project file was found in this repository."
        )

    if not_project > 0:
        comments.append(
            "NOT_A_PROJECT records had no successful file evidence sufficient for "
            "project-type derivation."
        )

    return comments


def add_rank_table_page(
    pdf: PdfPages,
    repository_id: int,
    class_counts: pd.DataFrame,
    results: pd.DataFrame,
) -> None:
    name = repo_name(repository_id)
    top = class_counts.head(20).copy()

    fig, ax = plt.subplots(figsize=(11.69, 8.27))
    ax.axis("off")

    fig.text(
        0.08,
        0.92,
        f"{name} - Rank-Ordered Primary ISIC Classes",
        fontsize=17,
        fontweight="bold",
    )

    if top.empty:
        fig.text(
            0.08,
            0.82,
            "No classified QDA/QD projects are available for this repository.",
            fontsize=12,
        )
    else:
        table_rows = [
            [
                rank,
                wrap_label(primary_class, 55),
                int(count),
            ]
            for rank, (primary_class, count) in enumerate(
                zip(top["primary_class"], top["count"]),
                start=1,
            )
        ]

        table = ax.table(
            cellText=table_rows,
            colLabels=["Rank", "Primary ISIC Class", "Projects"],
            colWidths=[0.08, 0.72, 0.15],
            cellLoc="left",
            loc="upper left",
            bbox=[0.07, 0.36, 0.86, 0.48],
        )

        table.auto_set_font_size(False)
        table.set_fontsize(9)
        table.scale(1, 1.4)

    comments = repository_comments(
        repository_id,
        results,
        class_counts,
    )

    fig.text(
        0.08,
        0.28,
        "Comments on Findings",
        fontsize=13,
        fontweight="bold",
    )
    fig.text(
        0.10,
        0.23,
        "\n".join(f"- {comment}" for comment in comments),
        fontsize=10,
        va="top",
        linespacing=1.7,
    )

    pdf.savefig(fig)
    plt.close(fig)


def add_data_challenges_page(
    pdf: PdfPages,
    results: pd.DataFrame,
) -> None:
    total = len(results)
    qda = int((results["project_type"] == "QDA_PROJECT").sum())
    qd = int((results["project_type"] == "QD_PROJECT").sum())
    other = int((results["project_type"] == "OTHER_PROJECT").sum())
    not_project = int((results["project_type"] == "NOT_A_PROJECT").sum())

    fig = plt.figure(figsize=(11.69, 8.27))
    fig.text(
        0.08,
        0.92,
        "Technical Challenges with the Data",
        fontsize=18,
        fontweight="bold",
    )

    challenges = [
        (
            "1. Lack of QDA analysis files",
            f"No QDA project file was found among {total} canonical projects. "
            "This limits the ability to identify projects containing coding "
            "structures, memos, categories, and other QDA-specific artefacts."
        ),
        (
            "2. Ambiguous generic file extensions",
            "TXT, PDF, CSV, XLSX, and ZIP formats do not prove that a project "
            "contains qualitative research material. For example, text files "
            "may be interview transcripts, GPS logs, software output, or "
            "scientific measurement records."
        ),
        (
            "3. Incomplete accessible evidence",
            f"{not_project} projects were classified as NOT_A_PROJECT because "
            "no successfully downloaded file evidence was available for "
            "reliable project-type derivation."
        ),
        (
            "4. Archive and compound datasets",
            "ZIP files can contain mixed project content. Archive-member inspection "
            "helps, but the full meaning of archived data cannot always be inferred "
            "without extracting and reviewing all files."
        ),
        (
            "5. Uneven repository coverage",
            "DataverseNO supplied most QD projects, while ICPSR contributed fewer "
            "accessible projects and more records without downloadable file evidence. "
            "Repository-level statistics should therefore not be interpreted as a "
            "general representation of all qualitative research."
        ),
        (
            "6. File-level volume imbalance",
            "Some projects contain very large numbers of similar files. Therefore, "
            "the report uses project-level ISIC distributions to avoid allowing "
            "one large collection to dominate the descriptive statistics."
        ),
    ]

    y_position = 0.84
    for title, body in challenges:
        fig.text(
            0.10,
            y_position,
            title,
            fontsize=11,
            fontweight="bold",
            va="top",
        )
        fig.text(
            0.12,
            y_position - 0.035,
            textwrap.fill(body, width=120),
            fontsize=9.5,
            va="top",
        )
        y_position -= 0.125

    fig.text(
        0.10,
        0.08,
        (
            f"Observed project-type totals: {qda} QDA_PROJECT, {qd} QD_PROJECT, "
            f"{other} OTHER_PROJECT, {not_project} NOT_A_PROJECT."
        ),
        fontsize=10,
        fontweight="bold",
    )

    pdf.savefig(fig)
    plt.close(fig)


def add_conclusion_page(
    pdf: PdfPages,
    results: pd.DataFrame,
    metrics: pd.DataFrame,
) -> None:
    fig = plt.figure(figsize=(11.69, 8.27))

    fig.text(
        0.08,
        0.92,
        "Conclusion",
        fontsize=18,
        fontweight="bold",
    )

    total = len(results)
    classified = int(results["primary_class"].notna().sum())
    qd = int((results["project_type"] == "QD_PROJECT").sum())

    conclusion = [
        (
            f"The classification database contains {total} canonical projects. "
            f"{classified} QDA/QD projects received an ISIC Rev. 5 section and "
            "division classification."
        ),
        (
            f"{qd} projects were identified as QD_PROJECT, but no confirmed "
            "QDA_PROJECT was found. This supports the Part 1 observation that "
            "public repositories rarely expose QDA software project files."
        ),
        (
            "Scientific research and development is the dominant ISIC division "
            "in the collected project-level dataset. This result reflects the "
            "repository collection and search coverage, and should not be treated "
            "as a general estimate for all qualitative research."
        ),
        (
            "The generated XLSX provides the requested simple classification table, "
            "while the SQLite database preserves detailed type, file, archive, "
            "content-extraction, and classification-evidence information."
        ),
    ]

    fig.text(
        0.10,
        0.82,
        "\n\n".join(textwrap.fill(item, width=120) for item in conclusion),
        fontsize=11,
        va="top",
        linespacing=1.6,
    )

    pdf.savefig(fig)
    plt.close(fig)


def create_pdf(
    results: pd.DataFrame,
    classified: pd.DataFrame,
    project_type_summary: pd.DataFrame,
    metrics: pd.DataFrame,
) -> None:
    with PdfPages(PDF_PATH) as pdf:
        add_title_page(pdf, metrics)
        add_method_page(pdf)
        add_project_type_page(pdf, project_type_summary)

        for repository_id in sorted(results["repository_id"].dropna().unique()):
            repo_classes = (
                classified[classified["repository_id"] == repository_id]
                .groupby("primary_class")
                .size()
                .reset_index(name="count")
                .sort_values(
                    ["count", "primary_class"],
                    ascending=[False, True],
                )
            )

            add_histogram(pdf, int(repository_id), repo_classes)
            add_rank_table_page(
                pdf,
                int(repository_id),
                repo_classes,
                results,
            )

        add_data_challenges_page(pdf, results)
        add_conclusion_page(pdf, results, metrics)


def main() -> None:
    if not DB_PATH.exists():
        raise FileNotFoundError(f"Missing database: {DB_PATH.resolve()}")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(DB_PATH)

    try:
        results, classified = get_data(conn)

        total_projects = len(results)
        total_files = scalar(conn, "SELECT COUNT(*) FROM FILES")
        qda_projects = int((results["project_type"] == "QDA_PROJECT").sum())
        qd_projects = int((results["project_type"] == "QD_PROJECT").sum())
        other_projects = int((results["project_type"] == "OTHER_PROJECT").sum())
        not_projects = int((results["project_type"] == "NOT_A_PROJECT").sum())

        parsed_files = scalar(
            conn,
            """
            SELECT COUNT(*)
            FROM CONTENT_EXTRACTION_LOG
            WHERE chars_extracted > 0
            """,
        )

        duplicate_projects = scalar(
            conn,
            "SELECT COUNT(*) FROM PROJECT_DEDUPLICATION",
        )

        metrics = pd.DataFrame(
            [
                ["Canonical projects", total_projects],
                ["Files in classification database", total_files],
                ["QDA_PROJECT", qda_projects],
                ["QD_PROJECT", qd_projects],
                ["OTHER_PROJECT", other_projects],
                ["NOT_A_PROJECT", not_projects],
                ["Projects with ISIC classification", len(classified)],
                ["Parsable files with extracted content", parsed_files],
                ["Duplicate mappings", duplicate_projects],
            ],
            columns=["Metric", "Value"],
        )

        project_type_summary = (
            results.groupby(["repository_id", "project_type"])
            .size()
            .reset_index(name="count")
            .sort_values(["repository_id", "project_type"])
        )
        project_type_summary.insert(
            1,
            "repository_name",
            project_type_summary["repository_id"].map(repo_name),
        )

        top_classes_frames = []
        for repository_id in sorted(results["repository_id"].dropna().unique()):
            repo_top = (
                classified[classified["repository_id"] == repository_id]
                .groupby("primary_class")
                .size()
                .reset_index(name="count")
                .sort_values(
                    ["count", "primary_class"],
                    ascending=[False, True],
                )
                .head(20)
            )

            repo_top.insert(0, "repository_id", repository_id)
            repo_top.insert(1, "repository_name", repo_name(int(repository_id)))
            repo_top.insert(
                2,
                "rank",
                range(1, len(repo_top) + 1),
            )
            top_classes_frames.append(repo_top)

        top_classes = (
            pd.concat(top_classes_frames, ignore_index=True)
            if top_classes_frames
            else pd.DataFrame(
                columns=[
                    "repository_id",
                    "repository_name",
                    "rank",
                    "primary_class",
                    "count",
                ]
            )
        )

        write_excel(
            results,
            classified,
            project_type_summary,
            top_classes,
            metrics,
        )

        create_pdf(
            results,
            classified,
            project_type_summary,
            metrics,
        )

        print("[OK] Generated deliverables:")
        print(f"  XLSX: {XLSX_PATH}")
        print(f"  PDF:  {PDF_PATH}")
        print(f"  DB:   {DB_PATH}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()