![License](https://img.shields.io/badge/license-MIT-green)
![Status](https://img.shields.io/badge/status-validated-brightgreen)
![Validation](https://img.shields.io/badge/SQ26-10%2F10%20PASS-success)

# QDArchive — Part 1: Data Acquisition
---

# 📌 Project Overview

This project is part of the *Seeding QDArchive* seminar at FAU Erlangen.  
The goal is to automatically collect and store qualitative research data from public repositories to seed the upcoming **QDArchive** platform.

The system:

- Searches repositories using multiple qualitative research queries  
- Downloads datasets and associated files  
- Stores structured metadata in SQLite  
- Logs failures for transparency  

---

# 📂 Assigned Repositories

| # | Repository | URL | Method |
|--|------------|-----|--------|
| 1 | DataverseNO | https://dataverse.no | API |
| 2 | ICPSR | https://www.icpsr.umich.edu | HTML + Seed-based scraping |

---

# 🔍 Search Queries Used

The following queries were used to maximize qualitative dataset discovery:
- qdpx
- nvivo
- nvpx
- maxqda
- atlas.ti
- atlproj
- qualitative data
- qualitative research
- interview study
- interview transcript
- focus group
- focus group data
- oral history
- transcript
- coded interview
- thematic analysis
- ethnography
- case study
- field notes
- qualitative dataset


---

# ⚙️ Approach

## DataverseNO

- Used official REST API
- Iterated through search results using pagination
- Extracted metadata from:
  - `latestVersion`
  - `metadataBlocks`
- Downloaded:
  - QDA files (if available)
  - Associated files (PDF, TXT, CSV, ZIP, etc.)

---

## ICPSR

Due to technical limitations:

- Standard scraping **did not work**
- ICPSR uses **JavaScript-based rendering**
- No accessible public API

### Solution:
- Used **seed-based crawling**
- Manually provided study URLs
- Extracted:
  - metadata
  - documentation files
  - available downloads

---

## 📊 Final Results Summary

### 📦 Overall Data Acquisition

| Metric | Value |
|------|------|
| Total queries executed | 20 |
| Total dataset hits (raw) | ~11,000+ |
| Total unique datasets processed | ~100–200+ |
| Total QDA files found | 0 |
| Total associated files downloaded | ~1,500+ |
| Total failures logged | ~400–500 |
| Total data downloaded | **144 GB** |

---

### 📁 Repository-wise Results

| Repository | Datasets Processed | QDA Files | Associated Files | Failures |
|-----------|------------------|----------|------------------|----------|
| DataverseNO | ~100+ | 0 | ~1,500+ | ~400+ |
| ICPSR | ~3–60 | 0 | ~10–100 | ~20 |
| **Total** | ~150+ | **0** | **~1,600+** | **~420+** |

---

### 📂 File Types Downloaded

| File Type | Description |
|----------|------------|
| `.txt` | Logs, transcripts, raw text data |
| `.csv` | Tabular and survey data |
| `.xlsx` | Structured datasets |
| `.pdf` | Documentation and reports |
| `.zip` | Complete dataset packages |
| `.xml` | Metadata files |
| `.json` | Structured data |

---

### 🎯 QDA File Availability

| Target File Types | Result |
|-----------------|--------|
| `.qdpx`, `.nvpx`, `.maxqda`, `.atlproj`, etc. | **0 found** |

---

### ⚠️ Failure Summary

| Failure Type | Reason |
|-------------|--------|
| Login required | Restricted datasets |
| 404 Not Found | Broken file links |
| Server errors | Temporary API / server issues |
| Non-QDA datasets | Scientific datasets without qualitative data |

---

### 💾 Storage Summary

| Category | Value |
|---------|------|
| Total storage used | **144 GB** |
| Download location | `my_downloads/` |
| Structure | Repository-wise folder separation |

---

### 🚀 System Performance

| Feature | Status |
|--------|--------|
| Automated scraping | ✅ |
| API integration | ✅ |
| Duplicate control | ✅ |
| Error logging | ✅ |
| Large-scale download handling | ✅ |

---

### 🧠 Key Findings

| Observation | Insight |
|------------|--------|
| No QDA files found | Major research gap |
| Large number of associated files | Repositories store raw data, not analysis |
| Dataverse data | Mostly scientific datasets |
| ICPSR access | Limited without login |

---

# 📁 File Types Downloaded

### QDA Files (target)
- None found (0)

### Associated Files
- `.txt`
- `.pdf`
- `.csv`
- `.xlsx`
- `.zip`
- `.xml`
- `.json`

---

# 🗄️ Database Structure

SQLite database: `metadata.db`

## Tables

### acquisitions
Stores downloaded files

### failures
Stores failed downloads and reasons

---

## Validation

The submission database file is:
23071082-seeding.db

This database was validated using the official SQ26 grading tool:
https://github.com/riehlegroup/sq26-grading

Validation result:
10 passed, 0 warnings, 0 errors

The database fully complies with all required schema and value constraints and is ready for submission.

# ⚠️ Technical Challenges (DATA)

## 1. No QDA files available
- No `.qdpx`, `.nvpx`, `.maxqda` found
- Indicates real-world problem:
  → researchers do not publish analysis files

---

## 2. Dataverse datasets mostly scientific
- Many datasets are:
  - physics
  - biology
  - chemistry

→ not qualitative research

---

## 3. ICPSR search is not accessible
- Uses JavaScript rendering
- HTML scraping returns empty results

---

## 4. Missing metadata
- Some datasets:
  - no authors
  - no license
  - incomplete fields

---

## 5. File access issues
- Some files:
  - return 404
  - require login
  - restricted access

---

# ⚠️ Technical Challenges (PROGRAMMING)

## 1. Duplicate downloads
Problem:
- Same file appeared in multiple queries

Solution:
- Added:
  - `UNIQUE(file_url)`
  - `exists_file_url()` check
  - URL normalization

---

## 2. SQLite insertion error
- Error binding parameter: type 'dict' not supported

Solution:
- Converted all values to string before DB insert
---

## 3. TLS / certificate error
- invalid path: certifi cacert.pem

Solution:
- Fixed environment issue / reinstalled dependencies

---

## 4. Dataverse API inconsistency
- Some datasets:
  - `latestVersion` missing
  - files empty

Solution:
- Added fallback logic

---

## 5. ICPSR scraping failure
Problem:
- No results from search

Solution:
- Implemented **seed-based crawling**

---

# 🧠 Key Findings

## 1. QDA files are extremely rare
→ major gap in research data sharing

## 2. Qualitative data is often:
- hidden
- restricted
- incomplete

## 3. Current repositories are not optimized for:
- qualitative analysis reuse
- structured QDA storage

---

# 🚀 Improvements Made

- Duplicate control (file-level)
- Robust error logging
- Multi-query search system
- Dataset deduplication
- Clean database schema

---

## 📁 Project Structure

```text
Applied-SE-Project/
│
├── connectors/
│   ├── __init__.py
│   ├── dataverse_no_pipeline.py        # DataverseNO acquisition pipeline
│   └── icpsr_pipeline.py               # ICPSR acquisition pipeline
│
├── core/
│   ├── __init__.py
│   ├── config.py                       # Project configuration
│   ├── db.py                           # SQLite database operations
│   ├── downloader.py                   # File download logic
│   └── folder_manager.py               # Download-folder management
│
├── my_downloads/                       # Downloaded datasets; excluded from Git
│   ├── dataverse_no/
│   └── icpsr/
│
├── part2/
│   ├── outputs/
│   │   ├── 23071082-classification-report.pdf
│   │   ├── 23071082-classification-results.xlsx
│   │   └── classification_audit.txt
│   │
│   ├── audit_classification.py          # Validates and summarizes Part 2 results
│   ├── classify_isic.py                 # ISIC Rev. 5 section/division classifier
│   ├── classify_project_types.py        # QDA/QD/OTHER/NOT_A project classifier
│   ├── generate_outputs.py              # Generates final XLSX and PDF report
│   ├── isic_rev5_structure.csv          # Official ISIC Rev. 5 taxonomy structure
│   └── prepare_classification_db.py     # Creates Part 2 DB from Part 1 DB
│
├── sq26-grading/                        # Official Part 1 validation tool
│
├── 23071082-seeding.db                  # Validated Part 1 submission database
├── 23071082-sq26-classification.db      # Part 2 classification database
├── convert_to_submission.py             # Converts metadata DB into Part 1 schema
├── icpsr_seed_urls.txt                  # ICPSR fallback seed-study URLs
├── LICENSE                              # Repository code licence
├── metadata.db                          # Working acquisition database; excluded from Git
├── README.md                            # Project documentation
├── requirements.txt                     # Python dependencies
├── run.py                               # Main Part 1 acquisition entry point
└── .gitignore                           # Excludes environments, raw data, working DB
```


## ▶️ Usage

```bash
python run.py --repo dataverse_no
python run.py --repo icpsr
python run.py --repo all

```

## 📌 Conclusion
- Data acquisition pipeline works successfully
- Large number of datasets processed
- Significant number of files collected
- No QDA files found → confirms research gap  

This validates the importance of QDArchive.

---

## 📎 Submission

- **SQLite Database:**  
  [Download metadata.db](https://github.com/Rinco400/Applied-SE-Project/raw/main/metadata.db)

- **Source Code:**  
  [GitHub Repository](https://github.com/Rinco400/Applied-SE-Project)

- **Downloaded Data:**  
  Stored locally (~144 GB total size)  
  Due to large size, full dataset is not included in GitHub.

- **FAUbox Data Access:**  
  [Download Dataset (FAUbox)](https://faubox.rrze.uni-erlangen.de/getlink/fiknN6PrhXTQzzuXBSRj9/)

- **Access Note:**  
  Metadata database contains complete records of all processed datasets,  
  including file URLs, metadata, and download status.

---

## ✅ Final Remark

This project successfully demonstrates:

✔ Automated data acquisition  
✔ Real-world data challenges  
✔ Robust engineering solutions


## Seeding QDArchive - Part 2 Classification

**Status:** Classification  
**Student ID:** `23071082`

---

## Overview

Part 2 classifies the projects acquired during Part 1 of the Seeding QDArchive project.

The classification workflow uses:

- Project metadata
- File names and extensions
- ZIP archive member inspection
- Limited content extraction from parsable files
- ISIC Rev. 5 section and division categories

The work is descriptive. No labelled ground-truth dataset was provided, so the classification is not evaluated with accuracy metrics.

---

## Input Database

Part 2 uses the validated Part 1 database:

```text
23071082-seeding.db
```

A separate classification database is created so that the Part 1 submission database remains unchanged:

```text
23071082-sq26-classification.db
```

---

## Project Type Classification

Each project is assigned one of the following project types:

```text
QDA_PROJECT
QD_PROJECT
OTHER_PROJECT
NOT_A_PROJECT
```

The classification follows this cascade:

| Project Type | Classification Rule |
|---|---|
| `QDA_PROJECT` | At least one recognised QDA software project file is available. |
| `QD_PROJECT` | No QDA project file is found, but primary qualitative-data files are available. |
| `OTHER_PROJECT` | No QDA or primary qualitative-data file is found, but valid structured-data files are available. |
| `NOT_A_PROJECT` | No reliable evidence can be derived from available file information. |

### Recognised QDA File Types

Examples include:

```text
.qdpx, .qda, .qde, .qdas
.nvp, .nvpx
.atlproj, .hpr7, .hpr6
.mx, .maxqda, .maxqdaproject
```

### Primary Qualitative Data File Types

Examples include:

```text
.txt, .rtf, .doc, .docx, .pdf, .md
.mp3, .wav, .m4a
.mp4, .mov, .avi
.jpg, .jpeg, .png
.vtt, .srt
```

### Valid Structured Data File Types

Examples include:

```text
.csv, .tsv, .xlsx, .xls
.json, .xml, .yaml
.sav, .dta, .rds
.parquet, .geojson
```

ZIP archives are inspected through their member lists without fully extracting every archive.

---

## ISIC Rev. 5 Classification

Eligible `QDA_PROJECT` and `QD_PROJECT` records are classified using ISIC Rev. 5.

| Field | Meaning |
|---|---|
| `primary_class` | ISIC section |
| `secondary_class` | ISIC division |

The classifier uses transparent keyword-based rules from:

- Project title
- Project description
- Project keywords
- File names
- Sampled content from parsable files

Parsable files include:

```text
.txt, .md, .csv, .tsv, .json, .xml
.html, .rtf, .docx, .pdf
```

For performance control, a maximum of four parsable primary-data files per project is sampled.

---

## Classification Results

### Overall Statistics

| Measure | Result |
|---|---:|
| Canonical projects | 133 |
| File records | 67,169 |
| Duplicate mappings | 0 |
| `QDA_PROJECT` | 0 |
| `QD_PROJECT` | 75 |
| `OTHER_PROJECT` | 3 |
| `NOT_A_PROJECT` | 55 |
| Projects with ISIC classification | 75 |
| Primary files with ISIC classification | 67,021 |
| Files with extracted content | 261 |

### Project Types by Repository

| Repository | `QDA_PROJECT` | `QD_PROJECT` | `OTHER_PROJECT` | `NOT_A_PROJECT` |
|---|---:|---:|---:|---:|
| DataverseNO | 0 | 74 | 0 | 20 |
| ICPSR | 0 | 1 | 3 | 35 |

### Dominant ISIC Divisions

| Repository | Dominant ISIC Division | Classified Projects |
|---|---|---:|
| DataverseNO | N72 - Scientific research and development | 67 |
| ICPSR | N72 - Scientific research and development | 1 |

The dominant category reflects the acquired dataset and search coverage. It should not be interpreted as the general distribution of qualitative research worldwide.

---

## Deliverables

The Part 2 workflow produces:

```text
23071082-sq26-classification.db
part2/outputs/23071082-classification-results.xlsx
part2/outputs/23071082-classification-report.pdf
part2/outputs/classification_audit.txt
```

### Required XLSX Columns

```text
repository_id
project_type
project_title
primary_class
secondary_class
no_project_files
```

### PDF Report Contents

The generated PDF report includes:

- Project-type distribution by repository
- Repository-specific primary-class histograms
- Full ISIC class names on chart labels
- Count values displayed on chart bars
- Rank-ordered primary-class tables
- Comments on repository findings
- Technical data challenges
- Conclusion

---

## Technical Data Challenges

### 1. No Confirmed QDA Project Files

No QDPX, NVivo, ATLAS.ti, MAXQDA, or equivalent QDA software project files were identified among the collected canonical projects.

### 2. Generic File Extensions Are Ambiguous

Formats such as TXT, PDF, CSV, XLSX, and ZIP do not prove that a project contains qualitative research material. They may represent interview transcripts, sensor logs, scientific measurements, technical documentation, or software output.

### 3. Incomplete Downloadable Evidence

Some projects had no successfully downloaded files that could support project-type derivation. These records were classified as `NOT_A_PROJECT`.

### 4. Archive and Compound Datasets

ZIP archives can contain mixed material such as primary data, documentation, structured files, and technical outputs. File-name and extension inspection cannot always determine the semantic meaning of every archive member.

### 5. Uneven Repository Coverage

DataverseNO provided most `QD_PROJECT` records, while ICPSR contributed fewer accessible primary-data projects in this acquisition run. Repository-level statistics should therefore be interpreted carefully.

### 6. File-Volume Imbalance

A few projects contain thousands of similar files. Therefore, the report uses project-level classification distributions rather than file-level distributions, preventing a single large project from dominating the statistics.

---

## Reproducibility

Install required dependencies:

```powershell
python -m pip install pypdf python-docx pandas xlsxwriter matplotlib
```

Run the Part 2 workflow in this order:

```powershell
python .\part2\prepare_classification_db.py
python .\part2\classify_project_types.py
python .\part2\classify_isic.py
python .\part2\audit_classification.py
python .\part2\generate_outputs.py
```

> **Important:** `prepare_classification_db.py` creates a new classification database. Running it again resets the Part 2 database and requires all later scripts to be run again.

---

## Part 2 Project Structure

```text
part2/
├── outputs/
│   ├── 23071082-classification-report.pdf
│   ├── 23071082-classification-results.xlsx
│   └── classification_audit.txt
│
├── audit_classification.py
├── classify_isic.py
├── classify_project_types.py
├── generate_outputs.py
├── isic_rev5_structure.csv
└── prepare_classification_db.py
```

---

## Final Part 2 Submission

Final classification database:

```text
23071082-sq26-classification.db
```

Final Git tag:

```text
classification-results
```