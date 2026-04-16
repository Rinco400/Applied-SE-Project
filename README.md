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
│   ├── dataverse_no_pipeline.py   # DataverseNO API pipeline
│   └── icpsr_pipeline.py          # ICPSR scraping pipeline
│
├── core/
│   ├── db.py                      # SQLite database handling
│   ├── downloader.py              # File download logic
│   └── folder_manager.py          # Folder utilities
│
├── my_downloads/                  # Downloaded datasets (excluded from Git)
│   ├── dataverse_no/
│   └── icpsr/
│
├── metadata.db                   # SQLite database (excluded from Git)
├── run.py                        # Main entry point
├── requirements.txt              # Dependencies
├── README.md                     # Project documentation
└── .gitignore                    # Ignore large files & environments

---


## ▶️ Usage

```bash
python run.py --repo dataverse_no
python run.py --repo icpsr
python run.py --repo all

##📌 Conclusion
Data acquisition pipeline works successfully
Large number of datasets processed
Significant number of files collected
No QDA files found → confirms research gap

This validates the importance of QDArchive.

##🔗 Submission
SQLite DB: metadata.db
Source code: GitHub repository
Downloaded data: local storage
##✅ Final Remark

This project successfully demonstrates:

✔ automated data acquisition
✔ real-world data challenges
✔ robust engineering solutions

and provides a strong foundation for Part 2: Data Classification.

