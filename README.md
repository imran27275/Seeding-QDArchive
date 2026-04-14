# Seeding-QDArchive

A research data pipeline for discovering, downloading, and archiving **Qualitative Data Analysis (QDA)** datasets from open academic repositories.

Built as part of the *Seeding QDArchive* research project at FAU Erlangen-Nürnberg, supervised by Prof. Riehle.

---

## Repositories Covered

| # | Repository | URL | Access Method |
|---|-----------|-----|---------------|
| 5 | DANS (Dutch National Data Archive) | [dans.knaw.nl](https://dans.knaw.nl) | Dataverse REST API |
| 16 | opendata.uni-halle.de (Share_it) | [opendata.uni-halle.de](https://opendata.uni-halle.de) | OAI-PMH harvest |

> **Note on uni-halle:** The DSpace 7 REST API (`/server/api/...`) is blocked by a site-wide CAPTCHA. Metadata is instead harvested via **OAI-PMH** (`/oai/request`), which is a standard machine-harvest protocol not subject to the CAPTCHA wall. File downloads use direct bitstream URLs discovered during the OAI harvest. `download_method` is recorded as `SCRAPING` in the database.

---

## What Gets Downloaded

The pipeline downloads **all files** in every QDA-identified project — not just the QDA files themselves:

| Category | Extensions |
|----------|-----------|
| **QDA project files** | `.qdpx`, `.nvpx`, `.nvp`, `.atlproj`, `.atl`, `.mx`, `.mx24`, `.mx20`, `.mx18`, `.mxd`, `.qda`, `.f4a`, `.f4p`, `.quirkos` |
| **Documents / transcripts** | `.pdf`, `.txt`, `.rtf`, `.docx`, `.doc`, `.odt` |
| **Spreadsheets / data** | `.xlsx`, `.xls`, `.csv` |
| **Audio recordings** | `.mp3`, `.wav`, `.m4a`, `.aac`, `.ogg`, `.flac` |
| **Video recordings** | `.mp4`, `.mov`, `.avi`, `.mkv`, `.wmv` |
| **Images / scans** | `.jpg`, `.jpeg`, `.png`, `.tiff`, `.tif`, `.bmp` |
| **Archives** | `.zip`, `.tar`, `.gz`, `.7z` |

A project is included only if it contains **at least one QDA file**. Once included, all associated files are downloaded.

---

## Project Structure

```
Seeding-QDArchive/
│
├── pipeline.py               # ★ Main entry point — run this
├── config.py                 # All settings: paths, repos, queries, extensions
├── database.py               # SQLite schema, inserts, export helpers
├── downloader.py             # File download with retry & atomic write
├── requirements.txt
├── .gitignore
│
├── scrapers/                 # One file per repository
│   ├── __init__.py
│   ├── base_scraper.py       # Abstract base class
│   ├── dans_scraper.py       # DANS — Dataverse API (repo #5)
│   └── uni_halle_scraper.py  # uni-halle — OAI-PMH (repo #16)
│
├── data/
│   ├── 23293639-sq26.db      # SQLite database (auto-created)
│   ├── progress.json         # Resume state (auto-created)
│   ├── pipeline.log          # Run log (auto-created)
│   └── csv/                  # CSV exports (auto-created)
│       ├── projects.csv
│       ├── files.csv
│       ├── keywords.csv
│       ├── person_role.csv
│       ├── licenses.csv
│       └── projects_full.csv # Flat joined view of all tables
│
└── files/                    # Downloaded files (auto-created)
    ├── DANS/
    └── uni_halle/
```

---

## Database Schema

The SQLite database (`data/23293639-sq26.db`) stores all metadata in **5 tables**, following the professor's specification. Raw values are stored exactly as returned by the source — **no cleaning at this stage**.

### `projects` — one row per research project
| Column | Type | Notes |
|--------|------|-------|
| `id` | INTEGER | Primary key |
| `query_string` | TEXT | Query that found this project |
| `repository_id` | INTEGER | Our repo list ID (5 or 16) |
| `repository_url` | TEXT | Top-level repo URL |
| `project_url` | TEXT | Full URL to the project page |
| `version` | TEXT | Version string if any |
| `title` | TEXT | Project title |
| `description` | TEXT | Abstract / description |
| `language` | TEXT | BCP 47 e.g. `en-US` |
| `doi` | TEXT | DOI URL |
| `upload_date` | TEXT | Publication date from source |
| `download_date` | TEXT | Timestamp of our download |
| `download_repository_folder` | TEXT | e.g. `DANS` |
| `download_project_folder` | TEXT | e.g. `doi_10.34894_XP9ZCU_...` |
| `download_version_folder` | TEXT | e.g. `v1.0` if versioned |
| `download_method` | TEXT | `API-CALL` or `SCRAPING` |

### `files` — one row per file
| Column | Type | Notes |
|--------|------|-------|
| `id` | INTEGER | Primary key |
| `project_id` | INTEGER | FK → projects.id |
| `file_name` | TEXT | Original filename |
| `file_type` | TEXT | Extension without dot, e.g. `qdpx` |
| `status` | TEXT | `SUCCESS` \| `FAILED` \| `RESTRICTED` \| `SKIPPED` \| `ALREADY_EXISTS` |

### `keywords` — raw, not split or cleaned
| Column | Notes |
|--------|-------|
| `project_id` | FK → projects.id |
| `keyword` | Raw keyword string from source |

### `person_role` — authors, uploaders, contributors
| Column | Notes |
|--------|-------|
| `project_id` | FK → projects.id |
| `name` | Full name string |
| `role` | `AUTHOR` \| `UPLOADER` \| `CONTRIBUTOR` \| `UNKNOWN` |

### `licenses`
| Column | Notes |
|--------|-------|
| `project_id` | FK → projects.id |
| `license` | License string as returned by source |

---

## Setup

### Requirements
- Python 3.10+

```bash
pip install -r requirements.txt
```

---

## Usage

### Step 1 — Run the pipeline

```bash
# Both repositories (default)
python pipeline.py

# Only DANS
python pipeline.py --source dans

# Only uni-halle
python pipeline.py --source uni_halle

# Metadata only — no file downloads
python pipeline.py --no-download

# View database statistics
python pipeline.py --stats

# Export CSVs only
python pipeline.py --export

# Ignore saved progress, start fresh scan (DB is NOT cleared)
python pipeline.py --reset-progress
```

### Progress saving & resuming

- Progress is **auto-saved every 25 projects** to `data/progress.json`
- Press **Ctrl+C** to stop gracefully — the current project finishes, progress is saved, then the pipeline exits cleanly
- On the next run, already-completed projects are automatically skipped
- Press Ctrl+C **twice** to force quit immediately

CSV files are also exported automatically at the end of every run.

---

## Search Queries

**Primary (QDA file extensions / software):**
`qdpx`, `nvpx`, `atlproj`, `MAXQDA`, `ATLAS.ti`, `NVivo`

**Secondary (broader qualitative research terms):**
`qualitative research data`, `qualitative data analysis`, `interview study`, `interview transcript`, `thematic analysis`, `grounded theory`, `QDA`

Results are de-duplicated by project URL / DOI before being inserted into the database.

---

## Notes on Restricted Files

- **DANS:** Files flagged `restricted` in the Dataverse API are recorded with `status = RESTRICTED` and skipped. Submit a formal access request via the [DANS portal](https://dans.knaw.nl).
- **uni-halle:** HTTP 403 responses during download are caught and recorded as `RESTRICTED`.

---

## Known Limitations

| Repository | Issue | Workaround |
|-----------|-------|-----------|
| uni-halle | REST API blocked by CAPTCHA | OAI-PMH harvest used instead |
| uni-halle | File sizes unavailable | OAI-PMH (Dublin Core) carries no byte counts |
| DANS | Some files access-restricted (GDPR/embargo) | Skipped, logged as RESTRICTED |

---

## Author

**Md Imran Hossain** (23293639)
Part of the *Seeding QDArchive* research project — FAU Erlangen-Nürnberg.