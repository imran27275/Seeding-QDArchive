# Seeding-QDArchive

A research data pipeline for discovering, downloading, and archiving **Qualitative Data Analysis (QDA)** datasets from open academic repositories.

Built as part of the *Seeding QDArchive* research project at FAU Erlangen-Nürnberg, supervised by Prof. Riehle.

---

## Repositories Covered

| # | Repository | URL | Access Method |
|---|-----------|-----|---------------|
| 5 | DANS (Dutch National Data Archive) | [dans.knaw.nl](https://dans.knaw.nl) | Dataverse REST API |
| 16 | opendata.uni-halle.de (Share_it) | [opendata.uni-halle.de](https://opendata.uni-halle.de) | OAI-PMH harvest |

> **Note on DANS:** DANS splits its repository into 4 domain-specific data stations. All 4 share repository ID 5 and download to the same `files/DANS/` folder. A global DOI deduplication system ensures the same dataset is never downloaded twice even if it appears in multiple stations.
> 
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
│   ├── 23293539-sq26.db      # SQLite database (auto-created)
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
| `type` | TEXT | Project type |

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


## Part 2 — ISIC Classification Pipeline

Once Part 1 acquisition is complete and every project in the database has a `type` (`QDA_PROJECT` / `QD_PROJECT` / `OTHER_PROJECT` / `NOT_A_PROJECT`), the classification pipeline assigns each `QDA_PROJECT` and `QD_PROJECT` an **ISIC Rev. 5** division (2-level taxonomy, e.g. `A01 — Crop and animal production...`), both at the project level and for each individual primary data file.

### Scripts

These live directly in the project root:

```
Seeding-QDArchive/
├── classifier.py             # TF-IDF + cosine similarity classifier (imported by the others)
├── run_classification.py     # ★ Main entry point — classifies projects & files
├── repository_summary.py     # Prints per-repository stats (for the results form)
├── export_table.py           # Exports the flat results table as XLSX
├── generate_report.py        # Generates the final PDF report (histograms + tables)
│
└── data/
    └── isic_taxonomy.json    # Pre-built ISIC Rev. 5 division-level taxonomy
```

### Requirements

```bash
pip install -r requirements.txt
```

### Step 1 — Run classification

```bash
python run_classification.py data/23293539-sq26.db 23293539-sq26-classification.db
```

This copies the input database (via SQLite's backup API, safe even in WAL mode — the original db is never modified) and, for every `QDA_PROJECT` / `QD_PROJECT`:

- classifies the project as a whole (title + description + license + keywords + filenames + best-effort extracted text of its primary files) into `projects.primary_class` / `projects.secondary_class`
- classifies each of its primary data files individually into `files.primary_class` / `files.secondary_class`
- adds the classifier's top terms as extra rows in `keywords`

`OTHER_PROJECT` and `NOT_A_PROJECT` rows are copied over unchanged but are not classified, per the task spec.

**Project classification:**
 
| project_type | Count | Description |
|---|---|---|
| `QDA_PROJECT` | 10 | Contains at least one QDA file (`.qdpx`, `.atlproj`, `.mx` etc.) |
| `QD_PROJECT` | 201 | No QDA file but has primary qualitative data (transcripts, audio, video) |
| `OTHER_PROJECT` | 72 | Has other valid data files (spreadsheets, images, archives) |
| `NOT_A_PROJECT` | 29 | No usable files could be determined |


### Step 2 — Repository statistics

```bash
python repository_summary.py 23293539-sq26-classification.db
```

Prints, per `repository_id`: the count of each project type found, and the dominant (most common) primary class.

### Step 3 — Export results table

```bash
python export_table.py 23293539-sq26-classification.db results.xlsx
```

Produces `results.xlsx` with columns: `repository_id`, `project_type`, `project_title`, `primary_class`, `secondary_class`, `no_project_files`.

### Step 4 — Generate PDF report

```bash
python generate_report.py 23293539-sq26-classification.db report.pdf
```

Produces a vector-graphics PDF report, one section per repository: a horizontal-bar histogram of primary classes (full class name as the label, count printed at the end of each bar), a rank-ordered top-20 table, and a comments section.

### Regenerating the ISIC taxonomy

`data/isic_taxonomy.json` is pre-built from the ISIC Rev. 5 spreadsheet. Only regenerate it if a new/updated spreadsheet is issued:

```bash
python build_isic_taxonomy.py path/to/ISIC5_reference.xlsx
```

---

## Author

**Md Imran Hossain** (23293539)
Part of the *Seeding QDArchive* research project — FAU Erlangen-Nürnberg.
