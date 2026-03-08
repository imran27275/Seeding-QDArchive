# Seeding-QDArchive

A research data pipeline for discovering, downloading, and archiving **Qualitative Data Analysis (QDA)** datasets from open academic repositories.

---

## What This Project Does

This pipeline automatically searches two university research repositories for datasets that contain QDA project files (MAXQDA, ATLAS.ti, NVivo, etc.), downloads all available files, stores metadata in a local database, and exports everything to CSV for analysis.

---

## Data Sources

| Repository | Software | URL |
|---|---|---|
| opendata.uni-halle.de | DSpace 6 REST API | https://opendata.uni-halle.de |
| DANS (Dutch research archive) | Dataverse API | https://dans.knaw.nl |

---

## QDA File Types Collected

| Extension | Software |
|---|---|
| `.qdpx` | REFI-QDA standard (cross-platform) |
| `.nvpx` | NVivo |
| `.atlproj` | ATLAS.ti |
| `.mx` / `.mx24` / `.mx20` | MAXQDA |

---

## Database Schema

**`datasets` table** — one row per downloaded dataset:

| Column | Description |
|---|---|
| `id` | Auto-increment primary key |
| `source` | Repository source (`uni_halle` or `dans_knaw`) |
| `source_record_id` | Original ID from the repository |
| `source_url` | Link to the dataset page |
| `title` | Dataset title |
| `license` | License (e.g. CC-BY 4.0) |
| `published` | Publication date |
| `downloaded_at` | Timestamp of download |
| `local_folder` | Path to local files |

**`files` table** — one row per downloaded file:

| Column | Description |
|---|---|
| `id` | Auto-increment primary key |
| `dataset_id` | Foreign key → datasets.id |
| `file_name` | Original filename |
| `file_url` | Direct download URL |
| `size` | File size in bytes |
| `local_path` | Local file path |
| `downloaded_at` | Timestamp of download |

---

## Notes on Access-Restricted Files

Some files on DANS are access-restricted due to privacy regulations (GDPR) or embargo periods. The pipeline automatically detects and skips these files, logging them as `🔒 Restricted`. To access restricted files, a formal data request must be submitted through the DANS portal.

---

## Requirements

- Python 3.8+
- `requests`
- `tqdm`

---

## Author

Md Imran Hossain