"""
database.py
─────────────────────────────────────────────────────────────────
All SQLite database logic for the Seeding-QDArchive pipeline.

Schema (5 tables, per professor's specification):
  projects    — one row per research project/dataset
  files       — id, project_id, file_name, file_type, status
  keywords    — raw keywords as returned by the source
  person_role — authors, uploaders, contributors
  licenses    — one row per license per project

Rule: store raw values exactly as received. No cleaning at this stage.
"""

import csv
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from config import DB_PATH, CSV_DIR

logger = logging.getLogger(__name__)

# ── File status constants ──────────────────────────────────────
SUCCESS                    = "SUCCESS"
FAILED                     = "FAILED"
RESTRICTED                 = "RESTRICTED"
SKIPPED                    = "SKIPPED"
ALREADY_EXISTS             = "ALREADY_EXISTS"
FAILED_SERVER_UNRESPONSIVE = "FAILED_SERVER_UNRESPONSIVE"
FAILED_LOGIN_REQUIRED      = "FAILED_LOGIN_REQUIRED"

# ── Person role constants ──────────────────────────────────────
ROLE_AUTHOR      = "AUTHOR"
ROLE_UPLOADER    = "UPLOADER"
ROLE_OWNER       = "OWNER"
ROLE_CONTRIBUTOR = "CONTRIBUTOR"
ROLE_UNKNOWN     = "UNKNOWN"


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_connection() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(DB_PATH)
    con.execute("PRAGMA foreign_keys = ON")
    con.execute("PRAGMA journal_mode = WAL")
    return con


def init_db() -> None:
    """Create all tables and indexes if they don't exist."""
    con = get_connection()
    cur = con.cursor()
    cur.executescript("""
        CREATE TABLE IF NOT EXISTS projects (
            id                          INTEGER PRIMARY KEY AUTOINCREMENT,
            query_string                TEXT,
            repository_id               INTEGER NOT NULL,
            repository_url              TEXT    NOT NULL,
            project_url                 TEXT    NOT NULL,
            version                     TEXT,
            title                       TEXT    NOT NULL,
            description                 TEXT,
            language                    TEXT,
            doi                         TEXT,
            upload_date                 TEXT,
            download_date               TEXT    NOT NULL,
            download_repository_folder  TEXT    NOT NULL,
            download_project_folder     TEXT    NOT NULL,
            download_version_folder     TEXT,
            download_method             TEXT    NOT NULL DEFAULT 'API-CALL',
            UNIQUE(repository_id, project_url)
        );

        CREATE TABLE IF NOT EXISTS files (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id  INTEGER NOT NULL,
            file_name   TEXT    NOT NULL,
            file_type   TEXT,
            status      TEXT    NOT NULL,
            FOREIGN KEY(project_id) REFERENCES projects(id),
            UNIQUE(project_id, file_name)
        );

        CREATE TABLE IF NOT EXISTS keywords (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER NOT NULL,
            keyword    TEXT    NOT NULL,
            FOREIGN KEY(project_id) REFERENCES projects(id),
            UNIQUE(project_id, keyword)
        );

        CREATE TABLE IF NOT EXISTS person_role (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER NOT NULL,
            name       TEXT    NOT NULL,
            role       TEXT    NOT NULL DEFAULT 'UNKNOWN',
            FOREIGN KEY(project_id) REFERENCES projects(id),
            UNIQUE(project_id, name, role)
        );

        CREATE TABLE IF NOT EXISTS licenses (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER NOT NULL,
            license    TEXT    NOT NULL,
            FOREIGN KEY(project_id) REFERENCES projects(id),
            UNIQUE(project_id, license)
        );

        CREATE INDEX IF NOT EXISTS idx_projects_repo
            ON projects(repository_id);
        CREATE INDEX IF NOT EXISTS idx_projects_doi
            ON projects(doi);
        CREATE INDEX IF NOT EXISTS idx_files_project
            ON files(project_id);
        CREATE INDEX IF NOT EXISTS idx_files_status
            ON files(status);
        CREATE INDEX IF NOT EXISTS idx_keywords_project
            ON keywords(project_id);
        CREATE INDEX IF NOT EXISTS idx_person_role_project
            ON person_role(project_id);
        CREATE INDEX IF NOT EXISTS idx_licenses_project
            ON licenses(project_id);
    """)
    con.commit()
    con.close()
    logger.info("Database initialised at: %s", DB_PATH)


# ── Insert helpers ─────────────────────────────────────────────

def insert_project(con: sqlite3.Connection, *,
                   query_string, repository_id, repository_url,
                   project_url, version, title, description,
                   language, doi, upload_date,
                   download_repository_folder, download_project_folder,
                   download_version_folder, download_method) -> int | None:
    cur = con.cursor()
    cur.execute("""
        INSERT OR IGNORE INTO projects (
            query_string, repository_id, repository_url,
            project_url, version, title, description,
            language, doi, upload_date, download_date,
            download_repository_folder, download_project_folder,
            download_version_folder, download_method
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        query_string, repository_id, repository_url,
        project_url, version, title, description,
        language, doi, upload_date, now_utc(),
        download_repository_folder, download_project_folder,
        download_version_folder, download_method,
    ))
    con.commit()
    cur.execute(
        "SELECT id FROM projects WHERE repository_id=? AND project_url=?",
        (repository_id, project_url)
    )
    row = cur.fetchone()
    return row[0] if row else None


def insert_file(con: sqlite3.Connection, *,
                project_id, file_name, file_type,
                status) -> int | None:
    """Insert a file record with only the professor-specified columns."""
    cur = con.cursor()
    cur.execute("""
        INSERT OR IGNORE INTO files
            (project_id, file_name, file_type, status)
        VALUES (?, ?, ?, ?)
    """, (project_id, file_name, file_type, status))
    con.commit()
    cur.execute(
        "SELECT id FROM files WHERE project_id=? AND file_name=?",
        (project_id, file_name)
    )
    row = cur.fetchone()
    return row[0] if row else None


def update_file_status(con: sqlite3.Connection, file_id: int,
                       status: str, **kwargs) -> None:
    """Update the status of a file record."""
    cur = con.cursor()
    cur.execute("UPDATE files SET status=? WHERE id=?", (status, file_id))
    con.commit()


def insert_keyword(con: sqlite3.Connection, project_id: int,
                   keyword: str) -> None:
    if not keyword or not keyword.strip():
        return
    cur = con.cursor()
    cur.execute("""
        INSERT OR IGNORE INTO keywords (project_id, keyword)
        VALUES (?, ?)
    """, (project_id, keyword.strip()))
    con.commit()


def insert_person(con: sqlite3.Connection, project_id: int,
                  name: str, role: str = ROLE_UNKNOWN) -> None:
    if not name or not name.strip():
        return
    cur = con.cursor()
    cur.execute("""
        INSERT OR IGNORE INTO person_role (project_id, name, role)
        VALUES (?, ?, ?)
    """, (project_id, name.strip(),
          role.upper() if role else ROLE_UNKNOWN))
    con.commit()


def insert_license(con: sqlite3.Connection, project_id: int,
                   license_str: str) -> None:
    """
    Insert a license. Normalises raw values to standard SPDX-style strings.
    
    uni-halle returns dc:rights as URLs like:
      http://creativecommons.org/licenses/by/4.0/
    These are converted to readable strings like CC-BY-4.0.
    DANS already returns clean strings like CC0-1.0, CC-BY-4.0.
    """
    if not license_str or not license_str.strip():
        return
    normalised = _normalise_license(license_str.strip())
    cur = con.cursor()
    cur.execute("""
        INSERT OR IGNORE INTO licenses (project_id, license)
        VALUES (?, ?)
    """, (project_id, normalised))
    con.commit()


def _normalise_license(raw: str) -> str:
    """
    Convert license URLs and variant strings to clean SPDX-style labels.
    If the value is not a recognised URL, return it as-is (raw principle).
    """
    s = raw.lower().rstrip("/")

    # Creative Commons URL patterns
    _CC_MAP = {
        "creativecommons.org/licenses/by/4.0":          "CC-BY-4.0",
        "creativecommons.org/licenses/by/3.0":          "CC-BY-3.0",
        "creativecommons.org/licenses/by-sa/4.0":       "CC-BY-SA-4.0",
        "creativecommons.org/licenses/by-sa/3.0":       "CC-BY-SA-3.0",
        "creativecommons.org/licenses/by-nd/4.0":       "CC-BY-ND-4.0",
        "creativecommons.org/licenses/by-nc/4.0":       "CC-BY-NC-4.0",
        "creativecommons.org/licenses/by-nc-sa/4.0":    "CC-BY-NC-SA-4.0",
        "creativecommons.org/licenses/by-nc-nd/4.0":    "CC-BY-NC-ND-4.0",
        "creativecommons.org/publicdomain/zero/1.0":    "CC0-1.0",
        "creativecommons.org/publicdomain/mark/1.0":    "Public Domain",
    }
    for url_fragment, label in _CC_MAP.items():
        if url_fragment in s:
            return label

    # Other common open licenses
    if "opensource.org/licenses/mit" in s:
        return "MIT"
    if "gnu.org/licenses/gpl" in s:
        return "GPL"
    if "apache.org/licenses/license-2.0" in s:
        return "Apache-2.0"

    # Already a clean SPDX string — return as-is
    return raw


# ── Stats & export ─────────────────────────────────────────────

def print_stats(con: sqlite3.Connection) -> None:
    cur = con.cursor()
    print("\n📊 Database Statistics")
    print("─" * 50)

    cur.execute("SELECT COUNT(*) FROM projects")
    print(f"  Total projects   : {cur.fetchone()[0]}")

    cur.execute("""
        SELECT repository_id, download_repository_folder, COUNT(*)
        FROM projects GROUP BY repository_id
    """)
    for row in cur.fetchall():
        print(f"    Repo #{row[0]} ({row[1]}): {row[2]} projects")

    cur.execute("SELECT COUNT(*) FROM files")
    print(f"  Total files      : {cur.fetchone()[0]}")

    cur.execute("SELECT status, COUNT(*) FROM files GROUP BY status")
    for row in cur.fetchall():
        print(f"    {row[0]:<35}: {row[1]}")

    cur.execute("SELECT COUNT(*) FROM keywords")
    print(f"  Total keywords   : {cur.fetchone()[0]}")

    cur.execute("SELECT COUNT(*) FROM person_role")
    print(f"  Total persons    : {cur.fetchone()[0]}")

    cur.execute("SELECT COUNT(*) FROM licenses")
    print(f"  Total licenses   : {cur.fetchone()[0]}")
    print("─" * 50)


def export_all(con: sqlite3.Connection) -> None:
    """Export all 5 tables + a flat joined view to CSV."""
    CSV_DIR.mkdir(parents=True, exist_ok=True)
    tables = ["projects", "files", "keywords", "person_role", "licenses"]
    cur    = con.cursor()

    for table in tables:
        out = CSV_DIR / f"{table}.csv"
        cur.execute(f"SELECT * FROM {table}")
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        with open(out, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(cols)
            w.writerows(rows)
        print(f"  ✅ {table:<15} → {out}  ({len(rows)} rows)")

    # Joined flat view
    out = CSV_DIR / "projects_full.csv"
    cur.execute("""
        SELECT
            p.id AS project_id,
            p.repository_id, p.repository_url, p.project_url,
            p.title, p.description, p.language, p.doi,
            p.upload_date, p.download_date, p.download_method,
            p.download_repository_folder, p.download_project_folder,
            p.download_version_folder, p.version, p.query_string,
            f.file_name, f.file_type, f.status,
            (SELECT GROUP_CONCAT(k.keyword, ' | ')
             FROM keywords k WHERE k.project_id = p.id) AS keywords,
            (SELECT GROUP_CONCAT(pr.name || ' (' || pr.role || ')', ' | ')
             FROM person_role pr WHERE pr.project_id = p.id) AS persons,
            (SELECT GROUP_CONCAT(l.license, ' | ')
             FROM licenses l WHERE l.project_id = p.id) AS licenses
        FROM projects p
        LEFT JOIN files f ON f.project_id = p.id
        ORDER BY p.id, f.file_name
    """)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cols)
        w.writerows(rows)
    print(f"  ✅ {'projects_full':<15} → {out}  ({len(rows)} rows, joined view)")
    print(f"\n📁 CSVs saved to: {CSV_DIR.resolve()}")