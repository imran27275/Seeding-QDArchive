"""
database.py
─────────────────────────────────────────────────────────────────
All SQLite database logic for the Seeding-QDArchive pipeline.

Schema (5 tables, per professor's specification):
  projects    — one row per research project/dataset
  files       — id, project_id, file_name, file_type, status
  keywords    — raw keywords as returned by the source
  person_role — authors, uploaders, contributors
  licenses    — normalized license strings

Person roles: AUTHOR | UPLOADER | OWNER | OTHER | UNKNOWN
"""

import csv
import logging
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from config import DB_PATH, CSV_DIR

logger = logging.getLogger(__name__)

# ── File status constants — professor-specified (exactly 4) ────
SUCCEEDED                  = "SUCCEEDED"
FAILED_SERVER_UNRESPONSIVE = "FAILED_SERVER_UNRESPONSIVE"
FAILED_LOGIN_REQUIRED      = "FAILED_LOGIN_REQUIRED"
FAILED_TOO_LARGE           = "FAILED_TOO_LARGE"


# ── Person role constants (professor-specified) ────────────────
ROLE_AUTHOR    = "AUTHOR"
ROLE_UPLOADER  = "UPLOADER"
ROLE_OWNER     = "OWNER"
ROLE_OTHER     = "OTHER"
ROLE_UNKNOWN   = "UNKNOWN"
VALID_ROLES    = {ROLE_AUTHOR, ROLE_UPLOADER, ROLE_OWNER,
                  ROLE_OTHER, ROLE_UNKNOWN}


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_connection() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(DB_PATH)
    con.execute("PRAGMA foreign_keys = ON")
    con.execute("PRAGMA journal_mode = WAL")
    return con




def init_db() -> None:
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

def insert_project(con, *, query_string, repository_id, repository_url,
                   project_url, version, title, description, language,
                   doi, upload_date, download_repository_folder,
                   download_project_folder, download_version_folder,
                   download_method) -> int | None:
    cur = con.cursor()
    cur.execute("""
        INSERT OR IGNORE INTO projects (
            query_string, repository_id, repository_url,
            project_url, version, title, description,
            language, doi, upload_date, download_date,
            download_repository_folder, download_project_folder,
            download_version_folder, download_method
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (query_string, repository_id, repository_url,
          project_url, version, title, description,
          language, doi, upload_date, now_utc(),
          download_repository_folder, download_project_folder,
          download_version_folder, download_method))
    con.commit()
    cur.execute(
        "SELECT id FROM projects WHERE repository_id=? AND project_url=?",
        (repository_id, project_url)
    )
    row = cur.fetchone()
    return row[0] if row else None


def insert_file(con, *, project_id, file_name,
                file_type, status) -> int | None:
    cur = con.cursor()
    cur.execute("""
        INSERT OR IGNORE INTO files (project_id, file_name, file_type, status)
        VALUES (?, ?, ?, ?)
    """, (project_id, file_name, file_type, status))
    con.commit()
    cur.execute(
        "SELECT id FROM files WHERE project_id=? AND file_name=?",
        (project_id, file_name)
    )
    row = cur.fetchone()
    return row[0] if row else None


def update_file_status(con, file_id: int, status: str, **kwargs) -> None:
    cur = con.cursor()
    cur.execute("UPDATE files SET status=? WHERE id=?", (status, file_id))
    con.commit()


def insert_keyword(con, project_id: int, keyword: str) -> None:
    if not keyword or not keyword.strip():
        return
    cur = con.cursor()
    cur.execute("""
        INSERT OR IGNORE INTO keywords (project_id, keyword)
        VALUES (?, ?)
    """, (project_id, keyword.strip()))
    con.commit()


def insert_person(con, project_id: int,
                  name: str, role: str = ROLE_UNKNOWN) -> None:
    """
    Insert a person with role.
    Roles: AUTHOR | UPLOADER | OWNER | OTHER | UNKNOWN
    Any unrecognised role maps to UNKNOWN.
    """
    if not name or not name.strip():
        return
    normalised_role = _normalise_role(role)
    cur = con.cursor()
    cur.execute("""
        INSERT OR IGNORE INTO person_role (project_id, name, role)
        VALUES (?, ?, ?)
    """, (project_id, name.strip(), normalised_role))
    con.commit()


def insert_license(con, project_id: int, license_str: str) -> None:
    """
    Insert a license, normalising URLs to readable SPDX-style strings.
    e.g. http://creativecommons.org/licenses/by/4.0/ → CC-BY-4.0
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


# ── Normalization helpers ──────────────────────────────────────

def _normalise_role(raw: str) -> str:
    """
    Map any role string to one of the 5 allowed values:
    AUTHOR | UPLOADER | OWNER | OTHER | UNKNOWN
    """
    if not raw:
        return ROLE_UNKNOWN
    r = raw.strip().upper()
    if r in VALID_ROLES:
        return r
    # Common variations
    _MAP = {
        "CREATOR":      ROLE_AUTHOR,
        "PI":           ROLE_AUTHOR,
        "PRINCIPAL_INVESTIGATOR": ROLE_AUTHOR,
        "RESEARCHER":   ROLE_AUTHOR,
        "DEPOSITOR":    ROLE_UPLOADER,
        "SUBMITTER":    ROLE_UPLOADER,
        "DATA_MANAGER": ROLE_OWNER,
        "MANAGER":      ROLE_OWNER,
        "CONTRIBUTOR":  ROLE_OTHER,
        "EDITOR":       ROLE_OTHER,
        "CONTACT":      ROLE_OTHER,
        "DISTRIBUTOR":  ROLE_OTHER,
        "FUNDER":       ROLE_OTHER,
        "SPONSOR":      ROLE_OTHER,
        "SUPERVISOR":   ROLE_OTHER,
        "TRANSLATOR":   ROLE_OTHER,
        "PRODUCER":     ROLE_OTHER,
        "PUBLISHER":    ROLE_OTHER,
        "PROJECT_MEMBER": ROLE_OTHER,
        "RELATED_PERSON": ROLE_OTHER,
    }
    return _MAP.get(r, ROLE_UNKNOWN)


def _extract_version(s: str) -> str:
    # Extract version from lowercase URL path e.g. /4.0/ -> ' 4.0'
    m = re.search(r'/(\d+\.\d+)/?$', s)
    return f" {m.group(1)}" if m else ""


def _normalise_license(raw: str) -> str:
    """
    Convert license URLs to professor-specified format.
    Valid: CC BY, CC BY-SA, CC BY-NC, CC BY-ND, CC BY-NC-ND, CC0,
           ODbL, ODC-By, PDDL (each optionally with version e.g. CC BY 4.0).
    rightsstatements.org URLs -> short code e.g. InC/1.0
    Unknown values kept raw -- professor will fix later.
    """
    if not raw or not raw.strip():
        return raw

    s = raw.lower().rstrip("/").strip()

    # Creative Commons URLs
    if "creativecommons.org" in s:
        ver = _extract_version(s)
        if "publicdomain/zero" in s: return f"CC0{ver}".strip()
        if "by-nc-nd" in s: return f"CC BY-NC-ND{ver}".strip()
        if "by-nc-sa" in s: return f"CC BY-NC-SA{ver}".strip()
        if "by-nc" in s:    return f"CC BY-NC{ver}".strip()
        if "by-nd" in s:    return f"CC BY-ND{ver}".strip()
        if "by-sa" in s:    return f"CC BY-SA{ver}".strip()
        if "/by/" in s or s.endswith("/by"): return f"CC BY{ver}".strip()
        return raw

    # rightsstatements.org -- preserve original case from URL path
    if "rightsstatements.org" in s:
        m = re.search(r"rightsstatements\.org/vocab/([^/]+)/(\d+\.\d+)",
                      raw, re.IGNORECASE)
        if m:
            return f"{m.group(1)}/{m.group(2)}"
        return raw

    # ODbL
    if "odbl" in s or "open database" in s:
        ver = _extract_version(s)
        return f"ODbL{ver}".strip() if ver else "ODbL"

    # ODC-By
    if "odc-by" in s or "odc by" in s:
        ver = _extract_version(s)
        return f"ODC-By{ver}".strip() if ver else "ODC-By"

    # PDDL
    if "pddl" in s:
        return "PDDL"

    # CC0 short forms
    if s in ("cc0", "cc 0", "cc-0", "creative commons zero"):
        return "CC0"

    # CC dash-format e.g. CC-BY-4.0 -> CC BY 4.0
    m = re.match(
        r"^(CC-BY(?:-NC-ND|-NC-SA|-NC|-ND|-SA)?)[-\s]?(\d+\.\d+)?$",
        raw.strip(), re.IGNORECASE
    )
    if m:
        base = (m.group(1).upper()
                .replace("CC-BY-NC-ND", "CC BY-NC-ND")
                .replace("CC-BY-NC-SA", "CC BY-NC-SA")
                .replace("CC-BY-NC",    "CC BY-NC")
                .replace("CC-BY-ND",    "CC BY-ND")
                .replace("CC-BY-SA",    "CC BY-SA")
                .replace("CC-BY",       "CC BY"))
        ver = f" {m.group(2)}" if m.group(2) else ""
        return f"{base}{ver}".strip()

    return raw


def print_stats(con) -> None:
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

    cur.execute("SELECT status, COUNT(*) FROM files GROUP BY status ORDER BY COUNT(*) DESC")
    for row in cur.fetchall():
        print(f"    {row[0]:<35}: {row[1]}")

    cur.execute("SELECT COUNT(*) FROM keywords")
    print(f"  Total keywords   : {cur.fetchone()[0]}")

    cur.execute("SELECT COUNT(*) FROM person_role")
    print(f"  Total persons    : {cur.fetchone()[0]}")

    cur.execute("SELECT COUNT(*) FROM licenses")
    print(f"  Total licenses   : {cur.fetchone()[0]}")
    print("─" * 50)


def export_all(con) -> None:
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
        SELECT p.id AS project_id,
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