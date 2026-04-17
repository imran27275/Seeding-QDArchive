import csv
import logging
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from config import DB_PATH, CSV_DIR

logger = logging.getLogger(__name__)

# File status constants 
SUCCESS                    = "SUCCESS"
FAILED                     = "FAILED"
RESTRICTED                 = "RESTRICTED"
SKIPPED                    = "SKIPPED"
ALREADY_EXISTS             = "ALREADY_EXISTS"
FAILED_SERVER_UNRESPONSIVE = "FAILED_SERVER_UNRESPONSIVE"
FAILED_LOGIN_REQUIRED      = "FAILED_LOGIN_REQUIRED"

# Person role constants (professor-specified)
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


# Insert helpers

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


# Normalization helpers

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


def _normalise_license(raw: str) -> str:
    """
    Convert license URLs and variant strings to clean readable labels.

    Handles:
      - Full Creative Commons URLs (http and https, with/without trailing /)
      - Short CC abbreviations (cc-by, cc0, etc.)
      - SPDX identifiers
      - Other common open licenses
      - Falls back to the raw string if unrecognised
    """
    s = raw.lower().rstrip("/").strip()

    # Creative Commons URL patterns
    _CC_URL = [
        # CC0 / Public Domain
        ("creativecommons.org/publicdomain/zero/1.0",    "CC0-1.0"),
        ("creativecommons.org/publicdomain/mark/1.0",    "Public Domain Mark 1.0"),
        # CC BY
        ("creativecommons.org/licenses/by/4.0",          "CC-BY-4.0"),
        ("creativecommons.org/licenses/by/3.0",          "CC-BY-3.0"),
        ("creativecommons.org/licenses/by/2.5",          "CC-BY-2.5"),
        ("creativecommons.org/licenses/by/2.0",          "CC-BY-2.0"),
        ("creativecommons.org/licenses/by/1.0",          "CC-BY-1.0"),
        # CC BY-SA
        ("creativecommons.org/licenses/by-sa/4.0",       "CC-BY-SA-4.0"),
        ("creativecommons.org/licenses/by-sa/3.0",       "CC-BY-SA-3.0"),
        ("creativecommons.org/licenses/by-sa/2.5",       "CC-BY-SA-2.5"),
        # CC BY-ND
        ("creativecommons.org/licenses/by-nd/4.0",       "CC-BY-ND-4.0"),
        ("creativecommons.org/licenses/by-nd/3.0",       "CC-BY-ND-3.0"),
        # CC BY-NC
        ("creativecommons.org/licenses/by-nc/4.0",       "CC-BY-NC-4.0"),
        ("creativecommons.org/licenses/by-nc/3.0",       "CC-BY-NC-3.0"),
        # CC BY-NC-SA
        ("creativecommons.org/licenses/by-nc-sa/4.0",    "CC-BY-NC-SA-4.0"),
        ("creativecommons.org/licenses/by-nc-sa/3.0",    "CC-BY-NC-SA-3.0"),
        # CC BY-NC-ND
        ("creativecommons.org/licenses/by-nc-nd/4.0",    "CC-BY-NC-ND-4.0"),
        ("creativecommons.org/licenses/by-nc-nd/3.0",    "CC-BY-NC-ND-3.0"),
        # RightsStatements.org - In Copyright
        ("rightsstatements.org/vocab/InC/1.0",           "InC-1.0"),
    ]
    for fragment, label in _CC_URL:
        if fragment in s:
            return label

    # DANS-specific license labels
    _DANS = {
        "dans licence":                   "DANS Licence",
        "dans license":                   "DANS Licence",
        "dare":                           "DARE",
        "open access for registered":     "DANS Open Access (Registered)",
        "restricted access":              "Restricted Access",
        "geen":                           "No Licence Specified",  # Dutch for "none"
    }
    for fragment, label in _DANS.items():
        if fragment in s:
            return label

    # Short CC abbreviations
    _SHORT = {
        "cc0":              "CC0-1.0",
        "cc-0":             "CC0-1.0",
        "cc by 4.0":        "CC-BY-4.0",
        "cc by 3.0":        "CC-BY-3.0",
        "cc by-sa 4.0":     "CC-BY-SA-4.0",
        "cc by-sa 3.0":     "CC-BY-SA-3.0",
        "cc by-nd 4.0":     "CC-BY-ND-4.0",
        "cc by-nc 4.0":     "CC-BY-NC-4.0",
        "cc by-nc-sa 4.0":  "CC-BY-NC-SA-4.0",
        "cc by-nc-nd 4.0":  "CC-BY-NC-ND-4.0",
    }
    for short, label in _SHORT.items():
        if s == short or s.startswith(short):
            return label

    # Other common licenses 
    _OTHER = [
        ("opensource.org/licenses/mit",    "MIT"),
        ("opensource.org/licenses/apache", "Apache-2.0"),
        ("apache.org/licenses/license-2.0","Apache-2.0"),
        ("gnu.org/licenses/gpl-3",         "GPL-3.0"),
        ("gnu.org/licenses/gpl-2",         "GPL-2.0"),
        ("gnu.org/licenses/lgpl",          "LGPL"),
        ("opensource.org/licenses/bsd",    "BSD"),
        ("eupl",                           "EUPL-1.2"),
        ("pddl",                           "PDDL"),   # Open Data Commons
        ("odbl",                           "ODbL"),   # Open Database Licence
        ("odc-by",                         "ODC-By"),
    ]
    for fragment, label in _OTHER:
        if fragment in s:
            return label

    # Already a clean SPDX string — return as-is
    # Check if it looks like a known SPDX pattern (e.g. CC-BY-4.0)
    if re.match(r'^[A-Z0-9][A-Z0-9\.\-]+$', raw.strip()):
        return raw.strip()   # already clean

    # Unknown — return raw (no cleaning principle)
    return raw


# Stats & export

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