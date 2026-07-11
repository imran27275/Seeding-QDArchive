"""
  1. Adds 'type' column to projects table
  2. Populates 'type' with one of 4 values:
       QDA_PROJECT   — has at least one QDA file (SUCCEEDED)
       QD_PROJECT    — no QDA file but has primary data (transcripts, audio, video)
       OTHER_PROJECT — no primary data but has other valid files
       NOT_A_PROJECT — no usable files found
A backup is created automatically before any changes.
"""

import shutil
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

DB_PATH = Path("data/23293539-sq26.db")

# ── QDA extensions ─────
QDA_EXTS = (
    "'.qdpx','.nvpx','.nvp','.atlproj','.atl',"
    "'.mx','.mx24','.mx20','.mx18','.mxd',"
    "'.qda','.f4a','.f4p','.quirkos'"
)

# ── Primary qualitative data extensions ────
PRIMARY_EXTS = (
    "'.pdf','.txt','.rtf','.docx','.doc','.odt',"
    "'.mp3','.wav','.m4a','.aac','.ogg','.flac',"
    "'.mp4','.mov','.avi','.mkv','.wmv'"
)

# ── Other valid data extensions ─────
OTHER_EXTS = (
    "'.xlsx','.xls','.csv',"
    "'.jpg','.jpeg','.png','.tiff','.tif','.bmp',"
    "'.zip','.tar','.gz','.7z'"
)


def backup_db() -> None:
    ts     = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    backup = DB_PATH.parent / f"{DB_PATH.stem}_backup_{ts}{DB_PATH.suffix}"
    shutil.copy2(DB_PATH, backup)
    print(f"📦 Backup created: {backup.name}")


def has_col(cur: sqlite3.Cursor, table: str, col: str) -> bool:
    cur.execute(f"PRAGMA table_info({table})")
    return col in [r[1] for r in cur.fetchall()]


def migrate(con: sqlite3.Connection) -> None:
    cur = con.cursor()

    # ── Step 1: Add 'type' column ───
    print("\n🔧 Setting up 'type' column on projects table...")

    if has_col(cur, "projects", "project_type") and \
       not has_col(cur, "projects", "type"):
        # Old column name exists — recreate table with correct name
        print("   Found old 'project_type' column — renaming to 'type'...")
        cur.execute("PRAGMA foreign_keys = OFF")
        cur.execute("""
            CREATE TABLE projects_new (
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
                type                        TEXT,
                UNIQUE(repository_id, project_url)
            )""")
        cur.execute("""
            INSERT INTO projects_new
                SELECT id, query_string, repository_id, repository_url,
                       project_url, version, title, description,
                       language, doi, upload_date, download_date,
                       download_repository_folder, download_project_folder,
                       download_version_folder, download_method,
                       project_type
                FROM projects""")
        cur.execute("DROP TABLE projects")
        cur.execute("ALTER TABLE projects_new RENAME TO projects")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_projects_repo ON projects(repository_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_projects_doi  ON projects(doi)")
        cur.execute("PRAGMA foreign_keys = ON")
        con.commit()
        print("   ✅ Renamed 'project_type' → 'type'.")

    elif not has_col(cur, "projects", "type"):
        # Fresh DB — just add the column
        cur.execute("ALTER TABLE projects ADD COLUMN type TEXT")
        con.commit()
        print("   ✅ Added 'type' column.")

    else:
        print("   'type' column already exists — skipped.")

    # ── Step 2: Populate 'type' ────
    print("\n🔧 Populating 'type' column...")

    # Reset any existing values so we start clean
    cur.execute("UPDATE projects SET type = NULL")

    # QDA_PROJECT: has at least one SUCCEEDED QDA file
    cur.execute(f"""
        UPDATE projects SET type = 'QDA_PROJECT'
        WHERE id IN (
            SELECT DISTINCT project_id FROM files
            WHERE '.' || LOWER(file_type) IN ({QDA_EXTS})
            AND status = 'SUCCEEDED'
        )
    """)
    qda_count = cur.rowcount
    print(f"   QDA_PROJECT   : {qda_count}")

    # QD_PROJECT: not QDA_PROJECT, but has primary qualitative data
    cur.execute(f"""
        UPDATE projects SET type = 'QD_PROJECT'
        WHERE type IS NULL
        AND id IN (
            SELECT DISTINCT project_id FROM files
            WHERE '.' || LOWER(file_type) IN ({PRIMARY_EXTS})
            AND status = 'SUCCEEDED'
        )
    """)
    qd_count = cur.rowcount
    print(f"   QD_PROJECT    : {qd_count}")

    # OTHER_PROJECT: not QD_PROJECT, but has other valid files
    cur.execute(f"""
        UPDATE projects SET type = 'OTHER_PROJECT'
        WHERE type IS NULL
        AND id IN (
            SELECT DISTINCT project_id FROM files
            WHERE '.' || LOWER(file_type) IN ({OTHER_EXTS})
            AND status = 'SUCCEEDED'
        )
    """)
    other_count = cur.rowcount
    print(f"   OTHER_PROJECT : {other_count}")

    # NOT_A_PROJECT: nothing useful found
    cur.execute("""
        UPDATE projects SET type = 'NOT_A_PROJECT'
        WHERE type IS NULL
    """)
    not_count = cur.rowcount
    print(f"   NOT_A_PROJECT : {not_count}")

    con.commit()
    print(f"\n   ✅ 'type' populated for all {qda_count + qd_count + other_count + not_count} projects.")

    # ── Summary ────
    print("\n📊 Final project type breakdown:")
    cur.execute("SELECT type, COUNT(*) FROM projects GROUP BY type ORDER BY COUNT(*) DESC")
    for row in cur.fetchall():
        print(f"   {row[0]:<20}: {row[1]}")


def main():
    if not DB_PATH.exists():
        print(f"Database not found: {DB_PATH}")
        print("Run from the project root folder.")
        return

    print("╔══════════════════════════════════════════════════════════════╗")
    print("║     Seeding-QDArchive  —  Part 2 Migration                  ║")
    print("║     Adds and populates 'type' column on projects table       ║")
    print("╚══════════════════════════════════════════════════════════════╝")

    backup_db()

    con = sqlite3.connect(DB_PATH)
    con.execute("PRAGMA foreign_keys = ON")
    con.execute("PRAGMA journal_mode = WAL")

    try:
        migrate(con)
        print("\n✅ Migration complete.")
    except Exception as e:
        print(f"\n❌ Migration failed: {e}")
        print("   Your backup is safe — restore it if needed.")
        raise
    finally:
        con.close()


if __name__ == "__main__":
    main()