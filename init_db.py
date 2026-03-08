import sqlite3
import sys
from pathlib import Path

DB_PATH = Path("data/metadata.db")

def main():
    try:
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        con = sqlite3.connect(DB_PATH)
        cur = con.cursor()

        cur.executescript("""
        CREATE TABLE IF NOT EXISTS datasets (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            source           TEXT NOT NULL,
            source_record_id TEXT NOT NULL,
            source_url       TEXT,
            title            TEXT,
            license          TEXT,
            published        TEXT,
            downloaded_at    TEXT,
            local_folder     TEXT,
            notes            TEXT,
            UNIQUE(source, source_record_id)
        );

        CREATE TABLE IF NOT EXISTS files (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            dataset_id    INTEGER NOT NULL,
            file_name     TEXT,
            file_url      TEXT,
            size          INTEGER,
            local_path    TEXT,
            downloaded_at TEXT,
            FOREIGN KEY(dataset_id) REFERENCES datasets(id)
        );

        CREATE INDEX IF NOT EXISTS idx_datasets_source_record
            ON datasets(source, source_record_id);

        CREATE INDEX IF NOT EXISTS idx_files_dataset_id
            ON files(dataset_id);
        """)

        con.commit()
        con.close()
        print(f"✅ Database initialized at: {DB_PATH}")

    except sqlite3.Error as e:
        print(f"❌ Database error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()