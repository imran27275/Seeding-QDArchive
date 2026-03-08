import csv
import sqlite3
import sys
from pathlib import Path

DB_PATH  = Path("data/metadata.db")
OUT_DIR  = Path("data/csv")

ALLOWED_TABLES = {"datasets", "files"}


def export_table(con: sqlite3.Connection, table: str, out_path: Path) -> int:
    if table not in ALLOWED_TABLES:
        raise ValueError(f"Table '{table}' is not in the allowed list.")

    cur = con.cursor()
    cur.execute(f"SELECT * FROM {table}")  # safe — whitelisted above
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cols)
        w.writerows(rows)

    return len(rows)


def main():
    if not DB_PATH.exists():
        print(f"❌ DB not found at {DB_PATH}. Run init_db.py and download.py first.", file=sys.stderr)
        sys.exit(1)

    try:
        con = sqlite3.connect(DB_PATH)
        for table in sorted(ALLOWED_TABLES):
            out_path = OUT_DIR / f"{table}.csv"
            count    = export_table(con, table, out_path)
            print(f"✅ '{table}' → {out_path} ({count} rows)")
        con.close()
        print(f"\n📁 CSVs saved to: {OUT_DIR}")

    except sqlite3.Error as e:
        print(f"❌ Database error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()