"""
Usage:
    python run_classification.py 23293539-sq26.db 23293539-sq26-classification.db
"""
import sqlite3
import sys
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from classifier import IsicClassifier, best_effort_extract_text  # noqa: E402

CLASSIFIABLE_TYPES = {"QDA_PROJECT", "QD_PROJECT"}

PRIMARY_EXTS = {
    ".txt", ".pdf", ".rtf", ".docx", ".doc", ".odt",
    ".mp3", ".wav", ".m4a",
    ".mp4", ".mov", ".avi",
    ".jpg", ".jpeg", ".png", ".tif", ".tiff",
}


def ext_of(filename: str) -> str:
    return Path((filename or "").lower()).suffix


def ensure_column(con, table, col, coltype="TEXT"):
    cols = [r[1] for r in con.execute(f"PRAGMA table_info({table})").fetchall()]
    if col not in cols:
        con.execute(f"ALTER TABLE {table} ADD COLUMN {col} {coltype}")


def prepare_primary_class_column(con, table):
    cols = [r[1] for r in con.execute(f"PRAGMA table_info({table})").fetchall()]
    if "primary_class" in cols:
        return

    if "class" in cols:
        has_data = con.execute(
            f"SELECT COUNT(*) FROM {table} WHERE class IS NOT NULL AND class != ''"
        ).fetchone()[0]
        if has_data:
            con.execute(f"ALTER TABLE {table} RENAME COLUMN class TO primary_class")
            print(f"↪ {table}.class had existing data — renamed to primary_class")
            return
        else:
            try:
                con.execute(f"ALTER TABLE {table} DROP COLUMN class")
                print(f"🗑 Dropped empty legacy column {table}.class")
            except sqlite3.OperationalError:
                # Very old SQLite without DROP COLUMN support (< 3.35) — leave
                # the unused column in place, it's harmless either way.
                print(f"⚠ Could not drop {table}.class (old SQLite) — leaving it, "
                      f"it's unused and harmless")

    con.execute(f"ALTER TABLE {table} ADD COLUMN primary_class TEXT")


def guess_file_path(project_row, file_name):
    candidates = []
    folders = [
        project_row["download_version_folder"],
        project_row["download_project_folder"],
        project_row["download_repository_folder"],
    ]
    for f in folders:
        if f:
            candidates.append(Path(f) / file_name)
    for i in range(len(folders) - 1):
        if folders[i] and folders[i + 1]:
            candidates.append(Path(folders[i]) / Path(folders[i + 1]).name / file_name)

    for c in candidates:
        if c.exists():
            return c
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("in_db", help="your existing db, e.g. 23293539-sq26.db")
    ap.add_argument("out_db", help="e.g. 23293539-sq26-classification.db")
    ap.add_argument("--taxonomy", default="data/isic_taxonomy.json")
    args = ap.parse_args()

    in_path = Path(args.in_db)
    out_path = Path(args.out_db)
    if out_path.exists():
        print(f"⚠ {out_path} exists — deleting to rebuild fresh.")
        out_path.unlink()

    # Use SQLite's backup API rather than a raw file copy: if the source db
    # was ever opened in WAL mode, some committed data can still be sitting
    # in a separate -wal file rather than the main .db file, and a plain
    # file copy would silently miss it (this is what caused the earlier
    # "no such table: projects" error). The backup API always produces a
    # fully consistent copy regardless of WAL state.
    src_con = sqlite3.connect(in_path)
    out_con = sqlite3.connect(out_path)
    src_con.backup(out_con)
    src_con.close()
    print(f"Copied {in_path} -> {out_path} (via sqlite3 backup API)")

    con = out_con
    con.row_factory = sqlite3.Row

    prepare_primary_class_column(con, "projects")
    prepare_primary_class_column(con, "files")
    ensure_column(con, "projects", "secondary_class")
    ensure_column(con, "files", "secondary_class")
    con.commit()

    clf = IsicClassifier(args.taxonomy)

    projects = con.execute("SELECT * FROM projects").fetchall()
    n_classified = 0

    for proj in projects:
        if (proj["type"] or "") not in CLASSIFIABLE_TYPES:
            continue

        pid = proj["id"]
        files = con.execute("SELECT * FROM files WHERE project_id=?", (pid,)).fetchall()
        licenses = [r[0] for r in con.execute(
            "SELECT license FROM licenses WHERE project_id=?", (pid,)).fetchall()]
        existing_keywords = [r[0] for r in con.execute(
            "SELECT keyword FROM keywords WHERE project_id=?", (pid,)).fetchall()]

        primary_files = [f for f in files if ext_of(f["file_name"]) in PRIMARY_EXTS]

        # ---- Project-level classification --------------------------------
        text_parts = [
            proj["title"] or "", proj["description"] or "", proj["language"] or "",
            " ".join(licenses), " ".join(existing_keywords),
        ]
        text_parts += [f["file_name"] or "" for f in files]
        for f in primary_files[:5]:
            path = guess_file_path(proj, f["file_name"])
            if path:
                text_parts.append(best_effort_extract_text(str(path)))
        project_text = " ".join(text_parts)

        p_class, s_class, tags = clf.classify(project_text)
        con.execute("UPDATE projects SET primary_class=?, secondary_class=? WHERE id=?",
                    (p_class, s_class, pid))
        for tag in tags:
            con.execute("INSERT OR IGNORE INTO keywords (project_id, keyword) VALUES (?, ?)",
                        (pid, tag))
        n_classified += 1

        # ---- File-level classification (primary data files only) ---------
        for f in primary_files:
            path = guess_file_path(proj, f["file_name"])
            file_text = " ".join([
                f["file_name"] or "",
                best_effort_extract_text(str(path)) if path else "",
            ])
            fp_class, fs_class, _ftags = clf.classify(file_text)
            con.execute("UPDATE files SET primary_class=?, secondary_class=? WHERE id=?",
                        (fp_class, fs_class, f["id"]))

    con.commit()
    con.close()
    print(f"Classified {n_classified} projects (QDA_PROJECT + QD_PROJECT) "
          f"in {out_path}")

if __name__ == "__main__":
    main()