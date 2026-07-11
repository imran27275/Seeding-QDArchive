"""
Usage:
    python repository_summary.py 23293539-sq26-classification.db
"""
import sqlite3
import json
import argparse
from collections import Counter
from pathlib import Path


def load_titles(taxonomy_path):
    taxonomy = json.loads(Path(taxonomy_path).read_text(encoding="utf-8"))
    return {code: entry["title"] for code, entry in taxonomy.items()}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("db_path")
    ap.add_argument("--taxonomy", default="data/isic_taxonomy.json")
    args = ap.parse_args()

    titles = load_titles(args.taxonomy)
    con = sqlite3.connect(args.db_path)

    repo_ids = [r[0] for r in con.execute(
        "SELECT DISTINCT repository_id FROM projects").fetchall()]

    for repo in repo_ids:
        print(f"\n{'=' * 60}\nRepository ID: {repo}\n{'=' * 60}")

        type_counts = Counter(
            r[0] for r in con.execute(
                "SELECT type FROM projects WHERE repository_id=?", (repo,))
        )
        print("Project types found:")
        for t, c in type_counts.most_common():
            print(f"   {t or '(unclassified)':<15} {c}")

        class_counts = Counter(
            r[0] for r in con.execute("""
                SELECT primary_class FROM projects
                WHERE repository_id=? AND type IN ('QDA_PROJECT','QD_PROJECT')
                      AND primary_class IS NOT NULL AND primary_class != ''
            """, (repo,))
        )
        if class_counts:
            dominant, n = class_counts.most_common(1)[0]
            print(f"\nDominant primary class: {dominant} — "
                  f"{titles.get(dominant, '?')} ({n} project(s))")
        else:
            print("\nNo classified projects (QDA_PROJECT/QD_PROJECT) in this repository.")

    con.close()


if __name__ == "__main__":
    main()