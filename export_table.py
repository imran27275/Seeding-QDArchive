"""
Usage:
    python export_table.py 23293539-sq26-classification.db results.xlsx
"""
import sqlite3
import argparse
import pandas as pd


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("db_path")
    ap.add_argument("out_xlsx")
    args = ap.parse_args()

    con = sqlite3.connect(args.db_path)
    df = pd.read_sql_query("""
        SELECT
            p.repository_id      AS repository_id,
            p.type                AS project_type,
            p.title               AS project_title,
            p.primary_class       AS primary_class,
            p.secondary_class     AS secondary_class,
            (SELECT COUNT(*) FROM files f WHERE f.project_id = p.id) AS no_project_files
        FROM projects p
        WHERE p.type IN ('QDA_PROJECT', 'QD_PROJECT')
        ORDER BY p.repository_id, p.type
    """, con)
    con.close()

    df.to_excel(args.out_xlsx, index=False)
    print(f"Wrote {len(df)} rows to {args.out_xlsx}")


if __name__ == "__main__":
    main()