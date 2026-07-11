"""
Part 2 / Step 4d — generate the final PDF report (real schema version).

Usage:
    python generate_report.py 23293539-sq26-classification.db report.pdf
"""
import sqlite3
import json
import argparse
import textwrap
from collections import Counter
from pathlib import Path

import matplotlib
matplotlib.use("pdf")
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

# Fill in your own observations here, keyed by repository_id (as it appears in
# your `projects` table). Run repository_summary.py first to see the ids.
# Leave a repository OUT of this dict (or set it to "") to skip its comments
# page entirely in the final PDF.
# COMMENTS = {
#     5: "Dominated by Scientific research and development (34 of 68 projects), reflecting...",
#     16: "Printing and reproduction of recorded media dominates unusually strongly (49 of ~150)...",
# }
COMMENTS = {}


def load_titles(taxonomy_path):
    taxonomy = json.loads(Path(taxonomy_path).read_text(encoding="utf-8"))
    return {code: entry["title"] for code, entry in taxonomy.items()}


def top_classes(con, repo, titles, top_n=20):
    rows = con.execute("""
        SELECT primary_class FROM projects
        WHERE repository_id=? AND type IN ('QDA_PROJECT','QD_PROJECT')
              AND primary_class IS NOT NULL AND primary_class != ''
    """, (repo,)).fetchall()
    counts = Counter(titles.get(r[0], r[0]) for r in rows)
    return counts.most_common(top_n)


def draw_histogram(pdf, repo, ranked):
    # Horizontal bars read much better than rotated vertical labels once class
    # names get long (ISIC titles can run 40-100+ characters). Long labels are
    # wrapped onto 2 lines instead of stretching the chart sideways.
    WRAP_WIDTH = 52
    n = len(ranked)
    fig_height = max(4, 0.55 * n + 1.5)  # a bit taller since wrapped labels take 2 lines
    fig, ax = plt.subplots(figsize=(11, fig_height))

    # Most common class at the TOP of the chart (reverse order, since barh
    # plots bottom-to-top).
    raw_labels = [name for name, _ in ranked][::-1]
    labels = ["\n".join(textwrap.wrap(name, WRAP_WIDTH)) for name in raw_labels]
    values = [count for _, count in ranked][::-1]

    bars = ax.barh(range(len(labels)), values, color="#4C72B0", height=0.6)
    ax.set_yticks(range(len(labels)))
    ax.set_yticklabels(labels, fontsize=9)
    ax.set_xlabel("Number of projects")

    max_val = max(values) if values else 1
    ax.set_xlim(0, max_val * 1.12)  # headroom so count labels don't get clipped
    for bar, val in zip(bars, values):
        ax.text(bar.get_width() + max_val * 0.015, bar.get_y() + bar.get_height() / 2,
                str(val), va="center", ha="left", fontsize=9)

    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    # Reserve a fixed fraction of the figure for labels so the plot area
    # stays a consistent size regardless of label length.
    fig.subplots_adjust(left=0.32, right=0.95, top=0.90, bottom=0.08)
    # fig.suptitle centers over the whole page, not just the (shrunk) axes,
    # so it stays visually centered regardless of label width.
    fig.suptitle(f"Repository {repo} — Primary class distribution", fontsize=13, x=0.5)
    pdf.savefig(fig)
    plt.close(fig)


def draw_table(pdf, repo, ranked):
    fig, ax = plt.subplots(figsize=(11, 8.5))
    ax.axis("off")
    ax.set_title(f"Repository {repo} — Top {len(ranked)} classes",
                 fontsize=13, pad=20)
    table_data = [[str(i + 1), name, str(count)] for i, (name, count) in enumerate(ranked)]
    table = ax.table(
        cellText=table_data,
        colLabels=["Rank", "Primary class", "Count"],
        colWidths=[0.06, 0.88, 0.06],  # sums to 1.0 -> no dead gap with bbox=[0,0,1,1]
        loc="center",
        cellLoc="left",
        bbox=[0, 0, 1, 1],
    )
    table.auto_set_font_size(False)
    table.set_fontsize(9)
    table.scale(1, 1.4)
    pdf.savefig(fig)
    plt.close(fig)


def draw_comments(pdf, repo):
    comment = COMMENTS.get(repo, "").strip()
    if not comment:
        return  # no comment provided -> skip this page entirely

    wrapped_lines = textwrap.wrap(comment, 100)
    wrapped = "\n".join(wrapped_lines)
    # Grow the page a bit for longer comments so text never gets clipped.
    fig_height = max(4, 0.28 * len(wrapped_lines) + 1.5)
    fig, ax = plt.subplots(figsize=(11, fig_height))
    ax.axis("off")
    ax.set_title(f"Repository {repo} — Comments", fontsize=13)
    ax.text(0.02, 0.92, wrapped, fontsize=10, va="top", ha="left")
    pdf.savefig(fig)
    plt.close(fig)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("db_path")
    ap.add_argument("out_pdf")
    ap.add_argument("--taxonomy", default="data/isic_taxonomy.json")
    ap.add_argument("--top-n", type=int, default=20)
    args = ap.parse_args()

    titles = load_titles(args.taxonomy)
    con = sqlite3.connect(args.db_path)
    repo_ids = [r[0] for r in con.execute("SELECT DISTINCT repository_id FROM projects")]

    with PdfPages(args.out_pdf) as pdf:
        for repo in repo_ids:
            ranked = top_classes(con, repo, titles, args.top_n)
            if not ranked:
                continue
            draw_histogram(pdf, repo, ranked)
            draw_table(pdf, repo, ranked)
            draw_comments(pdf, repo)

    con.close()
    print(f"Wrote report to {args.out_pdf}")


if __name__ == "__main__":
    main()