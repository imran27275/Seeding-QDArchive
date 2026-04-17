import argparse
import json
import logging
import signal
import sys
from datetime import datetime, timezone
from pathlib import Path

from config import (
    LOG_FILE, DATA_DIR, DOWNLOAD_DELAY, FILES_DIR,
    SEARCH_KEYWORDS, PROGRESS_FILE, PROGRESS_SAVE_INTERVAL,
    DOWNLOAD_TARGET_BYTES,
)
from database import (
    init_db, get_connection,
    insert_project, insert_file,
    insert_keyword, insert_person, insert_license,
    update_file_status, print_stats, export_all,
    SUCCESS, FAILED, RESTRICTED, SKIPPED,
)
from downloader import download_file, AccessRestrictedError, polite_delay
from scrapers.dans_scraper import DANSScraper
from scrapers.uni_halle_scraper import UniHalleScraper

# Logging 
DATA_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("pipeline")

# Graceful shutdown flag 
_shutdown_requested = False


def _handle_sigint(sig, frame):
    """Called when user presses Ctrl+C."""
    global _shutdown_requested
    if not _shutdown_requested:
        _shutdown_requested = True
        print("\n\n⚠️  Ctrl+C detected — finishing current project then saving progress...")
        print("   Press Ctrl+C again to force quit immediately.\n")
    else:
        print("\n🛑 Force quit.")
        sys.exit(1)


signal.signal(signal.SIGINT, _handle_sigint)


# Progress tracking

def load_progress() -> dict:
    """Load saved progress from disk. Returns empty dict if none."""
    if PROGRESS_FILE.exists():
        try:
            with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            logger.info("Loaded progress: %d projects already completed.",
                        sum(len(v) for v in data.get("completed", {}).values()))
            return data
        except Exception as e:
            logger.warning("Could not read progress file: %s", e)
    return {"completed": {}, "last_saved": None}


def save_progress(progress: dict) -> None:
    """Save progress to disk."""
    PROGRESS_FILE.parent.mkdir(parents=True, exist_ok=True)
    progress["last_saved"] = datetime.now(timezone.utc).isoformat()
    try:
        with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
            json.dump(progress, f, indent=2)
        logger.info("💾 Progress saved (%d total projects completed).",
                    sum(len(v) for v in progress.get("completed", {}).values()))
    except Exception as e:
        logger.error("Could not save progress: %s", e)


def mark_completed(progress: dict, source: str, project_url: str) -> None:
    """Mark a project URL as completed in the progress dict."""
    if source not in progress["completed"]:
        progress["completed"][source] = []
    if project_url not in progress["completed"][source]:
        progress["completed"][source].append(project_url)


def is_completed(progress: dict, source: str, project_url: str) -> bool:
    """Check if a project was already processed in a previous run."""
    return project_url in progress.get("completed", {}).get(source, [])


# Core runner

def _get_disk_usage(folder: Path) -> int:
    """Return total bytes of all files in folder recursively."""
    if not folder.exists():
        return 0
    return sum(f.stat().st_size for f in folder.rglob("*")
               if f.is_file() and f.suffix != ".tmp")


def run_scraper(scraper, con, download: bool = True,
                progress: dict = None,
                cap_bytes: int = None) -> int:
    """
    1. Harvest all projects from the scraper
    2. Skip already-completed ones (from progress.json)
    3. For each new project:
       - Insert project + keywords/persons/licenses into DB
       - Download ALL files (QDA + companion: PDF, audio, video, etc.)
       - Mark as completed in progress
    4. Auto-save progress every PROGRESS_SAVE_INTERVAL projects
    5. Stop cleanly if Ctrl+C was pressed
    6. Stop if cap_bytes is set and disk usage exceeds it

    Returns number of new projects saved this run.
    """
    global _shutdown_requested

    if progress is None:
        progress = {"completed": {}}

    source = scraper.SOURCE_NAME

    logger.info("=" * 60)
    logger.info("Starting %s (repo #%d)", source, scraper.REPO_ID)
    logger.info("=" * 60)

    projects = scraper.scrape_all(SEARCH_KEYWORDS)
    logger.info("[%s] %d projects found.", source, len(projects))

    # Filter out already-completed projects
    pending = [p for p in projects
               if not is_completed(progress, source, p["project_url"])]
    skipped_count = len(projects) - len(pending)
    if skipped_count:
        logger.info("[%s] Skipping %d already-completed projects.",
                    source, skipped_count)
    logger.info("[%s] %d projects to process this run.", source, len(pending))

    saved             = 0
    since_last_save   = 0

    for idx, project in enumerate(pending, start=1):

        # Graceful shutdown check 
        if _shutdown_requested:
            logger.info("[%s] Shutdown requested. Stopping after %d projects.",
                        source, saved)
            save_progress(progress)
            break

        title = project.get("title", "untitled")
        logger.info("[%s] [%d/%d] %s", source, idx, len(pending), title[:65])

        # Insert project 
        project_id = insert_project(
            con,
            query_string               = project.get("query_string"),
            repository_id              = project["repository_id"],
            repository_url             = project["repository_url"],
            project_url                = project["project_url"],
            version                    = project.get("version"),
            title                      = title,
            description                = project.get("description"),
            language                   = project.get("language"),
            doi                        = project.get("doi"),
            upload_date                = project.get("upload_date"),
            download_repository_folder = project["download_repository_folder"],
            download_project_folder    = project["download_project_folder"],
            download_version_folder    = project.get("download_version_folder"),
            download_method            = project["download_method"],
        )
        if not project_id:
            # Already in DB from a previous run — still mark complete
            mark_completed(progress, source, project["project_url"])
            continue

        # Insert keywords / persons / licenses
        for kw in scraper.get_keywords(project):
            insert_keyword(con, project_id, kw)
        for name, role in scraper.get_persons(project):
            insert_person(con, project_id, name, role)
        for lic in scraper.get_licenses(project):
            insert_license(con, project_id, lic)

        # Process ALL files
        files = scraper.get_files(project)

        qda_files  = [f for f in files if scraper.is_qda_file(f["file_name"])]
        comp_files = [f for f in files if scraper.is_companion_file(f["file_name"])]
        other_files= [f for f in files
                      if not scraper.is_qda_file(f["file_name"])
                      and not scraper.is_companion_file(f["file_name"])]

        logger.info(
            "  Files: %d QDA | %d companion (PDF/audio/video/etc.) | %d other",
            len(qda_files), len(comp_files), len(other_files)
        )

        for f in files:
            restricted  = f.get("restricted", False)
            file_url    = f.get("file_url")
            local_path  = f.get("local_path")
            status      = RESTRICTED if restricted else SKIPPED

            # Insert file record (only professor-specified columns)
            file_id = insert_file(
                con,
                project_id = project_id,
                file_name  = f["file_name"],
                file_type  = f["file_type"],
                status     = status,
            )

            if restricted:
                logger.info("  🔒 Restricted: %s", f["file_name"])
                continue

            if not download or not file_url or not local_path:
                continue

            # Size cap check (real disk usage)
            if cap_bytes is not None and download:
                used = _get_disk_usage(FILES_DIR)
                if used >= cap_bytes:
                    logger.warning(
                        "🛑 Size cap reached (%.1f GB used / %.1f GB limit). "
                        "Stopping downloads for this scraper.",
                        used / (1024**3), cap_bytes / (1024**3)
                    )
                    update_file_status(con, file_id, status=SKIPPED)
                    # Skip remaining files in this project too
                    continue

            # Download
            polite_delay(DOWNLOAD_DELAY)
            try:
                ok, dl_status = download_file(
                    url      = file_url,
                    out_path = Path(local_path),
                    session  = scraper.session,
                )
                update_file_status(
                    con, file_id,
                    status = SUCCESS if ok else FAILED,
                )
                icon = "✅" if ok else "❌"
                logger.info("  %s %s", icon, f["file_name"])

            except AccessRestrictedError as e:
                update_file_status(con, file_id, status=RESTRICTED)
                logger.info("  🔒 Access denied: %s", f["file_name"])

            except Exception as e:
                update_file_status(con, file_id, status=FAILED)
                logger.error("  ❌ Error for %s: %s", f["file_name"], e)

        # Mark project complete
        mark_completed(progress, source, project["project_url"])
        saved           += 1
        since_last_save += 1

        # Auto-save progress every N projects
        if since_last_save >= PROGRESS_SAVE_INTERVAL:
            save_progress(progress)
            since_last_save = 0
            logger.info("💾 Auto-saved progress after %d projects.", saved)

    # Final save for this source
    save_progress(progress)
    logger.info("[%s] Done. %d new projects saved this run.", source, saved)
    return saved


# MAIN

def _print_final_report(con, totals: dict, progress: dict,
                        stopped_early: bool) -> None:
    """
    Print a detailed, human-readable final report to the terminal
    showing exactly what was downloaded and what the DB contains.
    """
    from config import QDA_EXTENSIONS, FILES_DIR
    cur = con.cursor()
    sep  = "═" * 62
    sep2 = "─" * 62
    sep3 = "·" * 62

    print(f"\n{sep}")
    if stopped_early:
        print("  ⚠️   PIPELINE STOPPED EARLY (Ctrl+C)")
    else:
        print("  ✅  PIPELINE COMPLETE")
    print(sep)

    # Per-repository breakdown
    for repo_label, new_count in totals.items():
        print(f"\n  📦 {repo_label}")
        print(sep2)
        print(f"  New projects this run    : {new_count}")

    # Database totals
    print(f"\n{sep2}")
    print("  📊 DATABASE TOTALS")
    print(sep2)

    cur.execute("SELECT COUNT(*) FROM projects")
    total_projects = cur.fetchone()[0]
    print(f"  Total projects           : {total_projects}")

    cur.execute("""
        SELECT download_repository_folder, COUNT(*)
        FROM projects GROUP BY download_repository_folder
    """)
    for folder, count in cur.fetchall():
        print(f"    {folder:<30} : {count}")

    # Files breakdown
    print(sep3)
    cur.execute("SELECT COUNT(*) FROM files")
    total_files = cur.fetchone()[0]
    print(f"  Total files in DB        : {total_files}")

    cur.execute("SELECT status, COUNT(*) FROM files GROUP BY status ORDER BY COUNT(*) DESC")
    for status, count in cur.fetchall():
        icon = {"SUCCESS": "✅", "FAILED": "❌", "RESTRICTED": "🔒",
                "SKIPPED": "⏭", "ALREADY_EXISTS": "♻️"}.get(status, "•")
        print(f"    {icon} {status:<28} : {count}")

    # QDA file breakdown
    print(sep3)
    # Build QDA extension filter for SQL
    qda_ext_list = "', '".join(e.lstrip(".") for e in QDA_EXTENSIONS)
    cur.execute(f"""
        SELECT COUNT(*) FROM files
        WHERE status = 'SUCCESS'
        AND LOWER(file_type) IN ('{qda_ext_list}')
    """)
    qda_downloaded = cur.fetchone()[0]

    cur.execute(f"""
        SELECT file_type, COUNT(*) FROM files
        WHERE status = 'SUCCESS'
        AND LOWER(file_type) IN ('{qda_ext_list}')
        GROUP BY file_type ORDER BY COUNT(*) DESC
    """)
    qda_rows = cur.fetchall()

    cur.execute("SELECT COUNT(*) FROM files WHERE status = 'SUCCESS'")
    total_success = cur.fetchone()[0]

    print(f"  Successfully downloaded  : {total_success} files")
    print(f"  Of which QDA files       : {qda_downloaded}")
    if qda_rows:
        for ftype, count in qda_rows:
            print(f"    .{ftype:<29} : {count}")
    else:
        print("    (none found)")

    # Companion file types
    print(sep3)
    cur.execute(f"""
        SELECT file_type, COUNT(*) FROM files
        WHERE status = 'SUCCESS'
        AND LOWER(file_type) NOT IN ('{qda_ext_list}')
        GROUP BY file_type ORDER BY COUNT(*) DESC
        LIMIT 10
    """)
    companion_rows = cur.fetchall()
    companion_count = total_success - qda_downloaded
    print(f"  Companion files          : {companion_count}")
    if companion_rows:
        for ftype, count in companion_rows:
            print(f"    .{ftype:<29} : {count}")

    # Disk usage
    print(sep3)
    total_disk = 0
    if FILES_DIR.exists():
        for f in FILES_DIR.rglob("*"):
            if f.is_file() and not f.suffix == ".tmp":
                total_disk += f.stat().st_size
    print(f"  Disk usage (files/)      : {_human_size(total_disk)}")

    # Metadata tables
    print(sep3)
    for table in ("keywords", "person_role", "licenses"):
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        print(f"  {table:<27}  : {cur.fetchone()[0]} rows")

    # All-run totals
    print(sep3)
    total_ever = sum(len(v) for v in progress.get("completed", {}).values())
    print(f"  Total projects (all runs): {total_ever}")

    print(f"\n{sep}")
    print(f"  📁 Files   : {FILES_DIR.resolve()}")
    from config import DB_PATH, CSV_DIR
    print(f"  🗄  DB     : {DB_PATH.resolve()}")
    print(f"  📄 CSVs   : {CSV_DIR.resolve()}")
    print(sep)


def _human_size(num_bytes: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(num_bytes) < 1024.0:
            return f"{num_bytes:.2f} {unit}"
        num_bytes /= 1024.0
    return f"{num_bytes:.2f} PB"



def main():
    parser = argparse.ArgumentParser(
        description="Seeding-QDArchive — Data Acquisition Pipeline"
    )
    parser.add_argument(
        "--source",
        choices=["dans", "uni_halle", "both"],
        default="both",
        help="Which repository to acquire from (default: both)",
    )
    parser.add_argument(
        "--no-download",
        action="store_true",
        help="Collect metadata only — do not download files",
    )
    parser.add_argument(
        "--stats",
        action="store_true",
        help="Print database statistics and exit",
    )
    parser.add_argument(
        "--export",
        action="store_true",
        help="Export all tables to CSV and exit",
    )
    parser.add_argument(
        "--reset-progress",
        action="store_true",
        help="Ignore saved progress and start fresh (does NOT clear the DB)",
    )
    args = parser.parse_args()

    init_db()
    con = get_connection()

    if args.stats:
        print_stats(con)
        con.close()
        return

    if args.export:
        print("\n📤 Exporting to CSV...")
        export_all(con)
        print_stats(con)
        con.close()
        return

    # Load or reset progress
    if args.reset_progress:
        progress = {"completed": {}}
        logger.info("Progress reset — starting fresh scan.")
    else:
        progress = load_progress()

    download = not args.no_download
    source   = args.source
    totals   = {}

    logger.info("=" * 60)
    logger.info("Seeding-QDArchive Pipeline started")
    logger.info("  Source    : %s", source)
    logger.info("  Download  : %s", "YES — all file types" if download
                else "NO — metadata only")
    logger.info("  Auto-save : every %d projects", PROGRESS_SAVE_INTERVAL)
    logger.info("  Stop      : Ctrl+C for graceful exit with progress saved")
    logger.info("=" * 60)

    # DANS — all 4 stations with shared deduplication
    if source in ("dans", "both") and not _shutdown_requested:
        shared_dois  = set()   # global DOI deduplication across stations
        bytes_so_far = 0       # running total for size cap
        dans_total   = 0

        station_keys = ["dans_ssh", "dans_archaeology",
                        "dans_lifesciences", "dans_phys"]

        for station_key in station_keys:
            if _shutdown_requested:
                break
            # Check real disk usage — stop if cap already reached
            current_usage = _get_disk_usage(FILES_DIR)
            if download and current_usage >= DOWNLOAD_TARGET_BYTES:
                logger.warning(
                    "🛑 Size cap reached (%.1f GB). "
                    "Skipping remaining DANS stations.",
                    current_usage / (1024**3)
                )
                break
            scraper = DANSScraper(
                repo_key  = station_key,
                seen_dois = shared_dois,
            )
            n = run_scraper(scraper, con,
                            download=download, progress=progress,
                            cap_bytes=DOWNLOAD_TARGET_BYTES)
            dans_total += n
            totals[f"DANS {station_key}"] = n

        totals["DANS TOTAL (all stations)"] = dans_total

    # uni-halle (Repository #16) 
    if source in ("uni_halle", "both") and not _shutdown_requested:
        n = run_scraper(UniHalleScraper(), con,
                        download=download, progress=progress)
        totals["uni-halle (repo #16)"] = n

    # Auto-export CSVs
    logger.info("Exporting to CSV...")
    export_all(con)

    # Final rich terminal report 
    _print_final_report(con, totals, progress, _shutdown_requested)

    con.close()
    logger.info("Progress saved to: %s", PROGRESS_FILE)


if __name__ == "__main__":
    main()