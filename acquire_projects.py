"""
Pipeline to download QDA datasets from two repositories:
  1. opendata.uni-halle.de  — DSpace 6 REST API
  2. dans.knaw.nl / dataverse.nl — Dataverse API
"""

import sqlite3
import sys
import time
from pathlib import Path
from datetime import datetime, timezone

import requests
from tqdm import tqdm

# ── Paths ──────────────────────────────────────────────────────────────────────
DB_PATH     = Path("data/metadata.db")
ARCHIVE_DIR = Path("archive/opendata")

# ── API base URLs ──────────────────────────────────────────────────────────────
UNI_HALLE_REST = "https://opendata.uni-halle.de/rest"   # DSpace 6 REST API
DANS_API       = "https://dataverse.nl/api"             # Dataverse API

# ── QDA file extensions ────────────────────────────────────────────────────────
QDA_EXTS = {".qdpx", ".nvpx", ".atlproj", ".mx", ".mx24", ".mx20"}

# ── Retry config ───────────────────────────────────────────────────────────────
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds

QDA_KEYWORDS = ["qdpx", "nvpx", "MAXQDA", "ATLAS.ti", "atlproj"]


# ── Shared helpers ─────────────────────────────────────────────────────────────

def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_name(s: str, max_len: int = 80) -> str:
    s = "".join(ch if ch.isalnum() or ch in "._- " else "" for ch in (s or ""))
    s = "_".join(s.split())
    return (s[:max_len] or "untitled").strip("_")


def is_qda_file(filename: str) -> bool:
    return Path(filename.lower()).suffix in QDA_EXTS


class AccessRestrictedError(Exception):
    """Raised when a file is behind a login / access request (401, 403)."""
    pass


def download_file(url: str, out_path: Path, session: requests.Session) -> bool:
    """Download file with retry + atomic write (.tmp rename).
    Returns True on success.
    Raises AccessRestrictedError immediately on 401/403 — no point retrying.
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if out_path.exists() and out_path.stat().st_size > 0:
        print(f"    ⏭  Already exists: {out_path.name}")
        return True

    tmp_path = out_path.with_suffix(out_path.suffix + ".tmp")

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with session.get(url, stream=True, timeout=120) as r:
                # 401/403 = access restricted — retrying won't help
                if r.status_code in (401, 403):
                    raise AccessRestrictedError(
                        f"HTTP {r.status_code} — file is access-restricted: {url}"
                    )
                r.raise_for_status()
                total = int(r.headers.get("content-length", 0))
                with open(tmp_path, "wb") as f, tqdm(
                    total=total, unit="B", unit_scale=True,
                    desc=f"    {out_path.name[:50]}", leave=True
                ) as bar:
                    for chunk in r.iter_content(chunk_size=256 * 1024):
                        if chunk:
                            f.write(chunk)
                            bar.update(len(chunk))
            tmp_path.rename(out_path)
            return True

        except AccessRestrictedError:
            if tmp_path.exists():
                tmp_path.unlink()
            raise  # bubble up immediately — do not retry

        except requests.RequestException as e:
            print(f"    ⚠  Attempt {attempt}/{MAX_RETRIES} failed: {e}", file=sys.stderr)
            if tmp_path.exists():
                tmp_path.unlink()
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)

    print(f"    ❌ All retries exhausted: {url}", file=sys.stderr)
    return False


def db_upsert_dataset(cur, source, rec_id, source_url, title, license_, pub, folder) -> int:
    cur.execute("""
        INSERT OR IGNORE INTO datasets
        (source, source_record_id, source_url, title, license, published,
         downloaded_at, local_folder, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (source, str(rec_id), source_url, title, license_, pub, now_utc(), str(folder), ""))
    cur.execute(
        "SELECT id FROM datasets WHERE source=? AND source_record_id=?",
        (source, str(rec_id))
    )
    return cur.fetchone()[0]


def db_insert_file(cur, dataset_id, name, url, size, local_path):
    cur.execute("""
        INSERT OR IGNORE INTO files
        (dataset_id, file_name, file_url, size, local_path, downloaded_at)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (dataset_id, name, url, size, str(local_path), now_utc()))


# ══════════════════════════════════════════════════════════════════════════════
# SOURCE 1 — opendata.uni-halle.de (DSpace 6 REST API)
# Docs: https://wiki.lyrasis.org/display/DSDOC6x/REST+API
# ══════════════════════════════════════════════════════════════════════════════

def dspace_get(path: str, session: requests.Session, params=None):
    url = f"{UNI_HALLE_REST}{path}"
    r = session.get(url, params=params, headers={"Accept": "application/json"}, timeout=60)
    r.raise_for_status()
    return r.json()


def dspace_metadata_value(meta_list: list, key: str) -> str:
    for m in meta_list:
        if m.get("key") == key:
            return m.get("value", "")
    return ""


def download_from_uni_halle(con, session: requests.Session, target: int = 5) -> int:
    print("\n🔍 [uni-halle] Crawling DSpace items...")
    cur = con.cursor()
    downloaded = 0
    offset = 0
    page_size = 50

    while downloaded < target:
        try:
            items = dspace_get("/items", session, params={"limit": page_size, "offset": offset, "expand": "metadata,bitstreams"})
        except requests.RequestException as e:
            print(f"❌ [uni-halle] API error: {e}", file=sys.stderr)
            break

        if not items:
            print("ℹ  [uni-halle] No more items.")
            break

        for item in items:
            if downloaded >= target:
                break

            item_id = item.get("uuid") or str(item.get("id", ""))
            handle  = item.get("handle", "")

            # Use expanded bitstreams if available, else fetch separately
            bitstreams = item.get("bitstreams") or []
            if not bitstreams:
                try:
                    bitstreams = dspace_get(f"/items/{item_id}/bitstreams", session)
                except requests.RequestException:
                    continue

            # Only proceed if dataset has at least one QDA file
            qda_found = any(is_qda_file(b.get("name", "")) for b in bitstreams)
            if not qda_found:
                continue

            # Use expanded metadata if available, else fetch separately
            meta = item.get("metadata") or []
            if not meta:
                try:
                    meta = dspace_get(f"/items/{item_id}/metadata", session)
                except requests.RequestException:
                    pass

            title      = dspace_metadata_value(meta, "dc.title") or item.get("name", "untitled")
            pub        = dspace_metadata_value(meta, "dc.date.issued")
            license_   = dspace_metadata_value(meta, "dc.rights")
            source_url = f"https://opendata.uni-halle.de/handle/{handle}" if handle else ""

            folder = ARCHIVE_DIR / f"uni_halle_{item_id}_{safe_name(title)}"
            folder.mkdir(parents=True, exist_ok=True)

            dataset_db_id = db_upsert_dataset(
                cur, "uni_halle", item_id, source_url, title, license_, pub, folder
            )

            # Download ALL files in the dataset — skip restricted ones
            skipped_restricted = 0
            for b in bitstreams:
                name     = b.get("name", f"file_{b.get('uuid','')}")
                bs_id    = b.get("uuid") or str(b.get("id", ""))
                dl_url   = f"{UNI_HALLE_REST}/bitstreams/{bs_id}/retrieve"
                size     = b.get("sizeBytes")
                out_path = folder / name

                try:
                    if download_file(dl_url, out_path, session):
                        db_insert_file(cur, dataset_db_id, name, dl_url, size, out_path)
                except AccessRestrictedError:
                    print(f"    🔒 Access denied (skipping): {name}", file=sys.stderr)
                    skipped_restricted += 1

            con.commit()
            downloaded += 1
            skip_note = f" ({skipped_restricted} restricted files skipped)" if skipped_restricted else ""
            print(f"  ✅ [{downloaded}/{target}] uni-halle | {item_id} — {title}{skip_note}")

        offset += page_size

    print(f"  → [uni-halle] {downloaded} datasets saved.")
    return downloaded


# ══════════════════════════════════════════════════════════════════════════════
# SOURCE 2 — dans.knaw.nl (Dataverse API at dataverse.nl)
# Docs: https://guides.dataverse.org/en/latest/api/
# ══════════════════════════════════════════════════════════════════════════════

def dataverse_search(query: str, session: requests.Session, start=0, per_page=10):
    url = f"{DANS_API}/search"
    params = {"q": query, "type": "dataset", "start": start,
              "per_page": per_page, "show_entity_ids": True}
    r = session.get(url, params=params, timeout=60)
    r.raise_for_status()
    return r.json()


def dataverse_get_dataset(persistent_id: str, session: requests.Session):
    """Returns (files_list, license_info) for a dataset persistent ID (DOI)."""
    url = f"{DANS_API}/datasets/:persistentId/"
    r = session.get(url, params={"persistentId": persistent_id}, timeout=60)
    r.raise_for_status()
    data      = r.json().get("data", {})
    latest    = data.get("latestVersion", {})
    files     = latest.get("files", [])
    license_  = latest.get("license", {})
    return files, license_


def download_from_dans(con, session: requests.Session, target: int = 5) -> int:
    print("\n🔍 [dans.knaw.nl] Searching Dataverse datasets...")
    cur = con.cursor()
    downloaded = 0
    start = 0
    per_page = 10
    query = " OR ".join(QDA_KEYWORDS)

    while downloaded < target:
        try:
            result = dataverse_search(query, session, start=start, per_page=per_page)
        except requests.RequestException as e:
            print(f"❌ [dans] API error: {e}", file=sys.stderr)
            break

        items = result.get("data", {}).get("items", [])
        if not items:
            print("ℹ  [dans] No more results.")
            break

        for item in items:
            if downloaded >= target:
                break

            global_id  = item.get("global_id", "")   # e.g. "doi:10.xxx/yyy"
            title      = item.get("name", "untitled")
            pub        = item.get("published_at", "")
            source_url = item.get("url", "")

            if not global_id:
                continue

            try:
                files, lic_info = dataverse_get_dataset(global_id, session)
            except requests.RequestException as e:
                print(f"  ⚠  Skipping {global_id}: {e}", file=sys.stderr)
                continue

            # Only proceed if dataset has at least one QDA file
            qda_found = any(
                is_qda_file(f.get("dataFile", {}).get("filename", ""))
                for f in files
            )
            if not qda_found:
                continue

            # Parse license
            if isinstance(lic_info, dict):
                license_ = lic_info.get("name") or lic_info.get("uri", "")
            else:
                license_ = str(lic_info) if lic_info else ""

            safe_id = global_id.replace(":", "_").replace("/", "_")
            folder  = ARCHIVE_DIR / f"dans_{safe_id}_{safe_name(title)}"
            folder.mkdir(parents=True, exist_ok=True)

            dataset_db_id = db_upsert_dataset(
                cur, "dans_knaw", global_id, source_url, title, license_, pub, folder
            )

            # Download ALL files in the dataset — skip restricted ones
            skipped_restricted = 0
            for f in files:
                df           = f.get("dataFile", {})
                file_id      = df.get("id")
                name         = df.get("filename", f"file_{file_id}")
                size         = df.get("filesize")
                dl_url       = f"{DANS_API}/access/datafile/{file_id}"
                out_path     = folder / name

                # Dataverse marks restricted files in the metadata — skip early
                if f.get("restricted") or df.get("restricted"):
                    print(f"    🔒 Restricted (skipping): {name}")
                    skipped_restricted += 1
                    continue

                try:
                    if download_file(dl_url, out_path, session):
                        db_insert_file(cur, dataset_db_id, name, dl_url, size, out_path)
                except AccessRestrictedError as e:
                    print(f"    🔒 Access denied (skipping): {name}", file=sys.stderr)
                    skipped_restricted += 1

            con.commit()
            downloaded += 1
            skip_note = f" ({skipped_restricted} restricted files skipped)" if skipped_restricted else ""
            print(f"  ✅ [{downloaded}/{target}] dans | {global_id} — {title}{skip_note}")

        start += per_page

    print(f"  → [dans] {downloaded} datasets saved.")
    return downloaded


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    if not DB_PATH.exists():
        print(f"❌ DB not found at {DB_PATH}. Run init_db.py first.", file=sys.stderr)
        sys.exit(1)

    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(DB_PATH)

    with requests.Session() as session:
        session.headers.update({"User-Agent": "QDA-Research-Downloader/1.0"})

        n1 = download_from_uni_halle(con, session, target=5)
        n2 = download_from_dans(con, session, target=5)

    con.close()

    print(f"\n{'='*55}")
    print(f"✅ Pipeline complete.")
    print(f"   opendata.uni-halle.de : {n1} datasets")
    print(f"   dans.knaw.nl          : {n2} datasets")
    print(f"📁 Archive  : {ARCHIVE_DIR}")
    print(f"🗄  Database : {DB_PATH}")


if __name__ == "__main__":
    main()