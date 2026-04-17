import logging
import time
from pathlib import Path

from config import (
    REPOSITORIES, SEARCH_KEYWORDS, FILES_DIR,
    PAGE_SIZE, API_TIMEOUT, MAX_RETRIES, RETRY_DELAY,
    QDA_EXTENSIONS,
)
from downloader import safe_filename
from scrapers.base_scraper import BaseScraper

logger = logging.getLogger(__name__)

# All 4 stations in priority order (SSH most relevant for QDA)
STATION_KEYS = [
    "dans_ssh",
    "dans_archaeology",
    "dans_lifesciences",
    "dans_phys",
]


class DANSScraper(BaseScraper):
    """
    Scraper for a single DANS Dataverse station.
    The pipeline instantiates one per station.
    """

    SOURCE_NAME   = "DANS"
    REPO_ID       = 5
    REPO_FOLDER   = "DANS"
    ACCESS_METHOD = "API-CALL"

    def __init__(self, repo_key: str = "dans_ssh",
                 seen_dois: set = None):
        """
        repo_key : which DANS station key from config.REPOSITORIES
        seen_dois: shared set across all stations for deduplication.
                   Pass the same set to all station scrapers to prevent
                   downloading the same DOI from multiple stations.
        """
        super().__init__()
        repo             = REPOSITORIES[repo_key]
        self.REPO_URL    = repo["url"]
        self.API_BASE    = repo["api_base"]
        self.SOURCE_NAME = repo["name"]
        self.seen_dois   = seen_dois if seen_dois is not None else set()

    # Low-level API helper

    def _api_get(self, url: str, params: dict = None) -> dict | None:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                r = self.session.get(url, params=params, timeout=API_TIMEOUT)
                if r.status_code in (401, 403, 404):
                    return None
                r.raise_for_status()
                return r.json()
            except Exception as e:
                self.logger.warning("API attempt %d/%d failed: %s",
                                    attempt, MAX_RETRIES, e)
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)
        return None

    # Main harvest

    def scrape_all(self, keywords: list[str] = None) -> list[dict]:
        """
        Search this station with all keywords.

        Returns two lists merged in order:
          1. QDA projects (have at least one QDA file) — always included
          2. Companion-only projects — included until size cap reached

        Deduplication: projects already seen (by DOI) across other
        stations are skipped automatically via shared seen_dois set.
        """
        if keywords is None:
            keywords = SEARCH_KEYWORDS

        self.logger.info("[%s] Starting search...", self.SOURCE_NAME)

        qda_projects      = []   # have at least one QDA file
        companion_projects= []   # qualitative but no QDA file
        local_seen        = set()

        for keyword in keywords:
            self.logger.info("[%s] Query: \"%s\"", self.SOURCE_NAME, keyword)
            start    = 0
            per_page = PAGE_SIZE

            while True:
                data = self._api_get(
                    f"{self.API_BASE}/search",
                    params={"q": keyword, "type": "dataset",
                            "start": start, "per_page": per_page,
                            "show_entity_ids": True},
                )
                if not data:
                    break

                body        = data.get("data", {})
                items       = body.get("items", [])
                total_count = body.get("total_count", 0)
                if not items:
                    break

                for item in items:
                    global_id = item.get("global_id", "")
                    if not global_id:
                        continue

                    # Deduplicate across all stations
                    if global_id in self.seen_dois or global_id in local_seen:
                        continue
                    local_seen.add(global_id)

                    ds_data = self._api_get(
                        f"{self.API_BASE}/datasets/:persistentId/",
                        params={"persistentId": global_id},
                    )
                    if not ds_data:
                        continue

                    latest = ds_data.get("data", {}).get("latestVersion", {})
                    files  = latest.get("files", [])

                    if not files:
                        continue

                    has_qda = any(
                        self.is_qda_file(
                            f.get("dataFile", {}).get("filename", ""))
                        for f in files
                    )

                    project = self._build_project(
                        item, latest, keyword, files, has_qda
                    )

                    if has_qda:
                        qda_projects.append(project)
                    else:
                        companion_projects.append(project)

                if start + per_page >= total_count:
                    break
                start += per_page

        # Register all found DOIs as seen
        for p in qda_projects + companion_projects:
            self.seen_dois.add(p["doi"])

        self.logger.info(
            "[%s] Found %d QDA projects + %d companion-only projects",
            self.SOURCE_NAME, len(qda_projects), len(companion_projects)
        )

        # Return QDA projects first, then companion projects.
        # The actual size cap is enforced during download in pipeline.py
        # by checking real disk usage — not by pre-estimating metadata sizes.
        result = qda_projects + companion_projects

        self.logger.info(
            "[%s] Returning %d projects (%d QDA-first, %d companion).",
            self.SOURCE_NAME, len(result),
            len(qda_projects), len(companion_projects)
        )
        return result

    # Project builder

    def _build_project(self, item, latest, query, files, has_qda) -> dict:
        global_id = item.get("global_id", "")
        title     = item.get("name", "untitled")

        fields = {
            f["typeName"]: f
            for f in latest.get("metadataBlocks", {})
                           .get("citation", {}).get("fields", [])
        }

        description = ""
        desc_vals   = fields.get("dsDescription", {}).get("value", [])
        if isinstance(desc_vals, list) and desc_vals:
            description = desc_vals[0].get(
                "dsDescriptionValue", {}).get("value", "")

        language  = ""
        lang_vals = fields.get("language", {}).get("value", [])
        if isinstance(lang_vals, list):
            language = ", ".join(lang_vals)
        elif isinstance(lang_vals, str):
            language = lang_vals

        upload_date   = (item.get("published_at", "")
                         or latest.get("releaseTime", ""))
        version_major = latest.get("versionNumber")
        version_minor = latest.get("versionMinorNumber")
        version_str   = (f"v{version_major}.{version_minor}"
                         if version_major is not None else None)

        safe_id     = global_id.replace(":", "_").replace("/", "_")
        proj_folder = safe_filename(f"{safe_id}_{title}")
        ver_folder  = safe_filename(version_str) if version_str else None

        qda_count  = sum(1 for f in files
                         if self.is_qda_file(
                             f.get("dataFile", {}).get("filename", "")))
        restricted = sum(1 for f in files
                         if f.get("restricted") or
                         f.get("dataFile", {}).get("restricted"))
        tag = "📌 QDA" if has_qda else "📄 companion"
        self.logger.info(
            "[%s] %s %-50s | %d files (%d QDA, %d restricted)",
            self.SOURCE_NAME, tag, title[:50],
            len(files), qda_count, restricted
        )

        return {
            "query_string":               query,
            "repository_id":              self.REPO_ID,
            "repository_url":             self.REPO_URL,
            "project_url":                item.get("url", ""),
            "version":                    version_str,
            "title":                      title,
            "description":                description or None,
            "language":                   language or None,
            "doi":                        global_id,
            "upload_date":                upload_date or None,
            "download_repository_folder": self.REPO_FOLDER,
            "download_project_folder":    proj_folder,
            "download_version_folder":    ver_folder,
            "download_method":            self.ACCESS_METHOD,
            "_fields": fields,
            "_files":  files,
            "_latest": latest,
        }

    # Files

    def get_files(self, project: dict) -> list[dict]:
        files      = project.get("_files", [])
        proj_folder= project["download_project_folder"]
        ver_folder = project.get("download_version_folder")

        local_dir = FILES_DIR / self.REPO_FOLDER / proj_folder
        if ver_folder:
            local_dir = local_dir / ver_folder
        local_dir.mkdir(parents=True, exist_ok=True)

        result = []
        for f in files:
            df         = f.get("dataFile", {})
            file_id    = df.get("id")
            name       = df.get("filename", f"file_{file_id}")
            size       = df.get("filesize")
            dl_url     = f"{self.API_BASE}/access/datafile/{file_id}"
            restricted = bool(f.get("restricted") or df.get("restricted"))

            result.append({
                "file_name":   name,
                "file_type":   self.file_extension(name),
                "file_url":    dl_url,
                "size":        size,
                "local_path":  local_dir / name,
                "restricted":  restricted,
                "status_note": "Restricted per Dataverse metadata"
                               if restricted else None,
            })
        return result

    # Enrichment

    def get_keywords(self, project: dict) -> list[str]:
        fields   = project.get("_fields", {})
        keywords = []
        for entry in (fields.get("keyword", {}).get("value") or []):
            kw = entry.get("keywordValue", {}).get("value", "")
            if kw:
                keywords.append(kw)
        for subj in (fields.get("subject", {}).get("value") or []):
            if isinstance(subj, str) and subj:
                keywords.append(subj)
        return keywords

    def get_persons(self, project: dict) -> list[tuple[str, str]]:
        fields  = project.get("_fields", {})
        persons = []
        for entry in (fields.get("author", {}).get("value") or []):
            name = entry.get("authorName", {}).get("value", "")
            if name:
                persons.append((name, "AUTHOR"))
        return persons

    def get_licenses(self, project: dict) -> list[str]:
        latest   = project.get("_latest", {})
        lic_info = latest.get("license", {})
        if isinstance(lic_info, dict):
            lic = lic_info.get("name") or lic_info.get("uri", "")
        else:
            lic = str(lic_info) if lic_info else ""
        return [lic] if lic else []