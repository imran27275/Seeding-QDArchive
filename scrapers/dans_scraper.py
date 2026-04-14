"""
scrapers/dans_scraper.py
─────────────────────────────────────────────────────────────────
Scraper for DANS (Dutch National Data Archive) — Repository #5
Access method: Dataverse REST API
API base:      https://dataverse.nl/api

Strategy:
  1. Search Dataverse with QDA keywords
  2. Keep only projects that contain at least one QDA file
  3. Download ALL files in those projects (QDA + companion files:
     PDFs, transcripts, audio, video, images, spreadsheets, etc.)

Restricted files are flagged in the metadata and skipped.
"""

import logging
import time
from pathlib import Path

from config import (
    REPOSITORIES, SEARCH_KEYWORDS, FILES_DIR,
    PAGE_SIZE, API_TIMEOUT, MAX_RETRIES, RETRY_DELAY,
    QDA_EXTENSIONS, ALL_DOWNLOAD_EXTENSIONS,
)
from downloader import safe_filename
from scrapers.base_scraper import BaseScraper

logger = logging.getLogger(__name__)

_REPO = REPOSITORIES["dans"]


class DANSScraper(BaseScraper):

    SOURCE_NAME   = "DANS"
    REPO_ID       = _REPO["id"]
    REPO_URL      = _REPO["url"]
    REPO_FOLDER   = _REPO["folder"]
    ACCESS_METHOD = _REPO["access_method"]
    API_BASE      = _REPO["api_base"]

    # ── Low-level API helper ───────────────────────────────────

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

    # ── Main harvest ───────────────────────────────────────────

    def scrape_all(self, keywords: list[str] = None) -> list[dict]:
        """
        Search Dataverse with all QDA keywords.
        Only keep projects that contain at least one QDA file.
        All files (QDA + companion) in those projects are returned.
        """
        if keywords is None:
            keywords = SEARCH_KEYWORDS

        self.logger.info("[DANS] Starting Dataverse search...")
        seen_dois = set()
        projects  = []

        for keyword in keywords:
            self.logger.info("[DANS] Query: \"%s\"", keyword)
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
                    if not global_id or global_id in seen_dois:
                        continue

                    ds_data = self._api_get(
                        f"{self.API_BASE}/datasets/:persistentId/",
                        params={"persistentId": global_id},
                    )
                    if not ds_data:
                        continue

                    ds_body = ds_data.get("data", {})
                    latest  = ds_body.get("latestVersion", {})
                    files   = latest.get("files", [])

                    # Gate: only include if at least one QDA file present
                    has_qda = any(
                        self.is_qda_file(f.get("dataFile", {})
                                          .get("filename", ""))
                        for f in files
                    )
                    if not has_qda:
                        continue

                    seen_dois.add(global_id)
                    project = self._build_project(
                        item, latest, keyword, files
                    )
                    projects.append(project)

                if start + per_page >= total_count:
                    break
                start += per_page

        self.logger.info("[DANS] Found %d QDA projects total.", len(projects))
        return projects

    # ── Project builder ────────────────────────────────────────

    def _build_project(self, item: dict, latest: dict,
                       query: str, files: list) -> dict:
        global_id = item.get("global_id", "")
        title     = item.get("name", "untitled")

        fields = {
            f["typeName"]: f
            for f in latest.get("metadataBlocks", {})
                           .get("citation", {})
                           .get("fields", [])
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

        # Count file categories for logging
        qda_count       = sum(1 for f in files
                              if self.is_qda_file(
                                  f.get("dataFile", {}).get("filename", "")))
        companion_count = sum(1 for f in files
                              if self.is_companion_file(
                                  f.get("dataFile", {}).get("filename", "")))
        other_count     = len(files) - qda_count - companion_count

        self.logger.info(
            "[DANS] Project: %s | files: %d QDA, %d companion, %d other",
            title[:50], qda_count, companion_count, other_count
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
            # Internal use
            "_fields":  fields,
            "_files":   files,
            "_latest":  latest,
        }

    # ── Files ──────────────────────────────────────────────────

    def get_files(self, project: dict) -> list[dict]:
        """
        Return file metadata for ALL files in the project.

        File categories:
          - QDA files      (.qdpx, .atlproj, .mx, etc.)   ← always download
          - Companion files (.pdf, .docx, .mp3, .mp4, etc.) ← always download
          - Other files    (anything else)                  ← still download
            (professor said download everything in a QDA project)
          - Restricted     (any category)                  ← skip, log only
        """
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
                "status_note": "Access restricted per Dataverse metadata"
                               if restricted else None,
            })
        return result

    # ── Enrichment ─────────────────────────────────────────────

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