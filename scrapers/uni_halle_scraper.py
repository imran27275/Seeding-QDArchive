import logging
import random
import re
import time
import xml.etree.ElementTree as ET
from pathlib import Path

from config import (
    REPOSITORIES, FILES_DIR, QDA_EXTENSIONS, ALL_DOWNLOAD_EXTENSIONS,
    UNI_HALLE_OAI_ENDPOINT, UNI_HALLE_OAI_SET,
    OAI_METADATA_PREFIX, API_TIMEOUT, MAX_RETRIES, RETRY_DELAY,
)
from downloader import safe_filename
from scrapers.base_scraper import BaseScraper

logger = logging.getLogger(__name__)

_REPO = REPOSITORIES["uni_halle"]

# OAI-PMH XML namespaces
NS = {
    "oai":    "http://www.openarchives.org/OAI/2.0/",
    "dc":     "http://purl.org/dc/elements/1.1/",
    "oai_dc": "http://www.openarchives.org/OAI/2.0/oai_dc/",
}

# Keywords that hint a record is QDA-related
_QDA_HINTS = {
    "qdpx", "nvpx", "atlproj", "maxqda", "atlas.ti",
    "nvivo", "qualitative data", "qda", "qualitative research",
    "interview", "thematic analysis",
}

# Realistic browser User-Agent — critical for bypassing CAPTCHA
_BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# Polite jitter range in seconds between HTML page requests
_PAGE_JITTER = (1.5, 4.0)

# Regex to extract bitstream hrefs from DSpace item HTML
# Matches: href="/bitstream/handle/123456/file.qdpx"
_BITSTREAM_RE = re.compile(r'href="(/bitstream/[^"?]+)"')

# Patterns that indicate a thumbnail/preview — skip these
_SKIP_PATTERNS = (
    "/thumbnail", "/preview", ".gif", "_thumb",
    "isAllowed=n", "format=jpg",
)


class UniHalleScraper(BaseScraper):

    SOURCE_NAME   = "uni_halle"
    REPO_ID       = _REPO["id"]
    REPO_URL      = _REPO["url"]
    REPO_FOLDER   = _REPO["folder"]
    ACCESS_METHOD = _REPO["access_method"]
    OAI_ENDPOINT  = UNI_HALLE_OAI_ENDPOINT

    def __init__(self):
        super().__init__()
        # Override User-Agent with a real browser string
        # This is the key to avoiding CAPTCHA on HTML page requests
        self.session.headers.update({
            "User-Agent": _BROWSER_UA,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
        })

    # OAI-PMH request helper
    # OAI uses Accept: application/xml, not the browser headers above

    def _oai_request(self, params: dict) -> ET.Element | None:
        """Send OAI-PMH request. Uses XML accept header, not browser UA."""
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                r = self.session.get(
                    self.OAI_ENDPOINT,
                    params=params,
                    headers={"Accept": "application/xml",
                             "User-Agent": "QDA-OAI-Harvester/2.0"},
                    timeout=API_TIMEOUT,
                )
                if r.status_code in (401, 403):
                    self.logger.error(
                        "OAI-PMH access denied (HTTP %d).", r.status_code
                    )
                    return None
                r.raise_for_status()
                return ET.fromstring(r.content)
            except ET.ParseError as e:
                self.logger.error("XML parse error: %s", e)
                return None
            except Exception as e:
                self.logger.warning("OAI attempt %d/%d failed: %s",
                                    attempt, MAX_RETRIES, e)
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)
        return None

    # Main harvest

    def scrape_all(self, keywords: list[str] = None) -> list[dict]:
        """
        Full OAI-PMH harvest → filter QDA records locally.
        keywords param kept for interface compatibility but not used
        (OAI-PMH is a full harvest — no keyword filtering at API level).
        """
        self.logger.info("[uni-halle] Starting OAI-PMH harvest from %s",
                         self.OAI_ENDPOINT)

        all_records      = []
        params           = {"verb": "ListRecords",
                            "metadataPrefix": OAI_METADATA_PREFIX}
        if UNI_HALLE_OAI_SET:
            params["set"] = UNI_HALLE_OAI_SET

        page             = 1
        resumption_token = None

        while True:
            if resumption_token:
                params = {"verb": "ListRecords",
                          "resumptionToken": resumption_token}

            self.logger.info("[uni-halle] OAI page %d...", page)
            root = self._oai_request(params)

            if root is None:
                self.logger.error("[uni-halle] OAI harvest failed.")
                break

            error_el = root.find(".//oai:error", NS)
            if error_el is not None:
                self.logger.error("[uni-halle] OAI error %s: %s",
                                  error_el.get("code"), error_el.text)
                break

            records = root.findall(".//oai:record", NS)
            self.logger.info("[uni-halle] Page %d: %d records",
                             page, len(records))

            for record in records:
                parsed = self._parse_record(record)
                if parsed:
                    all_records.append(parsed)

            token_el = root.find(".//oai:resumptionToken", NS)
            if token_el is not None and token_el.text:
                resumption_token = token_el.text.strip()
                page += 1
                time.sleep(1)   # polite delay between OAI pages
            else:
                break

        qda_projects = [p for p in all_records if p.get("_has_qda_hint")]
        self.logger.info(
            "[uni-halle] Harvest complete. Total records: %d | QDA-related: %d",
            len(all_records), len(qda_projects)
        )
        return qda_projects

    # Record parser

    def _parse_record(self, record: ET.Element) -> dict | None:
        header = record.find("oai:header", NS)
        if header is not None and header.get("status") == "deleted":
            return None

        id_el  = record.find("oai:header/oai:identifier", NS)
        oai_id = id_el.text.strip() if id_el is not None else ""

        dc = record.find(".//oai_dc:dc", NS)
        if dc is None:
            return None

        def dc_val(tag: str) -> str:
            el = dc.find(f"dc:{tag}", NS)
            return el.text.strip() if el is not None and el.text else ""

        def dc_all(tag: str) -> list[str]:
            return [
                el.text.strip()
                for el in dc.findall(f"dc:{tag}", NS)
                if el.text and el.text.strip()
            ]

        title            = dc_val("title") or "untitled"
        description      = dc_val("description")
        language         = dc_val("language")
        date             = dc_val("date")
        rights_list      = dc_all("rights")
        subject_list     = dc_all("subject")
        creator_list     = dc_all("creator")
        contributor_list = dc_all("contributor")

        doi    = ""
        handle = ""
        for ident in dc_all("identifier"):
            if "doi.org" in ident:
                doi = ident
            elif "opendata.uni-halle.de/handle" in ident:
                handle = ident
            elif not doi and ident.startswith("http"):
                doi = ident

        project_url = handle or doi or oai_id

        # QDA hint detection across all text fields
        all_text = " ".join([
            title, description,
            " ".join(subject_list),
            " ".join(dc_all("relation")),
        ]).lower()

        has_qda_hint = any(hint in all_text for hint in _QDA_HINTS)

        proj_folder = safe_filename(
            oai_id.split(":")[-1].replace("/", "_") + "_" + title
        )

        return {
            "query_string":               "OAI-PMH full harvest",
            "repository_id":              self.REPO_ID,
            "repository_url":             self.REPO_URL,
            "project_url":                project_url,
            "version":                    None,
            "title":                      title,
            "description":                description or None,
            "language":                   language or None,
            "doi":                        doi or None,
            "upload_date":                date or None,
            "download_repository_folder": self.REPO_FOLDER,
            "download_project_folder":    proj_folder,
            "download_version_folder":    None,
            "download_method":            self.ACCESS_METHOD,
            "_oai_id":         oai_id,
            "_handle":         handle,
            "_subjects":       subject_list,
            "_creators":       creator_list,
            "_contributors":   contributor_list,
            "_rights":         rights_list,
            "_has_qda_hint":   has_qda_hint,
        }

    # Files

    def get_files(self, project: dict) -> list[dict]:
        """
        Discover ALL files for a project by scraping the item HTML page.

        Key technique (from Anita Kamani's pipeline):
          Visit the DSpace item HTML page with a browser User-Agent,
          then extract bitstream paths using:
            r'href="(/bitstream/[^"?]+)"'
          This bypasses the CAPTCHA because it mimics a real browser
          visiting the page, not a bot hammering an API endpoint.

        A random jitter delay is added between requests to be polite
        and further reduce bot-detection risk.
        """
        handle      = project.get("_handle", "")
        project_url = project.get("project_url", "")
        proj_folder = project["download_project_folder"]

        local_dir = FILES_DIR / self.REPO_FOLDER / proj_folder
        local_dir.mkdir(parents=True, exist_ok=True)

        # Try handle URL first, then project_url as fallback
        page_url = handle or project_url
        bitstream_paths = []

        if page_url:
            bitstream_paths = self._scrape_bitstream_paths(page_url)

        result     = []
        seen_names = set()
        base_url   = self.REPO_URL.rstrip("/")

        for path in bitstream_paths:
            # Build full URL from relative path
            full_url = f"{base_url}{path}"
            name     = path.split("/")[-1].split("?")[0]

            if not name or name in seen_names:
                continue
            seen_names.add(name)

            result.append({
                "file_name":   name,
                "file_type":   self.file_extension(name),
                "file_url":    full_url,
                "size":        None,   # size not available from HTML scrape
                "local_path":  local_dir / name,
                "restricted":  False,
                "status_note": None,
            })

        if not result:
            result.append({
                "file_name":   "no_files_found.txt",
                "file_type":   "txt",
                "file_url":    None,
                "size":        None,
                "local_path":  None,
                "restricted":  False,
                "status_note": "No bitstream hrefs found on item page",
            })

        qda_count  = sum(1 for f in result if self.is_qda_file(f["file_name"]))
        comp_count = sum(1 for f in result if self.is_companion_file(f["file_name"]))
        self.logger.info(
            "[uni-halle] %s | %d files (%d QDA, %d companion)",
            project["title"][:50], len(result), qda_count, comp_count
        )
        return result

    def _scrape_bitstream_paths(self, page_url: str) -> list[str]:
        """
        Visit a DSpace item page with browser-like headers and extract
        all bitstream href paths using Anita's regex approach.

        Returns a list of relative paths like:
          ["/bitstream/handle/123456/study.qdpx",
           "/bitstream/handle/123456/interview.mp3"]
        """
        # Polite random delay — reduces bot-detection risk
        time.sleep(random.uniform(*_PAGE_JITTER))

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                r = self.session.get(page_url, timeout=API_TIMEOUT)

                if r.status_code == 403:
                    self.logger.warning(
                        "[uni-halle] 403 on %s — CAPTCHA may have triggered. "
                        "Try running again later with a longer delay.",
                        page_url
                    )
                    return []

                if r.status_code != 200:
                    self.logger.warning("[uni-halle] HTTP %d for %s",
                                        r.status_code, page_url)
                    return []

                # Anita's regex — extracts relative bitstream paths from href attributes
                paths = list(dict.fromkeys(_BITSTREAM_RE.findall(r.text)))

                # Filter out thumbnails, previews, and other non-data files
                paths = [
                    p for p in paths
                    if not any(skip in p.lower() for skip in _SKIP_PATTERNS)
                ]

                self.logger.debug("[uni-halle] Found %d bitstream paths on %s",
                                  len(paths), page_url)
                return paths

            except Exception as e:
                self.logger.warning("[uni-halle] Scrape attempt %d/%d failed: %s",
                                    attempt, MAX_RETRIES, e)
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)

        return []

    # Enrichment

    def get_keywords(self, project: dict) -> list[str]:
        return project.get("_subjects", [])

    def get_persons(self, project: dict) -> list[tuple[str, str]]:
        persons  = [(n, "AUTHOR") for n in project.get("_creators", [])]
        persons += [(n, "CONTRIBUTOR") for n in project.get("_contributors", [])]
        return persons

    def get_licenses(self, project: dict) -> list[str]:
        return project.get("_rights", [])