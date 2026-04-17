from pathlib import Path

# Paths 
BASE_DIR      = Path(__file__).parent
DATA_DIR      = BASE_DIR / "data"
FILES_DIR     = BASE_DIR / "files"
DB_PATH       = DATA_DIR / "23293539-sq26.db"
CSV_DIR       = DATA_DIR / "csv"
LOG_FILE      = DATA_DIR / "pipeline.log"
PROGRESS_FILE = DATA_DIR / "progress.json"

# ── DANS Repository registry (4 data stations)
# All share the same repository_id = 5 (DANS) and folder = DANS.
# De-duplication by DOI prevents downloading the same dataset twice.
REPOSITORIES = {
    "dans_ssh": {
        "id":            5,
        "name":          "DANS SSH (Social Sciences & Humanities)",
        "url":           "https://ssh.datastations.nl",
        "api_base":      "https://ssh.datastations.nl/api",
        "folder":        "DANS",
        "access_method": "API-CALL",
        "enabled":       True,
    },
    "dans_archaeology": {
        "id":            5,
        "name":          "DANS Archaeology",
        "url":           "https://archaeology.datastations.nl",
        "api_base":      "https://archaeology.datastations.nl/api",
        "folder":        "DANS",
        "access_method": "API-CALL",
        "enabled":       True,
    },
    "dans_lifesciences": {
        "id":            5,
        "name":          "DANS Life & Medical Sciences",
        "url":           "https://lifesciences.datastations.nl",
        "api_base":      "https://lifesciences.datastations.nl/api",
        "folder":        "DANS",
        "access_method": "API-CALL",
        "enabled":       True,
    },
    "dans_phys": {
        "id":            5,
        "name":          "DANS Physical & Technical Sciences",
        "url":           "https://phys-techsciences.datastations.nl",
        "api_base":      "https://phys-techsciences.datastations.nl/api",
        "folder":        "DANS",
        "access_method": "API-CALL",
        "enabled":       True,
    },
    "uni_halle": {
        "id":            16,
        "name":          "opendata.uni-halle.de (Share_it)",
        "url":           "https://opendata.uni-halle.de",
        "api_base":      None,
        "folder":        "opendata",
        "access_method": "SCRAPING",
        "enabled":       True,
    },
}

# Download size cap 
# Target total download size across all DANS stations.
# Phase 1: download ALL projects that have a QDA file (no cap).
# Phase 2: fill remaining budget with companion-only projects.
# Set to None to disable the cap (download everything).
DOWNLOAD_TARGET_GB   = 50
DOWNLOAD_TARGET_BYTES = DOWNLOAD_TARGET_GB * (1024 ** 3)

# Search queries 
SEARCH_KEYWORDS = [
    # QDA software names
    "qdpx", "nvpx", "atlproj", "MAXQDA", "ATLAS.ti", "NVivo",
    # Broad qualitative research terms
    "qualitative research", "qualitative data",
    "interview transcript", "interview study",
    "focus group", "ethnographic", "field notes",
    "oral history", "thematic analysis",
    "grounded theory", "qualitative data analysis", "QDA",
]

# QDA file extensions 
QDA_EXTENSIONS = {
    ".qdpx",   ".nvpx",   ".nvp",
    ".atlproj",".atl",
    ".mx",     ".mx24",   ".mx20", ".mx18", ".mxd",
    ".qda",    ".f4a",    ".f4p",  ".quirkos",
}

# Qualitative companion extensions 
QUALITATIVE_DATA_EXTENSIONS = {
    ".pdf", ".txt", ".rtf", ".docx", ".doc", ".odt",
    ".xlsx", ".xls", ".csv",
    ".mp3", ".wav", ".m4a", ".aac", ".ogg", ".flac",
    ".mp4", ".mov", ".avi", ".mkv", ".wmv",
    ".jpg", ".jpeg", ".png", ".tiff", ".tif", ".bmp",
    ".zip", ".tar", ".gz", ".7z",
}

ALL_DOWNLOAD_EXTENSIONS = QDA_EXTENSIONS | QUALITATIVE_DATA_EXTENSIONS

# Download / pipeline settings
MAX_RETRIES            = 3
RETRY_DELAY            = 5
DOWNLOAD_DELAY         = 2
API_TIMEOUT            = 60
DOWNLOAD_TIMEOUT       = 120
PAGE_SIZE              = 20
PROGRESS_SAVE_INTERVAL = 25

# OAI-PMH settings (uni-halle) 
UNI_HALLE_OAI_ENDPOINT = "https://opendata.uni-halle.de/oai/request"
UNI_HALLE_OAI_SET      = None
OAI_METADATA_PREFIX    = "oai_dc"