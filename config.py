from pathlib import Path

# Paths 
BASE_DIR    = Path(__file__).parent
DATA_DIR    = BASE_DIR / "data"
FILES_DIR   = BASE_DIR / "files"       # downloaded files live here
DB_PATH     = DATA_DIR / "23293539-sq26.db"
CSV_DIR     = DATA_DIR / "csv"
LOG_FILE    = DATA_DIR / "pipeline.log"
PROGRESS_FILE = DATA_DIR / "progress.json"  # tracks resume state

# Repository registry 
REPOSITORIES = {
    "dans": {
        "id":            5,
        "name":          "DANS (Dutch National Data Archive)",
        "url":           "https://dans.knaw.nl",
        "api_base":      "https://dataverse.nl/api",
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
        "note": (
            "This repository uses a site-wide CAPTCHA that blocks "
            "programmatic REST API access. Metadata is harvested via "
            "OAI-PMH (/oai/request) which is not CAPTCHA-protected. "
            "File downloads use direct bitstream URLs after OAI harvest."
        ),
    },
}

# Search queries 
SEARCH_KEYWORDS = [
    # Primary — QDA file extensions / software names
    "qdpx",
    "nvpx",
    "atlproj",
    "MAXQDA",
    "ATLAS.ti",
    "NVivo",
    # Secondary — broader qualitative research terms
    "qualitative research data",
    "qualitative data analysis",
    "interview study",
    "interview transcript",
    "thematic analysis",
    "grounded theory",
    "QDA",
]

# QDA file extensions
# Projects must contain at least one of these to be included.
QDA_EXTENSIONS = {
    ".qdpx",     # REFI-QDA standard (cross-platform)
    ".nvpx",     # NVivo
    ".nvp",      # NVivo (older)
    ".atlproj",  # ATLAS.ti
    ".atl",      # ATLAS.ti (older)
    ".mx",       # MAXQDA
    ".mx24",     # MAXQDA 24
    ".mx20",     # MAXQDA 20
    ".mx18",     # MAXQDA 18
    ".mxd",      # MAXQDA data
    ".qda",      # QDA Miner
    ".f4a",      # f4analyse
    ".f4p",      # f4analyse project
    ".quirkos",  # Quirkos
}

# Qualitative data companion extensions
# Once a project is identified as QDA-relevant (contains a QDA
# file), ALL files in that project are downloaded — including
# these companion formats (transcripts, audio, video, images etc.)
QUALITATIVE_DATA_EXTENSIONS = {
    # Documents / transcripts
    ".pdf",
    ".txt",
    ".rtf",
    ".docx",
    ".doc",
    ".odt",
    # Spreadsheets / data tables
    ".xlsx",
    ".xls",
    ".csv",
    # Audio recordings
    ".mp3",
    ".wav",
    ".m4a",
    ".aac",
    ".ogg",
    ".flac",
    # Video recordings
    ".mp4",
    ".mov",
    ".avi",
    ".mkv",
    ".wmv",
    # Images / scans
    ".jpg",
    ".jpeg",
    ".png",
    ".tiff",
    ".tif",
    ".bmp",
    # Archives (may contain any of the above)
    ".zip",
    ".tar",
    ".gz",
    ".7z",
}

# All downloadable extensions = QDA + qualitative companion files
ALL_DOWNLOAD_EXTENSIONS = QDA_EXTENSIONS | QUALITATIVE_DATA_EXTENSIONS

# Download / pipeline settings
MAX_RETRIES        = 3
RETRY_DELAY        = 5      # seconds between retries
DOWNLOAD_DELAY     = 2      # seconds between file downloads (polite)
API_TIMEOUT        = 60     # seconds for API calls
DOWNLOAD_TIMEOUT   = 120    # seconds for file downloads
PAGE_SIZE          = 20     # results per API page

# Progress auto-save: save state to disk every N projects processed.
# On Ctrl+C the pipeline saves immediately and exits cleanly.
PROGRESS_SAVE_INTERVAL = 25

# OAI-PMH settings (uni-halle)
UNI_HALLE_OAI_ENDPOINT = "https://opendata.uni-halle.de/oai/request"
UNI_HALLE_OAI_SET      = None   # None = harvest all; e.g. "openaire_data"
OAI_METADATA_PREFIX    = "oai_dc"