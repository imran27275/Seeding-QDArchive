import logging
from abc import ABC, abstractmethod
from pathlib import Path

import requests

from config import QDA_EXTENSIONS, QUALITATIVE_DATA_EXTENSIONS, ALL_DOWNLOAD_EXTENSIONS

logger = logging.getLogger(__name__)


class BaseScraper(ABC):
    """
    All scrapers inherit from this class and implement:
      - scrape_all(keywords)   → list of project metadata dicts
      - get_files(project)     → list of file metadata dicts
    """

    # Set these in each subclass
    SOURCE_NAME:   str = ""   # e.g. "DANS", "uni_halle"
    REPO_ID:       int = 0
    REPO_URL:      str = ""
    REPO_FOLDER:   str = ""   # subfolder under files/
    ACCESS_METHOD: str = "API-CALL"  # or "SCRAPING"

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "QDA-Acquirer/2.0 (FAU research project)"
        })
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def scrape_all(self, keywords: list[str]) -> list[dict]:
        """
        Search/harvest the repository for QDA-related projects.
        Returns a list of project dicts ready for insert_project().
        """
        ...

    @abstractmethod
    def get_files(self, project: dict) -> list[dict]:
        """
        Given a project metadata dict, return a list of file dicts
        ready for insert_file(). Must return ALL files in the project
        (QDA + companion), not just QDA files.
        """
        ...

    # Shared file-type helpers

    @staticmethod
    def is_qda_file(filename: str) -> bool:
        """True if this is a QDA project file (.qdpx, .atlproj, .mx, etc.)"""
        return Path(filename.lower()).suffix in QDA_EXTENSIONS

    @staticmethod
    def is_companion_file(filename: str) -> bool:
        """True if this is a qualitative data companion file
        (PDF, transcript, audio, video, image, spreadsheet, etc.)"""
        return Path(filename.lower()).suffix in QUALITATIVE_DATA_EXTENSIONS

    @staticmethod
    def is_downloadable_file(filename: str) -> bool:
        """True if file should be downloaded (QDA or companion type)."""
        return Path(filename.lower()).suffix in ALL_DOWNLOAD_EXTENSIONS

    @staticmethod
    def file_extension(filename: str) -> str:
        """Return the extension without leading dot, e.g. 'qdpx'."""
        suffix = Path(filename.lower()).suffix
        return suffix.lstrip(".") if suffix else ""