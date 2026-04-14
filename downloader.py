import logging
import re
import time
from pathlib import Path

import requests
from tqdm import tqdm

from config import MAX_RETRIES, RETRY_DELAY, DOWNLOAD_DELAY, DOWNLOAD_TIMEOUT
from database import SUCCESS, FAILED, RESTRICTED, ALREADY_EXISTS

logger = logging.getLogger(__name__)


class AccessRestrictedError(Exception):
    """Raised on HTTP 401/403 — no point retrying."""
    pass


def create_session(user_agent: str = "QDA-Acquirer/2.0") -> requests.Session:
    """Create a requests session with sensible defaults."""
    session = requests.Session()
    session.headers.update({"User-Agent": user_agent})
    return session


def polite_delay(seconds: float = DOWNLOAD_DELAY) -> None:
    """Sleep between requests to be a polite crawler."""
    time.sleep(seconds)


def safe_filename(name: str, max_len: int = 100) -> str:
    """Sanitise a string for use as a folder/file name."""
    name = re.sub(r'[\\/:*?"<>|]', "_", name or "untitled")
    name = "_".join(name.split())
    return name[:max_len].strip("_") or "untitled"


def download_file(url: str, out_path: Path,
                  session: requests.Session) -> tuple[bool, str]:
    """
    Download a single file to out_path with retry and atomic write.

    Returns:
        (success: bool, status: str)

    Raises:
        AccessRestrictedError on HTTP 401/403
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Skip if already fully downloaded
    if out_path.exists() and out_path.stat().st_size > 0:
        logger.debug("Already exists, skipping: %s", out_path.name)
        return True, ALREADY_EXISTS

    tmp_path = out_path.with_suffix(out_path.suffix + ".tmp")
    last_error = ""

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with session.get(url, stream=True,
                             timeout=DOWNLOAD_TIMEOUT) as r:
                if r.status_code in (401, 403):
                    raise AccessRestrictedError(
                        f"HTTP {r.status_code} — access restricted: {url}"
                    )
                r.raise_for_status()

                total = int(r.headers.get("content-length", 0))
                with open(tmp_path, "wb") as f, tqdm(
                    total=total, unit="B", unit_scale=True,
                    desc=f"    {out_path.name[:45]}",
                    leave=False,
                ) as bar:
                    for chunk in r.iter_content(chunk_size=256 * 1024):
                        if chunk:
                            f.write(chunk)
                            bar.update(len(chunk))

            tmp_path.rename(out_path)
            return True, SUCCESS

        except AccessRestrictedError:
            if tmp_path.exists():
                tmp_path.unlink()
            raise   # bubble up — do not retry

        except requests.RequestException as e:
            last_error = str(e)
            logger.warning("Attempt %d/%d failed for %s: %s",
                           attempt, MAX_RETRIES, url, e)
            if tmp_path.exists():
                tmp_path.unlink()
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)

    logger.error("All %d retries exhausted for: %s", MAX_RETRIES, url)
    return False, f"{FAILED}: {last_error}"