"""
Scrape World Flora Online species pages from the plant list JSON.
Reads data/raw/plant_list_2025-12.json, filters by role_s='accepted' and rank_s='species',
fetches https://www.worldfloraonline.org/taxon/{wfo_id_s} for each, and appends
the same JSONL format as world_flora_online.py to data/raw/world_flora_online_plant_list.jsonl.

Does not use data/raw/completed_items.jsonl. Progress is reported as the index in the
(filtered) plant list (1-based).
"""

import argparse
import json
import logging
import random
import ssl
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from threading import Lock


def format_eta_seconds(seconds):
    """Format seconds as human-readable ETA e.g. '2h 15m' or '45s'."""
    if seconds is None or seconds < 0 or not float("inf") > seconds:
        return "?"
    s = int(round(seconds))
    if s < 60:
        return f"{s}s"
    m, s = divmod(s, 60)
    if m < 60:
        return f"{m}m {s}s"
    h, m = divmod(m, 60)
    return f"{h}h {m}m"

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Paths (use plant_list_2025-12.json; if your file is plant_list-2025-12.json, change below)
PLANT_LIST_PATH = Path("data/raw/plant_list_2025-12.json")
OUTPUT_PATH = Path("data/raw/world_flora_online_plant_list.jsonl")
COMPLETE_JSONL_PATH = Path("data/raw/world_flora_online_complete.jsonl")
LOG_DIR = Path("logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "world_flora_online_plant_list.log"

OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger()

# Thread-local session
import threading
_thread_local = threading.local()
_file_handle = None
_file_lock = Lock()


def get_session():
    if not hasattr(_thread_local, "session"):
        _thread_local.session = requests.Session()
        _thread_local.session.verify = False
        _thread_local.session.headers.update(
            {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        )
    return _thread_local.session


def get_page_content(url, max_retries=5):
    """Fetch page HTML with retries (same behavior as world_flora_online.py)."""
    session = get_session()
    for attempt in range(max_retries):
        try:
            response = session.get(url, timeout=30)
            if response.status_code == 200:
                return response.text
            if response.status_code == 404:
                logger.warning("404 Not Found: %s", url)
                return None
            logger.warning("Status %s for %s, attempt %s", response.status_code, url, attempt + 1)
            if attempt < max_retries - 1:
                time.sleep(10)
        except (ssl.SSLEOFError, requests.exceptions.SSLError) as e:
            err = str(e)
            if "SSLEOFError" in err or "UNEXPECTED_EOF_WHILE_READING" in err:
                logger.error("SSLEOFError %s, attempt %s: %s", url, attempt + 1, e)
                if attempt < max_retries - 1:
                    time.sleep(180)
            else:
                logger.error("SSL error %s, attempt %s: %s", url, attempt + 1, e)
                if attempt < max_retries - 1:
                    time.sleep(10)
        except Exception as e:
            logger.error("Error %s, attempt %s: %s", url, attempt + 1, e)
            if attempt < max_retries - 1:
                time.sleep(3)
    return None


def names_from_item(item):
    """Extract order/family/genus/species from plant list item. Uses placed_in_*_s and full_name_string_no_authors_plain_s.
    Raises ValueError if full_name_string_no_authors_plain_s != genus_name + ' ' + placed_in_species_s.
    """
    order_name = item.get("placed_in_order_s")
    family_name = item.get("placed_in_family_s")
    genus_name = item.get("placed_in_genus_s")
    species_name = item.get("full_name_string_no_authors_plain_s")
    placed_in_species_s = item.get("placed_in_species_s")

    expected_full = None
    if genus_name is not None and placed_in_species_s is not None:
        expected_full = f"{genus_name} {placed_in_species_s}"
    if species_name is not None and expected_full is not None and species_name != expected_full:
        raise ValueError(
            f"full_name_string_no_authors_plain_s ({species_name!r}) != genus + ' ' + placed_in_species_s ({expected_full!r})"
        )
    return order_name, family_name, genus_name, species_name


def save_species_page(url, wfo_id, html_content, item):
    """Write one species page in the same JSONL format as world_flora_online.save_page."""
    global _file_handle
    order_name, family_name, genus_name, species_name = names_from_item(item)
    page_data = {
        "order_name": order_name,
        "family_name": family_name,
        "genus_name": genus_name,
        "species_name": species_name,
        "subspecies": None,
        "source": "World Flora Online",
        "identifier": wfo_id,
        "page_type": "species",
        "url": url,
        "raw_html": html_content,
        "timestamp": datetime.now().isoformat(),
    }
    with _file_lock:
        if _file_handle is None:
            _file_handle = open(OUTPUT_PATH, "a", encoding="utf-8")
        _file_handle.write(json.dumps(page_data, ensure_ascii=False) + "\n")
        _file_handle.flush()
    return True


def load_complete_identifiers():
    """Load set of identifiers already present in world_flora_online_complete.jsonl (skip these)."""
    done = set()
    if not COMPLETE_JSONL_PATH.exists():
        return done
    try:
        with open(COMPLETE_JSONL_PATH, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                    ident = rec.get("identifier")
                    if ident:
                        done.add(ident)
                except json.JSONDecodeError:
                    pass
        logger.info("Loaded %s identifiers from %s (will skip these)", len(done), COMPLETE_JSONL_PATH)
    except Exception as e:
        logger.warning("Could not load complete identifiers from %s: %s", COMPLETE_JSONL_PATH, e)
    return done


def scrape_one(plant_list_index, item, total):
    """Fetch one species page and append to JSONL. Returns (plant_list_index, wfo_id, success)."""
    wfo_id = item.get("wfo_id_s") or item.get("wfo_id")
    if not wfo_id:
        logger.warning("Skipping item with no wfo_id_s (index %s): %s", plant_list_index, item)
        return plant_list_index, None, False
    logger.info("Processing index %s / %s (%s)", plant_list_index, total, wfo_id)
    url = f"https://www.worldfloraonline.org/taxon/{wfo_id}"
    html = get_page_content(url)
    if not html:
        logger.error("Failed to fetch index %s: %s", plant_list_index, url)
        return plant_list_index, wfo_id, False
    try:
        save_species_page(url, wfo_id, html, item)
    except Exception as e:
        logger.error("Failed to save index %s %s: %s", plant_list_index, url, e)
        return plant_list_index, wfo_id, False
    time.sleep(random.uniform(1, 3))
    return plant_list_index, wfo_id, True


def main():
    parser = argparse.ArgumentParser(
        description="Scrape WFO species pages from plant list JSON (accepted species only)."
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=3,
        metavar="N",
        help="Parallel workers (default 3).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Stop after processing N species (for testing).",
    )
    parser.add_argument(
        "--start-index",
        type=int,
        default=1,
        metavar="N",
        help="Start at this index in the (filtered) plant list (1-based). Default 1.",
    )
    args = parser.parse_args()

    try:
        import ijson
    except ImportError:
        logger.error("Install ijson: pip install ijson")
        return

    if not PLANT_LIST_PATH.exists():
        logger.error("Plant list not found: %s", PLANT_LIST_PATH)
        return

    complete_ids = load_complete_identifiers()

    # Collect accepted species from JSON (single-threaded stream). Index = position in this list (1-based).
    species_items = []
    logger.info("Streaming plant list and filtering accepted species...")
    with open(PLANT_LIST_PATH, "rb") as f:
        for item in ijson.items(f, "item"):
            if item.get("role_s") != "accepted" or item.get("rank_s") != "species":
                continue
            wfo_id = item.get("wfo_id_s") or item.get("wfo_id")
            if not wfo_id:
                continue
            species_items.append(item)
            if args.limit and len(species_items) >= args.limit:
                break
    total_in_plant_list = len(species_items)
    to_process = [(i, item) for i, item in enumerate(species_items, start=1) if i >= args.start_index]
    # Skip any whose identifier is already in world_flora_online_complete.jsonl
    before_skip = len(to_process)
    to_process = [(i, item) for i, item in to_process if (item.get("wfo_id_s") or item.get("wfo_id")) not in complete_ids]
    skipped = before_skip - len(to_process)
    if skipped:
        logger.info("Skipping %s already in %s", skipped, COMPLETE_JSONL_PATH)
    if args.start_index > 1:
        logger.info("Starting at index %s; accepted species to process: %s (indices %s-%s)", args.start_index, len(to_process), args.start_index, total_in_plant_list)
    else:
        logger.info("Accepted species to process: %s", len(to_process))

    total = len(to_process)
    done = 0
    failed = 0
    start_time = time.perf_counter()
    progress_lock = Lock()

    def print_progress():
        nonlocal done, failed
        completed = done + failed
        if total == 0:
            pct = 100.0
            bar = "[" + "=" * 30 + "]"
        else:
            pct = 100.0 * completed / total
            bar_filled = min(30, int(30 * completed / total))
            if completed >= total:
                bar = "[" + "=" * 30 + "]"
            else:
                bar = "[" + "=" * bar_filled + ">" + " " * (29 - bar_filled) + "]"
        elapsed = time.perf_counter() - start_time
        rate = completed / elapsed if elapsed > 0 else 0
        remaining = (total - completed) / rate if rate > 0 else None
        eta_str = format_eta_seconds(remaining)
        line = f"{bar} {pct:5.1f}%  done: {done}  failed: {failed}  ETA: {eta_str}   "
        with progress_lock:
            sys.stderr.write("\r" + line)
            sys.stderr.flush()

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(scrape_one, idx, item, total): (idx, item)
            for idx, item in to_process
        }
        for future in as_completed(futures):
            idx, item = futures[future]
            try:
                plant_list_index, wfo_id, ok = future.result()
                if wfo_id is None:
                    continue
                if ok:
                    done += 1
                else:
                    failed += 1
                print_progress()
                if (done + failed) % 100 == 0:
                    logger.info("Progress: %s done, %s failed (index in plant list: %s / %s)", done, failed, plant_list_index, total)
            except Exception as e:
                logger.exception("Task failed (index %s): %s", idx, e)
                failed += 1
                print_progress()

    sys.stderr.write("\n")
    sys.stderr.flush()

    global _file_handle
    with _file_lock:
        if _file_handle is not None:
            _file_handle.close()
            _file_handle = None

    logger.info("Done. Written: %s, failed: %s", done, failed)


if __name__ == "__main__":
    main()
