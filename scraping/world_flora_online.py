"""
Scraper for World Flora Online website.
Saves raw HTML and text for all pages in the hierarchy as JSONL:
Orders → Families → Genera → Species → Subspecies
"""

import requests
from bs4 import BeautifulSoup
import time
import random
import json
import logging
from datetime import datetime
from pathlib import Path
from threading import Lock
from concurrent.futures import ThreadPoolExecutor, as_completed
import urllib3
import ssl
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Global configuration
BASE_API_URL = "https://www.worldfloraonline.org/taxonTree"
BASE_URL = "https://www.worldfloraonline.org"
OUTPUT_DIR = Path("data/raw")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR = Path("logs")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Setup logging
LOG_FILE = LOG_DIR / "world_flora_online.log"
ERROR_FILE = LOG_DIR / "world_flora_online_errors.log"

# Configure root logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Formatter for all handlers
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# Handler for main log file (all messages)
main_file_handler = logging.FileHandler(LOG_FILE, encoding='utf-8')
main_file_handler.setLevel(logging.INFO)
main_file_handler.setFormatter(formatter)

# Handler for console (all messages)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)

# Handler for error file (ERROR level only)
error_file_handler = logging.FileHandler(ERROR_FILE, encoding='utf-8')
error_file_handler.setLevel(logging.ERROR)  # Only ERROR and above
error_file_handler.setFormatter(formatter)

# Add all handlers to root logger
logger.addHandler(main_file_handler)
logger.addHandler(console_handler)
logger.addHandler(error_file_handler)

# Global file handles and locks
jsonl_file = None
file_write_lock = Lock()
completion_file_lock = Lock()  # Lock for thread-safe writes to completion file
completed_lock = Lock()  # Lock for thread-safe access to completed sets
COMPLETION_FILE = OUTPUT_DIR / "completed_items.jsonl"

# Thread-local storage for sessions (each thread gets its own session)
import threading
thread_local = threading.local()

def get_session():
    """Get or create a thread-local session with retry adapter."""
    if not hasattr(thread_local, 'session'):
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry

        thread_local.session = requests.Session()
        thread_local.session.verify = False
        thread_local.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    return thread_local.session


def get_jsonl_file():
    """Get or create the single JSONL file handle."""
    global jsonl_file
    if jsonl_file is None:
        jsonl_path = OUTPUT_DIR / "world_flora_online.jsonl"
        jsonl_file = open(jsonl_path, 'a', encoding='utf-8')
    return jsonl_file


def close_jsonl_file():
    """Close the JSONL file handle."""
    global jsonl_file
    if jsonl_file:
        jsonl_file.close()
        jsonl_file = None


def get_page_content(url, max_retries=5):
    """Fetch page content with retries."""
    session = get_session()
    for attempt in range(max_retries):
        try:
            response = session.get(url, timeout=30)
            if response.status_code == 200:
                return response.text
            elif response.status_code == 404:
                logging.warning(f"404 Not Found: {url}")
                return None
            else:
                logging.warning(f"Status {response.status_code} for {url}, attempt {attempt + 1}")
                if attempt < max_retries - 1:
                    time.sleep(10)
        except (ssl.SSLEOFError, requests.exceptions.SSLError) as e:
            # Check if it's an SSLEOFError specifically
            error_str = str(e)
            if 'SSLEOFError' in error_str or 'UNEXPECTED_EOF_WHILE_READING' in error_str:
                logging.error(f"SSLEOFError fetching {url}, attempt {attempt + 1}: {e}")
                if attempt < max_retries - 1:
                    logging.info(f"Sleeping for 3 minutes before retry...")
                    time.sleep(180)  # 3 minutes = 180 seconds
            else:
                logging.error(f"SSL error fetching {url}, attempt {attempt + 1}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(10)
        except Exception as e:
            logging.error(f"Error fetching {url}, attempt {attempt + 1}: {e}")
            if attempt < max_retries - 1:
                time.sleep(3)

    return None


def get_api_data(api_url, max_retries=5):
    """Fetch JSON data from API with retries."""
    session = get_session()
    for attempt in range(max_retries):
        try:
            response = session.get(api_url, timeout=30)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                logging.warning(f"404 Not Found: {api_url}")
                return None
            else:
                logging.warning(f"Status {response.status_code} for {api_url}, attempt {attempt + 1}")
                if attempt < max_retries - 1:
                    time.sleep(10)
        except (ssl.SSLEOFError, requests.exceptions.SSLError) as e:
            # Check if it's an SSLEOFError specifically
            error_str = str(e)
            if 'SSLEOFError' in error_str or 'UNEXPECTED_EOF_WHILE_READING' in error_str:
                logging.error(f"SSLEOFError fetching {api_url}, attempt {attempt + 1}: {e}")
                if attempt < max_retries - 1:
                    logging.info(f"Sleeping for 3 minutes before retry...")
                    time.sleep(180)  # 3 minutes = 180 seconds
            else:
                logging.error(f"SSL error fetching {api_url}, attempt {attempt + 1}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(10)
        except Exception as e:
            logging.error(f"Error fetching {api_url}, attempt {attempt + 1}: {e}")
            if attempt < max_retries - 1:
                time.sleep(3)

    return None


def extract_text_from_html(html_content):
    """Extract clean text from HTML content."""
    soup = BeautifulSoup(html_content, 'html.parser')
    for script in soup(["script", "style"]):
        script.decompose()

    text_content = soup.get_text()
    lines = (line.strip() for line in text_content.splitlines())
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    text_content = '\n'.join(chunk for chunk in chunks if chunk)
    return text_content


def save_page(url, page_type, identifier, html_content, order_name=None, family_name=None,
              genus_name=None, species_name=None, subspecies=None):
    """Save raw HTML and text for a page as JSONL (thread-safe)."""
    try:
        text_content = extract_text_from_html(html_content)

        page_data = {
            "order_name": order_name,
            "family_name": family_name,
            "genus_name": genus_name,
            "species_name": species_name,
            "subspecies": subspecies,
            "source": "World Flora Online",
            "identifier": identifier,
            "page_type": page_type,
            "url": url,
            "raw_html": html_content,
            "raw_text": text_content,
            "timestamp": datetime.now().isoformat()
        }

        jsonl_file = get_jsonl_file()
        # Thread-safe write
        with file_write_lock:
            jsonl_file.write(json.dumps(page_data, ensure_ascii=False) + '\n')
            jsonl_file.flush()

        return True

    except Exception as e:
        logging.error(f"Error saving {url}: {e}")
        return False


def load_completed_items():
    """Load fully completed items from completion file."""
    completed = {
        'order': set(),
        'family': set(),
        'genus': set(),
        'species': set()
    }

    if not COMPLETION_FILE.exists():
        return completed

    try:
        logging.info("Loading completed items from completion file...")
        with open(COMPLETION_FILE, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                if not line.strip():
                    continue
                try:
                    data = json.loads(line)
                    page_type = data.get('page_type')
                    identifier = data.get('identifier')
                    if page_type and identifier:
                        completed[page_type].add(identifier)
                except json.JSONDecodeError:
                    logging.warning(f"Skipping invalid JSON on line {line_num} in completion file")

        total = sum(len(s) for s in completed.values())
        logging.info(f"Loaded {total} completed items (orders: {len(completed['order'])}, "
                    f"families: {len(completed['family'])}, genera: {len(completed['genus'])}, "
                    f"species: {len(completed['species'])})")
    except Exception as e:
        logging.error(f"Error loading completed items: {e}")

    return completed


def mark_item_completed(page_type, identifier):
    """Thread-safe function to mark an item as fully completed."""
    with completion_file_lock:
        try:
            with open(COMPLETION_FILE, 'a', encoding='utf-8') as f:
                completion_data = {
                    'page_type': page_type,
                    'identifier': identifier,
                    'timestamp': datetime.now().isoformat()
                }
                f.write(json.dumps(completion_data, ensure_ascii=False) + '\n')
                f.flush()
        except Exception as e:
            logging.error(f"Error writing to completion file: {e}")


def get_taxon_children(taxon_id):
    """Get list of child taxa for a given taxon ID."""
    api_url = f"{BASE_API_URL}/{taxon_id}"
    data = get_api_data(api_url)
    if not data:
        return []

    children = []
    for item in data:
        if 'data' in item and 'attr' in item.get('data', {}):
            title = item['data'].get('title', '')
            href = item['data']['attr'].get('href', '')
            child_id = item.get('attr', {}).get('id', '')
            if title and href and child_id:
                full_url = f"{BASE_URL}/{href}" if not href.startswith('http') else href
                children.append({
                    'name': title,
                    'url': full_url,
                    'id': child_id
                })

    return children


def process_family(family, order_name, order_id, completed, max_workers=5):
    """Process a single family and all its genera/species."""
    family_name = family['name']
    family_url = family['url']
    family_id = family['id']

    # Thread-safe check if already completed
    with completed_lock:
        if family_id in completed['family']:
            logging.info(f"    Skipping family {family_name} (already completed)")
            return

    logging.info(f"    Processing Family {family_name}'s description ({family_url})")

    # Process family description page
    family_content = get_page_content(family_url)
    family_saved = False
    if family_content:
        family_saved = save_page(family_url, "family", family_id, family_content,
                                order_name=order_name, family_name=family_name)
        if not family_saved:
            logging.error(f"    Failed to save family {family_name} - will not mark as completed")
    else:
        logging.error(f"    Failed to fetch family {family_name} - will not mark as completed")
    time.sleep(random.uniform(1, 2))

    # Get list of genera for this family
    genera = get_taxon_children(family_id)

    # Process genera in parallel (in reverse order)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for genus in reversed(genera):
            future = executor.submit(process_genus, genus, order_name, family_name, completed, max_workers)
            futures.append(future)

        # Wait for all genera to complete
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logging.error(f"Error processing genus: {e}")

    # Only mark as completed if page was saved AND all genera are completed
    with completed_lock:
        all_genera_completed = all(genus['id'] in completed['genus'] for genus in genera)
        if family_saved and all_genera_completed:
            mark_item_completed('family', family_id)
            completed['family'].add(family_id)
            logging.info(f"    Completed family {family_name} (all genera processed)")
        elif not family_saved:
            logging.warning(f"    Family {family_name} not marked as completed (page not saved)")
        elif not all_genera_completed:
            logging.warning(f"    Family {family_name} not marked as completed (some genera incomplete)")


def process_genus(genus, order_name, family_name, completed, max_workers=5):
    """Process a single genus and all its species."""
    genus_name = genus['name']
    genus_url = genus['url']
    genus_id = genus['id']

    # Thread-safe check if already completed
    with completed_lock:
        if genus_id in completed['genus']:
            logging.info(f"        Skipping genus {genus_name} (already completed)")
            return

    logging.info(f"        Processing Genus {genus_name}'s description ({genus_url})")

    # Process genus description page
    genus_content = get_page_content(genus_url)
    genus_saved = False
    if genus_content:
        genus_saved = save_page(genus_url, "genus", genus_id, genus_content,
                               order_name=order_name, family_name=family_name, genus_name=genus_name)
        if not genus_saved:
            logging.error(f"        Failed to save genus {genus_name} - will not mark as completed")
    else:
        logging.error(f"        Failed to fetch genus {genus_name} - will not mark as completed")
    time.sleep(random.uniform(1, 3))

    # Get list of species for this genus
    species_list = get_taxon_children(genus_id)

    # Process species in parallel (in reverse order)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for species in reversed(species_list):
            future = executor.submit(process_species, species, order_name, family_name, genus_name, completed)
            futures.append(future)

        # Wait for all species to complete
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logging.error(f"Error processing species: {e}")

    # Only mark as completed if page was saved AND all species are completed
    with completed_lock:
        all_species_completed = all(species['id'] in completed['species'] for species in species_list)
        if genus_saved and all_species_completed:
            mark_item_completed('genus', genus_id)
            completed['genus'].add(genus_id)
            logging.info(f"        Completed genus {genus_name} (all species processed)")
        elif not genus_saved:
            logging.warning(f"        Genus {genus_name} not marked as completed (page not saved)")
        elif not all_species_completed:
            logging.warning(f"        Genus {genus_name} not marked as completed (some species incomplete)")


def process_species(species, order_name, family_name, genus_name, completed):
    """Process a single species."""
    species_name = species['name']
    species_url = species['url']
    species_id = species['id']

    # Thread-safe check if already completed
    with completed_lock:
        if species_id in completed['species']:
            logging.info(f"            Skipping species {species_name} (already completed)")
            return

    logging.info(f"            Processing Species {species_name}'s description ({species_url})")

    # Process species description page
    species_content = get_page_content(species_url)
    species_saved = False
    if species_content:
        species_saved = save_page(species_url, "species", species_id, species_content,
                                  order_name=order_name, family_name=family_name,
                                  genus_name=genus_name, species_name=species_name)
    else:
        logging.error(f"            Failed to fetch species {species_name} - will not mark as completed")

    # Species are leaf nodes, only mark as completed if page was successfully saved
    if species_saved:
        mark_item_completed('species', species_id)
        with completed_lock:
            completed['species'].add(species_id)
    else:
        logging.error(f"            Failed to save species {species_name} - will not mark as completed")

    time.sleep(random.uniform(1, 3))


def main():
    """Main scraping function."""
    logging.info("Starting World Flora Online scraper...")

    # Load completed items from completion file
    completed = load_completed_items()
    if any(completed.values()):
        logging.info(f"\nResuming: Found {sum(len(s) for s in completed.values())} already completed items")
    else:
        logging.info("\nNo previous progress found. Starting from beginning.")

    # Step 1: Get list of orders
    logging.info("\n=== Step 1: Fetching orders ===")
    orders_data = get_api_data(BASE_API_URL)
    if not orders_data:
        logging.error("Failed to fetch orders")
        return

    orders = []
    for item in orders_data:
        if 'data' in item and 'attr' in item.get('data', {}):
            title = item['data'].get('title', '')
            href = item['data']['attr'].get('href', '')
            order_id = item.get('attr', {}).get('id', '')
            if title and href and order_id:
                full_url = f"{BASE_URL}/{href}" if not href.startswith('http') else href
                orders.append({
                    'name': title,
                    'url': full_url,
                    'id': order_id
                })

    logging.info(f"Found {len(orders)} orders")

    # Configuration for multithreading
    MAX_WORKERS = 3  # Number of parallel threads

    # Step 2: Process each order sequentially (to maintain hierarchy) - in reverse order
    for order_idx, order in enumerate(reversed(orders), 1):
        order_name = order['name']
        order_url = order['url']
        order_id = order['id']

        logging.info(f"\n=== Processing {len(orders) - order_idx + 1}/{len(orders)}: Order {order_name} ({order_url}) ===")

        # Skip if already completed
        if order_id in completed['order']:
            logging.info(f"  Skipping order {order_name} (already completed)")
            continue

        # Step 2: Process order description page
        order_content = get_page_content(order_url)
        order_saved = False
        if order_content:
            order_saved = save_page(order_url, "order", order_id, order_content, order_name=order_name)
            if not order_saved:
                logging.error(f"  Failed to save order {order_name} - will not mark as completed")
        else:
            logging.error(f"  Failed to fetch order {order_name} - will not mark as completed")
        time.sleep(random.uniform(1, 3))

        # Step 3: Get list of families for this order
        families = get_taxon_children(order_id)
        logging.info(f"  Found {len(families)} families for order {order_name}")

        # Step 4: Process families in parallel (in reverse order)
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = []
            for family in reversed(families):
                future = executor.submit(process_family, family, order_name, order_id, completed, MAX_WORKERS)
                futures.append(future)

            # Wait for all families to complete
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logging.error(f"Error processing family: {e}")

        # Only mark as completed if page was saved AND all families are completed
        all_families_completed = all(family['id'] in completed['family'] for family in families)
        if order_saved and all_families_completed:
            mark_item_completed('order', order_id)
            with completed_lock:
                completed['order'].add(order_id)
            logging.info(f"\nCompleted Order {order_name} (all families processed)")
        elif not order_saved:
            logging.warning(f"\nOrder {order_name} not marked as completed (page not saved)")
        elif not all_families_completed:
            logging.warning(f"\nOrder {order_name} partially processed (some families incomplete)")

        time.sleep(random.uniform(2, 5))

    # Close JSONL file
    close_jsonl_file()
    logging.info("\n=== Scraping completed ===")


if __name__ == "__main__":
    main()
