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
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Global configuration
BASE_API_URL = "https://www.worldfloraonline.org/taxonTree"
BASE_URL = "https://www.worldfloraonline.org"
OUTPUT_DIR = Path("data/world_flora_online_raw")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Setup logging
LOG_FILE = OUTPUT_DIR / "world_flora_online.log"
ERROR_FILE = OUTPUT_DIR / "world_flora_online_errors.log"

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

# Global session and file handles
session = requests.Session()
session.verify = False
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
})
jsonl_file = None


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
    global session
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
        except Exception as e:
            logging.error(f"Error fetching {url}, attempt {attempt + 1}: {e}")
            if attempt < max_retries - 1:
                time.sleep(10)

    return None


def get_api_data(api_url, max_retries=5):
    """Fetch JSON data from API with retries."""
    global session
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
        except Exception as e:
            logging.error(f"Error fetching {api_url}, attempt {attempt + 1}: {e}")
            if attempt < max_retries - 1:
                time.sleep(10)

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
    """Save raw HTML and text for a page as JSONL."""
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
        jsonl_file.write(json.dumps(page_data, ensure_ascii=False) + '\n')
        jsonl_file.flush()

        return True

    except Exception as e:
        logging.error(f"Error saving {url}: {e}")
        return False


def get_last_processed_item():
    """Read the last line from JSONL file to determine where to resume."""
    jsonl_path = OUTPUT_DIR / "world_flora_online.jsonl"
    if not jsonl_path.exists():
        return None

    try:
        with open(jsonl_path, 'r', encoding='utf-8') as f:
            # Read last line
            lines = f.readlines()
            if not lines:
                return None

            last_line = lines[-1].strip()
            if not last_line:
                return None

            data = json.loads(last_line)
            return {
                'order_name': data.get('order_name'),
                'family_name': data.get('family_name'),
                'genus_name': data.get('genus_name'),
                'species_name': data.get('species_name'),
                'page_type': data.get('page_type'),
                'identifier': data.get('identifier')
            }
    except Exception as e:
        logging.error(f"Error reading last processed item: {e}")
        return None


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


def main():
    """Main scraping function."""
    logging.info("Starting World Flora Online scraper...")

    # Check for resume point
    last_item = get_last_processed_item()
    if last_item:
        logging.info(f"\nResuming from last processed item:")
        logging.info(f"  Type: {last_item.get('page_type')}")
        logging.info(f"  Order: {last_item.get('order_name')}")
        logging.info(f"  Family: {last_item.get('family_name')}")
        logging.info(f"  Genus: {last_item.get('genus_name')}")
        logging.info(f"  Species: {last_item.get('species_name')}")
        resume_order = last_item.get('order_name')
        resume_family = last_item.get('family_name')
        resume_genus = last_item.get('genus_name')
        resume_species = last_item.get('species_name')
    else:
        logging.info("\nNo previous progress found. Starting from beginning.")
        resume_order = None
        resume_family = None
        resume_genus = None
        resume_species = None

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

    # Step 2: Process each order
    skip_orders = resume_order is not None
    for order_idx, order in enumerate(orders, 1):
        order_name = order['name']
        order_url = order['url']
        order_id = order['id']

        logging.info(f"\n=== Processing {order_idx}/{len(orders)}: Order {order_name} ({order_url}) ===")

        # Skip orders before resume point
        if skip_orders:
            if order_name != resume_order:
                logging.info(f"  Skipping order {order_name} (already processed)")
                continue
            else:
                logging.info(f"  Resuming from order {order_name}")
                skip_orders = False

        # Step 2: Process order description page
        order_content = get_page_content(order_url)
        if order_content:
            save_page(order_url, "order", order_id, order_content, order_name=order_name)
        time.sleep(random.uniform(1, 3))

        # Step 3: Get list of families for this order
        logging.info(f"  Getting families for order {order_name}...")
        families = get_taxon_children(order_id)
        logging.info(f"  Found {len(families)} families")

        # Step 4: Process each family
        skip_families = resume_family is not None and order_name == resume_order
        for fam_idx, family in enumerate(families, 1):
            family_name = family['name']
            family_url = family['url']
            family_id = family['id']

            # Skip families before resume point
            if skip_families:
                if family_name != resume_family:
                    logging.info(f"    Skipping family {family_name} (already processed)")
                    continue
                else:
                    logging.info(f"    Resuming from family {family_name}")
                    skip_families = False

            logging.info(f"    Processing {fam_idx}/{len(families)}: Family {family_name}'s description ({family_url})")

            # Step 4: Process family description page
            family_content = get_page_content(family_url)
            if family_content:
                save_page(family_url, "family", family_id, family_content,
                         order_name=order_name, family_name=family_name)
            time.sleep(random.uniform(1, 3))

            # Step 5: Get list of genera for this family
            logging.info(f"      Getting genera for family {family_name}...")
            genera = get_taxon_children(family_id)
            logging.info(f"      Found {len(genera)} genera")

            # Step 6: Process each genus
            skip_genera = resume_genus is not None and order_name == resume_order and family_name == resume_family
            for gen_idx, genus in enumerate(genera, 1):
                genus_name = genus['name']
                genus_url = genus['url']
                genus_id = genus['id']

                # Skip genera before resume point
                if skip_genera:
                    if genus_name != resume_genus:
                        logging.info(f"        Skipping genus {genus_name} (already processed)")
                        continue
                    else:
                        logging.info(f"        Resuming from genus {genus_name}")
                        skip_genera = False

                logging.info(f"        Processing {gen_idx}/{len(genera)}: Genus {genus_name}'s description ({genus_url})")

                # Step 6: Process genus description page
                genus_content = get_page_content(genus_url)
                if genus_content:
                    save_page(genus_url, "genus", genus_id, genus_content,
                             order_name=order_name, family_name=family_name, genus_name=genus_name)
                time.sleep(random.uniform(1, 3))

                # Step 7: Get list of species for this genus
                logging.info(f"          Getting species for genus {genus_name}...")
                species_list = get_taxon_children(genus_id)
                logging.info(f"          Found {len(species_list)} species")

                # Step 8: Process each species
                skip_species = (resume_species is not None and order_name == resume_order and
                               family_name == resume_family and genus_name == resume_genus)
                for spec_idx, species in enumerate(species_list, 1):
                    species_name = species['name']
                    species_url = species['url']
                    species_id = species['id']

                    # Skip species before resume point
                    if skip_species:
                        if species_name != resume_species:
                            logging.info(f"            Skipping species {species_name} (already processed)")
                            continue
                        else:
                            logging.info(f"            Resuming from species {species_name}")
                            skip_species = False

                    logging.info(f"            Processing {spec_idx}/{len(species_list)}: Species {species_name}'s description ({species_url})")

                    # Step 8: Process species description page
                    species_content = get_page_content(species_url)
                    if species_content:
                        save_page(species_url, "species", species_id, species_content,
                                 order_name=order_name, family_name=family_name,
                                 genus_name=genus_name, species_name=species_name)
                    time.sleep(random.uniform(1, 3))

                    # Step 9: Get list of subspecies for this species
                    # print(f"              Getting subspecies for species {species_name}...")
                    # subspecies_list = get_taxon_children(species_id)
                    # print(f"              Found {len(subspecies_list)} subspecies")

                    # Step 10: Process each subspecies
                    # for subsp_idx, subspecies in enumerate(subspecies_list, 1):
                    #     subspecies_name = subspecies['name']
                    #     subspecies_url = subspecies['url']
                    #     subspecies_id = subspecies['id']
                    #
                    #     print(f"                Processing {subsp_idx}/{len(subspecies_list)}: Subspecies {subspecies_name}'s description ({subspecies_url})")
                    #
                    #     # Step 10: Process subspecies description page
                    #     subspecies_content = get_page_content(subspecies_url)
                    #     if subspecies_content:
                    #         save_page(subspecies_url, "subspecies", subspecies_id, subspecies_content,
                    #                  order_name=order_name, family_name=family_name,
                    #                  genus_name=genus_name, species_name=species_name,
                    #                  subspecies=subspecies_name)
                    #     time.sleep(random.uniform(1, 3))

                    time.sleep(random.uniform(1, 2))

                time.sleep(random.uniform(1, 2))

            time.sleep(random.uniform(1, 2))

        logging.info(f"\nCompleted Order {order_name}")
        time.sleep(random.uniform(2, 5))

    # Close JSONL file
    close_jsonl_file()
    logging.info("\n=== Scraping completed ===")


if __name__ == "__main__":
    main()
