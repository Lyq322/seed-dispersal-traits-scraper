"""
Scraper for Flora of China website.
Saves raw HTML and text for all pages in the hierarchy as JSONL:
Base → Volumes → Families → Genus Lists → Genus Descriptions → Species Lists → Species Descriptions
"""

import requests
from bs4 import BeautifulSoup
import re
import time
import random
import json
from datetime import datetime
from urllib.parse import urljoin, urlparse, parse_qs
from pathlib import Path

# Global configuration
BASE_URL = "http://www.efloras.org/flora_page.aspx?flora_id=2"
OUTPUT_DIR = Path("data/flora_of_china_raw")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Global session and file handles
session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
})
jsonl_file = None


def get_jsonl_file():
    """Get or create the single JSONL file handle."""
    global jsonl_file
    if jsonl_file is None:
        jsonl_path = OUTPUT_DIR/"flora_of_china.jsonl"
        jsonl_file = open(jsonl_path, 'a', encoding='utf-8')
    return jsonl_file


def close_jsonl_file():
    """Close the JSONL file handle."""
    global jsonl_file
    if jsonl_file:
        jsonl_file.close()
        jsonl_file = None


def extract_taxon_name(html_content):
    """Extract taxon name from page title.
    Title format: "TaxonName in Flora of China @ efloras.org" or "TaxonName"
    """
    soup = BeautifulSoup(html_content, 'html.parser')

    title_tag = soup.find('title')
    if title_tag:
        title_text = title_tag.get_text().strip()

        if "in Flora of China" in title_text:
            match = re.match(r'^(.+?)\s+in\s+Flora\s+of\s+China', title_text)
            if match:
                return match.group(1).strip()
        else:
            return title_text.strip()

    return None


def save_page(url, page_type, identifier, html_content=None, family_name=None, genus_name=None, species_name=None):
    """Save raw HTML and text for a page as JSONL."""
    global session
    try:
        if html_content is None:
            response = session.get(url, timeout=30)
            if response.status_code != 200:
                print(f"Error {response.status_code} for {url}")
                return False
            html_content = response.text

        soup = BeautifulSoup(html_content, 'html.parser')
        for script in soup(["script", "style"]):
            script.decompose()

        text_content = soup.get_text()
        lines = (line.strip() for line in text_content.splitlines())
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        text_content = '\n'.join(chunk for chunk in chunks if chunk)

        # Extract taxon name if not provided
        taxon_name = extract_taxon_name(html_content)

        # Set names based on page type if not provided
        if page_type == "family" and not family_name:
            family_name = taxon_name
        elif page_type == "genus" and not genus_name:
            genus_name = taxon_name
        elif page_type == "species" and not species_name:
            species_name = taxon_name

        page_data = {
            "url": url,
            "page_type": page_type,
            "identifier": identifier,
            "family_name": family_name,
            "genus_name": genus_name,
            "species_name": species_name,
            "raw_html": html_content,
            "raw_text": text_content,
            "timestamp": datetime.now().isoformat()
        }

        jsonl_file = get_jsonl_file()
        jsonl_file.write(json.dumps(page_data, ensure_ascii=False) + '\n')
        jsonl_file.flush()

        # print(f"Saved {page_type}: {identifier}")
        return True

    except Exception as e:
        print(f"Error saving {url}: {e}")
        return False


def extract_links(html_content, url_pattern, base_url=None, return_text=False, container_id=None):
    """Extract links matching a pattern from HTML content.
    If return_text is True, returns list of (url, text) tuples.
    Otherwise returns list of URLs.
    If container_id is provided, only searches within that element.
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    links = []

    # If container_id is specified, search only within that element
    if container_id:
        container = soup.find(id=container_id)
        if not container:
            return links
        search_area = container
    else:
        search_area = soup

    if isinstance(url_pattern, str):
        pattern = re.compile(url_pattern)
    else:
        pattern = url_pattern

    for link in search_area.find_all('a', href=pattern):
        href = link.get('href')
        if href:
            if base_url:
                full_url = urljoin(base_url, href)
            else:
                full_url = href

            if return_text:
                link_text = link.get_text().strip()
                links.append((full_url, link_text))
            else:
                links.append(full_url)

    return links


def get_all_pages_from_browse(browse_url, max_pages=10):
    """Get all pages from a browse.aspx URL, handling pagination.
    Returns list of HTML content from all pages.
    """
    pages = []
    page_num = 1

    while page_num <= max_pages:
        # Construct URL for current page
        if page_num == 1:
            current_url = browse_url
        else:
            # Add or update page parameter
            if '&page=' in browse_url:
                current_url = re.sub(r'&page=\d+', f'&page={page_num}', browse_url)
            else:
                separator = '&' if '?' in browse_url else '?'
                current_url = f"{browse_url}{separator}page={page_num}"

        content = get_page_content(current_url)
        if not content:
            break

        pages.append(content)

        # Check if there's a next page by looking for page links in the content
        soup = BeautifulSoup(content, 'html.parser')
        # Look for links with page parameter greater than current page
        next_page_pattern = re.compile(rf'browse\.aspx\?.*&page={page_num + 1}')
        next_page_links = soup.find_all('a', href=next_page_pattern)

        # If no next page link found, we've reached the last page
        if not next_page_links:
            break

        page_num += 1

    return pages


def extract_links_from_all_pages(browse_url, url_pattern, base_url=None, return_text=False, container_id=None):
    """Extract links matching a pattern from all pages of a browse.aspx URL.
    If return_text is True, returns list of (url, text) tuples.
    Otherwise returns list of URLs.
    If container_id is provided, only searches within that element.
    """
    all_links = []
    pages = get_all_pages_from_browse(browse_url)

    for page_content in pages:
        links = extract_links(page_content, url_pattern, base_url, return_text=return_text, container_id=container_id)
        all_links.extend(links)

    # Remove duplicates - handle both URL strings and (url, text) tuples
    if return_text:
        seen_urls = set()
        unique_links = []
        for url, text in all_links:
            if url not in seen_urls:
                seen_urls.add(url)
                unique_links.append((url, text))
        return unique_links
    else:
        return list(set(all_links))


def get_page_content(url, max_retries=5):
    """Fetch page content with retries."""
    global session
    for attempt in range(max_retries):
        try:
            response = session.get(url, timeout=30)
            if response.status_code == 200:
                return response.text
            elif response.status_code == 404:
                print(f"404 Not Found: {url}")
                return None
            else:
                print(f"Status {response.status_code} for {url}, attempt {attempt + 1}")
                if attempt < max_retries - 1:
                    time.sleep(10)
        except Exception as e:
            print(f"Error fetching {url}, attempt {attempt + 1}: {e}")
            if attempt < max_retries - 1:
                time.sleep(10)

    return None


def extract_id_from_url(url, param_name):
    """Extract ID parameter from URL."""
    parsed = urlparse(url)
    params = parse_qs(parsed.query)
    if param_name in params:
        return params[param_name][0]
    return None


def main():
    """Main scraping function."""
    print("Starting Flora of China scraper...")

    # Step 1: Get base page
    print("\n=== Step 1: Fetching base page ===")
    base_content = get_page_content(BASE_URL)
    if not base_content:
        print("Failed to fetch base page")
        return

    # Step 2: Extract volume (list of families) links
    print("\n=== Step 2: Extracting volume links ===")
    volume_pattern = re.compile(r'volume_page\.aspx\?volume_id=\d+&flora_id=2')

    # Skip first introductory volume
    volume_links = extract_links(base_content, volume_pattern, "http://www.efloras.org/", return_text=True)[1:]

    print(f"Found {len(volume_links)} volumes")

    # Step 3: Process each volume
    for volume_url, volume_text in volume_links:
        print(f"\n=== Processing {volume_text} ({volume_url}) ===")

        volume_id = extract_id_from_url(volume_url, 'volume_id')
        if not volume_id:
            print(f"Could not extract volume_id from {volume_url}")
            continue

        volume_content = get_page_content(volume_url)
        if not volume_content:
            print(f"Failed to fetch volume page: {volume_url}")
            continue

        family_desc_pattern = re.compile(r'florataxon\.aspx\?flora_id=2&taxon_id=\d+')
        family_desc_links = extract_links(volume_content, family_desc_pattern, "http://www.efloras.org/", return_text=True)
        genus_list_pattern = re.compile(r'browse\.aspx\?flora_id=2&start_taxon_id=\d+')
        genus_list_urls = extract_links(volume_content, genus_list_pattern, "http://www.efloras.org/")
        print(f"      Found {len(family_desc_links)} family descriptions and {len(genus_list_urls)} genus lists")

        # Step 4: Process each family description page
        for fam_idx, (family_desc_url, family_text) in enumerate(family_desc_links, 1):
            family_name = family_text
            print(f"    Processing {fam_idx}/{len(family_desc_links)}: Family {family_name}'s description ({family_desc_url})")

            family_id = extract_id_from_url(family_desc_url, 'taxon_id')
            family_content = get_page_content(family_desc_url)
            if family_content:
                save_page(family_desc_url, "family", f"family_{family_id}", family_content,
                         family_name=family_name)

            time.sleep(random.uniform(1, 3))

        # Step 5: Process each family's genus list page
        for gen_list_idx, genus_list_url in enumerate(genus_list_urls, 1):
            family_name = extract_taxon_name(get_page_content(genus_list_url))
            print(f"    Processing {gen_list_idx}/{len(genus_list_urls)}: Family {family_name}'s genus list ({genus_list_url})")

            # Extract genus description links from all pages (florataxon.aspx format)
            genus_desc_pattern = re.compile(r'florataxon\.aspx\?flora_id=2&taxon_id=\d+')
            genus_desc_links = extract_links_from_all_pages(genus_list_url, genus_desc_pattern, "http://www.efloras.org/", return_text=True, container_id="ucFloraTaxonList_panelTaxonList")

            # Extract each genus's species list links from all pages (browse.aspx format)
            species_list_pattern = re.compile(r'browse\.aspx\?flora_id=2&start_taxon_id=\d+')
            genus_list_content = get_page_content(genus_list_url)
            species_list_urls = extract_links(genus_list_content, species_list_pattern, "http://www.efloras.org/", container_id="ucFloraTaxonList_panelTaxonList")

            print(f"      Found {len(genus_desc_links)} genus descriptions and {len(species_list_urls)} species lists")

            # Step 6: Process each genus description page
            for gen_desc_idx, (genus_desc_url, genus_text) in enumerate(genus_desc_links, 1):
                genus_name = genus_text
                print(f"        Processing {gen_desc_idx}/{len(genus_desc_links)}: Genus {genus_name}'s description ({genus_desc_url})")

                genus_id = extract_id_from_url(genus_desc_url, 'taxon_id')
                if not genus_id:
                    continue

                genus_desc_content = get_page_content(genus_desc_url)
                if genus_desc_content:
                    genus_name = extract_taxon_name(genus_desc_content)
                    save_page(genus_desc_url, "genus", f"genus_{genus_id}", genus_desc_content,
                             family_name=family_name, genus_name=genus_name)

                time.sleep(random.uniform(1, 3))

            # Step 7: Process each species list page
            for spec_list_idx, species_list_url in enumerate(species_list_urls, 1):
                genus_name = extract_taxon_name(get_page_content(species_list_url))
                print(f"        Processing {spec_list_idx}/{len(species_list_urls)}: Genus {genus_name}'s species list ({species_list_url})")

                # Extract species description links from all pages (florataxon.aspx format)
                species_desc_pattern = re.compile(r'florataxon\.aspx\?flora_id=2&taxon_id=\d+')
                species_desc_links = extract_links_from_all_pages(species_list_url, species_desc_pattern, "http://www.efloras.org/", return_text=True, container_id="ucFloraTaxonList_panelTaxonList")
                print(f"          Found {len(species_desc_links)} species descriptions")

                # Step 8: Process each species description page
                for spec_desc_idx, (species_desc_url, species_text) in enumerate(species_desc_links, 1):
                    species_name = species_text
                    print(f"            Processing {spec_desc_idx}/{len(species_desc_links)}: Species {species_name}'s description ({species_desc_url})")

                    species_id = extract_id_from_url(species_desc_url, 'taxon_id')
                    if not species_id:
                        continue

                    species_desc_content = get_page_content(species_desc_url)
                    if species_desc_content:
                        save_page(species_desc_url, "species", f"species_{species_id}", species_desc_content,
                                 family_name=family_name, genus_name=genus_name, species_name=species_name)

                    time.sleep(random.uniform(1, 3))

                time.sleep(random.uniform(1, 2))

            time.sleep(random.uniform(1, 2))

        print(f"\nCompleted {volume_text}")
        time.sleep(random.uniform(2, 5))

    # Close JSONL file
    close_jsonl_file()
    print("\n=== Scraping completed ===")


if __name__ == "__main__":
    main()
