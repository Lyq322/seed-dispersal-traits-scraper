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
        jsonl_path = OUTPUT_DIR / "flora_of_china.jsonl"
        jsonl_file = open(jsonl_path, 'a', encoding='utf-8')
    return jsonl_file


def close_jsonl_file():
    """Close the JSONL file handle."""
    global jsonl_file
    if jsonl_file:
        jsonl_file.close()
        jsonl_file = None


def extract_taxon_name(html_content):
    """Extract taxon name from HTML content."""
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Try to extract from title tag first
    title_tag = soup.find('title')
    if title_tag:
        title_text = title_tag.get_text()
        # Pattern: "FamilyName in Flora of China @ efloras.org"
        match = re.match(r'^([^i]+?)\s+in\s+Flora', title_text)
        if match:
            return match.group(1).strip()
    
    # Try to extract from content - look for pattern like "24. <b>Aspleniaceae</b>"
    lbl_taxon_desc = soup.find('span', id='lblTaxonDesc')
    if lbl_taxon_desc:
        # Look for bold tags that might contain the taxon name
        bold_tags = lbl_taxon_desc.find_all('b')
        if bold_tags:
            # Usually the first bold tag contains the taxon name
            name = bold_tags[0].get_text().strip()
            # Remove leading numbers and periods if present (e.g., "24. Aspleniaceae")
            name = re.sub(r'^\d+\.\s*', '', name)
            if name:
                return name
    
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

        print(f"Saved {page_type}: {identifier}")
        return True

    except Exception as e:
        print(f"Error saving {url}: {e}")
        return False


def extract_links(html_content, url_pattern, base_url=None):
    """Extract links matching a pattern from HTML content."""
    soup = BeautifulSoup(html_content, 'html.parser')
    links = []

    if isinstance(url_pattern, str):
        pattern = re.compile(url_pattern)
    else:
        pattern = url_pattern

    for link in soup.find_all('a', href=pattern):
        href = link.get('href')
        if href:
            if base_url:
                full_url = urljoin(base_url, href)
            else:
                full_url = href
            links.append(full_url)

    return links


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

    # Step 2: Extract volume links
    print("\n=== Step 2: Extracting volume links ===")
    volume_pattern = re.compile(r'volume_page\.aspx\?volume_id=\d+&flora_id=2')
    volume_urls = extract_links(base_content, volume_pattern, "http://www.efloras.org/")

    print(f"Found {len(volume_urls)} volumes")

    # Step 3: Process each volume
    for vol_idx, volume_url in enumerate(volume_urls, 1):
        print(f"\n=== Processing Volume {vol_idx}/{len(volume_urls)}: {volume_url} ===")

        volume_id = extract_id_from_url(volume_url, 'volume_id')
        if not volume_id:
            print(f"Could not extract volume_id from {volume_url}")
            continue

        volume_content = get_page_content(volume_url)
        if not volume_content:
            print(f"Failed to fetch volume page: {volume_url}")
            continue

        # Extract family links (florataxon.aspx format)
        print(f"  Extracting family links from volume {volume_id}...")
        family_pattern = re.compile(r'florataxon\.aspx\?flora_id=2&taxon_id=\d+')
        family_desc_urls = extract_links(volume_content, family_pattern, "http://www.efloras.org/")

        # Extract genus list links (browse.aspx format)
        print(f"  Extracting genus list links from volume {volume_id}...")
        genus_list_pattern = re.compile(r'browse\.aspx\?flora_id=2&start_taxon_id=\d+')
        genus_list_urls = extract_links(volume_content, genus_list_pattern, "http://www.efloras.org/")

        family_desc_urls = list(set(family_desc_urls))
        genus_list_urls = list(set(genus_list_urls))

        print(f"  Found {len(family_desc_urls)} family description links and {len(genus_list_urls)} genus list links")

        # Step 4: Process each family description page
        for fam_idx, family_desc_url in enumerate(family_desc_urls, 1):
            print(f"    Processing Family {fam_idx}/{len(family_desc_urls)}: {family_desc_url}")

            family_id = extract_id_from_url(family_desc_url, 'taxon_id')
            if not family_id:
                continue

            family_content = get_page_content(family_desc_url)
            if family_content:
                family_name = extract_taxon_name(family_content)
                save_page(family_desc_url, "family", f"family_{family_id}", family_content, 
                         family_name=family_name)

            time.sleep(random.uniform(1, 3))

        # Step 5: Process each genus list page
        for gen_list_idx, genus_list_url in enumerate(genus_list_urls, 1):
            print(f"    Processing Genus List {gen_list_idx}/{len(genus_list_urls)}: {genus_list_url}")

            genus_list_id = extract_id_from_url(genus_list_url, 'start_taxon_id')
            if not genus_list_id:
                continue

            genus_list_content = get_page_content(genus_list_url)
            if not genus_list_content:
                continue

            # Extract genus description links (florataxon.aspx format)
            print(f"      Extracting genus description links...")
            genus_desc_pattern = re.compile(r'florataxon\.aspx\?flora_id=2&taxon_id=\d+')
            genus_desc_urls = extract_links(genus_list_content, genus_desc_pattern, "http://www.efloras.org/")
            genus_desc_urls = list(set(genus_desc_urls))

            # Extract species list links (browse.aspx format)
            print(f"      Extracting species list links...")
            species_list_pattern = re.compile(r'browse\.aspx\?flora_id=2&start_taxon_id=\d+')
            species_list_urls = extract_links(genus_list_content, species_list_pattern, "http://www.efloras.org/")
            species_list_urls = list(set(species_list_urls))

            print(f"      Found {len(genus_desc_urls)} genus descriptions and {len(species_list_urls)} species lists")

            # Step 6: Process each genus description page
            # We need to find which family this genus belongs to
            # Try to extract from the genus list page or we'll need to track it
            # For now, we'll extract it from the genus description page's breadcrumb or content
            for gen_desc_idx, genus_desc_url in enumerate(genus_desc_urls, 1):
                print(f"        Processing Genus Description {gen_desc_idx}/{len(genus_desc_urls)}: {genus_desc_url}")

                genus_id = extract_id_from_url(genus_desc_url, 'taxon_id')
                if not genus_id:
                    continue

                genus_desc_content = get_page_content(genus_desc_url)
                if genus_desc_content:
                    genus_name = extract_taxon_name(genus_desc_content)
                    # Try to extract family name from the page (might be in breadcrumb or content)
                    family_name = None
                    soup = BeautifulSoup(genus_desc_content, 'html.parser')
                    # Look for family link in breadcrumb or in the page
                    family_link = soup.find('a', href=re.compile(r'florataxon\.aspx\?flora_id=2&taxon_id=\d+'))
                    if family_link:
                        # The link text might be the family name, but we need to check the hierarchy
                        # For now, we'll try to get it from the taxon chain
                        taxon_chain = soup.find('span', id='lblTaxonChain')
                        if taxon_chain:
                            # Look for family links in the chain
                            family_links = taxon_chain.find_all('a', href=re.compile(r'florataxon\.aspx\?flora_id=2&taxon_id=\d+'))
                            if family_links:
                                # Get the family name from the link text or fetch the family page
                                family_href = family_links[-1].get('href')
                                if family_href:
                                    family_url = urljoin("http://www.efloras.org/", family_href)
                                    # We could fetch it, but for now, try to extract from link text
                                    family_name = family_links[-1].get_text().strip()
                    
                    save_page(genus_desc_url, "genus", f"genus_{genus_id}", genus_desc_content,
                             family_name=family_name, genus_name=genus_name)

                time.sleep(random.uniform(1, 3))

            # Step 7: Process each species list page
            # We need to track the genus and family for species
            # Get the genus name from the previous loop
            current_genus_name = None
            current_family_name = None
            
            for spec_list_idx, species_list_url in enumerate(species_list_urls, 1):
                print(f"        Processing Species List {spec_list_idx}/{len(species_list_urls)}: {species_list_url}")

                species_list_id = extract_id_from_url(species_list_url, 'start_taxon_id')
                if not species_list_id:
                    continue

                species_list_content = get_page_content(species_list_url)
                if not species_list_content:
                    continue

                # Extract species description links (florataxon.aspx format)
                print(f"          Extracting species description links...")
                species_desc_pattern = re.compile(r'florataxon\.aspx\?flora_id=2&taxon_id=\d+')
                species_desc_urls = extract_links(species_list_content, species_desc_pattern, "http://www.efloras.org/")
                species_desc_urls = list(set(species_desc_urls))

                print(f"          Found {len(species_desc_urls)} species descriptions")

                # Step 8: Process each species description page
                for spec_desc_idx, species_desc_url in enumerate(species_desc_urls, 1):
                    print(f"            Processing Species Description {spec_desc_idx}/{len(species_desc_urls)}: {species_desc_url}")

                    species_id = extract_id_from_url(species_desc_url, 'taxon_id')
                    if not species_id:
                        continue

                    species_desc_content = get_page_content(species_desc_url)
                    if species_desc_content:
                        species_name = extract_taxon_name(species_desc_content)
                        # Try to extract family and genus from the page
                        soup = BeautifulSoup(species_desc_content, 'html.parser')
                        family_name = current_family_name
                        genus_name = current_genus_name
                        
                        # Look in taxon chain for family and genus
                        taxon_chain = soup.find('span', id='lblTaxonChain')
                        if taxon_chain:
                            # Find all taxon links in the chain
                            taxon_links = taxon_chain.find_all('a', href=re.compile(r'florataxon\.aspx\?flora_id=2&taxon_id=\d+'))
                            if len(taxon_links) >= 2:
                                # Usually: Family | Genus | Species
                                family_name = taxon_links[-2].get_text().strip() if len(taxon_links) >= 2 else None
                            if len(taxon_links) >= 1:
                                genus_name = taxon_links[-1].get_text().strip() if len(taxon_links) >= 1 else None
                        
                        # If not found in chain, try to extract from content
                        if not family_name or not genus_name:
                            # Look for genus name in the species name (usually "Genus species")
                            if species_name and ' ' in species_name:
                                parts = species_name.split()
                                if len(parts) >= 2:
                                    genus_name = parts[0]
                        
                        save_page(species_desc_url, "species", f"species_{species_id}", species_desc_content,
                                 family_name=family_name, genus_name=genus_name, species_name=species_name)

                    time.sleep(random.uniform(1, 3))

                time.sleep(random.uniform(1, 2))

            time.sleep(random.uniform(1, 2))

        print(f"\nCompleted Volume {vol_idx}")
        time.sleep(random.uniform(2, 5))

    # Close JSONL file
    close_jsonl_file()
    print("\n=== Scraping completed ===")


if __name__ == "__main__":
    main()
