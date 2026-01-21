"""
Script to extract descriptions from world_flora_online_complete.jsonl.
Processes raw_html to find descriptions and creates a new JSONL file.
"""

import json
import sys
import logging
from pathlib import Path
from bs4 import BeautifulSoup

# Setup logging
LOG_DIR = Path("logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "extract_descriptions.log"

# Configure logger
logger = logging.getLogger('extract_descriptions')
logger.setLevel(logging.INFO)

# Remove existing handlers to avoid duplicates
logger.handlers.clear()

# File handler
file_handler = logging.FileHandler(LOG_FILE, encoding='utf-8')
file_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


def extract_descriptions_from_html(raw_html, original_record):
    """
    Extract descriptions from raw_html following the specified structure.

    Args:
        raw_html: HTML content string
        original_record: Original JSON record to preserve fields from

    Returns:
        List of dictionaries, each representing a description source
    """
    descriptions = []
    identifier = original_record.get('identifier', 'unknown')
    url = original_record.get('url', 'unknown')

    try:
        soup = BeautifulSoup(raw_html, 'html.parser')

        # Step 1: Find section tag with id="local"
        section = soup.find('section', id='local')
        if not section:
            logger.warning(f"No description section found - identifier: {identifier}, url: {url}")
            return descriptions

        # Step 2: Inside find div with class="tab-pane" and id="4"
        tab_pane = section.find('div', class_='tab-pane', id='4')
        if not tab_pane:
            logger.warning(f"No tab-pane with id=4 found - identifier: {identifier}, url: {url}")
            return descriptions

        # Step 3: Inside find div with class="box clearfix"
        box = tab_pane.find('div', class_='box clearfix')
        if not box:
            logger.warning(f"No box clearfix found - identifier: {identifier}, url: {url}")
            return descriptions

        # Step 4: Inside find all divs with class="inner"
        inner_divs = box.find_all('div', class_='inner')
        if not inner_divs:
            logger.warning(f"No inner divs found - identifier: {identifier}, url: {url}")
            return descriptions

        # Step 5: Process each inner div
        for inner_div in inner_divs:
            try:
                # 5a. Store the entire HTML of the div as raw_description_html
                raw_description_html = str(inner_div)

                # 5b. Find summary tag inside div, inside find a tag and find the link
                summary = inner_div.find('summary')
                if not summary:
                    continue

                a_tag = summary.find('a')
                if not a_tag or not a_tag.get('href'):
                    continue

                link_href = a_tag.get('href')

                # Extract the id from the link (assuming format like "#some-id" or "some-id")
                link_id = link_href.lstrip('#')

                # 5b. Find the tag with the id associated with that link
                # Store that tag and everything inside that tag in raw_license_html
                license_element = soup.find(id=link_id)
                if license_element:
                    raw_license_html = str(license_element)
                else:
                    # Try finding by the full href if it's a fragment
                    if link_href.startswith('#'):
                        license_element = soup.find(id=link_id)
                        raw_license_html = str(license_element) if license_element else None
                    else:
                        raw_license_html = None

                    if not raw_license_html:
                        logger.warning(f"License not found for link {link_href} - identifier: {identifier}, url: {url}")

                # 5c. Find the first dt tag before the tag with id associated with that link
                # Find a tag inside dt. Store the href of that a tag as source_url
                # and store the text of the a tag as source_name
                source_url = None
                source_name = None

                if license_element:
                    # Find the first dt tag that comes before license_element in document order
                    # Use find_previous to search backwards from the license element
                    dt = license_element.find_previous('dt')
                    if dt:
                        dt_a = dt.find('a')
                        if dt_a:
                            source_url = dt_a.get('href', '')
                            source_name = dt_a.get_text(strip=True)
                        else:
                            logger.warning(f"Source URL <a> tag not found in dt - identifier: {identifier}, url: {url}")
                    else:
                        logger.warning(f"Source URL dt tag not found before license - identifier: {identifier}, url: {url}")
                else:
                    logger.warning(f"Source URL not found (no license element) - identifier: {identifier}, url: {url}")

                # Create description record
                description_record = {
                    # Preserve original fields
                    'identifier': original_record.get('identifier'),
                    'page_type': original_record.get('page_type'),
                    'url': original_record.get('url'),
                    'order_name': original_record.get('order_name'),
                    'family_name': original_record.get('family_name'),
                    'genus_name': original_record.get('genus_name'),
                    'species_name': original_record.get('species_name'),
                    'subspecies': original_record.get('subspecies'),
                    'timestamp': original_record.get('timestamp'),

                    # New fields
                    'raw_description_html': raw_description_html,
                    'raw_license_html': raw_license_html,
                    'source_url': source_url,
                    'source_name': source_name
                }

                descriptions.append(description_record)

            except Exception as e:
                # Continue processing other divs if one fails
                print(f"Warning: Error processing inner div: {e}")
                continue

    except Exception as e:
        print(f"Warning: Error parsing HTML: {e}")
        return descriptions

    return descriptions


def process_jsonl(input_path, output_path):
    """
    Process JSONL file and extract descriptions.

    Args:
        input_path: Path to input JSONL file
        output_path: Path to output JSONL file
    """
    input_path = Path(input_path)
    output_path = Path(output_path)

    if not input_path.exists():
        print(f"Error: File not found: {input_path}")
        return False

    # Create output directory if it doesn't exist
    output_path.parent.mkdir(parents=True, exist_ok=True)

    total_input_records = 0
    total_output_records = 0
    records_with_descriptions = 0
    records_without_descriptions = 0

    print(f"Processing {input_path}...")
    print(f"Output will be written to {output_path}")
    print(f"Log file: {LOG_FILE}")
    print("-" * 60)
    logger.info(f"Starting processing: input={input_path}, output={output_path}")

    with open(input_path, 'r', encoding='utf-8') as infile, \
         open(output_path, 'w', encoding='utf-8') as outfile:

        for line_num, line in enumerate(infile, 1):
            if not line.strip():
                continue

            try:
                record = json.loads(line)
                total_input_records += 1

                raw_html = record.get('raw_html')
                if not raw_html:
                    identifier = record.get('identifier', 'unknown')
                    url = record.get('url', 'unknown')
                    logger.warning(f"No raw_html field - identifier: {identifier}, url: {url}")
                    records_without_descriptions += 1
                    continue

                # Extract descriptions
                descriptions = extract_descriptions_from_html(raw_html, record)

                if descriptions:
                    records_with_descriptions += 1
                    for desc in descriptions:
                        outfile.write(json.dumps(desc, ensure_ascii=False) + '\n')
                        total_output_records += 1
                else:
                    records_without_descriptions += 1

                # Progress indicator
                if line_num % 1000 == 0:
                    print(f"  Processed {line_num:,} records, "
                          f"found {total_output_records:,} descriptions...", end='\r')

            except json.JSONDecodeError as e:
                print(f"\nWarning: Error parsing line {line_num}: {e}")
                continue
            except Exception as e:
                print(f"\nWarning: Error processing line {line_num}: {e}")
                continue

    print()  # New line after progress indicator

    # Print statistics
    print("-" * 60)
    print("PROCESSING STATISTICS")
    print("-" * 60)
    print(f"Total input records: {total_input_records:,}")
    print(f"Records with descriptions: {records_with_descriptions:,}")
    print(f"Records without descriptions: {records_without_descriptions:,}")
    print(f"Total output records (descriptions): {total_output_records:,}")
    print(f"Output file: {output_path}")
    print(f"Log file: {LOG_FILE}")
    print("-" * 60)

    logger.info(f"Processing completed: input_records={total_input_records}, "
                f"with_descriptions={records_with_descriptions}, "
                f"without_descriptions={records_without_descriptions}, "
                f"output_records={total_output_records}")

    return True


def main():
    """Main function."""
    if len(sys.argv) < 2:
        print("Usage: python extract_descriptions.py <input_jsonl> [output_jsonl]")
        print("Example: python extract_descriptions.py data/raw/world_flora_online_complete.jsonl data/processed/descriptions.jsonl")
        sys.exit(1)

    input_path = sys.argv[1]

    if len(sys.argv) >= 3:
        output_path = sys.argv[2]
    else:
        # Default output path
        input_file = Path(input_path)
        output_path = input_file.parent.parent / 'processed' / 'descriptions.jsonl'

    success = process_jsonl(input_path, output_path)

    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()
