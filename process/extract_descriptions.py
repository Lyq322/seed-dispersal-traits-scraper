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
            logger.info(f"No description section found - identifier: {identifier}, url: {url}")
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
                    logger.error(f"No summary tag found - identifier: {identifier}, url: {url}")
                    continue

                source_name = summary.get_text(strip=True)

                # 5c. Find <a> tag containing source_name with href that isn't just an ID (e.g. #H)
                source_url = None
                raw_license_html = None
                found_a_tag = None

                # Search for <a> tags that contain the source_name text
                # Look in the entire soup, not just inner_div, to find the source link
                all_a_tags = soup.find_all('a')
                for a_tag in all_a_tags:
                    a_text = a_tag.get_text(strip=True)
                    href = a_tag.get('href', '')

                    # Check if this <a> tag contains the source_name and has a non-ID href
                    if source_name == a_text:
                        # Check if href is not just an ID (doesn't start with just #)
                        if href and not (href.startswith('#') and len(href) <= 2):
                            source_url = href
                            found_a_tag = a_tag
                            break

                if not source_url:
                    logger.error(f"Source URL not found for source_name '{source_name}' - identifier: {identifier}, url: {url}")
                else:
                    # Extract raw_license_html: all <dd> tags after the <dt> parent of a_tag,
                    # before the next <dt> tag or end of parent container
                    if found_a_tag:
                        # Find the parent <dt> tag
                        dt_tag = found_a_tag.find_parent('dt')
                        if dt_tag:
                            # Get all following siblings
                            all_following = dt_tag.find_next_siblings()

                            # Collect all <dd> tags until we hit a <dt> tag or run out
                            dd_tags = []
                            for sibling in all_following:
                                if sibling.name == 'dd':
                                    dd_tags.append(sibling)
                                elif sibling.name == 'dt':
                                    # Stop at next <dt> tag
                                    break

                            # Convert all <dd> tags to HTML string
                            if dd_tags:
                                raw_license_html = ''.join(str(dd) for dd in dd_tags)
                            else:
                                logger.error(f"No <dd> tags found after <dt> for source_name '{source_name}' - identifier: {identifier}, url: {url}")
                        else:
                            logger.error(f"No <dt> parent found for <a> tag with source_url - identifier: {identifier}, url: {url}, source_name: {source_name}")

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
                    logger.error(f"No raw_html field - identifier: {identifier}, url: {url}")
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
