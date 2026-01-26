"""
Script to extract description text from raw_description_html in descriptions_by_source.jsonl.
Converts HTML structure to plain text.
"""

import json
import sys
import logging
from pathlib import Path
from bs4 import BeautifulSoup

# Setup logging
LOG_DIR = Path("logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "extract_description_text.log"

# Configure logger
logger = logging.getLogger('extract_description_text')
logger.setLevel(logging.INFO)

# Remove existing handlers to avoid duplicates
logger.handlers.clear()

# File handler
file_handler = logging.FileHandler(LOG_FILE, encoding='utf-8')
file_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


def extract_text_from_description_html(raw_description_html):
    """
    Extract text from raw_description_html following the specified structure.

    Args:
        raw_description_html: HTML content string

    Returns:
        String containing concatenated description text, or None if extraction fails
    """
    if not raw_description_html:
        return None

    try:
        soup = BeautifulSoup(raw_description_html, 'html.parser')

        # Step 1: Find summary tag
        summary = soup.find('summary')
        if not summary:
            return None

        # Step 2: Get all siblings after the summary tag
        # They come in pairs of (b, div)
        description_parts = []

        # Get all next siblings after summary
        siblings = []
        for sibling in summary.next_siblings:
            # Only include element nodes (skip text/whitespace nodes)
            if hasattr(sibling, 'name') and sibling.name in ['b', 'div']:
                siblings.append(sibling)

        # Step 3: Process siblings in pairs (b, div)
        i = 0
        while i < len(siblings):
            if siblings[i].name == 'b' and i + 1 < len(siblings) and siblings[i + 1].name == 'div':
                # Found a pair: b and div
                b_tag = siblings[i]
                div_tag = siblings[i + 1]

                # Extract text from b (outside span)
                b_text = extract_text_outside_span(b_tag)

                # Extract text from div
                div_text = div_tag.get_text(separator=' ', strip=True)

                # Concatenate b_text and div_text for this pair
                pair_text_parts = []
                if b_text:
                    pair_text_parts.append(b_text)
                if div_text:
                    pair_text_parts.append(div_text)

                if pair_text_parts:
                    pair_text = ' '.join(pair_text_parts)
                    description_parts.append(pair_text)

                i += 2  # Move to next pair
            else:
                # Skip if not a valid pair
                i += 1

        # Step 4: Concatenate all pairs
        if description_parts:
            descriptions_text = ' '.join(description_parts)
            return descriptions_text
        else:
            return None

    except Exception as e:
        logger.warning(f"Error extracting text from HTML: {e}")
        return None


def extract_text_outside_span(b_tag):
    """
    Extract text from <b> tag but exclude text inside <span> tags.

    Args:
        b_tag: BeautifulSoup Tag object for <b> element

    Returns:
        String containing text outside span tags
    """
    if not b_tag:
        return None

    # Clone the tag to avoid modifying the original
    b_clone = BeautifulSoup(str(b_tag), 'html.parser').find('b')
    if not b_clone:
        return None

    # Remove all span tags
    for span in b_clone.find_all('span'):
        span.decompose()

    # Get the remaining text
    text = b_clone.get_text(separator=' ', strip=True)
    return text if text else None


def process_jsonl(input_path, output_path):
    """
    Process JSONL file and extract description text.

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
    records_with_text = 0
    records_without_text = 0

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

                raw_description_html = record.get('raw_description_html')
                if not raw_description_html:
                    identifier = record.get('identifier', 'unknown')
                    logger.warning(f"No raw_description_html field - identifier: {identifier}")
                    records_without_text += 1
                    # Still write the record without descriptions_text
                    record['descriptions_text'] = None
                    outfile.write(json.dumps(record, ensure_ascii=False) + '\n')
                    total_output_records += 1
                    continue

                # Extract description text
                descriptions_text = extract_text_from_description_html(raw_description_html)

                # Add descriptions_text to record
                record['descriptions_text'] = descriptions_text

                if descriptions_text:
                    records_with_text += 1
                else:
                    records_without_text += 1
                    identifier = record.get('identifier', 'unknown')
                    logger.warning(f"No text extracted from HTML - identifier: {identifier}")

                # Write the updated record
                outfile.write(json.dumps(record, ensure_ascii=False) + '\n')
                total_output_records += 1

                # Progress indicator
                if line_num % 1000 == 0:
                    print(f"  Processed {line_num:,} records, "
                          f"extracted text from {records_with_text:,}...", end='\r')

            except json.JSONDecodeError as e:
                print(f"\nWarning: Error parsing line {line_num}: {e}")
                logger.warning(f"Error parsing line {line_num}: {e}")
                continue
            except Exception as e:
                print(f"\nWarning: Error processing line {line_num}: {e}")
                logger.warning(f"Error processing line {line_num}: {e}")
                continue

    print()  # New line after progress indicator

    # Print statistics
    print("-" * 60)
    print("PROCESSING STATISTICS")
    print("-" * 60)
    print(f"Total input records: {total_input_records:,}")
    print(f"Records with extracted text: {records_with_text:,}")
    print(f"Records without extracted text: {records_without_text:,}")
    print(f"Total output records: {total_output_records:,}")
    print(f"Output file: {output_path}")
    print(f"Log file: {LOG_FILE}")
    print("-" * 60)

    logger.info(f"Processing completed: input_records={total_input_records}, "
                f"with_text={records_with_text}, "
                f"without_text={records_without_text}, "
                f"output_records={total_output_records}")

    return True


def main():
    """Main function."""
    if len(sys.argv) < 2:
        print("Usage: python extract_description_text.py <input_jsonl> [output_jsonl]")
        print("Example: python extract_description_text.py data/raw/descriptions_by_source.jsonl data/processed/descriptions_with_text.jsonl")
        sys.exit(1)

    input_path = sys.argv[1]

    if len(sys.argv) >= 3:
        output_path = sys.argv[2]
    else:
        # Default output path
        input_file = Path(input_path)
        output_path = input_file.parent.parent / 'processed' / 'descriptions_with_text.jsonl'

    success = process_jsonl(input_path, output_path)

    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()
