"""
Script to convert descriptions_text_by_source.jsonl into a folder of txt files.
Each txt file contains the descriptions_text field, with a unique filename.
"""

import json
import sys
import re
import logging
from pathlib import Path
from hashlib import md5

# Setup logging
LOG_DIR = Path("logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "jsonl_to_txt_files.log"

# Configure logger
logger = logging.getLogger('jsonl_to_txt_files')
logger.setLevel(logging.INFO)

# Remove existing handlers to avoid duplicates
logger.handlers.clear()

# File handler
file_handler = logging.FileHandler(LOG_FILE, encoding='utf-8')
file_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


def sanitize_filename(text, max_length=100):
    """
    Sanitize text to be used in a filename.

    Args:
        text: Text to sanitize
        max_length: Maximum length of the sanitized text

    Returns:
        Sanitized string safe for use in filenames
    """
    if not text:
        return "unknown"

    # Remove or replace invalid filename characters
    # Replace spaces and special chars with underscores
    sanitized = re.sub(r'[<>:"/\\|?*\s]+', '_', str(text))
    # Remove any remaining non-ASCII or problematic characters
    sanitized = re.sub(r'[^\w\-_\.]', '', sanitized)
    # Remove multiple consecutive underscores
    sanitized = re.sub(r'_+', '_', sanitized)
    # Remove leading/trailing underscores
    sanitized = sanitized.strip('_')

    # Truncate if too long
    if len(sanitized) > max_length:
        sanitized = sanitized[:max_length]

    # Ensure it's not empty
    if not sanitized:
        sanitized = "unknown"

    return sanitized


def create_unique_filename(record, line_num):
    """
    Create a unique filename for a record.

    Args:
        record: JSON record
        line_num: Line number in the JSONL file

    Returns:
        Unique filename string
    """
    identifier = record.get('identifier', 'unknown')
    source_name = record.get('source_name', '')

    # Sanitize identifier and source_name
    safe_identifier = sanitize_filename(identifier, 50)
    safe_source_name = sanitize_filename(source_name, 50)

    # Create filename: identifier_source_name_line.txt
    # If source_name is empty, just use identifier and line number
    if safe_source_name and safe_source_name != "unknown":
        filename = f"{safe_identifier}_{safe_source_name}_{line_num:06d}.txt"
    else:
        filename = f"{safe_identifier}_{line_num:06d}.txt"

    return filename


def convert_jsonl_to_txt_files(jsonl_path, output_dir):
    """
    Convert JSONL file to a folder of txt files.

    Args:
        jsonl_path: Path to input JSONL file
        output_dir: Path to output directory for txt files
    """
    jsonl_path = Path(jsonl_path)
    output_dir = Path(output_dir)

    if not jsonl_path.exists():
        error_msg = f"File not found: {jsonl_path}"
        print(f"Error: {error_msg}")
        logger.error(error_msg)
        return False

    # Create output directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)

    total_records = 0
    files_created = 0
    records_with_text = 0
    records_without_text = 0
    duplicate_files = 0

    # Create a mapping file to track line numbers to filenames
    mapping_file = output_dir / "mapping.jsonl"

    print(f"Converting {jsonl_path} to txt files...")
    print(f"Output directory: {output_dir}")
    print(f"Log file: {LOG_FILE}")
    print("-" * 60)
    logger.info(f"Starting conversion: input={jsonl_path}, output={output_dir}")

    with open(jsonl_path, 'r', encoding='utf-8') as infile, \
         open(mapping_file, 'w', encoding='utf-8') as mapfile:

        for line_num, line in enumerate(infile, 1):
            if not line.strip():
                continue

            try:
                record = json.loads(line)
                total_records += 1

                descriptions_text = record.get('descriptions_text')

                if descriptions_text:
                    records_with_text += 1

                    # Create unique filename
                    filename = create_unique_filename(record, line_num)
                    filepath = output_dir / filename

                    # Check if file already exists
                    if filepath.exists():
                        duplicate_files += 1
                        identifier = record.get('identifier', 'unknown')
                        source_name = record.get('source_name', 'unknown')
                        error_msg = f"File already exists: {filename} (line {line_num}, identifier: {identifier}, source_name: {source_name})"
                        print(f"\nError: {error_msg}")
                        logger.error(error_msg)
                        continue

                    # Write descriptions_text to txt file
                    with open(filepath, 'w', encoding='utf-8') as txtfile:
                        txtfile.write(descriptions_text)

                    files_created += 1

                    # Write mapping entry: line number -> filename -> record identifier info
                    mapping_entry = {
                        'line_number': line_num,
                        'filename': filename,
                        'identifier': record.get('identifier'),
                        'source_name': record.get('source_name'),
                        'source_url': record.get('source_url'),
                        'species_name': record.get('species_name'),
                        'genus_name': record.get('genus_name'),
                        'family_name': record.get('family_name')
                    }
                    mapfile.write(json.dumps(mapping_entry, ensure_ascii=False) + '\n')
                else:
                    records_without_text += 1

                # Progress indicator
                if line_num % 1000 == 0:
                    print(f"  Processed {line_num:,} records, "
                          f"created {files_created:,} txt files...", end='\r')

            except json.JSONDecodeError as e:
                error_msg = f"Error parsing line {line_num}: {e}"
                print(f"\nWarning: {error_msg}")
                logger.error(error_msg)
                continue
            except Exception as e:
                error_msg = f"Error processing line {line_num}: {e}"
                print(f"\nWarning: {error_msg}")
                logger.error(error_msg)
                continue

    print()  # New line after progress indicator

    # Print statistics
    print("-" * 60)
    print("CONVERSION STATISTICS")
    print("-" * 60)
    print(f"Total records processed: {total_records:,}")
    print(f"Records with descriptions_text: {records_with_text:,}")
    print(f"Records without descriptions_text: {records_without_text:,}")
    print(f"Txt files created: {files_created:,}")
    if duplicate_files > 0:
        print(f"Duplicate files skipped: {duplicate_files:,}")
    print(f"Output directory: {output_dir}")
    print(f"Mapping file: {mapping_file}")
    print(f"Log file: {LOG_FILE}")
    print("-" * 60)

    logger.info(f"Conversion completed: total_records={total_records}, "
                f"with_text={records_with_text}, without_text={records_without_text}, "
                f"files_created={files_created}, duplicate_files={duplicate_files}")

    return True


def main():
    """Main function."""
    if len(sys.argv) < 2:
        print("Usage: python jsonl_to_txt_files.py <input_jsonl> [output_dir]")
        print("Example: python jsonl_to_txt_files.py data/processed/descriptions_text_by_source.jsonl data/processed/descriptions_txt")
        sys.exit(1)

    jsonl_path = sys.argv[1]

    if len(sys.argv) >= 3:
        output_dir = sys.argv[2]
    else:
        # Default output directory
        input_file = Path(jsonl_path)
        output_dir = input_file.parent / f"{input_file.stem}_txt"

    success = convert_jsonl_to_txt_files(jsonl_path, output_dir)

    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()
