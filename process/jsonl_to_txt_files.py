"""
Script to convert descriptions_text_by_source.jsonl into a folder of txt files.
Each txt file contains the descriptions_text field, with a unique filename.
Writes the generated txt filename (relative to output_dir) into each record
as "txt_filename" and outputs an updated JSONL (overwriting input by default,
or to --output-jsonl if set).
"""

import argparse
import json
import sys
import re
import logging
from pathlib import Path

# Max files per batch folder (avoids huge single directories)
BATCH_SIZE = 10_000

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


# Field name written into each record with the generated txt filename (relative path)
TXT_FILENAME_FIELD = "txt_filename"


def convert_jsonl_to_txt_files(jsonl_path, output_dir, output_jsonl_path=None):
    """
    Convert JSONL file to a folder of txt files.
    Writes the generated txt filename into each record as txt_filename (relative to output_dir),
    and writes an updated JSONL to output_jsonl_path (default: overwrite input).

    Args:
        jsonl_path: Path to input JSONL file
        output_dir: Path to output directory for txt files
        output_jsonl_path: Path for updated JSONL (default: overwrite input)
    """
    jsonl_path = Path(jsonl_path).resolve()
    output_dir = Path(output_dir).resolve()
    use_temp = output_jsonl_path is None
    if use_temp:
        out_jsonl = jsonl_path.with_suffix(".jsonl.tmp")
    else:
        out_jsonl = Path(output_jsonl_path).resolve()

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
    batch_index = 0
    files_in_current_batch = 0
    current_batch_dir = None

    # Create a mapping file to track line numbers to filenames
    mapping_file = output_dir / "mapping.jsonl"

    def get_batch_dir():
        nonlocal batch_index, files_in_current_batch, current_batch_dir
        if current_batch_dir is None or files_in_current_batch >= BATCH_SIZE:
            if files_in_current_batch >= BATCH_SIZE:
                batch_index += 1
                files_in_current_batch = 0
            current_batch_dir = output_dir / f"batch_{batch_index:04d}"
            current_batch_dir.mkdir(parents=True, exist_ok=True)
        return current_batch_dir

    print(f"Converting {jsonl_path} to txt files...")
    print(f"Output directory: {output_dir} (batch size: {BATCH_SIZE:,})")
    print(f"Updated JSONL: {out_jsonl if not use_temp else str(jsonl_path) + ' (overwrite)'}")
    print(f"Log file: {LOG_FILE}")
    print("-" * 60)
    logger.info(f"Starting conversion: input={jsonl_path}, output={output_dir}")

    with open(jsonl_path, 'r', encoding='utf-8') as infile, \
         open(mapping_file, 'w', encoding='utf-8') as mapfile, \
         open(out_jsonl, 'w', encoding='utf-8') as outfile:

        for line_num, line in enumerate(infile, 1):
            if not line.strip():
                continue

            try:
                record = json.loads(line)
                if not isinstance(record, dict):
                    record = {}
                total_records += 1

                # Ensure we don't carry over a stale txt_filename from a previous run
                record.pop(TXT_FILENAME_FIELD, None)

                descriptions_text = record.get('descriptions_text')

                if descriptions_text:
                    records_with_text += 1

                    # Create unique filename and choose batch folder
                    filename = create_unique_filename(record, line_num)
                    batch_dir = get_batch_dir()
                    filepath = batch_dir / filename

                    # Relative path for mapping (includes batch subfolder)
                    relative_path = f"{batch_dir.name}/{filename}"

                    # Check if file already exists
                    if filepath.exists():
                        duplicate_files += 1
                        identifier = record.get('identifier', 'unknown')
                        source_name = record.get('source_name', 'unknown')
                        error_msg = f"File already exists: {filename} (line {line_num}, identifier: {identifier}, source_name: {source_name})"
                        print(f"\nError: {error_msg}")
                        logger.error(error_msg)
                        # Still write record to output JSONL (without txt_filename)
                        outfile.write(json.dumps(record, ensure_ascii=False) + '\n')
                        continue

                    # Write descriptions_text to txt file
                    with open(filepath, 'w', encoding='utf-8') as txtfile:
                        txtfile.write(descriptions_text)

                    files_created += 1
                    files_in_current_batch += 1

                    # Write txt filename into the record for the output JSONL
                    record[TXT_FILENAME_FIELD] = relative_path

                    # Write mapping entry: line number -> path -> record identifier info
                    mapping_entry = {
                        'line_number': line_num,
                        'filename': relative_path,
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

                # Write updated record to output JSONL
                outfile.write(json.dumps(record, ensure_ascii=False) + '\n')

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

    if use_temp and out_jsonl.exists():
        out_jsonl.replace(jsonl_path)

    print()  # New line after progress indicator

    # Print statistics
    print("-" * 60)
    print("CONVERSION STATISTICS")
    print("-" * 60)
    print(f"Total records processed: {total_records:,}")
    print(f"Records with descriptions_text: {records_with_text:,}")
    print(f"Records without descriptions_text: {records_without_text:,}")
    print(f"Txt files created: {files_created:,}")
    print(f"Batch folders used: {batch_index + 1:,}")
    if duplicate_files > 0:
        print(f"Duplicate files skipped: {duplicate_files:,}")
    print(f"Output directory: {output_dir}")
    print(f"Mapping file: {mapping_file}")
    print(f"Log file: {LOG_FILE}")
    print("-" * 60)

    logger.info(f"Conversion completed: total_records={total_records}, "
                f"with_text={records_with_text}, without_text={records_without_text}, "
                f"files_created={files_created}, batches={batch_index + 1}, duplicate_files={duplicate_files}")

    return True


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description="Convert descriptions JSONL to txt files; write generated txt filename into each record as txt_filename."
    )
    parser.add_argument("input_jsonl", help="Path to input JSONL file")
    parser.add_argument(
        "output_dir",
        nargs="?",
        default=None,
        help="Output directory for txt files (default: <input_stem>_txt next to input)",
    )
    parser.add_argument(
        "-o", "--output-jsonl",
        dest="output_jsonl",
        default=None,
        help="Path for updated JSONL with txt_filename field (default: overwrite input)",
    )
    args = parser.parse_args()

    jsonl_path = args.input_jsonl
    output_dir = args.output_dir
    if output_dir is None:
        output_dir = Path(jsonl_path).parent / f"{Path(jsonl_path).stem}_txt"

    success = convert_jsonl_to_txt_files(jsonl_path, output_dir, output_jsonl_path=args.output_jsonl)

    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()
