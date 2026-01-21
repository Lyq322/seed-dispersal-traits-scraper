"""
Script to combine world_flora_online.jsonl and world_flora_online2.jsonl,
removing duplicates by identifier field.
"""

import json
import sys
from pathlib import Path
from collections import OrderedDict


def combine_jsonl_files(file1_path, file2_path, output_path):
    """
    Combine two JSONL files, removing duplicates by identifier.

    Args:
        file1_path: Path to first JSONL file
        file2_path: Path to second JSONL file
        output_path: Path to output combined JSONL file
    """
    file1_path = Path(file1_path)
    file2_path = Path(file2_path)
    output_path = Path(output_path)

    # Check if input files exist
    if not file1_path.exists():
        print(f"Error: File not found: {file1_path}")
        return False

    if not file2_path.exists():
        print(f"Error: File not found: {file2_path}")
        return False

    # Dictionary to store records by identifier
    # Using OrderedDict to preserve insertion order (first file, then second)
    records_by_id = OrderedDict()

    total_read = 0
    duplicates_found = 0

    print(f"Reading {file1_path}...")
    # Read first file
    with open(file1_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            if not line.strip():
                continue

            try:
                record = json.loads(line)
                total_read += 1

                identifier = record.get('identifier')
                if identifier is None:
                    print(f"Warning: Line {line_num} in {file1_path} has no 'identifier' field, skipping")
                    continue

                # If identifier already exists, it will be overwritten (second file takes precedence)
                if identifier in records_by_id:
                    duplicates_found += 1

                records_by_id[identifier] = record

                # Progress indicator for large files
                if line_num % 10000 == 0:
                    print(f"  Processed {line_num:,} lines from {file1_path.name}...", end='\r')

            except json.JSONDecodeError as e:
                print(f"\nWarning: Error parsing line {line_num} in {file1_path}: {e}")
                continue

    print()  # New line after progress indicator

    print(f"Reading {file2_path}...")
    # Read second file (will overwrite duplicates from first file)
    with open(file2_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            if not line.strip():
                continue

            try:
                record = json.loads(line)
                total_read += 1

                identifier = record.get('identifier')
                if identifier is None:
                    print(f"Warning: Line {line_num} in {file2_path} has no 'identifier' field, skipping")
                    continue

                # If identifier already exists, it will be overwritten (second file takes precedence)
                if identifier in records_by_id:
                    duplicates_found += 1

                records_by_id[identifier] = record

                # Progress indicator for large files
                if line_num % 10000 == 0:
                    print(f"  Processed {line_num:,} lines from {file2_path.name}...", end='\r')

            except json.JSONDecodeError as e:
                print(f"\nWarning: Error parsing line {line_num} in {file2_path}: {e}")
                continue

    print()  # New line after progress indicator

    # Write combined output
    print(f"Writing combined output to {output_path}...")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, 'w', encoding='utf-8') as f:
        for idx, (identifier, record) in enumerate(records_by_id.items(), 1):
            f.write(json.dumps(record, ensure_ascii=False) + '\n')

            # Progress indicator
            if idx % 10000 == 0:
                print(f"  Written {idx:,} records...", end='\r')

    print()  # New line after progress indicator

    # Print statistics
    print("-" * 60)
    print("COMBINATION STATISTICS")
    print("-" * 60)
    print(f"Total records read: {total_read:,}")
    print(f"Unique records (after deduplication): {len(records_by_id):,}")
    print(f"Duplicates removed: {duplicates_found:,}")
    print(f"Output file: {output_path}")
    print("-" * 60)

    return True


def main():
    """Main function."""
    if len(sys.argv) < 4:
        print("Usage: python combine_world_flora_online.py <file1> <file2> <output>")
        print("Example: python combine_world_flora_online.py data/raw/world_flora_online.jsonl data/raw/world_flora_online2.jsonl data/raw/world_flora_online_combined.jsonl")
        sys.exit(1)

    file1_path = sys.argv[1]
    file2_path = sys.argv[2]
    output_path = sys.argv[3]

    success = combine_jsonl_files(file1_path, file2_path, output_path)

    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()
