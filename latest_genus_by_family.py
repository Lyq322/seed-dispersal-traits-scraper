"""
Script to output the latest genus of each family grouped by order, ordered alphabetically.
For each row in the JSONL file, tracks the most recent genus encountered for each family.
"""

import json
import sys
from pathlib import Path
from collections import defaultdict
from datetime import datetime


def parse_timestamp(ts_str):
    """Parse timestamp string to datetime object."""
    if not ts_str:
        return None
    try:
        return datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
    except (ValueError, AttributeError):
        return None


def process_jsonl(jsonl_path):
    """
    Process JSONL file and find the latest genus for each family.
    Returns a dictionary: {order: {family: (latest_genus, timestamp)}}
    """
    jsonl_path = Path(jsonl_path)

    if not jsonl_path.exists():
        print(f"Error: File not found: {jsonl_path}", file=sys.stderr)
        return None

    # Structure: {order: {family: (genus, timestamp)}}
    family_latest_genus = defaultdict(lambda: defaultdict(lambda: (None, None)))

    print(f"Processing {jsonl_path}...", file=sys.stderr)

    # Read file line by line (important for large files)
    with open(jsonl_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            if not line.strip():
                continue

            try:
                record = json.loads(line)

                # Extract taxonomic information
                order_name = record.get('order_name')
                family_name = record.get('family_name')
                genus_name = record.get('genus_name')
                timestamp_str = record.get('timestamp')

                # Skip if missing required fields
                if not order_name or not family_name or not genus_name:
                    continue

                # Parse timestamp
                timestamp = parse_timestamp(timestamp_str)

                # Get current latest for this family
                current_genus, current_timestamp = family_latest_genus[order_name][family_name]

                # Update if this is later (or if no previous record)
                if current_timestamp is None or (timestamp and timestamp > current_timestamp):
                    family_latest_genus[order_name][family_name] = (genus_name, timestamp)

                # Progress indicator for large files
                if line_num % 10000 == 0:
                    print(f"  Processed {line_num:,} lines...", file=sys.stderr, end='\r')

            except json.JSONDecodeError as e:
                print(f"\nWarning: Error parsing line {line_num}: {e}", file=sys.stderr)
                continue

    print()  # New line after progress indicator
    return family_latest_genus


def output_results(family_latest_genus):
    """
    Output results grouped by order, ordered alphabetically.
    Format: Order | Family | Latest Genus
    """
    # Sort orders alphabetically
    sorted_orders = sorted(family_latest_genus.keys())

    # Output header
    print("Order|Family|Latest Genus")
    print("-" * 80)

    # For each order
    for order in sorted_orders:
        families = family_latest_genus[order]

        # Sort families alphabetically
        sorted_families = sorted(families.keys())

        # For each family in this order
        for family in sorted_families:
            latest_genus, timestamp = families[family]
            if latest_genus:
                print(f"{order}|{family}|{latest_genus}")


def main():
    """Main function."""
    if len(sys.argv) < 2:
        print("Usage: python latest_genus_by_family.py <jsonl_path>", file=sys.stderr)
        print("Example: python latest_genus_by_family.py data/raw/world_flora_online_v1.jsonl", file=sys.stderr)
        sys.exit(1)

    jsonl_path = sys.argv[1]
    family_latest_genus = process_jsonl(jsonl_path)

    if family_latest_genus is None:
        sys.exit(1)

    print(f"\nFound {len(family_latest_genus)} orders with families.", file=sys.stderr)
    total_families = sum(len(families) for families in family_latest_genus.values())
    print(f"Total families: {total_families}", file=sys.stderr)
    print(file=sys.stderr)

    output_results(family_latest_genus)


if __name__ == "__main__":
    main()
