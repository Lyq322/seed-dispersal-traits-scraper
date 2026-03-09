"""
Script to analyze JSONL files from flora scrapers.
Counts unique families, orders, genera, and species by identifier (id), not by name.
"""

import json
from pathlib import Path
from collections import defaultdict


def analyze_jsonl(jsonl_path):
    """Analyze a JSONL file and print statistics."""
    jsonl_path = Path(jsonl_path)

    if not jsonl_path.exists():
        print(f"Error: File not found: {jsonl_path}")
        return

    # Sets to store unique values
    families = set()
    orders = set()
    genera = set()
    species = set()

    # Counts by page type
    page_type_counts = defaultdict(int)

    # Count total records
    total_records = 0

    print(f"Analyzing {jsonl_path}...")
    print("-" * 60)

    # Read file line by line (important for large files)
    with open(jsonl_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            if not line.strip():
                continue

            try:
                record = json.loads(line)
                total_records += 1

                # Track page types
                page_type = record.get('page_type', 'unknown')
                page_type_counts[page_type] += 1

                # Count unique by identifier (id) per page type
                identifier = record.get('identifier')
                page_type = record.get('page_type')
                if identifier and page_type == 'family':
                    families.add(identifier)
                elif identifier and page_type == 'order':
                    orders.add(identifier)
                elif identifier and page_type == 'genus':
                    genera.add(identifier)
                elif identifier and page_type == 'species':
                    species.add(identifier)

                # Progress indicator for large files
                if line_num % 10000 == 0:
                    print(f"  Processed {line_num:,} lines...", end='\r')

            except json.JSONDecodeError as e:
                print(f"\nWarning: Error parsing line {line_num}: {e}")
                continue

    print()  # New line after progress indicator
    print("-" * 60)
    print("STATISTICS")
    print("-" * 60)
    print(f"Total records: {total_records:,}")
    print()

    print("Unique counts:")
    print(f"  Families: {len(families):,}")
    if orders:
        print(f"  Orders: {len(orders):,}")
    print(f"  Genera: {len(genera):,}")
    print(f"  Species: {len(species):,}")
    print()

    print("Records by page type:")
    for page_type, count in sorted(page_type_counts.items()):
        print(f"  {page_type}: {count:,}")
    print()

    if orders:
        print(f"Order IDs (first 20):")
        for i, oid in enumerate(sorted(orders)[:20], 1):
            print(f"  {i}. {oid}")
        if len(orders) > 20:
            print(f"  ... and {len(orders) - 20} more")
        print()

    # Show some examples (identifiers)
    if families:
        print(f"Sample family IDs (first 10):")
        for i, fid in enumerate(sorted(families)[:10], 1):
            print(f"  {i}. {fid}")
        if len(families) > 10:
            print(f"  ... and {len(families) - 10} more")
        print()

    if species:
        print(f"Sample species IDs (first 10):")
        for i, sid in enumerate(sorted(species)[:10], 1):
            print(f"  {i}. {sid}")
        if len(species) > 10:
            print(f"  ... and {len(species) - 10} more")


def main():
    """Main function."""
    import sys

    if len(sys.argv) < 2:
        print("Usage: python analyze_jsonl.py <jsonl_path>")
        print("Example: python analyze_jsonl.py data/flora_of_china_raw/flora_of_china.jsonl")
        sys.exit(1)

    jsonl_path = sys.argv[1]
    analyze_jsonl(jsonl_path)


if __name__ == "__main__":
    main()
