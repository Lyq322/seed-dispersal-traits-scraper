"""
Script to analyze JSONL files from flora scrapers.
Counts unique families, orders, genera, and species.
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

                # Extract taxonomic information
                family_name = record.get('family_name')
                order_name = record.get('order_name')
                genus_name = record.get('genus_name')
                species_name = record.get('species_name')

                # Add to sets (only if not None/empty)
                if family_name:
                    families.add(family_name)

                if order_name:
                    orders.add(order_name)

                if genus_name:
                    genera.add(genus_name)

                if species_name:
                    species.add(species_name)

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
        print(f"Orders: {len(orders):,}")
        for i, order in enumerate(sorted(orders), 1):
            print(f"  {i}. {order}")
        print()

    # Show some examples
    if families:
        print(f"Sample families (first 10):")
        for i, fam in enumerate(sorted(families)[:10], 1):
            print(f"  {i}. {fam}")
        if len(families) > 10:
            print(f"  ... and {len(families) - 10} more")
        print()

    if species:
        print(f"Sample species (first 10):")
        for i, spec in enumerate(sorted(species)[:10], 1):
            print(f"  {i}. {spec}")
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
