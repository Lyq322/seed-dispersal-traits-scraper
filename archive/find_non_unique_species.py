"""
Script to find and print species that aren't unique.
Finds species that appear multiple times (by name or identifier).
"""

import json
import sys
from pathlib import Path
from collections import defaultdict


def find_non_unique_species(jsonl_path):
    """
    Find species that appear multiple times in the JSONL file.

    Args:
        jsonl_path: Path to JSONL file

    Returns:
        Dictionary with information about non-unique species
    """
    jsonl_path = Path(jsonl_path)

    if not jsonl_path.exists():
        print(f"Error: File not found: {jsonl_path}")
        return None

    # Track species by name
    species_by_name = defaultdict(list)
    # Track species by identifier
    species_by_identifier = defaultdict(list)
    # Track all records
    all_records = []

    print(f"Reading species from {jsonl_path}...")

    with open(jsonl_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            if not line.strip():
                continue

            try:
                record = json.loads(line)
                page_type = record.get('page_type')

                # Only process species
                if page_type == 'species':
                    species_name = record.get('species_name')
                    identifier = record.get('identifier')
                    genus_name = record.get('genus_name')
                    family_name = record.get('family_name')
                    order_name = record.get('order_name')

                    if species_name:
                        # Create a full species name with genus
                        if genus_name:
                            full_name = f"{genus_name} {species_name}"
                        else:
                            full_name = species_name

                        species_by_name[full_name].append({
                            'line_num': line_num,
                            'identifier': identifier,
                            'species_name': species_name,
                            'genus_name': genus_name,
                            'family_name': family_name,
                            'order_name': order_name,
                            'url': record.get('url'),
                            'record': record
                        })

                    if identifier:
                        species_by_identifier[identifier].append({
                            'line_num': line_num,
                            'identifier': identifier,
                            'species_name': species_name,
                            'genus_name': genus_name,
                            'family_name': family_name,
                            'order_name': order_name,
                            'url': record.get('url'),
                            'record': record
                        })

                    all_records.append(record)

                # Progress indicator
                if line_num % 10000 == 0:
                    print(f"  Processed {line_num:,} lines...", end='\r')

            except json.JSONDecodeError as e:
                print(f"\nWarning: Error parsing line {line_num}: {e}")
                continue

    print()  # New line after progress indicator

    # Find non-unique species
    non_unique_by_name = {name: records for name, records in species_by_name.items()
                          if len(records) > 1}
    non_unique_by_identifier = {ident: records for ident, records in species_by_identifier.items()
                                if len(records) > 1}

    return {
        'non_unique_by_name': non_unique_by_name,
        'non_unique_by_identifier': non_unique_by_identifier,
        'total_species': len(species_by_name),
        'total_unique_names': len([r for r in species_by_name.values() if len(r) == 1]),
        'total_unique_identifiers': len([r for r in species_by_identifier.values() if len(r) == 1])
    }


def print_non_unique_species(results):
    """
    Print information about non-unique species.

    Args:
        results: Dictionary with non-unique species information
    """
    if not results:
        print("No results to display.")
        return

    non_unique_by_name = results['non_unique_by_name']
    non_unique_by_identifier = results['non_unique_by_identifier']

    print("=" * 80)
    print("NON-UNIQUE SPECIES ANALYSIS")
    print("=" * 80)
    print(f"Total unique species names: {results['total_species']:,}")
    print(f"Species with unique names: {results['total_unique_names']:,}")
    print(f"Species with duplicate names: {len(non_unique_by_name):,}")
    print(f"Species with duplicate identifiers: {len(non_unique_by_identifier):,}")
    print("=" * 80)
    print()

    # Print species with duplicate names
    # if non_unique_by_name:
    #     print(f"\n{'='*80}")
    #     print(f"SPECIES WITH DUPLICATE NAMES ({len(non_unique_by_name)} species)")
    #     print(f"{'='*80}\n")

    #     # Sort by number of occurrences (most duplicates first)
    #     sorted_duplicates = sorted(non_unique_by_name.items(),
    #                               key=lambda x: len(x[1]), reverse=True)

    #     for idx, (species_name, records) in enumerate(sorted_duplicates, 1):
    #         print(f"{idx}. {species_name}")
    #         print(f"   Appears {len(records)} times:")
    #         print()

    #         for record_idx, rec in enumerate(records, 1):
    #             print(f"   Occurrence {record_idx}:")
    #             print(f"     Line: {rec['line_num']}")
    #             print(f"     Identifier: {rec['identifier']}")
    #             print(f"     Order: {rec['order_name']}")
    #             print(f"     Family: {rec['family_name']}")
    #             print(f"     Genus: {rec['genus_name']}")
    #             print(f"     Species: {rec['species_name']}")
    #             print(f"     URL: {rec['url']}")

    #             # Check if identifiers are different
    #             identifiers = [r['identifier'] for r in records]
    #             if len(set(identifiers)) > 1:
    #                 print(f"     ⚠️  Different identifiers: {set(identifiers)}")

    #             print()

    #         print("-" * 80)
    #         print()

    # # Print species with duplicate identifiers
    # if non_unique_by_identifier:
    #     print(f"\n{'='*80}")
    #     print(f"SPECIES WITH DUPLICATE IDENTIFIERS ({len(non_unique_by_identifier)} identifiers)")
    #     print(f"{'='*80}\n")

    #     # Sort by number of occurrences
    #     sorted_duplicates = sorted(non_unique_by_identifier.items(),
    #                               key=lambda x: len(x[1]), reverse=True)

    #     for idx, (identifier, records) in enumerate(sorted_duplicates, 1):
    #         print(f"{idx}. Identifier: {identifier}")
    #         print(f"   Appears {len(records)} times:")
    #         print()

    #         for record_idx, rec in enumerate(records, 1):
    #             print(f"   Occurrence {record_idx}:")
    #             print(f"     Line: {rec['line_num']}")
    #             if rec['genus_name'] and rec['species_name']:
    #                 species_name = f"{rec['genus_name']} {rec['species_name']}"
    #             else:
    #                 species_name = rec['species_name'] or 'N/A'
    #             print(f"     Species: {species_name}")
    #             print(f"     Order: {rec['order_name']}")
    #             print(f"     Family: {rec['family_name']}")
    #             print(f"     URL: {rec['url']}")

    #             # Check if species names are different
    #             species_names = [f"{r['genus_name']} {r['species_name']}" if r['genus_name'] else r['species_name']
    #                            for r in records if r['species_name']]
    #             if len(set(species_names)) > 1:
    #                 print(f"     ⚠️  Different species names: {set(species_names)}")

    #             print()

    #         print("-" * 80)
    #         print()


def main():
    """Main function."""
    if len(sys.argv) < 2:
        print("Usage: python find_non_unique_species.py <jsonl_file>")
        print("Example: python find_non_unique_species.py data/raw/world_flora_online_complete.jsonl")
        sys.exit(1)

    jsonl_path = sys.argv[1]

    # Find non-unique species
    results = find_non_unique_species(jsonl_path)

    if not results:
        print("Error: Could not process file")
        sys.exit(1)

    # Print results
    print_non_unique_species(results)


if __name__ == "__main__":
    main()
