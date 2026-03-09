"""
Script to find and print species with descriptions around 10^5 (100,000) words.
"""

import json
import sys
from pathlib import Path


def find_long_descriptions(jsonl_path, target_words=100000, tolerance=1000):
    """
    Find descriptions with approximately target_words words.

    Args:
        jsonl_path: Path to descriptions JSONL file
        target_words: Target word count (default 100000)
        tolerance: Acceptable range around target (default 1000)

    Returns:
        List of records matching the criteria
    """
    jsonl_path = Path(jsonl_path)

    if not jsonl_path.exists():
        print(f"Error: File not found: {jsonl_path}")
        return []

    matching_records = []
    min_words = target_words - tolerance
    max_words = target_words + tolerance

    print(f"Searching for descriptions with ~{target_words:,} words (range: {min_words:,} - {max_words:,})...")
    print(f"Reading from {jsonl_path}...")
    print("-" * 80)

    with open(jsonl_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            if not line.strip():
                continue

            try:
                record = json.loads(line)
                descriptions_text = record.get('descriptions_text')

                if descriptions_text:
                    # Count words
                    word_count = len(descriptions_text.split())
                    char_count = len(descriptions_text)

                    # Check if word count is in target range
                    if min_words <= word_count <= max_words:
                        matching_records.append({
                            'record': record,
                            'word_count': word_count,
                            'char_count': char_count
                        })

                # Progress indicator
                if line_num % 10000 == 0:
                    print(f"  Processed {line_num:,} records, found {len(matching_records)} matches...", end='\r')

            except json.JSONDecodeError as e:
                print(f"\nWarning: Error parsing line {line_num}: {e}")
                continue

    print()  # New line after progress indicator
    print(f"Found {len(matching_records)} descriptions with ~{target_words:,} words")
    print("=" * 80)

    return matching_records


def print_matching_species(matching_records):
    """
    Print information about matching species.

    Args:
        matching_records: List of matching records with word/char counts
    """
    if not matching_records:
        print("No matching descriptions found.")
        return

    # Sort by word count
    matching_records.sort(key=lambda x: x['word_count'], reverse=True)

    print(f"\nFound {len(matching_records)} descriptions with ~100,000 words:\n")

    for idx, match in enumerate(matching_records, 1):
        record = match['record']
        word_count = match['word_count']
        char_count = match['char_count']

        print(f"{idx}. Species Information:")
        print(f"   Identifier: {record.get('identifier', 'N/A')}")
        print(f"   Page Type: {record.get('page_type', 'N/A')}")
        print(f"   URL: {record.get('url', 'N/A')}")
        print()
        print(f"   Taxonomic Information:")
        print(f"     Order: {record.get('order_name', 'N/A')}")
        print(f"     Family: {record.get('family_name', 'N/A')}")
        print(f"     Genus: {record.get('genus_name', 'N/A')}")
        print(f"     Species: {record.get('species_name', 'N/A')}")
        if record.get('subspecies'):
            print(f"     Subspecies: {record.get('subspecies', 'N/A')}")
        print()
        print(f"   Description Statistics:")
        print(f"     Word Count: {word_count:,}")
        print(f"     Character Count: {char_count:,}")
        print()
        print(f"   Source Information:")
        print(f"     Source Name: {record.get('source_name', 'N/A')}")
        print(f"     Source Name from Description: {record.get('source_name_from_description', 'N/A')}")
        print(f"     Source URL: {record.get('source_url', 'N/A')}")
        print()

        # Print a preview of the description text (first 500 characters)
        descriptions_text = record.get('descriptions_text', '')
        if descriptions_text:
            preview = descriptions_text[:500]
            if len(descriptions_text) > 500:
                preview += "..."
            print(f"   Description Preview (first 500 chars):")
            print(f"     {preview}")

        print("=" * 80)
        print()


def main():
    """Main function."""
    if len(sys.argv) < 2:
        print("Usage: python find_long_descriptions.py <descriptions_jsonl> [target_words] [tolerance]")
        print("Example: python find_long_descriptions.py data/processed/descriptions_text_by_source.jsonl 100000 1000")
        sys.exit(1)

    descriptions_jsonl_path = sys.argv[1]

    target_words = 100000  # 10^5
    tolerance = 1000

    if len(sys.argv) >= 3:
        try:
            target_words = int(sys.argv[2])
        except ValueError:
            print(f"Warning: Invalid target_words '{sys.argv[2]}', using default 100000")

    if len(sys.argv) >= 4:
        try:
            tolerance = int(sys.argv[3])
        except ValueError:
            print(f"Warning: Invalid tolerance '{sys.argv[3]}', using default 1000")

    # Find matching descriptions
    matching_records = find_long_descriptions(descriptions_jsonl_path, target_words, tolerance)

    # Print results
    print_matching_species(matching_records)


if __name__ == "__main__":
    main()
