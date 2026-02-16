"""
Script to search for a keyword in JSONL file and print surrounding characters and URL.
"""

import json
import sys
import re
from pathlib import Path


def search_keyword_in_jsonl(jsonl_path, keyword, context_chars=20, case_sensitive=False):
    """
    Search for keyword in JSONL file and print context around matches with URLs.

    Args:
        jsonl_path: Path to JSONL file
        keyword: Keyword to search for
        context_chars: Number of characters before and after to show
        case_sensitive: Whether search should be case-sensitive
    """
    jsonl_path = Path(jsonl_path)

    if not jsonl_path.exists():
        print(f"Error: File not found: {jsonl_path}")
        return

    # Compile regex pattern
    flags = 0 if case_sensitive else re.IGNORECASE
    pattern = re.compile(re.escape(keyword), flags)

    total_matches = 0
    lines_with_matches = 0

    print(f"Searching for '{keyword}' in {jsonl_path}...")
    print(f"Context: {context_chars} characters before and after")
    print("=" * 80)

    with open(jsonl_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            if not line.strip():
                continue

            try:
                record = json.loads(line)

                # Only search species records
                page_type = record.get('page_type')
                if page_type != 'species':
                    continue

                # Search in descriptions_text field (or all text fields if not found)
                search_text = record.get('descriptions_text', '')
                if not search_text:
                    # Try other text fields
                    search_text = record.get('raw_text', '') or record.get('raw_description_html', '')

                if search_text:
                    matches = list(pattern.finditer(search_text))

                    if matches:
                        lines_with_matches += 1
                        total_matches += len(matches)

                        # Get URL information
                        url = record.get('url', 'N/A')

                        # Print each match with context
                        for match in matches:
                            start = match.start()
                            end = match.end()

                            # Get context before and after
                            context_start = max(0, start - context_chars)
                            context_end = min(len(search_text), end + context_chars)

                            before = search_text[context_start:start]
                            match_text = search_text[start:end]
                            after = search_text[end:context_end]

                            # Show just URL and match
                            print(f"{url} ...{before}[{match_text}]{after}...")

                # Progress indicator
                if line_num % 10000 == 0:
                    print(f"  Processed {line_num:,} lines, found {total_matches:,} matches in {lines_with_matches:,} records...", end='\r')

            except json.JSONDecodeError as e:
                print(f"\nWarning: Error parsing line {line_num}: {e}")
                continue
            except Exception as e:
                print(f"\nWarning: Error processing line {line_num}: {e}")
                continue

    print()  # New line after progress indicator

    # Print summary
    print("=" * 80)
    print("SEARCH SUMMARY")
    print("=" * 80)
    print(f"Keyword: '{keyword}'")
    print(f"Total lines processed: {line_num:,}")
    print(f"Lines with matches: {lines_with_matches:,}")
    print(f"Total matches: {total_matches:,}")
    print("=" * 80)


def main():
    """Main function."""
    if len(sys.argv) < 3:
        print("Usage: python search_jsonl.py <jsonl_file> <keyword> [context_chars] [--case-sensitive]")
        print("Example: python search_jsonl.py data/processed/descriptions_text_by_source.jsonl wingless 20")
        print("Example: python search_jsonl.py data/processed/descriptions_text_by_source.jsonl wingless 20 --case-sensitive")
        sys.exit(1)

    jsonl_path = sys.argv[1]
    keyword = sys.argv[2]

    context_chars = 20
    case_sensitive = False

    # Parse optional arguments
    for arg in sys.argv[3:]:
        if arg == '--case-sensitive':
            case_sensitive = True
        elif arg.isdigit():
            context_chars = int(arg)

    search_keyword_in_jsonl(jsonl_path, keyword, context_chars, case_sensitive)


if __name__ == "__main__":
    main()
