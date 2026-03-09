"""
Filter a JSONL file by field values and write matching rows to a new JSONL.

Example:
  python filter_jsonl.py input.jsonl output.jsonl --filter page_type=species
  python filter_jsonl.py input.jsonl output.jsonl -f page_type=species -f source_name=WFO
"""

import argparse
import json
import sys
from pathlib import Path


def parse_filter(s):
    """Parse a 'field=value' string into (field, value). Value is kept as string."""
    if "=" not in s:
        raise ValueError(f"Filter must be 'field=value', got: {s!r}")
    field, _, value = s.partition("=")
    field = field.strip()
    value = value.strip()
    if not field:
        raise ValueError(f"Empty field in filter: {s!r}")
    return field, value


def row_matches(record, filters):
    """Return True if record has all (field, value) pairs; values compared as strings."""
    for field, want in filters:
        actual = record.get(field)
        if actual is None:
            return False
        if str(actual).strip() != want:
            return False
    return True


def filter_jsonl(input_path, output_path, filters, verbose=True):
    """
    Read JSONL from input_path, keep only rows matching all filters, write to output_path.

    Returns (total_read, total_written).
    """
    input_path = Path(input_path)
    output_path = Path(output_path)

    if not input_path.exists():
        print(f"Error: File not found: {input_path}", file=sys.stderr)
        return 0, 0

    output_path.parent.mkdir(parents=True, exist_ok=True)
    total_read = 0
    total_written = 0

    with open(input_path, "r", encoding="utf-8") as fin, open(
        output_path, "w", encoding="utf-8"
    ) as fout:
        for line_num, line in enumerate(fin, 1):
            line = line.strip()
            if not line:
                continue
            total_read += 1
            try:
                record = json.loads(line)
            except json.JSONDecodeError as e:
                if verbose:
                    print(f"Warning: Line {line_num}: {e}", file=sys.stderr)
                continue
            if row_matches(record, filters):
                fout.write(line + "\n")
                total_written += 1
            if verbose and line_num % 50000 == 0:
                print(f"  Processed {line_num:,} lines, written {total_written:,}...", end="\r")
    if verbose:
        print()
    return total_read, total_written


def main():
    parser = argparse.ArgumentParser(
        description="Filter JSONL by field=value; only matching rows are written to the output file."
    )
    parser.add_argument("input", type=Path, help="Input JSONL file")
    parser.add_argument("output", type=Path, help="Output JSONL file")
    parser.add_argument(
        "-f",
        "--filter",
        dest="filters",
        action="append",
        required=True,
        metavar="FIELD=VALUE",
        help="Keep only rows where FIELD equals VALUE (e.g. page_type=species). Can be repeated for AND.",
    )
    parser.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="Suppress progress and summary",
    )
    args = parser.parse_args()

    try:
        parsed = [parse_filter(s) for s in args.filters]
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    if not args.quiet:
        print(f"Filters: {parsed}")
        print(f"Input:  {args.input}")
        print(f"Output: {args.output}")

    total_read, total_written = filter_jsonl(
        args.input, args.output, parsed, verbose=not args.quiet
    )

    if not args.quiet:
        print(f"Read {total_read:,} lines, wrote {total_written:,} lines.")


if __name__ == "__main__":
    main()
