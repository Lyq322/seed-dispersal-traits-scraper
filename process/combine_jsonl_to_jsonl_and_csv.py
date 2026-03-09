"""
Combine two JSONL files into a single JSONL and a CSV.

Reads two JSONL files, deduplicates by the 'identifier' field (later file wins),
writes the combined records to an output JSONL and to a CSV.
"""

import argparse
import csv
import json
import sys
from pathlib import Path
from collections import OrderedDict


def combine_jsonl_files(file1_path, file2_path, output_jsonl_path, output_csv_path):
    """
    Combine two JSONL files, deduplicate by identifier, write JSONL and CSV.

    Returns True on success.
    """
    file1_path = Path(file1_path)
    file2_path = Path(file2_path)
    output_jsonl_path = Path(output_jsonl_path)
    output_csv_path = Path(output_csv_path)

    if not file1_path.exists():
        print(f"Error: File not found: {file1_path}", file=sys.stderr)
        return False
    if not file2_path.exists():
        print(f"Error: File not found: {file2_path}", file=sys.stderr)
        return False

    records_by_id = OrderedDict()
    total_read = 0
    duplicates_found = 0

    for path in (file1_path, file2_path):
        print(f"Reading {path}...")
        with open(path, "r", encoding="utf-8") as f:
            for line_num, line in enumerate(f, 1):
                if not line.strip():
                    continue
                try:
                    record = json.loads(line)
                    total_read += 1
                    identifier = record.get("identifier")
                    if identifier is None:
                        print(f"Warning: Line {line_num} in {path} has no 'identifier', skipping", file=sys.stderr)
                        continue
                    if identifier in records_by_id:
                        duplicates_found += 1
                    records_by_id[identifier] = record
                    if line_num % 10000 == 0:
                        print(f"  Processed {line_num:,} lines...", end="\r")
                except json.JSONDecodeError as e:
                    print(f"Warning: Line {line_num} in {path}: {e}", file=sys.stderr)
        print()

    records = list(records_by_id.values())
    n = len(records)

    # CSV columns: all keys from records, in stable order
    all_keys = []
    seen = set()
    for rec in records:
        for k in rec:
            if k not in seen:
                seen.add(k)
                all_keys.append(k)
    # Prefer a consistent column order when present
    preferred = ["identifier", "order_name", "family_name", "genus_name", "species_name", "subspecies", "source", "page_type", "url", "timestamp"]
    ordered = [k for k in preferred if k in all_keys]
    ordered += [k for k in all_keys if k not in preferred]

    output_jsonl_path.parent.mkdir(parents=True, exist_ok=True)
    output_csv_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Writing {output_jsonl_path}...")
    with open(output_jsonl_path, "w", encoding="utf-8") as f:
        for i, record in enumerate(records):
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
            if (i + 1) % 10000 == 0:
                print(f"  Written {i + 1:,} records...", end="\r")
    print()

    print(f"Writing {output_csv_path}...")
    with open(output_csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=ordered, extrasaction="ignore")
        writer.writeheader()
        for i, record in enumerate(records):
            row = {k: record.get(k) for k in ordered}
            writer.writerow(row)
            if (i + 1) % 10000 == 0:
                print(f"  Written {i + 1:,} rows...", end="\r")
    print()

    print("-" * 60)
    print("COMBINE STATISTICS")
    print("-" * 60)
    print(f"Total lines read: {total_read:,}")
    print(f"Unique records: {n:,}")
    print(f"Duplicates removed: {duplicates_found:,}")
    print(f"Output JSONL: {output_jsonl_path}")
    print(f"Output CSV:   {output_csv_path}")
    print("-" * 60)
    return True


def main():
    parser = argparse.ArgumentParser(
        description="Combine two JSONL files into one JSONL and one CSV (dedupe by identifier).",
        epilog="Example: python combine_jsonl_to_jsonl_and_csv.py data/raw/a.jsonl data/raw/b.jsonl -j data/raw/combined.jsonl -c data/raw/combined.csv",
    )
    parser.add_argument("file1", type=Path, help="First JSONL file")
    parser.add_argument("file2", type=Path, help="Second JSONL file")
    parser.add_argument(
        "--jsonl",
        "-j",
        type=Path,
        default=None,
        metavar="PATH",
        help="Output combined JSONL path (default: combined.jsonl in current dir)",
    )
    parser.add_argument(
        "--csv",
        "-c",
        type=Path,
        default=None,
        metavar="PATH",
        help="Output CSV path (default: combined.csv in current dir)",
    )
    args = parser.parse_args()

    out_jsonl = args.jsonl
    out_csv = args.csv
    if out_jsonl is None and out_csv is None:
        out_jsonl = Path("combined.jsonl")
        out_csv = Path("combined.csv")
    if out_jsonl is None:
        out_jsonl = out_csv.with_suffix(".jsonl")
    if out_csv is None:
        out_csv = out_jsonl.with_suffix(".csv")

    success = combine_jsonl_files(args.file1, args.file2, out_jsonl, out_csv)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
