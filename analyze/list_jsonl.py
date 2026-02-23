#!/usr/bin/env python3
"""
List the first N lines and/or selected fields from a JSONL file.
Supports an optional regex filter on any or all fields, and optional sort.
"""

import argparse
import json
import re
import sys
from pathlib import Path


DEFAULT_JSONL = Path(__file__).resolve().parent.parent / "data" / "processed" / "descriptions_text_by_source.jsonl"


def _string_value(val):
    if val is None:
        return ""
    if isinstance(val, (str, int, float, bool)):
        return str(val)
    return json.dumps(val, ensure_ascii=False)


def _one_filter_match(obj, pattern, filter_fields):
    """Return True if pattern matches at least one of the given fields in obj."""
    if filter_fields is None:
        for k, v in obj.items():
            if pattern.search(_string_value(v)):
                return True
        return False
    for key in filter_fields:
        if key in obj and pattern.search(_string_value(obj[key])):
            return True
    return False


def _parse_filter_spec(spec):
    """Parse 'FIELD:REGEX' -> (field_name, pattern) or 'REGEX' -> (None, pattern)."""
    spec = spec.strip()
    if ":" in spec:
        idx = spec.index(":")
        left, right = spec[:idx].strip(), spec[idx + 1 :].strip()
        if left and re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", left):
            return (left, re.compile(right))
    return (None, re.compile(spec))


def _matches_filters(obj, filter_specs, default_filter_fields):
    """Return True iff obj passes every (field_or_none, pattern). All filters are ANDed."""
    for field, pattern in filter_specs:
        if field is not None:
            if field not in obj or not pattern.search(_string_value(obj[field])):
                return False
        else:
            if not _one_filter_match(obj, pattern, default_filter_fields):
                return False
    return True


def _slice_obj(obj, fields, max_fields):
    """Return a subset of obj: only `fields` if set, then optionally first max_fields keys."""
    if fields is not None:
        out = {k: obj[k] for k in fields if k in obj}
    else:
        out = dict(obj)
    if max_fields is not None:
        keys = list(out.keys())[: max_fields]
        out = {k: out[k] for k in keys}
    return out


def _sort_key(obj, sort_fields):
    """Return a tuple of string values for stable comparison (None -> "")."""
    return tuple(_string_value(obj.get(f)) for f in sort_fields)


def main():
    parser = argparse.ArgumentParser(
        description="List first N lines and/or selected fields from a JSONL file, with optional regex filter."
    )
    parser.add_argument(
        "jsonl_file",
        nargs="?",
        default=None,
        help="Path to JSONL file (default: data/processed/descriptions_text_by_source.jsonl)",
    )
    parser.add_argument(
        "-n",
        "--lines",
        type=int,
        default=10,
        help="Maximum number of records (lines) to output (default: 10). Use 0 for no limit.",
    )
    parser.add_argument(
        "-f",
        "--fields",
        type=str,
        default=None,
        metavar="FIELD1,FIELD2,...",
        help="Comma-separated field names to include (default: all fields).",
    )
    parser.add_argument(
        "--max-fields",
        type=int,
        default=None,
        metavar="N",
        help="Include only the first N keys per object (after --fields).",
    )
    parser.add_argument(
        "--filter",
        type=str,
        action="append",
        default=None,
        metavar="SPEC",
        help="Include only lines matching SPEC. May be repeated (AND). SPEC = REGEX or FIELD:REGEX.",
    )
    parser.add_argument(
        "--filter-fields",
        type=str,
        default=None,
        metavar="FIELD1,FIELD2,...",
        help="Default fields for bare REGEX in --filter (default: all fields). Ignored for FIELD:REGEX.",
    )
    parser.add_argument(
        "--compact",
        action="store_true",
        help="Output one compact JSON per line instead of pretty-printed.",
    )
    parser.add_argument(
        "-s",
        "--sort",
        type=str,
        default=None,
        metavar="FIELD1,FIELD2,...",
        help="Sort by these fields (primary, secondary, ...). Values are compared as strings.",
    )
    parser.add_argument(
        "-r",
        "--reverse",
        action="store_true",
        help="Reverse sort order.",
    )
    args = parser.parse_args()

    jsonl_path = Path(args.jsonl_file) if args.jsonl_file else DEFAULT_JSONL
    if not jsonl_path.is_absolute():
        jsonl_path = (Path(__file__).resolve().parent.parent / jsonl_path).resolve()
    if not jsonl_path.exists():
        print(f"File not found: {jsonl_path}", file=sys.stderr)
        sys.exit(1)

    fields = [s.strip() for s in args.fields.split(",")] if args.fields else None
    filter_fields = [s.strip() for s in args.filter_fields.split(",")] if args.filter_fields else None
    filter_specs = [_parse_filter_spec(s) for s in args.filter] if args.filter else None
    sort_fields = [s.strip() for s in args.sort.split(",")] if args.sort else None

    max_lines = None if args.lines == 0 else args.lines

    if sort_fields is not None:
        # Collect all matching records, sort, then output up to max_lines
        collected = []
        with open(jsonl_path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError as e:
                    print(f"Skip invalid JSON: {e}", file=sys.stderr)
                    continue
                if filter_specs is not None and not _matches_filters(obj, filter_specs, filter_fields):
                    continue
                collected.append(obj)
        collected.sort(key=lambda o: _sort_key(o, sort_fields), reverse=args.reverse)
        for obj in collected[:max_lines] if max_lines is not None else collected:
            out = _slice_obj(obj, fields, args.max_fields)
            if args.compact:
                print(json.dumps(out, ensure_ascii=False))
            else:
                print(json.dumps(out, indent=2, ensure_ascii=False))
                print()
        if not collected and filter_specs is not None:
            print("No records matched the filter.", file=sys.stderr)
            sys.exit(1)
    else:
        # Stream: filter, slice, output up to max_lines
        count = 0
        with open(jsonl_path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                if max_lines is not None and count >= max_lines:
                    break
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError as e:
                    print(f"Skip invalid JSON: {e}", file=sys.stderr)
                    continue
                if filter_specs is not None and not _matches_filters(obj, filter_specs, filter_fields):
                    continue
                out = _slice_obj(obj, fields, args.max_fields)
                if args.compact:
                    print(json.dumps(out, ensure_ascii=False))
                else:
                    print(json.dumps(out, indent=2, ensure_ascii=False))
                    print()
                count += 1
        if count == 0 and filter_specs is not None:
            print("No records matched the filter.", file=sys.stderr)
            sys.exit(1)


if __name__ == "__main__":
    main()
