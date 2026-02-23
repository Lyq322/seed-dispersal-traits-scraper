"""
Print sources and occurrence counts per language from data/processed/tags.jsonl.

Each line in tags.jsonl has: {"identifier": "<wfo_id>_<source_name>", "tags": [...]}.
Language tags are of the form lang_<code> (e.g. lang_en, lang_de).
"""

import argparse
import json
from collections import defaultdict
from pathlib import Path


LANG_TAG_PREFIX = "lang_"


def extract_source(identifier):
    """Extract source name from identifier (part after the first '_')."""
    if not identifier or not isinstance(identifier, str):
        return "unknown"
    parts = identifier.split("_", 1)
    return parts[1] if len(parts) > 1 else identifier


def main():
    parser = argparse.ArgumentParser(
        description="Print sources and counts per language from tags.jsonl."
    )
    parser.add_argument(
        "tags_jsonl",
        nargs="?",
        default=None,
        help="Path to tags JSONL (default: data/processed/tags.jsonl)",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parent.parent
    data_dir = repo_root / "data" / "processed"
    tags_path = Path(args.tags_jsonl) if args.tags_jsonl else data_dir / "tags.jsonl"

    if not tags_path.exists():
        print(f"Error: File not found: {tags_path}")
        return 1

    # lang -> source -> count
    by_lang = defaultdict(lambda: defaultdict(int))

    with open(tags_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as e:
                print(f"Warning: Skipping invalid JSON: {e}")
                continue
            identifier = obj.get("identifier")
            tags = obj.get("tags") or []
            source = extract_source(identifier)
            for tag in tags:
                if isinstance(tag, str) and tag.startswith(LANG_TAG_PREFIX):
                    lang = tag[len(LANG_TAG_PREFIX) :]
                    by_lang[lang][source] += 1
                    break  # at most one lang tag per row

    grand_total = sum(sum(sources.values()) for sources in by_lang.values())

    # Sort languages for stable output
    for lang in sorted(by_lang.keys()):
        sources = by_lang[lang]
        total = sum(sources.values())
        pct = 100.0 * total / grand_total if grand_total else 0
        print(f"\n=== {lang} (total: {total:,}, {pct:.1f}%) ===")
        # Sort sources by count descending, then by name
        for source, count in sorted(sources.items(), key=lambda x: (-x[1], x[0])):
            pct_source = 100.0 * count / total if total else 0
            print(f"  {count:>8,}  ({pct_source:5.1f}%)  {source}")

    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
