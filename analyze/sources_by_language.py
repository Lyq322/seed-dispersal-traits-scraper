"""
Print sources and occurrence counts per language from a descriptions JSONL.

Reads records with a "tags" field; language tags are of the form lang_<code>
(e.g. lang_en, lang_de). Source is taken from the "source_name" field.
"""

import argparse
import json
from collections import defaultdict
from pathlib import Path


LANG_TAG_PREFIX = "lang_"


def main():
    parser = argparse.ArgumentParser(
        description="Print sources and counts per language from a descriptions JSONL (with 'tags' field)."
    )
    parser.add_argument(
        "input_jsonl",
        nargs="?",
        default=None,
        help="Path to descriptions JSONL (default: data/processed/descriptions_text_by_source.jsonl)",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parent.parent
    data_dir = repo_root / "data" / "processed"
    input_path = Path(args.input_jsonl) if args.input_jsonl else data_dir / "descriptions_text_by_source.jsonl"

    if not input_path.exists():
        print(f"Error: File not found: {input_path}")
        return 1

    # lang -> source -> count
    by_lang = defaultdict(lambda: defaultdict(int))

    with open(input_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as e:
                print(f"Warning: Skipping invalid JSON: {e}")
                continue
            if not isinstance(obj, dict):
                continue
            source = (obj.get("source_name") or "").strip() or "unknown"
            tags = obj.get("tags") or []
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
