"""
Stream through a descriptions JSONL, detect language from descriptions_text,
and add/update language tags in each record's "tags" field. Writes the
updated records back (in place or to a file via -o).

On each run (including re-runs): first remove all language-related tags
(tags starting with "lang_") from each row, then append the updated
lang_<code> (e.g. lang_en, lang_zh). So re-runs stay consistent.
"""

import argparse
import json
import sys
from pathlib import Path

try:
    from langdetect import detect, LangDetectException
    HAS_LANGDETECT = True
except ImportError:
    HAS_LANGDETECT = False

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False

# Tag prefix for language. On re-run we delete any tag with this prefix, then append the new one.
LANG_TAG_PREFIX = "lang_"

# Overwrite rules: for rows labeled WRONG_LANG, set to CORRECT_LANG.
# Each tuple is (wrong_lang, source_name_or_none, correct_lang). If source_name is
# None or omitted (use 2-tuple (wrong_lang, correct_lang)), the rule applies to
# all sources with that wrong language. Edit this list as needed.
OVERWRITE_RULES = [
    # ("en", "Plants of Nepal", "ne"),
    ("ca", "la"),
    ("id", "la"),
    ("it", "la"),
]


def detect_language(text):
    """
    Detect language of text. Returns ISO 639-1 code or "unknown".
    Uses first 15000 chars to avoid timeouts on huge texts.
    """
    if not text or not isinstance(text, str):
        return "unknown"
    sample = text.strip()[:15000]
    if len(sample) < 20:
        return "unknown"
    try:
        if HAS_LANGDETECT:
            return detect(sample)
        return "unknown"
    except LangDetectException:
        return "unknown"
    except Exception:
        return "unknown"


def strip_language_tags(tags):
    """Return a copy of tags with all language-related tags (lang_*) removed."""
    if not tags:
        return []
    return [t for t in tags if not (isinstance(t, str) and t.startswith(LANG_TAG_PREFIX))]


def merge_language_tag(existing_tags, lang_code):
    """First delete all language-related tags (lang_*), then append the updated lang_<code>."""
    tags = strip_language_tags(existing_tags or [])
    code = (lang_code or "unknown").lower() if isinstance(lang_code, str) else "unknown"
    tags.append(f"{LANG_TAG_PREFIX}{code}")
    return tags


def _progress_iter(sequence, desc, total, use_tqdm):
    """Wrap sequence with tqdm if use_tqdm else simple progress every 10k rows."""
    if use_tqdm:
        return tqdm(sequence, desc=desc, unit=" rows", total=total)
    return sequence


def main():
    parser = argparse.ArgumentParser(
        description="Add language tags to each record's 'tags' field in a descriptions JSONL."
    )
    parser.add_argument(
        "input_jsonl",
        nargs="?",
        default=None,
        help="Path to descriptions JSONL (default: data/processed/descriptions_text_by_source.jsonl)",
    )
    parser.add_argument(
        "-o", "--output",
        type=str,
        default=None,
        help="Output path (default: overwrite input)",
    )
    parser.add_argument(
        "-n", "--dry-run",
        action="store_true",
        help="Only print language stats, do not write output",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Print each row identifier and detected language",
    )
    parser.add_argument(
        "--no-progress",
        action="store_true",
        help="Disable progress bar (e.g. for piping)",
    )
    args = parser.parse_args()
    show_progress = HAS_TQDM and not args.no_progress

    repo_root = Path(__file__).resolve().parent.parent
    data_dir = repo_root / "data" / "processed"
    input_path = Path(args.input_jsonl) if args.input_jsonl else data_dir / "descriptions_text_by_source.jsonl"
    input_path = input_path.resolve()
    out_path = Path(args.output).resolve() if args.output else input_path

    if not input_path.exists():
        print(f"Input file not found: {input_path}", file=sys.stderr)
        sys.exit(1)
    if not HAS_LANGDETECT:
        print("Warning: langdetect not installed. Install with: pip install langdetect", file=sys.stderr)
        print("All rows will get lang_unknown.", file=sys.stderr)

    # Normalize overwrite rules
    def normalize_rule(r):
        if len(r) == 2:
            return (r[0], None, r[1])
        return (r[0], r[1] or None, r[2])

    rules = [normalize_rule(r) for r in OVERWRITE_RULES] if OVERWRITE_RULES else []
    overwritten_by_rule = {r: 0 for r in rules}

    with open(input_path, "r", encoding="utf-8") as f:
        lines = [ln for ln in f if ln.strip()]
    n = len(lines)
    by_lang = {}
    records_out = []
    last_pct = -1

    for i, line in enumerate(_progress_iter(lines, "Detect language & merge tags", n, show_progress)):
        line = line.strip()
        if not line:
            continue
        if show_progress and not HAS_TQDM and n > 0:
            pct = (100 * (i + 1)) // n
            if pct >= last_pct + 10:
                print(f"  {i + 1}/{n} ({pct}%)", file=sys.stderr)
                last_pct = pct
        try:
            record = json.loads(line)
        except json.JSONDecodeError as e:
            print(f"Invalid JSON at line {i + 1}: {e}", file=sys.stderr)
            records_out.append({})
            continue
        if not isinstance(record, dict):
            records_out.append({"tags": []})
            continue

        source_name = (record.get("source_name") or "").strip() or "unknown"
        text = record.get("descriptions_text") or record.get("raw_description_html") or ""
        lang_code = detect_language(text)

        # Apply overwrite rules
        for wrong, source, correct in rules:
            if (lang_code or "").lower() != wrong:
                continue
            if source is None or source == "":
                match = True
            else:
                match = source_name == source
            if match:
                lang_code = correct
                overwritten_by_rule[(wrong, source, correct)] += 1
                break

        by_lang[lang_code] = by_lang.get(lang_code, 0) + 1
        existing = record.get("tags")
        if existing is None:
            existing = []
        elif not isinstance(existing, list):
            existing = []
        merged = merge_language_tag(existing, lang_code)
        record["tags"] = merged
        records_out.append(record)

        if args.verbose:
            row_id = (record.get("identifier") or "") + "_" + source_name
            print(f"  {row_id} -> {lang_code}", file=sys.stderr)

    total = len(records_out)
    if not show_progress:
        for (wrong, source, correct), count in overwritten_by_rule.items():
            if count:
                scope = "all sources" if source is None else f"source {source!r}"
                print(f"  Overwrite: {count} rows ({scope}) {wrong} -> {correct}.", file=sys.stderr)
        print(f"  {total} rows.", file=sys.stderr)

    if not args.dry_run:
        use_temp = out_path.resolve() == input_path.resolve()
        if use_temp:
            write_path = input_path.with_suffix(".jsonl.tmp")
        else:
            write_path = out_path
        with open(write_path, "w", encoding="utf-8") as f:
            for record in records_out:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
        if use_temp and write_path.exists():
            write_path.replace(input_path)

    print("By language:", file=sys.stderr)
    for lang in sorted(by_lang.keys()):
        count = by_lang[lang]
        pct = 100.0 * count / total if total else 0
        print(f"  {lang:12}  {count:6}  ({pct:5.1f}%)", file=sys.stderr)
    if not args.dry_run:
        print(f"Output: {out_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
