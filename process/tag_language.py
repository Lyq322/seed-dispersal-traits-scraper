"""
Stream through descriptions_text_by_source.jsonl, detect language from
descriptions_text, and add language tags to the corresponding rows in
the tags JSONL. Rows are aligned by line order (same row index).

Identifier format: original identifier + "_" + source_name (same as
filter_seedless.py), e.g. "wfo-7000000004_Flora of North America @ efloras.org".

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


def row_identifier(record):
    """Composite row id: identifier + '_' + source_name (same as filter_seedless)."""
    orig_id = (record.get("identifier") or "").strip()
    source_name = (record.get("source_name") or "").strip() or "unknown"
    return f"{orig_id}_{source_name}" if orig_id else source_name


def _progress_iter(sequence, desc, total, use_tqdm):
    """Wrap sequence with tqdm if use_tqdm else simple progress every 10k rows."""
    if use_tqdm:
        return tqdm(sequence, desc=desc, unit=" rows", total=total)
    return sequence


def build_row_languages(descriptions_path, progress=False):
    """
    Read descriptions_text_by_source.jsonl and return an ordered list of
    (row_identifier, lang_code) per row. row_identifier = identifier + "_" + source_name.
    """
    result = []
    with open(descriptions_path, "r", encoding="utf-8") as f:
        lines = [ln for ln in f if ln.strip()]
    n = len(lines)
    last_pct = -1
    for i, line in enumerate(_progress_iter(lines, "Pass 1 (detect language)", n, progress and HAS_TQDM)):
        line = line.strip()
        if not line:
            continue
        if progress and not HAS_TQDM and n > 0:
            pct = (100 * (i + 1)) // n
            if pct >= last_pct + 10:
                print(f"  Pass 1: {i + 1}/{n} ({pct}%)", file=sys.stderr)
                last_pct = pct
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        row_id = row_identifier(record)
        text = record.get("descriptions_text") or record.get("raw_description_html") or ""
        lang_code = detect_language(text)
        result.append((row_id, lang_code))
    return result


def main():
    parser = argparse.ArgumentParser(
        description="Add language tags to tags.jsonl from descriptions_text in descriptions_text_by_source.jsonl."
    )
    parser.add_argument(
        "descriptions_jsonl",
        nargs="?",
        default=None,
        help="Path to descriptions_text_by_source.jsonl (default: data/processed/descriptions_text_by_source.jsonl)",
    )
    parser.add_argument(
        "tags_jsonl",
        nargs="?",
        default=None,
        help="Path to tags JSONL to update (default: data/processed/tags.jsonl)",
    )
    parser.add_argument(
        "-o", "--output",
        type=str,
        default=None,
        help="Output path (default: overwrite tags_jsonl; use this to write to a different file)",
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
    descriptions_path = Path(args.descriptions_jsonl) if args.descriptions_jsonl else data_dir / "descriptions_text_by_source.jsonl"
    tags_path = Path(args.tags_jsonl) if args.tags_jsonl else data_dir / "tags.jsonl"
    out_path = Path(args.output) if args.output else tags_path

    if not descriptions_path.exists():
        print(f"Descriptions file not found: {descriptions_path}", file=sys.stderr)
        sys.exit(1)
    if not tags_path.exists():
        print(f"Tags file not found: {tags_path}", file=sys.stderr)
        sys.exit(1)
    if not HAS_LANGDETECT:
        print("Warning: langdetect not installed. Install with: pip install langdetect", file=sys.stderr)
        print("All rows will get lang_unknown.", file=sys.stderr)

    # Pass 1: ordered list of (row_identifier, lang) per description row
    if not show_progress:
        print("Pass 1: reading descriptions and detecting language...", file=sys.stderr)
    row_languages = build_row_languages(descriptions_path, progress=show_progress)
    total_descriptions = len(row_languages)
    by_lang = {}
    for _, lang in row_languages:
        by_lang[lang] = by_lang.get(lang, 0) + 1
    if not show_progress:
        print(f"  {total_descriptions} description rows.", file=sys.stderr)

    tmp_path = None
    if args.dry_run:
        out_file = None
    else:
        if out_path.resolve() == tags_path.resolve():
            tmp_path = tags_path.with_suffix(".jsonl.tmp")
            out_file = open(tmp_path, "w", encoding="utf-8")
        else:
            out_file = open(out_path, "w", encoding="utf-8")

    try:
        if not show_progress:
            print("Pass 2: reading tags and writing with language tags...", file=sys.stderr)
        row_index = 0
        tag_lines = []
        with open(tags_path, "r", encoding="utf-8") as f_tags:
            for line in f_tags:
                tag_lines.append(line)
        desc_index = 0  # index into row_languages (one per description row)
        n_tags = len(tag_lines)
        it = _progress_iter(tag_lines, "Pass 2 (write tags)", n_tags, show_progress)
        last_pct2 = -1
        for ti, line_tags in enumerate(it):
            if show_progress and not HAS_TQDM and n_tags > 0:
                pct = (100 * (ti + 1)) // n_tags
                if pct >= last_pct2 + 10:
                    print(f"  Pass 2: {ti + 1}/{n_tags} ({pct}%)", file=sys.stderr)
                    last_pct2 = pct
            line_tags = line_tags.strip()
            row_index += 1
            if not line_tags:
                if not args.dry_run and out_file is not None:
                    out_file.write("\n")
                continue
            try:
                tag_obj = json.loads(line_tags)
            except json.JSONDecodeError as e:
                if not show_progress:
                    print(f"Tags line {row_index}: JSON error: {e}", file=sys.stderr)
                if not args.dry_run and out_file is not None:
                    out_file.write(line_tags + "\n")
                continue
            existing = tag_obj.get("tags")
            if existing is None:
                existing = []

            # Match by line order; use composite row_identifier from Pass 1
            if desc_index < len(row_languages):
                row_id, lang_code = row_languages[desc_index]
                desc_index += 1
            else:
                row_id = tag_obj.get("identifier")
                lang_code = "unknown"
            merged = merge_language_tag(existing, lang_code)
            out_obj = {"identifier": row_id, "tags": merged}
            if args.verbose:
                print(f"  {row_id} -> {lang_code}", file=sys.stderr)
            if not args.dry_run and out_file is not None:
                out_file.write(json.dumps(out_obj, ensure_ascii=False) + "\n")

    finally:
        if out_file is not None:
            out_file.close()
            if not args.dry_run and tmp_path is not None and tmp_path.exists():
                tmp_path.replace(tags_path)

    if row_index != total_descriptions and not show_progress:
        print(f"Warning: tags has {row_index} rows, descriptions had {total_descriptions}.", file=sys.stderr)
    if not show_progress:
        print(f"Processed {row_index} tag rows.", file=sys.stderr)
    print("By language (from descriptions):", file=sys.stderr)
    for lang in sorted(by_lang.keys()):
        count = by_lang[lang]
        pct = 100.0 * count / total_descriptions if total_descriptions else 0
        print(f"  {lang:12}  {count:6}  ({pct:5.1f}%)", file=sys.stderr)
    if not args.dry_run and out_path.resolve() == tags_path.resolve():
        print(f"Updated: {tags_path}", file=sys.stderr)
    elif not args.dry_run:
        print(f"Written: {out_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
