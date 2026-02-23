"""
Script to randomly select x number of txt files from data/flora_traiter
and copy them to data/flora_traiter_test.

Supports optional --tag (one or more) to filter by identifier using data/processed/tags.jsonl (e.g. --tag has_seed has_fruit); identifier must have ALL of the tags.
Each line is JSON with "identifier" and "tags" (list); only txt files whose identifier has the tag are considered.

tags.jsonl uses composite identifiers (identifier_source_name, e.g. wfo-7000000004_Flora of North America @ efloras.org).
Txt filenames use a sanitized prefix before _NNNNNN: same id and source with spaces/special chars replaced by underscores.
Matching uses that sanitized prefix so tag filters work correctly.
"""

import argparse
import json
import random
import re
import shutil
import sys
from pathlib import Path


def _sanitize_filename(text, max_length=100):
    """Match jsonl_to_txt_files sanitization so we can match tags identifiers to txt filename prefixes."""
    if not text or not isinstance(text, str):
        return "unknown"
    s = re.sub(r'[<>:"/\\|?*\s]+', '_', text.strip())
    s = re.sub(r'[^\w\-_\.]', '', s)
    s = re.sub(r'_+', '_', s).strip('_')
    if len(s) > max_length:
        s = s[:max_length]
    return s or "unknown"


def _txt_stem_prefix(stem):
    """
    Return the part of a txt filename stem that corresponds to the composite identifier.
    Stem is like wfo-7000000004_Flora_of_North_America_efloras.org_000001 -> prefix is before _\\d{6}.
    """
    return re.sub(r"_\d{6}$", "", stem) if stem else ""


def _allowed_prefix_from_tags_identifier(composite_id):
    """
    Convert tags.jsonl composite identifier to the same prefix form used in txt filenames.
    composite_id is 'identifier_source_name' (e.g. wfo-7000000004_Flora of North America @ efloras.org).
    Txt files are named sanitize(id)_sanitize(source)_line.txt with 50-char truncation each.
    """
    if not composite_id or "_" not in composite_id:
        return _sanitize_filename(composite_id or "", 50)
    ident, _, source = composite_id.partition("_")
    safe_id = _sanitize_filename(ident, 50)
    safe_source = _sanitize_filename(source, 50)
    if not safe_source or safe_source == "unknown":
        return f"{safe_id}"
    return f"{safe_id}_{safe_source}"


def load_tagged_identifiers(tags_jsonl_path, tag):
    """
    Load the set of txt-filename prefixes that have the given tag.
    tags.jsonl has composite identifiers (id_source_name). We convert each to the
    same sanitized prefix form used in txt filenames so matching works.
    """
    allowed_prefixes = set()
    path = Path(tags_jsonl_path)
    if not path.exists():
        return None
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                tags = obj.get("tags") or []
                if tag in tags:
                    ident = obj.get("identifier")
                    if ident is not None:
                        allowed_prefixes.add(_allowed_prefix_from_tags_identifier(ident))
            except json.JSONDecodeError:
                continue
    return allowed_prefixes


def random_select_txt_files(source_dir, dest_dir, num_files, tags=None, tags_jsonl=None):
    """
    Randomly select num_files txt files from source_dir and copy to dest_dir.

    Args:
        source_dir: Path to source directory (e.g. data/flora_traiter)
        dest_dir: Path to destination directory (e.g. data/flora_traiter_test)
        num_files: Number of files to randomly select
        tags: Optional list of tags to filter by (e.g. ["has_seed", "has_fruit"]);
              only txt files whose identifier has ALL of these tags in tags_jsonl are considered.
        tags_jsonl: Path to JSONL with "identifier" and "tags" per line.

    Returns:
        True if successful
    """
    source_dir = Path(source_dir)
    dest_dir = Path(dest_dir)

    if not source_dir.exists():
        print(f"Error: Source directory not found: {source_dir}")
        return False

    # Get all .txt files (exclude mapping.jsonl and other non-txt)
    txt_files = [f for f in source_dir.glob("*.txt") if f.is_file()]

    if not txt_files:
        print(f"Error: No .txt files found in {source_dir}")
        return False

    if tags and tags_jsonl:
        allowed_sets = []
        for tag in tags:
            ids = load_tagged_identifiers(tags_jsonl, tag)
            if ids is None:
                print(f"Error: Tags file not found: {tags_jsonl}")
                return False
            allowed_sets.append(ids)
        # Identifier must have ALL tags: intersection of the sets
        allowed_identifiers = set.intersection(*allowed_sets) if allowed_sets else set()
        # Keep only txt files whose filename prefix matches a tagged row (composite id -> same prefix as filename)
        txt_files = [f for f in txt_files if _txt_stem_prefix(f.stem) in allowed_identifiers]
        if not txt_files:
            print(f"Error: No .txt files found with all tags {tags!r} in {tags_jsonl}")
            return False
        print(f"Filtered to {len(txt_files)} files with all tags {tags}")

    if num_files > len(txt_files):
        print(f"Warning: Requested {num_files} files but only {len(txt_files)} available. Selecting all.")
        num_files = len(txt_files)

    # Randomly select files
    selected = random.sample(txt_files, num_files)

    # Create destination directory if it doesn't exist
    dest_dir.mkdir(parents=True, exist_ok=True)

    # Copy selected files
    for src in selected:
        dest = dest_dir / src.name
        shutil.copy2(src, dest)
        print(f"  Copied: {src.name}")

    print()
    print(f"Copied {len(selected)} files from {source_dir} to {dest_dir}")
    return True


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description="Randomly select txt files from a directory and copy to a destination."
    )
    parser.add_argument(
        "num_files",
        type=int,
        help="Number of txt files to randomly select",
    )
    parser.add_argument(
        "source_dir",
        nargs="?",
        default="data/flora_traiter",
        help="Source directory (default: data/flora_traiter)",
    )
    parser.add_argument(
        "dest_dir",
        nargs="?",
        default="data/flora_traiter_test",
        help="Destination directory (default: data/flora_traiter_test)",
    )
    parser.add_argument(
        "--tag",
        type=str,
        action="append",
        default=None,
        metavar="TAG",
        dest="tags",
        help="Only consider txt files whose identifier has ALL of these tags (can be repeated, e.g. --tag has_seed --tag lang_es); uses data/processed/tags.jsonl",
    )
    args = parser.parse_args()

    if args.num_files < 1:
        print("Error: num_files must be at least 1")
        sys.exit(1)

    print(f"Selecting {args.num_files} random txt files from {args.source_dir}")
    print(f"Destination: {args.dest_dir}")
    if args.tags:
        print(f"Tag filter (all of): {args.tags}")
    print("-" * 60)

    tags_jsonl = "data/processed/tags.jsonl" if args.tags else None
    success = random_select_txt_files(
        args.source_dir,
        args.dest_dir,
        args.num_files,
        tags=args.tags,
        tags_jsonl=tags_jsonl,
    )

    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()
