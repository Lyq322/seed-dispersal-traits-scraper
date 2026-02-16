"""
Script to randomly select x number of txt files from data/flora_traiter
and copy them to data/flora_traiter_test.

Supports optional --tag to filter by identifier using data/processed/tags.jsonl (e.g. has_seed).
Each line is JSON with "identifier" and "tags" (list); only txt files whose identifier has the tag are considered.
"""

import argparse
import json
import random
import shutil
import sys
from pathlib import Path


def _identifier_from_txt_path(path):
    """Extract identifier from a txt filename (e.g. wfo-7000000004_Flora_of_China_....txt -> wfo-7000000004)."""
    stem = path.stem
    if "_" not in stem:
        return stem
    return stem.split("_")[0]


def load_tagged_identifiers(tags_jsonl_path, tag):
    """
    Load identifiers that have the given tag in at least one row.
    tags_jsonl must have one JSON object per line: {"identifier": "...", "tags": [...]}.
    """
    allowed = set()
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
                        allowed.add(ident)
            except json.JSONDecodeError:
                continue
    return allowed


def random_select_txt_files(source_dir, dest_dir, num_files, tag=None, tags_jsonl=None):
    """
    Randomly select num_files txt files from source_dir and copy to dest_dir.

    Args:
        source_dir: Path to source directory (e.g. data/flora_traiter)
        dest_dir: Path to destination directory (e.g. data/flora_traiter_test)
        num_files: Number of files to randomly select
        tag: Optional tag to filter by (e.g. "has_seed"); only txt files whose
             identifier has this tag in tags_jsonl are considered.
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

    if tag and tags_jsonl:
        allowed_identifiers = load_tagged_identifiers(tags_jsonl, tag)
        if allowed_identifiers is None:
            print(f"Error: Tags file not found: {tags_jsonl}")
            return False
        # Keep only txt files whose identifier (from filename) is in allowed set
        txt_files = [f for f in txt_files if _identifier_from_txt_path(f) in allowed_identifiers]
        if not txt_files:
            print(f"Error: No .txt files found with tag '{tag}' in {tags_jsonl}")
            return False
        print(f"Filtered to {len(txt_files)} files with tag '{tag}'")

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
        default=None,
        metavar="TAG",
        help="Only consider txt files whose identifier has this tag (e.g. has_seed); uses data/processed/tags.jsonl",
    )
    args = parser.parse_args()

    if args.num_files < 1:
        print("Error: num_files must be at least 1")
        sys.exit(1)

    print(f"Selecting {args.num_files} random txt files from {args.source_dir}")
    print(f"Destination: {args.dest_dir}")
    if args.tag:
        print(f"Tag filter: {args.tag}")
    print("-" * 60)

    tags_jsonl = "data/processed/tags.jsonl" if args.tag else None
    success = random_select_txt_files(
        args.source_dir,
        args.dest_dir,
        args.num_files,
        tag=args.tag,
        tags_jsonl=tags_jsonl,
    )

    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()
