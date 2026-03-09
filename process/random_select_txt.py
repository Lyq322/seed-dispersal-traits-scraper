"""
Script to randomly select x number of txt files from one or more source directories
and copy them to a destination (optionally distributed across y batch folders).

Source can be multiple directories; .txt files are collected from each and all
subfolders (e.g. result/flora_traiter, data/flora_traiter). Use --batches y to
put the x selected files into y batch subfolders (batch_0000, batch_0001, ...)
under the destination.

Supports optional --tag (one or more) to filter by the "tags" field in a descriptions JSONL
(e.g. --tag has_seed --tag lang_es); identifier must have ALL of the tags.
Uses a descriptions JSONL where each record has "identifier", "source_name", and "tags".

Row identity is identifier + "_" + source_name. Txt filenames use a sanitized prefix before
_NNNNNN: same id and source with spaces/special chars replaced by underscores. Matching
uses that sanitized prefix so tag filters work correctly.
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
    Convert composite identifier (identifier_source_name) to the same prefix form used in txt filenames.
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


def load_tagged_identifiers(descriptions_jsonl_path, tag):
    """
    Load the set of txt-filename prefixes that have the given tag.
    Reads a descriptions JSONL; each record has identifier, source_name, and tags.
    We build composite id = identifier + '_' + source_name and convert to the
    same sanitized prefix form used in txt filenames so matching works.
    """
    allowed_prefixes = set()
    path = Path(descriptions_jsonl_path)
    if not path.exists():
        return None
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                if not isinstance(obj, dict):
                    continue
                tags = obj.get("tags") or []
                if tag not in tags:
                    continue
                ident = (obj.get("identifier") or "").strip()
                source_name = (obj.get("source_name") or "").strip() or "unknown"
                composite_id = f"{ident}_{source_name}" if ident else source_name
                allowed_prefixes.add(_allowed_prefix_from_tags_identifier(composite_id))
            except json.JSONDecodeError:
                continue
    return allowed_prefixes


def random_select_txt_files(
    source_dirs,
    dest_dir,
    num_files,
    num_batches=None,
    tags=None,
    tags_jsonl=None,
):
    """
    Randomly select num_files txt files from one or more source dirs and copy to dest_dir.

    Args:
        source_dirs: List of paths to source directories (e.g. [data/flora_traiter, result/flora_traiter]).
                     .txt files are collected from each and all subfolders (rglob).
        dest_dir: Path to destination directory (e.g. data/flora_traiter_test).
        num_files: Number of files to randomly select.
        num_batches: If set, create this many batch subfolders (batch_0000, batch_0001, ...)
                     under dest_dir and distribute selected files across them (round-robin).
        tags: Optional list of tags to filter by (e.g. ["has_seed", "has_fruit"]);
              only txt files whose row has ALL of these tags in the descriptions JSONL are considered.
        tags_jsonl: Path to descriptions JSONL with "identifier", "source_name", and "tags" per record.

    Returns:
        True if successful
    """
    source_dirs = [Path(d) for d in source_dirs]
    dest_dir = Path(dest_dir)

    for d in source_dirs:
        if not d.exists():
            print(f"Error: Source directory not found: {d}")
            return False

    # Collect .txt from all source dirs and subfolders; use set of resolved paths to dedupe
    seen = set()
    txt_files = []
    for source_dir in source_dirs:
        for f in source_dir.rglob("*.txt"):
            if f.is_file():
                key = f.resolve()
                if key not in seen:
                    seen.add(key)
                    txt_files.append(f)

    if not txt_files:
        print(f"Error: No .txt files found under {[str(d) for d in source_dirs]}")
        return False

    if tags and tags_jsonl:
        allowed_sets = []
        for tag in tags:
            ids = load_tagged_identifiers(tags_jsonl, tag)
            if ids is None:
                print(f"Error: Descriptions JSONL not found: {tags_jsonl}")
                return False
            allowed_sets.append(ids)
        # Identifier must have ALL tags: intersection of the sets
        allowed_identifiers = set.intersection(*allowed_sets) if allowed_sets else set()
        # Keep only txt files whose filename prefix matches a tagged row (composite id -> same prefix as filename)
        txt_files = [f for f in txt_files if _txt_stem_prefix(f.stem) in allowed_identifiers]
        if not txt_files:
            print(f"Error: No .txt files found with all tags {tags!r} in descriptions JSONL {tags_jsonl}")
            return False
        print(f"Filtered to {len(txt_files)} files with all tags {tags}")

    if num_files > len(txt_files):
        print(f"Warning: Requested {num_files} files but only {len(txt_files)} available. Selecting all.")
        num_files = len(txt_files)

    # Randomly select files
    selected = random.sample(txt_files, num_files)

    # Create destination directory if it doesn't exist
    dest_dir.mkdir(parents=True, exist_ok=True)

    if num_batches is not None and num_batches > 0:
        # Create batch_0000 .. batch_(num_batches-1); distribute files round-robin
        batch_dirs = []
        for b in range(num_batches):
            batch_dir = dest_dir / f"batch_{b:04d}"
            batch_dir.mkdir(parents=True, exist_ok=True)
            batch_dirs.append(batch_dir)
        used_names = [set() for _ in batch_dirs]

        for i, src in enumerate(selected):
            batch_index = i % num_batches
            batch_dir = batch_dirs[batch_index]
            names_in_batch = used_names[batch_index]
            dest_name = src.name
            if dest_name in names_in_batch:
                stem, suf = src.stem, src.suffix
                c = 1
                while dest_name in names_in_batch:
                    dest_name = f"{stem}_{c}{suf}"
                    c += 1
                names_in_batch.add(dest_name)
            else:
                names_in_batch.add(dest_name)
            dest = batch_dir / dest_name
            shutil.copy2(src, dest)
            print(f"  Copied: {src.name} -> {batch_dir.name}/")
        print()
        print(f"Copied {len(selected)} files into {num_batches} batch folders under {dest_dir}")
    else:
        # Flat: all files in dest_dir; handle name collisions with suffix
        used_names = set()
        for src in selected:
            dest_name = src.name
            if dest_name in used_names:
                stem, suf = src.stem, src.suffix
                c = 1
                while dest_name in used_names:
                    dest_name = f"{stem}_{c}{suf}"
                    c += 1
                used_names.add(dest_name)
            else:
                used_names.add(dest_name)
            dest = dest_dir / dest_name
            shutil.copy2(src, dest)
            print(f"  Copied: {src.name}")
        print()
        print(f"Copied {len(selected)} files to {dest_dir}")
    return True


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description="Randomly select txt files from one or more source directories and copy to a destination (optionally into y batch folders)."
    )
    parser.add_argument(
        "num_files",
        type=int,
        help="Number of txt files to randomly select (x)",
    )
    parser.add_argument(
        "dest_dir",
        nargs="?",
        default="data/flora_traiter_test",
        help="Destination directory (default: data/flora_traiter_test)",
    )
    parser.add_argument(
        "-s",
        "--sources",
        type=str,
        action="append",
        default=None,
        metavar="DIR",
        dest="source_dirs",
        help="Source directory; .txt files are collected from it and all subfolders. Can be repeated for multiple roots (e.g. -s result/flora_traiter -s data/flora_traiter). Default: data/flora_traiter",
    )
    parser.add_argument(
        "-b",
        "--batches",
        type=int,
        default=None,
        metavar="Y",
        dest="num_batches",
        help="Put the x selected files into Y batch subfolders (batch_0000, batch_0001, ...) under the destination",
    )
    parser.add_argument(
        "--tag",
        type=str,
        action="append",
        default=None,
        metavar="TAG",
        dest="tags",
        help="Only consider txt files whose row has ALL of these tags (can be repeated, e.g. --tag has_seed --tag lang_es); uses descriptions JSONL with 'tags' field",
    )
    parser.add_argument(
        "--jsonl",
        type=str,
        default=None,
        metavar="PATH",
        help="Path to descriptions JSONL for tag filtering (default: data/processed/descriptions_text_by_source.jsonl)",
    )
    args = parser.parse_args()

    if args.source_dirs is None or len(args.source_dirs) == 0:
        args.source_dirs = ["data/flora_traiter"]

    if args.num_files < 1:
        print("Error: num_files must be at least 1")
        sys.exit(1)
    if args.num_batches is not None and args.num_batches < 1:
        print("Error: --batches must be at least 1")
        sys.exit(1)

    print(f"Selecting {args.num_files} random txt files from: {args.source_dirs}")
    print(f"Destination: {args.dest_dir}")
    if args.num_batches is not None:
        print(f"Batch folders: {args.num_batches}")
    if args.tags:
        print(f"Tag filter (all of): {args.tags}")
    print("-" * 60)

    tags_jsonl = None
    if args.tags:
        tags_jsonl = args.jsonl or "data/processed/descriptions_text_by_source.jsonl"
    success = random_select_txt_files(
        args.source_dirs,
        args.dest_dir,
        args.num_files,
        num_batches=args.num_batches,
        tags=args.tags,
        tags_jsonl=tags_jsonl,
    )

    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()
