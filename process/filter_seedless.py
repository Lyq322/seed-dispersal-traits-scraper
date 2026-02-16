"""
Seedless plant taxa (orders) and script to tag rows in
descriptions_text_by_source.jsonl. Writes a row-tags file (one line per row,
JSON array of tags). This script sets exactly one of "seedless" or "has_seed"
per row (overwriting any previous such tags) and merges with existing tags
from the file; all other tags are preserved.
"""

import json
import sys
from pathlib import Path

# Output: one line per row of descriptions_text_by_source.jsonl; each line is {"identifier": "...", "tags": [...]}.
ROW_TAGS_FILENAME = "descriptions_text_by_source_row_tags.jsonl"

# Tags this script owns; we remove any existing and set exactly one of seedless / has_seed.
SEED_STATUS_TAGS = {"seedless", "has_seed"}

seedless_plant_taxa = {
    "order": [
        # Marchantiophyta (Liverworts)
        "Blasiales",
        "Calobryales",
        "Fossombroniales",
        "Frullaniales",
        "Jubulales",
        "Jungermanniales",
        "Lejeuneales",
        "Lepidolaenales",
        "Lepidoziales",
        "Lophoziales",
        "Lunulariales",
        "Marchantiales",
        "Metzgeriales",
        "Myliales",
        "Neohodgsoniales",
        "Pallaviciniales",
        "Pelliales",
        "Perssoniellales",
        "Porellales",
        "Ptilidiales",
        "Radulales",
        "Rhizogemmales",
        "Sphaerocarpales",
        "Treubiales",

        # Bryophyta (Mosses)
        "Amphidiales",
        "Andreaeales",
        "Andreaeobryales",
        "Archidiales",
        "Aulacomniales",
        "Bartramiales",
        "Bruchiales",
        "Bryales",
        "Bryoxiphiales",
        "Buxbaumiales",
        "Catoscopiales",
        "Dicranales",
        "Diphysciales",
        "Disceliales",
        "Distichiales",
        "Encalyptales",
        "Erpodiales",
        "Eustichiales",
        "Flexitrichales",
        "Funariales",
        "Gigaspermales",
        "Grimmiales",
        "Hedwigiales",
        "Hookeriales",
        "Hypnales",
        "Hypnodendrales",
        "Hypopterygiales",
        "Mitteniales",
        "Oedipodiales",
        "Orthodontiales",
        "Orthotrichales",
        "Pleurophascales",
        "Pleuroziales",
        "Polytrichales",
        "Pottiales",
        "Pseudoditrichales",
        "Ptychomniales",
        "Rhabdoweisiales",
        "Rhizogoniales",
        "Scouleriales",
        "Sorapillales",
        "Sphagnales",
        "Splachnales",
        "Takakiales",
        "Tetraphidales",
        "Timmiales",

        # Anthocerotophyta (Hornworts)
        "Anthocerotales",
        "Dendrocerotales",
        "Leiosporocerotales",
        "Notothyladales",
        "Phymatocerotales",

        # Lycopodiophyta (Lycophytes)
        "Isoetales",
        "Lycopodiales",
        "Selaginellales",

        # Monilophyta (Ferns and allies)
        "Cyatheales",
        "Equisetales",
        "Gleicheniales",
        "Hymenophyllales",
        "Marattiales",
        "Ophioglossales",
        "Osmundales",
        "Polypodiales",
        "Psilotales",
        "Salviniales",
        "Schizaeales"
    ]
}

SEEDLESS_ORDERS = set(seedless_plant_taxa["order"])


def _parse_tags_from_line(obj: list | dict) -> list[str]:
    """Extract tag list from a line: either a JSON array (legacy) or object with 'tags' key."""
    if isinstance(obj, list):
        return obj
    if isinstance(obj, dict):
        return obj.get("tags", [])
    return []


def _load_existing_tags(tags_path: Path) -> list[list[str]]:
    """Load existing tag arrays from the row-tags file; return [] if file missing or empty."""
    if not tags_path.exists():
        return []
    out = []
    with open(tags_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                out.append([])
                continue
            try:
                parsed = json.loads(line)
                out.append(_parse_tags_from_line(parsed))
            except json.JSONDecodeError:
                out.append([])
    return out


def main():
    repo_root = Path(__file__).resolve().parent.parent
    input_path = repo_root / "data" / "processed" / "descriptions_text_by_source.jsonl"
    tags_path = repo_root / "data" / "processed" / ROW_TAGS_FILENAME

    if not input_path.exists():
        print(f"Input file not found: {input_path}", file=sys.stderr)
        sys.exit(1)

    existing_tags_per_row = _load_existing_tags(tags_path)
    seedless_count = 0
    has_seed_count = 0
    row_index = 0

    with open(input_path, "r", encoding="utf-8") as fin, open(
        tags_path, "w", encoding="utf-8"
    ) as fout:
        for line in fin:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError as e:
                print(f"Invalid JSON at line {row_index + 1}: {e}", file=sys.stderr)
                existing = (
                    existing_tags_per_row[row_index]
                    if row_index < len(existing_tags_per_row)
                    else []
                )
                merged = [t for t in existing if t not in SEED_STATUS_TAGS]
                fout.write(
                    json.dumps({"identifier": None, "tags": merged}) + "\n"
                )
                row_index += 1
                continue

            existing = (
                existing_tags_per_row[row_index]
                if row_index < len(existing_tags_per_row)
                else []
            )
            merged = [t for t in existing if t not in SEED_STATUS_TAGS]

            order_name = record.get("order_name")
            if order_name is not None and order_name in SEEDLESS_ORDERS:
                merged.append("seedless")
                seedless_count += 1
            else:
                merged.append("has_seed")
                has_seed_count += 1

            identifier = record.get("identifier")
            fout.write(
                json.dumps({"identifier": identifier, "tags": merged}) + "\n"
            )
            row_index += 1

    print(f"Total rows: {row_index}")
    print(f"Rows tagged seedless: {seedless_count}")
    print(f"Rows tagged has_seed: {has_seed_count}")
    print(f"Row tags file: {tags_path}")


if __name__ == "__main__":
    main()
