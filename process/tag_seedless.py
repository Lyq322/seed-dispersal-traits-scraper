"""
Seedless plant taxa (orders) and script to tag rows in a descriptions JSONL.
Sets exactly one of "seedless" or "has_seed" per row (overwriting any previous
such tags) and merges with existing tags in each record's "tags" field; all
other tags are preserved. Writes the updated records back to the JSONL (in
place or to a separate file via --output).
"""

import argparse
import json
import sys
from pathlib import Path

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


def _get_tags_from_record(record: dict) -> list[str]:
    """Extract tag list from a record; ensure we return a list of strings."""
    raw = record.get("tags")
    if isinstance(raw, list):
        return [str(t) for t in raw]
    return []


def main():
    parser = argparse.ArgumentParser(
        description="Tag rows from a descriptions JSONL as seedless or has_seed; merge with existing tags."
    )
    parser.add_argument(
        "input_jsonl",
        type=Path,
        help="Path to input JSONL (e.g. descriptions_text_by_source.jsonl)",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=None,
        help="Output JSONL path (default: overwrite input)",
    )
    args = parser.parse_args()
    input_path = args.input_jsonl.resolve()
    output_path = args.output.resolve() if args.output else input_path

    if not input_path.exists():
        print(f"Input file not found: {input_path}", file=sys.stderr)
        sys.exit(1)

    seedless_count = 0
    has_seed_count = 0
    row_index = 0
    records_out = []

    with open(input_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError as e:
                print(f"Invalid JSON at line {row_index + 1}: {e}", file=sys.stderr)
                record = {}
            row_index += 1

            existing = _get_tags_from_record(record)
            merged = [t for t in existing if t not in SEED_STATUS_TAGS]

            order_name = record.get("order_name") if isinstance(record, dict) else None
            if order_name is not None and order_name in SEEDLESS_ORDERS:
                merged.append("seedless")
                seedless_count += 1
            else:
                merged.append("has_seed")
                has_seed_count += 1

            if isinstance(record, dict):
                record["tags"] = merged
            else:
                record = {"tags": merged}
            records_out.append(record)

    with open(output_path, "w", encoding="utf-8") as f:
        for record in records_out:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    print(f"Total rows: {row_index}")
    print(f"Rows tagged seedless: {seedless_count}")
    print(f"Rows tagged has_seed: {has_seed_count}")
    print(f"Output: {output_path}")


if __name__ == "__main__":
    main()
