#!/usr/bin/env python3
"""Show count of unique species by family from a JSONL file (identifier or species_name)."""

import json
import sys
from pathlib import Path
from collections import defaultdict

DEFAULT_JSONL = Path(__file__).resolve().parent.parent / "data" / "processed" / "descriptions_text_by_source.jsonl"


def main():
    jsonl_path = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_JSONL
    if not jsonl_path.is_absolute():
        jsonl_path = (Path(__file__).resolve().parent.parent / jsonl_path).resolve()
    if not jsonl_path.exists():
        print(f"File not found: {jsonl_path}", file=sys.stderr)
        sys.exit(1)

    # family -> set of unique species (by identifier; fallback to "genus species" if no id)
    family_to_species = defaultdict(set)

    with open(jsonl_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            if obj.get("page_type") != "species":
                continue
            family = (obj.get("family_name") or "").strip() or "(no family)"
            species_name = (obj.get("species_name") or "").strip()
            if not species_name:
                continue
            identifier = (obj.get("identifier") or "").strip()
            genus = (obj.get("genus_name") or "").strip()
            if identifier:
                key = identifier
            else:
                key = f"{genus} {species_name}".strip() if genus else species_name
            family_to_species[family].add(key)

    # sort alphabetically by family name
    rows = [(fam, len(species)) for fam, species in family_to_species.items()]
    rows.sort(key=lambda x: x[0])

    width = max(len(str(c)) for _, c in rows) if rows else 0
    fmt = f"{{count:>{width}}}  {{family}}"
    for family, count in rows:
        print(fmt.format(count=count, family=family))
    print()
    print(f"Total families: {len(rows)}")
    print(f"Total unique species: {sum(c for _, c in rows)}")


if __name__ == "__main__":
    main()
