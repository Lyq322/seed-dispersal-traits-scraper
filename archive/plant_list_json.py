#!/usr/bin/env python3
"""Count and sample from data/raw/plant_list_2025-12.json (streaming, no full load).

Requires: pip install ijson
"""

import json
import random
import sys

K = 10

def main():
    path = "data/raw/plant_list_2025-12.json"
    try:
        import ijson
    except ImportError:
        print("Install ijson: pip install ijson (or use a venv)", file=sys.stderr)
        sys.exit(1)

    reservoir = []
    n = 0
    species_count = 0
    with open(path, "rb") as f:
        for item in ijson.items(f, "item"):
            n += 1
            if n % 100_000 == 0:
                print(f"{n} ...", file=sys.stderr)
            if item.get("rank_s") != "species" or item.get("role_s") != "accepted":
                continue
            species_count += 1
            if species_count <= K:
                reservoir.append(item)
            else:
                r = random.randrange(species_count)
                if r < K:
                    reservoir[r] = item

    for i, obj in enumerate(reservoir):
        print(f"--- random species (accepted) {i + 1}/{K} ---")
        print(json.dumps(obj, indent=2))
    print(f"length: {n}", file=sys.stderr)
    print(f"count rank_s='species' and role_s='accepted': {species_count}", file=sys.stderr)

if __name__ == "__main__":
    main()
