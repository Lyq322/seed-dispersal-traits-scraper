#!/usr/bin/env python3
"""
Remove variant rows where variant equals the canonical translation for the given
English terms. Reads translations.csv and edits each dispersal_*_variants.csv.
"""
import csv
from pathlib import Path

DATA = Path(__file__).resolve().parent.parent / "data" / "translations"

ENGLISH_TERMS = [
    "achene", "cypsela", "anthocarp", "capsule", "circumscissile",
    "circumscissile capsule", "loculicidal", "loculicidal capsule",
    "poricidal", "poricidal capsule", "pyxis", "septicidal",
    "septicidal capsule", "silicle", "silique", "caryopsis", "grain",
    "follicle", "cone", "megastrobilus", "microstrobilus", "ovulate cone",
    "pollen cone", "seed cone", "strobili", "strobilus", "legume", "loment",
    "pod", "nutlet", "camara", "carcerulus", "coccus", "cremocarp",
    "mericarp", "regma", "schizocarp", "utricle", "accessory fruit",
    "aggregate fruit", "compound fruit", "multiple fruit",
]

# translations.csv column -> dispersal_*_variants.csv suffix
LANG_COLS = {
    "spanish": "es",
    "portuguese": "pt",
    "french": "fr",
    "botanical latin": "la",
    "german": "de",
    "turkish": "tk",
}


def main():
    # Load canonical translations per language for our English terms
    trans_path = DATA / "translations.csv"
    canonicals_by_lang = {suffix: set() for suffix in LANG_COLS.values()}
    term_set = set(ENGLISH_TERMS)

    with open(trans_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        cols = reader.fieldnames
        if not cols or "keyword_term" not in cols:
            raise SystemExit("translations.csv missing keyword_term")
        for row in reader:
            term = (row.get("keyword_term") or "").strip()
            if term not in term_set:
                continue
            for col, suffix in LANG_COLS.items():
                if col not in row:
                    continue
                val = (row.get(col) or "").strip()
                if val:
                    canonicals_by_lang[suffix].add(val)

    # Process each dispersal_*_variants.csv
    for suffix, canonicals in canonicals_by_lang.items():
        path = DATA / f"dispersal_{suffix}_variants.csv"
        if not path.exists():
            print(f"Skip (not found): {path.name}")
            continue
        rows = []
        removed = 0
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames
            for row in reader:
                variant = (row.get("variant") or "").strip()
                if variant in canonicals:
                    removed += 1
                    continue
                rows.append(row)
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            w.writerows(rows)
        print(f"{path.name}: removed {removed} rows (variant in canonical set)")


if __name__ == "__main__":
    main()
