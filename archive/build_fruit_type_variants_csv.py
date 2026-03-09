#!/usr/bin/env python3
"""
Build a CSV of fruit_type_term rows: label, pattern (foreign variant), replace (fruit type).
Uses translations.csv for English -> canonical foreign, then language-specific variant CSVs
for canonical -> variants. Uses keyword_to_dispersal_traits_mapping.csv for English -> core_fruit_type.
"""

import csv
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "translations"

CANONICAL_ENGLISH_TERMS = [
    "winged achene", "achene", "cypsela", "anthocarp", "amphisarca", "arillocarpium",
    "balausta", "berry", "cactidium", "epispermatium", "fleshy berry", "hesperidium",
    "pepo", "pulpy berry", "syncarp", "capsule", "circumscissile", "circumscissile capsule",
    "loculicidal", "loculicidal capsule", "poricidal", "poricidal capsule", "pyxis",
    "septicidal", "septicidal capsule", "silicle", "silique", "caryopsis", "grain",
    "aggregate drupe", "compound drupe", "drupe", "drupelet", "drupetum", "pseudodrupe",
    "stone fruit", "follicle", "cone", "megastrobilus", "microstrobilus", "ovulate cone",
    "pollen cone", "seed cone", "strobili", "strobilus", "berry-like cone", "cone berry",
    "galbulus", "legume", "loment", "pod", "winged nut", "nut", "nutlet", "nuculanium",
    "pome", "samara", "samaras", "samaroid", "winged samara", "camara", "carcerulus",
    "coccus", "cremocarp", "mericarp", "regma", "schizocarp", "sorosis", "syconium", "utricle",
]

# translations.csv column name for botanical latin (has space)
LANG_COLS = ["spanish", "portuguese", "french", "botanical latin", "german", "turkish"]
LANG_TO_FILE = {
    "spanish": "dispersal_es_variants.csv",
    "portuguese": "dispersal_pt_variants.csv",
    "french": "dispersal_fr_variants.csv",
    "botanical latin": "dispersal_la_variants.csv",
    "german": "dispersal_de_variants.csv",
    "turkish": "dispersal_tk_variants.csv",
}


def main():
    # 0) English keyword -> fruit type (core_fruit_type) from mapping
    en_to_fruit_type = {}
    mapping_path = DATA_DIR / "keyword_to_dispersal_traits_mapping.csv"
    with open(mapping_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            kw = (row.get("keyword_term") or "").strip()
            ft = (row.get("core_fruit_type") or "").strip()
            if kw and ft:
                en_to_fruit_type[kw] = ft

    # 1) English -> canonical per language from translations.csv
    en_to_canonical = {}  # english -> { lang -> canonical }
    with open(DATA_DIR / "translations.csv", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            kw = row.get("keyword_term", "").strip()
            if not kw:
                continue
            en_to_canonical[kw] = {}
            for col in LANG_COLS:
                val = (row.get(col) or "").strip()
                if val:
                    en_to_canonical[kw][col] = val

    # 2) Per language: canonical -> set of variants (from variant CSVs)
    lang_canonical_to_variants = {}  # lang -> { canonical -> set(variants) }
    for lang, filename in LANG_TO_FILE.items():
        path = DATA_DIR / filename
        if not path.exists():
            continue
        canonical_to_variants = {}
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                can = (row.get("canonical") or "").strip()
                var = (row.get("variant") or "").strip()
                if not can or not var:
                    continue
                s = canonical_to_variants.setdefault(can, set())
                s.add(var)
                s.add(can)  # include canonical itself as a variant
        lang_canonical_to_variants[lang] = canonical_to_variants

    # 3) For each English term, collect (pattern=variant, replace=fruit_type)
    seen = set()
    rows = []
    for english in CANONICAL_ENGLISH_TERMS:
        fruit_type = en_to_fruit_type.get(english)
        if not fruit_type:
            continue
        if english not in en_to_canonical:
            continue
        for lang, canonical in en_to_canonical[english].items():
            variants_map = lang_canonical_to_variants.get(lang)
            if not variants_map:
                continue
            variants = variants_map.get(canonical)
            if not variants:
                # try case-insensitive match for German/Latin
                for can, vars_set in variants_map.items():
                    if can.lower() == canonical.lower():
                        variants = vars_set
                        break
            if not variants:
                # emit at least the canonical as one variant
                variants = {canonical}
            for pattern in sorted(variants):
                key = (pattern, fruit_type)
                if key in seen:
                    continue
                seen.add(key)
                rows.append({"label": "fruit_type_term", "pattern": pattern, "replace": fruit_type})

    out_path = DATA_DIR / "fruit_type_variants_to_english.csv"
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["label", "pattern", "replace"])
        w.writeheader()
        w.writerows(rows)

    print(f"Wrote {len(rows)} rows to {out_path}")


if __name__ == "__main__":
    main()
