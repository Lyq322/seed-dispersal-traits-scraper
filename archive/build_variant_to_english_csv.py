#!/usr/bin/env python3
"""
Build a CSV that maps foreign-language dispersal term variants to English canonical forms.

Reads:
- translations.csv: keyword_term (English) and columns per language (spanish, portuguese, etc.)
- dispersal_*_variants.csv: canonical, variant per language

Output CSV columns: label, pattern, replace, type
- label: always dispersal_term
- pattern: foreign language variant
- replace: English canonical term
- type: UNKNOWN
"""

import csv
from pathlib import Path

# Paths relative to this script so they work regardless of cwd
_SCRIPT_DIR = Path(__file__).resolve().parent
_TRANSLATIONS_DIR = _SCRIPT_DIR.parent / "data" / "translations"
TRANSLATIONS_CSV = _TRANSLATIONS_DIR / "translations.csv"
OUTPUT_CSV = _TRANSLATIONS_DIR / "dispersal_variants_to_english.csv"

# Variant filename suffix -> column name in translations.csv
VARIANT_FILE_TO_LANG = {
    "dispersal_es_variants.csv": "spanish",
    "dispersal_pt_variants.csv": "portuguese",
    "dispersal_fr_variants.csv": "french",
    "dispersal_la_variants.csv": "botanical latin",
    "dispersal_de_variants.csv": "german",
    "dispersal_tk_variants.csv": "turkish",
}


def main():
    # Build per-language map: canonical_foreign -> english (keyword_term)
    # Use str.strip() for canonical keys in case of stray spaces
    lang_to_foreign2english: dict[str, dict[str, str]] = {
        lang: {} for lang in VARIANT_FILE_TO_LANG.values()
    }

    with open(TRANSLATIONS_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            en = (row.get("keyword_term") or "").strip()
            if not en:
                continue
            for lang_col in VARIANT_FILE_TO_LANG.values():
                foreign = (row.get(lang_col) or "").strip()
                if foreign:
                    lang_to_foreign2english[lang_col][foreign] = en

    # Collect (pattern, replace) from each variant file; deduplicate
    seen: set[tuple[str, str]] = set()
    rows_out: list[tuple[str, str, str, str]] = []

    for variant_filename, lang_col in VARIANT_FILE_TO_LANG.items():
        variant_path = _TRANSLATIONS_DIR / variant_filename
        if not variant_path.exists():
            continue
        foreign2en = lang_to_foreign2english[lang_col]
        with open(variant_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                canonical = (row.get("canonical") or "").strip()
                variant = (row.get("variant") or "").strip()
                if not variant:
                    continue
                en = foreign2en.get(canonical)
                if en is None:
                    # Optional: log unmapped canonical for debugging
                    continue
                key = (variant, en)
                if key not in seen:
                    seen.add(key)
                    rows_out.append(("dispersal_term", variant, en, "UNKNOWN"))

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["label", "pattern", "replace", "type"])
        w.writerows(rows_out)

    print(f"Wrote {len(rows_out)} rows to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
