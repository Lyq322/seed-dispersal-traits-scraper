# Seed dispersal traits scraper

Pipeline for scraping plant species descriptions from World Flora Online, extracting and tagging text. Descriptions data is later used for seed dispersal trait extraction using NLP and other methods.

---

## Project structure

```
seed-dispersal-traits-scraper/
├── README.md
├── requirements.txt
│
├── scraping/
│   ├── world_flora_online.py             # Scrape WFO taxon tree from WFO website
│   ├── world_flora_online_plant_list.py  # Scrape accepted species in WFO plant list
│   └── flora_of_china.py                 # Flora of China scraper
│
├── process/                  # Data processing scripts (JSONL in/out)
│   ├── extract_descriptions.py           # Extract descriptions from raw HTML in WFO JSONL → descriptions_by_source*.jsonl
│   ├── extract_description_text.py       # Extract plain text from description records
│   ├── filter_jsonl.py                   # Filter JSONL by field=value (e.g. page_type=species)
│   ├── jsonl_to_txt_files.py             # Convert descriptions JSONL → folder of .txt files (batched by language)
│   ├── random_select_txt.py              # Randomly sample .txt files (e.g. for annotation)
│   ├── tag_language.py                   # Detect language of description text, add lang_* tags to each record's tags field
│   └── tag_seedless.py                   # Tag seedless / has_seed (or similar) for filtering
│
├── analyze/                  # Analysis and inspection
│   ├── analyze_jsonl.py              # Count unique families, orders, genera, species in JSONL
│   ├── list_jsonl.py                 # List/peek JSONL rows
│   ├── search_jsonl.py               # Search JSONL by field/value
│   └── sources_by_language.py        # Summary of sources by language
│
├── plots/                    # Plotting scripts and generated figures
│   ├── plot_description_lengths.py   # Plot distribution of description lengths
│   ├── plot_descriptions_distribution.py # Plot description stats
│   └── *.png                 # Generated plots (e.g. species_description_lengths.png)
│
├── archive/                  # Archived / legacy scripts (moved from process/ and analyze/)
│   ├── combine_world_flora_online.py     # Merge/combine WFO JSONL outputs
│   ├── combine_jsonl_to_jsonl_and_csv.py # Combine JSONL and export CSV
│   ├── build_fruit_type_variants_csv.py  # Build fruit-type variant mappings
│   ├── build_variant_to_english_csv.py   # Variant → English term CSV
│   ├── remove_canonical_variants.py      # Remove canonical variants from data
│   ├── find_long_descriptions.py     # Find descriptions above length threshold
│   ├── find_non_unique_species.py    # Find non-unique species identifiers
│   ├── species_count_by_family.py    # Species counts per family
│   ├── latest_genus_by_family.py     # Latest genus (e.g. by date) per family
│   └── plant_list_json.py            # Work with plant list JSON
│
├── web/                      # Browse descriptions (Flask app)
│   ├── README.md             # Web app usage
│   ├── app.py                # Flask server; builds index from descriptions_text_by_source.jsonl (tags in each record)
│   └── static/
│       └── index.html        # UI: filters (tags, order, family, source), pagination, “Show more”
│
├── data/                     # All data (git-ignored in practice; paths used by scripts)
│   ├── raw/                  # Scraper output: world_flora_online*.jsonl, etc.
│   ├── processed/            # Extracted/tagged: descriptions_by_source*.jsonl, descriptions_text_by_source.jsonl (with tags field)
│   ├── flora_traiter/        # Batched .txt by language (from jsonl_to_txt_files) for annotation
│   ├── flora_of_china_raw/   # Raw Flora of China data
│   ├── mistral_llm/          # LLM-related outputs
│   ├── translations/         # Translation outputs
│   └── ...                   # Other dataset variants (e.g. test_small, test_medium)
│
├── logs/                     # Scraper and script logs
├── result/                   # Result artifacts (e.g. flora_traiter HTML)
├── test_small/               # Small test .txt sample (by language)
└── test_medium/              # Medium test .txt sample (by language)
```

---

## Quick start

1. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

2. **Scraping** (optional; uses existing `data/raw` if present)
   - Run `scraping/world_flora_online.py` or `world_flora_online_plant_list.py` to fetch WFO pages into `data/raw/*.jsonl`.

3. **Processing**
   - Extract descriptions:
     `python process/extract_descriptions.py data/raw/world_flora_online_species.jsonl data/processed/descriptions_by_source_species.jsonl`
   - Build text-by-source and tags (see scripts in `process/`), then e.g. `tag_language.py`, `tag_seedless.py`.
   - Export to .txt:
     `python process/jsonl_to_txt_files.py` (inputs/outputs as in script/docstring).

4. **Browse**
   - From repo root: `python web/app.py` → open http://127.0.0.1:5000 (see `web/README.md`).

---

## Data flow (summary)

- **Raw**: Scrapers write `data/raw/*.jsonl` (e.g. `world_flora_online_species.jsonl`, `world_flora_online_complete.jsonl`).
- **Processed**: `extract_descriptions.py` and related scripts produce `data/processed/descriptions_by_source*.jsonl`, `descriptions_text_by_source.jsonl` (each record has a `tags` field after running tag_*.py).
- **Export**: `jsonl_to_txt_files.py` writes batched `.txt` files (e.g. under `data/flora_traiter/` by language) for annotation or downstream use.
- **Web**: `web/app.py` reads `descriptions_text_by_source.jsonl` (tags in each record), builds an in-memory index, and serves the browser UI with filters and pagination.
