# Seed dispersal traits scraper

Pipeline for scraping plant species descriptions from World Flora Online, extracting and tagging text. Descriptions are later used for seed dispersal trait extraction using NLP and other methods.

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
│   ├── extract_descriptions.py           # Extract description-related HTML, separated by source, from entire raw HTML of every page
│   ├── extract_description_text.py       # Extract plain text from description HTML
│   ├── filter_jsonl.py                   # Creates new JSONL from input JSON using filter field=value (e.g. page_type=species)
│   ├── jsonl_to_txt_files.py             # Convert descriptions JSONL → folder of .txt files
│   ├── random_select_txt.py              # Randomly sample .txt files by count/batches with optional filters
│   ├── tag_language.py                   # Detect language of description text, add lang_* tags to each record's tags field
│   └── tag_seedless.py                   # Tag seedless / has_seed for filtering
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
├── archive/                  # Archived / legacy scripts
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
│   └── ...
│
└── logs/                     # Scraper and script logs
```

---

## Quick start

1. **Install dependencies**
  ```bash
   pip install -r requirements.txt
  ```
2. **Scraping**
  `scraping/world_flora_online_plant_list.py` is the most up to date scraping script. Use this instead of `scraping/world_flora_online.py` for scraping WFO.
  1. Download a snapshot of WFO plant list from [https://wfoplantlist.org/classifications](https://wfoplantlist.org/classifications), e.g. `plant_list_2025-12.json.zip`. Update `PLANT_LIST_PATH` in `scraping/world_flora_online_plant_list.py` with the path of the plant list json.
  2. Run `scraping/world_flora_online_plant_list.py` to scrape all accepted species from World Flora Online. The script writes a jsonl file to `data/raw/world_flora_online_plant_list.jsonl` with the following fields:
    - source: always "World Flora Online"
    - identifier: WFO taxon ID, e.g. "wfo-0000814642" for Lobelia zwartkopensis
    - page_type: always "species"
    - order_name, family_name, genus_name, species_name
    - url: full page URL (e.g. [https://www.worldfloraonline.org/taxon/{identifier}](https://www.worldfloraonline.org/taxon/{identifier}))
    - raw_html: full HTML of the species page
    - timestamp: ISO timestamp when the page was fetched
3. **Processing**
  - Extract descriptions from raw WFO JSONL:
   `python process/extract_descriptions.py data/raw/world_flora_online.jsonl data/processed/descriptions_text_by_source.jsonl`
  - Extract plain text from description HTML (modifies file in place):
  `python process/extract_description_text.py data/processed/descriptions_text_by_source.jsonl`
  - Optionally add tags: run `process/tag_language.py data/processed/descriptions_text_by_source.jsonl` and `process/tag_seedless.py data/processed/descriptions_text_by_source.jsonl` on that JSONL (default path: `data/processed/descriptions_text_by_source.jsonl`).
  - Export to .txt:
  `python process/jsonl_to_txt_files.py data/processed/descriptions_text_by_source.jsonl`.
  - Optionally sample .txt files for annotation, e.g.
  `python process/random_select_txt.py 500 test_small --sources data/processed/descriptions_text_by_source_txt --batches 5 --tag has_seed --tag lang_en`
4. **Browse**
  - From repo root: `python web/app.py` → open [http://127.0.0.1:5000](http://127.0.0.1:5000) (see `web/README.md`).

---

## Data flow

- **Raw**: Scrapers write `data/raw/*.jsonl` (e.g. `world_flora_online.jsonl``).
- **Processed**: `extract_descriptions.py` → `descriptions_text_by_source.jsonl`. `extract_description_text.py` adds a `descriptions_text` field in place. `tag_language.py` and `tag_seedless.py` adds tags to a `tags` field (list) in place.
- **Export**: `jsonl_to_txt_files.py` reads `descriptions_text_by_source.jsonl`, writes batched `.txt` files under e.g. `data/processed/descriptions_text_by_source_txt/`, and adds `txt_filename` field to each record in jsonl.
- **Sampling**: `random_select_txt.py` can sample from those .txt dirs (optionally filtered by tags via the same JSONL) into e.g. `test_small/` or `test_medium/` with optional batch subfolders.
- **Web**: `web/app.py` reads `data/processed/descriptions_text_by_source.jsonl`, builds an in-memory index, and serves the browser UI with filters and pagination (see `web/README.md`).
