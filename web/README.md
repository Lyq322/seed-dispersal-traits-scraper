# Descriptions browser

Simple web UI to browse seed dispersal descriptions with filters (tags, order, family, source, genus).

## Run

From the repo root, with a virtualenv that has dependencies installed:

```bash
pip install -r requirements.txt   # if not already
python web/app.py
```

Then open **http://127.0.0.1:5000** in your system browser (Chrome, Safari, Firefox).
If you see "Access to 127.0.0.1 was denied", the in-editor browser may be blocking localhost—use your normal browser instead.

- **First run**: The app builds an in-memory index from `data/processed/descriptions_text_by_source.jsonl` and `tags.jsonl`; this can take 1–2 minutes. The page will show “Building index…” until then.
- **Filters**: Choose tags (e.g. `has_seed`, `lang_en`), order, family, and/or source, then click **Apply**. Results are paginated (20 per page).
- **Show more**: Long descriptions are truncated; click “Show more” to expand.

## Options

- `--port 5001` — use a different port
- `--host 0.0.0.0` — listen on all interfaces (e.g. if you need access from another machine or context)
- `--no-index-wait` — start the server immediately and build the index in the background (poll `/api/status` until ready)
