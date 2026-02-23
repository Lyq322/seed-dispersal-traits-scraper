"""
Simple web app to browse descriptions with filters (tags, sources, order, family, genus).
Builds an in-memory index at startup for fast filtering; description text is read from file on demand.
"""

import json
import os
import threading
from pathlib import Path

from flask import Flask, jsonify, request, send_from_directory

# Paths relative to repo root
REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data" / "processed"
DESCRIPTIONS_PATH = DATA_DIR / "descriptions_text_by_source.jsonl"
TAGS_PATH = DATA_DIR / "tags.jsonl"

# Global index state (filled by background thread)
_index_ready = False
_index_error = None
_meta_list = []  # list of {offset, row_id, order_name, family_name, genus_name, species_name, source_name}
_row_id_to_tags = {}  # row_id -> set(tags)
_filter_options = {"orders": [], "families": [], "sources": [], "tags": []}

app = Flask(__name__, static_folder="static")


def row_identifier(record):
    orig = (record.get("identifier") or "").strip()
    source = (record.get("source_name") or "").strip() or "unknown"
    return f"{orig}_{source}" if orig else source


def build_index():
    global _index_ready, _index_error, _meta_list, _row_id_to_tags, _filter_options
    try:
        # Load tags: row_id -> set(tags)
        _row_id_to_tags.clear()
        if TAGS_PATH.exists():
            with open(TAGS_PATH, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                        ident = obj.get("identifier")
                        tags = set(obj.get("tags") or [])
                        if ident is not None:
                            _row_id_to_tags[ident] = tags
                    except json.JSONDecodeError:
                        pass
        all_tags = set()
        for tags in _row_id_to_tags.values():
            all_tags.update(tags)
        _filter_options["tags"] = sorted(all_tags)

        # Build description index: offset + metadata per line
        _meta_list.clear()
        orders, families, sources = set(), set(), set()
        if not DESCRIPTIONS_PATH.exists():
            _index_error = f"Descriptions file not found: {DESCRIPTIONS_PATH}"
            return
        with open(DESCRIPTIONS_PATH, "rb") as f:
            offset = 0
            for line_bytes in f:
                line_start = offset
                offset += len(line_bytes)
                line = line_bytes.decode("utf-8", errors="replace").strip()
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                row_id = row_identifier(record)
                order_name = (record.get("order_name") or "").strip() or None
                family_name = (record.get("family_name") or "").strip() or None
                genus_name = (record.get("genus_name") or "").strip() or None
                species_name = (record.get("species_name") or "").strip() or None
                source_name = (record.get("source_name") or "").strip() or None
                text = (record.get("descriptions_text") or "").strip()
                word_count = len(text.split()) if text else 0
                if order_name:
                    orders.add(order_name)
                if family_name:
                    families.add(family_name)
                if source_name:
                    sources.add(source_name)
                _meta_list.append({
                    "offset": line_start,
                    "row_id": row_id,
                    "order_name": order_name,
                    "family_name": family_name,
                    "genus_name": genus_name,
                    "species_name": species_name,
                    "source_name": source_name,
                    "word_count": word_count,
                })
        _filter_options["orders"] = sorted(orders)
        _filter_options["families"] = sorted(families)
        _filter_options["sources"] = sorted(sources)
        _index_ready = True
        _index_error = None
    except Exception as e:
        _index_error = str(e)
        _index_ready = False


def matches_filter(value, filter_list, exact=False):
    if not filter_list:
        return True
    value = (value or "").strip()
    for f in filter_list:
        f = (f or "").strip()
        if not f:
            continue
        if exact:
            if value == f:
                return True
        else:
            if f.lower() in (value or "").lower():
                return True
    return False


@app.route("/")
def index():
    return send_from_directory(app.static_folder, "index.html")


@app.route("/api/status")
def status():
    if _index_error:
        return jsonify({"ready": False, "error": _index_error})
    return jsonify({"ready": _index_ready, "count": len(_meta_list) if _index_ready else 0})


@app.route("/api/filter-options")
def filter_options():
    if not _index_ready:
        return jsonify({"error": "Index not ready"}), 503
    return jsonify(_filter_options)


@app.route("/api/descriptions")
def descriptions():
    if not _index_ready:
        return jsonify({"error": "Index not ready", "detail": _index_error or "Building index..."}), 503
    tags_param = request.args.get("tags", "")
    required_tags = [t.strip() for t in tags_param.split(",") if t.strip()]
    sources_param = request.args.get("sources", "")
    source_list = [s.strip() for s in sources_param.split(",") if s.strip()] or None
    order_param = request.args.get("order", "")
    order_list = [s.strip() for s in order_param.split(",") if s.strip()] or None
    family_param = request.args.get("family", "")
    family_list = [s.strip() for s in family_param.split(",") if s.strip()] or None
    genus_param = request.args.get("genus", "")
    genus_list = [s.strip() for s in genus_param.split(",") if s.strip()] or None
    min_words = max(0, int(request.args.get("min_words", 0) or 0))
    limit = min(int(request.args.get("limit", 50)), 100)
    offset = max(0, int(request.args.get("offset", 0)))
    preview = int(request.args.get("preview", 800))
    exact = request.args.get("exact", "").lower() in ("1", "true", "yes")

    # Which row_ids are allowed by tags?
    allowed_row_ids = None
    if required_tags:
        allowed_row_ids = {
            rid for rid, tag_set in _row_id_to_tags.items()
            if all(t in tag_set for t in required_tags)
        }

    # Filter indices
    indices = []
    for i, meta in enumerate(_meta_list):
        if allowed_row_ids is not None and meta["row_id"] not in allowed_row_ids:
            continue
        if source_list and not matches_filter(meta["source_name"], source_list, exact):
            continue
        if order_list and not matches_filter(meta["order_name"], order_list, exact):
            continue
        if family_list and not matches_filter(meta["family_name"], family_list, exact):
            continue
        if genus_list and not matches_filter(meta["genus_name"], genus_list, exact):
            continue
        if min_words > 0 and meta.get("word_count", 0) < min_words:
            continue
        indices.append(i)
        if len(indices) >= offset + limit:
            break

    total = len(indices)
    page_indices = indices[offset : offset + limit]

    # Read description text for this page only
    results = []
    if page_indices and DESCRIPTIONS_PATH.exists():
        with open(DESCRIPTIONS_PATH, "rb") as f:
            for idx in page_indices:
                meta = _meta_list[idx]
                f.seek(meta["offset"])
                line = f.readline().decode("utf-8", errors="replace")
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    results.append({**meta, "descriptions_text": "", "preview": ""})
                    continue
                text = (record.get("descriptions_text") or "").strip()
                preview_text = text[:preview] + "..." if preview and len(text) > preview else text
                row_tags = sorted(_row_id_to_tags.get(meta["row_id"], []))
                results.append({
                    "identifier": record.get("identifier"),
                    "order_name": meta["order_name"],
                    "family_name": meta["family_name"],
                    "genus_name": meta["genus_name"],
                    "species_name": meta["species_name"],
                    "subspecies": record.get("subspecies"),
                    "source_name": meta["source_name"],
                    "descriptions_text": text,
                    "preview": preview_text,
                    "row_id": meta["row_id"],
                    "tags": row_tags,
                })

    return jsonify({"total": total, "offset": offset, "limit": limit, "results": results})


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Run the descriptions browser web app")
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind")
    parser.add_argument("--port", type=int, default=5000, help="Port to bind")
    parser.add_argument("--no-index-wait", action="store_true", help="Start server without waiting for index")
    args = parser.parse_args()

    if not args.no_index_wait:
        print("Building index (this may take a minute)...")
        build_index()
        if _index_ready:
            print(f"Index ready: {len(_meta_list):,} descriptions")
        else:
            print(f"Index failed: {_index_error}")
    else:
        thread = threading.Thread(target=build_index, daemon=True)
        thread.start()
        print("Server starting; index will build in background. Poll /api/status until ready.")

    app.run(host=args.host, port=args.port, debug=False, threaded=True)


if __name__ == "__main__":
    main()
