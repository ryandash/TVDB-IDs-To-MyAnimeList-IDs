import json
from pathlib import Path
import sys
import glob

# Look for all mapped files (series + movie)
mapped_files_series = glob.glob("mapped-tvdb-ids-series.json")
mapped_files_movie = glob.glob("mapped-tvdb-ids-movie.json")

if not mapped_files_series and not mapped_files_movie:
    print("No mapped-tvdb-ids-series.json or mapped-tvdb-ids-movie.json found. Skipping split_json.py.")
    sys.exit(0)

# Directories
mal_dir = Path("api/myanimelist")
tvdb_series_dir = Path("api/thetvdb-series")
tvdb_movie_dir = Path("api/thetvdb-movie")

mal_dir.mkdir(parents=True, exist_ok=True)
tvdb_series_dir.mkdir(parents=True, exist_ok=True)
tvdb_movie_dir.mkdir(parents=True, exist_ok=True)

mal_entries = {}  # key: mal_id, value: list of entries
tvdb_seen = set()
tvdb_count_series = 0
tvdb_count_movie = 0

# --- Process series first ---
for mapped_file in mapped_files_series:
    with open(mapped_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    for entry in data:
        mal_id = entry.get("myanimelist")
        tvdb_id = entry.get("thetvdb")

        # MAL output
        if mal_id is not None:
            mal_entries.setdefault(mal_id, []).append(entry)

        # TVDB series output
        if tvdb_id is not None and tvdb_id not in tvdb_seen:
            path = tvdb_series_dir / f"{tvdb_id}.json"
            with path.open("w", encoding="utf-8") as f:
                json.dump([entry], f, indent=4, ensure_ascii=False)
            tvdb_seen.add(tvdb_id)
            tvdb_count_series += 1

# --- Process movies ---
for mapped_file in mapped_files_movie:
    with open(mapped_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    for entry in data:
        mal_id = entry.get("myanimelist")
        tvdb_id = entry.get("thetvdb")

        # MAL output
        if mal_id is not None:
            mal_entries.setdefault(mal_id, []).append(entry)

        # TVDB movie output (only if not already in series)
        if tvdb_id is not None and tvdb_id not in tvdb_seen:
            path = tvdb_movie_dir / f"{tvdb_id}.json"
            with path.open("w", encoding="utf-8") as f:
                json.dump([entry], f, indent=4, ensure_ascii=False)
            tvdb_seen.add(tvdb_id)
            tvdb_count_movie += 1

# Write all MAL entries
mal_count = 0
for mal_id, entries in mal_entries.items():
    path = mal_dir / f"{mal_id}.json"
    with path.open("w", encoding="utf-8") as f:
        json.dump(entries, f, indent=4, ensure_ascii=False)
    mal_count += 1

print(f"Split complete. Wrote {mal_count} MAL files, {tvdb_count_series} TVDB series files, {tvdb_count_movie} TVDB movie files.")
