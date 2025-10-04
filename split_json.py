import json
from pathlib import Path
import sys

mapped_file = Path("mapped-tvdb-ids.json")

if not mapped_file.exists():
    print("No mapped-tvdb-ids.json found. Skipping split_json.py.")
    sys.exit(0)

with mapped_file.open("r", encoding="utf-8") as f:
    data = json.load(f)

mal_dir = Path("api/myanimelist")
tvdb_dir = Path("api/thetvdb")
mal_dir.mkdir(parents=True, exist_ok=True)
tvdb_dir.mkdir(parents=True, exist_ok=True)

mal_entries = {}  # key: mal_id, value: list of entries
tvdb_count = 0

for entry in data:
    mal_id = entry.get("myanimelist")
    tvdb_id = entry.get("thetvdb")

    if mal_id is not None:
        mal_entries.setdefault(mal_id, []).append(entry)

    if tvdb_id is not None:
        path = tvdb_dir / f"{tvdb_id}.json"
        with path.open("w", encoding="utf-8") as f:
            json.dump([entry], f, indent=4, ensure_ascii=False)
        tvdb_count += 1

# Write all MAL entries
mal_count = 0
for mal_id, entries in mal_entries.items():
    path = mal_dir / f"{mal_id}.json"
    with path.open("w", encoding="utf-8") as f:
        json.dump(entries, f, indent=4, ensure_ascii=False)
    mal_count += 1

print(f"Split complete. Wrote {mal_count} MAL files and {tvdb_count} TVDB files.")
