from pathlib import Path
import json

input_file = Path("mapped-tvdb-ids.json")
if not input_file.exists():
    print(f"[ERROR] {input_file} not found. Run merge_json_files.py first.")
    exit(1)

# Load full JSON
with input_file.open("r", encoding="utf-8") as f:
    data = json.load(f)

mal_dir = Path("api/myanimelist")
tvdb_dir = Path("api/thetvdb")
mal_dir.mkdir(parents=True, exist_ok=True)
tvdb_dir.mkdir(parents=True, exist_ok=True)

mal_map = {}
tvdb_map = {}

for entry in data:
    mal_id = entry.get("myanimelist")
    tvdb_id = entry.get("thetvdb")

    if mal_id:
        mal_map.setdefault(str(mal_id), []).append(entry)

    if tvdb_id:
        tvdb_map.setdefault(str(tvdb_id), []).append(entry)

# Write MAL entries
for mal_id, entries in mal_map.items():
    with open(mal_dir / f"{mal_id}.json", "w", encoding="utf-8") as f:
        json.dump(entries, f, indent=4, ensure_ascii=False)

# Write TVDB entries
for tvdb_id, entries in tvdb_map.items():
    with open(tvdb_dir / f"{tvdb_id}.json", "w", encoding="utf-8") as f:
        json.dump(entries, f, indent=4, ensure_ascii=False)
