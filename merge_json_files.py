# merge_json_files.py
import json
from pathlib import Path

ARTIFACTS_DIR = Path("artifacts")
FILES_TO_MERGE = ["mapped-tvdb-ids.json", "unmapped-tvdb-ids.json"]

def load_json(file_path: Path):
    """Load JSON file safely, return list of items."""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, list):
                return data
            elif isinstance(data, dict):
                return [data]
    except Exception as e:
        print(f"Skipping {file_path}: {e}")
    return []

def merge_json(name: str):
    combined = []

    root_file = Path(name)
    if root_file.exists():
        combined.extend(load_json(root_file))

    for file in ARTIFACTS_DIR.rglob(name):
        combined.extend(load_json(file))

    with open(name, "w", encoding="utf-8") as out:
        json.dump(combined, out, indent=2, ensure_ascii=False)

    print(f"Merged {name} -> {len(combined)} items")

if __name__ == "__main__":
    for filename in FILES_TO_MERGE:
        merge_json(filename)
