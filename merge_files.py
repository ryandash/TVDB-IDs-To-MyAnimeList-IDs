#!/usr/bin/env python3
import argparse
import json
from pathlib import Path
from typing import List, Union

def load_json(path: Path) -> Union[dict, list]:
    print(f"🔎 Loading JSON: {path}")
    if not path.exists():
        print(f"  ⚠ File not found: {path}")
        return {} if path.suffix == ".json" else []
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, dict):
                print(f"  ✅ Loaded dict with {len(data)} keys")
            elif isinstance(data, list):
                print(f"  ✅ Loaded list with {len(data)} items")
            else:
                print(f"  ⚠ Unexpected type: {type(data)}")
            return data
    except Exception as e:
        print(f"  ❌ Error loading {path}: {e}")
        return {}

def save_json(path: Path, data: Union[dict, list]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"💾 Saved {path} ({'dict' if isinstance(data, dict) else 'list'}) "
          f"with {len(data) if hasattr(data, '__len__') else 'unknown'} entries")

def merge_dicts(d1: dict, d2: dict) -> dict:
    for key, val in d2.items():
        if key in d1:
            if isinstance(val, dict) and isinstance(d1[key], dict):
                d1[key] = merge_dicts(d1[key], val)
            elif isinstance(val, list) and isinstance(d1[key], list):
                combined = d1[key] + val
                seen = []
                for item in combined:
                    if item not in seen:
                        seen.append(item)
                d1[key] = seen
            else:
                print(f"  ⚠ Overwriting key {key} in dict merge")
                d1[key] = val
        else:
            d1[key] = val
    return d1

def merge_json_files(target: Path, new_files: List[Path], mode: str):
    print(f"\n📂 Merging into {target} [mode={mode}]")
    print(f"   Incoming files: {[str(f) for f in new_files]}")

    # Load original
    original_data = load_json(target) if target.exists() else ({} if mode == "anime_data" else [])
    print(f"   Existing data type: {type(original_data).__name__}, size: {len(original_data) if hasattr(original_data, '__len__') else 'N/A'}")

    # Load new data
    if mode == "anime_data":
        merged = original_data.copy() if isinstance(original_data, dict) else {}
        for file in new_files:
            try:
                data = load_json(file)
                if isinstance(data, dict):
                    merged = merge_dicts(merged, data)
            except Exception as e:
                print(f"⚠ Skipping invalid JSON in {file}: {e}")

    elif mode == "api/thetvdb":
        merged = original_data if isinstance(original_data, list) else []
        if new_files:
            try:
                data = load_json(new_files[-1])
                if isinstance(data, dict):
                    print(f"   Replacing with single dict wrapped in list")
                    merged = [data]
                elif isinstance(data, list):
                    print(f"   Replacing with list of {len(data)} items")
                    merged = data
            except Exception as e:
                print(f"⚠ Skipping invalid JSON in {new_files[-1]}: {e}")

    elif mode == "api/myanimelist":
        merged_dict = {}
        # Start with existing
        if isinstance(original_data, list):
            for entry in original_data:
                tvdb_id = entry.get("thetvdb")
                if tvdb_id:
                    merged_dict[tvdb_id] = entry
        print(f"   Starting merged_dict with {len(merged_dict)} entries from existing data")

        # Add new
        for file in new_files:
            try:
                data = load_json(file)
                if isinstance(data, list):
                    print(f"   Processing list of {len(data)} entries from {file}")
                    for entry in data:
                        tvdb_id = entry.get("thetvdb")
                        if tvdb_id:
                            merged_dict[tvdb_id] = entry
                else:
                    print(f"   ⚠ Skipped non-list file {file}")
            except Exception as e:
                print(f"⚠ Skipping invalid JSON in {file}: {e}")
        merged = list(merged_dict.values())
        print(f"   Final merged_dict size: {len(merged)}")

    else:
        raise ValueError(f"Unknown merge mode: {mode}")

    save_json(target, merged)
    print(f"✅ Merged {len(new_files)} files → {target} ({'list' if isinstance(merged, list) else 'dict'})")

def main():
    parser = argparse.ArgumentParser(description="Merge JSON files from scraper artifacts into main repo.")
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("artifacts"),
        help="Directory containing scraper output (default: artifacts/)",
    )
    args = parser.parse_args()
    input_dir: Path = args.input_dir

    repo_root = Path.cwd()
    print(f"🚀 Starting merge, input_dir={input_dir}, repo_root={repo_root}")

    # Merge anime_data from all page-* folders
    anime_files = list(input_dir.glob("page-*/anime_data/*.json"))
    print(f"🔎 Found {len(anime_files)} anime_data files")
    for file in anime_files:
        target_file = repo_root / "anime_data" / file.name
        merge_json_files(target_file, [file], mode="anime_data")

    # Merge api/thetvdb
    tvdb_files = list(input_dir.glob("page-*/api/thetvdb/*.json"))
    print(f"🔎 Found {len(tvdb_files)} api/thetvdb files")
    for file in tvdb_files:
        target_file = repo_root / "api/thetvdb" / file.name
        merge_json_files(target_file, [file], mode="api/thetvdb")

    # Merge api/myanimelist
    mal_files = list(input_dir.glob("page-*/api/myanimelist/*.json"))
    print(f"🔎 Found {len(mal_files)} api/myanimelist files")
    for file in mal_files:
        target_file = repo_root / "api/myanimelist" / file.name
        merge_json_files(target_file, [file], mode="api/myanimelist")

    # Merge mapped/unmapped
    for root_file in ["mapped-tvdb-ids.json", "unmapped-tvdb-ids.json"]:
        artifact_files = list(input_dir.glob(f"page-*/{root_file}"))
        print(f"🔎 Found {len(artifact_files)} {root_file} files")
        target_file = repo_root / root_file
        if artifact_files:
            merge_json_files(target_file, artifact_files, mode="api/myanimelist")

if __name__ == "__main__":
    main()
