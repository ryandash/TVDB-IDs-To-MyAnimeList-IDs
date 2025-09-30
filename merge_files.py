#!/usr/bin/env python3
import argparse
import json
from pathlib import Path
from typing import List, Union

def load_json(path: Path) -> Union[dict, list]:
    if not path.exists():
        return {} if path.suffix == ".json" else []
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def save_json(path: Path, data: Union[dict, list]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

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
                d1[key] = val
        else:
            d1[key] = val
    return d1

def merge_json_files(target: Path, new_files: List[Path], mode: str):
    """Merge new_files into the target file in the main repo."""
    # Load original
    original_data = load_json(target) if target.exists() else ({} if mode == "anime_data" else [])
    
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
                    merged = [data]
                elif isinstance(data, list):
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
        # Add new
        for file in new_files:
            try:
                data = load_json(file)
                if isinstance(data, list):
                    for entry in data:
                        tvdb_id = entry.get("thetvdb")
                        if tvdb_id:
                            merged_dict[tvdb_id] = entry
            except Exception as e:
                print(f"⚠ Skipping invalid JSON in {file}: {e}")
        merged = list(merged_dict.values())
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

    # Merge anime_data
    anime_folder = input_dir / "anime_data"
    if anime_folder.is_dir():
        for file in anime_folder.glob("*.json"):
            target_file = repo_root / "anime_data" / file.name
            merge_json_files(target_file, [file], mode="anime_data")

    # Merge api/thetvdb
    tvdb_folder = input_dir / "api/thetvdb"
    if tvdb_folder.is_dir():
        for file in tvdb_folder.glob("*.json"):
            target_file = repo_root / "api/thetvdb" / file.name
            merge_json_files(target_file, [file], mode="api/thetvdb")

    # Merge api/myanimelist
    mal_folder = input_dir / "api/myanimelist"
    if mal_folder.is_dir():
        for file in mal_folder.glob("*.json"):
            target_file = repo_root / "api/myanimelist" / file.name
            merge_json_files(target_file, [file], mode="api/myanimelist")

    # Merge root-level mapped/unmapped
    for root_file in ["mapped-tvdb-ids.json", "unmapped-tvdb-ids.json"]:
        artifact_files = list(input_dir.rglob(root_file))
        target_file = repo_root / root_file
        if artifact_files:
            merge_json_files(target_file, artifact_files, mode="api/myanimelist")

if __name__ == "__main__":
    main()
