#!/usr/bin/env python3
import argparse
import json
from pathlib import Path
from typing import List, Dict, Union


def load_json(path: Path) -> Union[dict, list]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: Path, data: Union[dict, list]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def merge_dicts(d1: dict, d2: dict) -> dict:
    """Recursively merge d2 into d1."""
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


def merge_json_files(target: Path, files: List[Path], mode: str):
    """
    Merge JSON files depending on mode:
    - "anime_data" → single dict per file, merge recursively
    - "api/thetvdb" → overwrite with new data, always wrapped in a list
    - "api/myanimelist" → merge lists, deduplicate by "thetvdb"
    """
    if mode == "anime_data":
        merged: dict = {}
        for file in files:
            try:
                data = load_json(file)
                if isinstance(data, dict):
                    merged = merge_dicts(merged, data)
            except Exception as e:
                print(f"⚠ Skipping invalid JSON in {file}: {e}")

    elif mode == "api/thetvdb":
        merged: list = []
        if files:
            file = files[-1]
            try:
                data = load_json(file)
                if isinstance(data, dict):
                    merged = [data]
                elif isinstance(data, list):
                    merged = data
            except Exception as e:
                print(f"⚠ Skipping invalid JSON in {file}: {e}")

    elif mode == "api/myanimelist":
        merged_dict = {}  # key = tvdb_id
        for file in files:
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
    print(f"✅ Merged {len(files)} files → {target} ({'list' if isinstance(merged, list) else 'dict'})")



def main():
    parser = argparse.ArgumentParser(description="Merge JSON files from scraper artifacts.")
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("artifacts"),
        help="Directory containing scraper output (default: artifacts/)",
    )
    args = parser.parse_args()
    input_dir: Path = args.input_dir

    # Merge anime_data files
    anime_folder = input_dir / "anime_data"
    if anime_folder.is_dir():
        for file in anime_folder.glob("*.json"):
            merge_json_files(file, [file], mode="anime_data")

    # Merge api/thetvdb files (overwrite, always list)
    tvdb_folder = input_dir / "api/thetvdb"
    if tvdb_folder.is_dir():
        for file in tvdb_folder.glob("*.json"):
            merge_json_files(file, [file], mode="api/thetvdb")

    # Merge api/myanimelist files (deduplicate by tvdb)
    mal_folder = input_dir / "api/myanimelist"
    if mal_folder.is_dir():
        for file in mal_folder.glob("*.json"):
            merge_json_files(file, [file], mode="api/myanimelist")

    # Optionally handle root-level mapped/unmapped tvdb-ids.json
    for root_file in ["mapped-tvdb-ids.json", "unmapped-tvdb-ids.json"]:
        artifact_files = list(input_dir.rglob(root_file))
        if artifact_files:
            merge_json_files(Path(root_file), artifact_files, mode="api/myanimelist")


if __name__ == "__main__":
    main()
