#!/usr/bin/env python3
import argparse
import json
from pathlib import Path
from typing import List, Union
from concurrent.futures import ThreadPoolExecutor

JSONType = Union[dict, list]

def load_json(path: Path) -> JSONType:
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"⚠ Failed to load {path}: {e}")
        return {} if path.suffix == ".json" else []

def save_json(path: Path, data: JSONType):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def merge_dicts(d1: dict, d2: dict) -> dict:
    for k, v in d2.items():
        if k in d1:
            if isinstance(d1[k], dict) and isinstance(v, dict):
                d1[k] = merge_dicts(d1[k], v)
            elif isinstance(d1[k], list) and isinstance(v, list):
                d1[k] = list(dict.fromkeys(d1[k] + v))
            else:
                d1[k] = v
        else:
            d1[k] = v
    return d1

def collect_files(input_dir: Path, pattern: str) -> List[Path]:
    return [*input_dir.glob(pattern), *input_dir.glob(f"page-*/{pattern}")]

def merge_category(input_dir: Path, repo_root: Path, pattern: str):
    """Merge all JSON files matching pattern into one file in repo_root."""
    files = collect_files(input_dir, pattern)
    file_groups = {}
    for f in files:
        file_groups.setdefault(f.name, []).append(f)

    with ThreadPoolExecutor() as executor:
        for name, paths in file_groups.items():
            data_list = list(executor.map(load_json, paths))
            target_file = repo_root / Path(pattern).parent / name

            merged: dict = {}
            for data in data_list:
                if isinstance(data, dict):
                    merged = merge_dicts(merged, data)

            save_json(target_file, merged)

def main():
    parser = argparse.ArgumentParser(description="Merge JSON files into repo root.")
    parser.add_argument("--input-dir", type=Path, required=True)
    args = parser.parse_args()
    input_dir = args.input_dir
    repo_root = Path.cwd()

    # Merge all anime_data JSON files
    merge_category(input_dir, repo_root, "anime_data/*.json")

    # Merge root-level mapping files
    for root_file in ["mapped-tvdb-ids.json", "unmapped-series.json", "unmapped-seasons.json", "unmapped-episodes.json"]:
        target_file = repo_root / root_file
        artifact_files = collect_files(input_dir, root_file)

        # Include repo file if it exists
        if target_file.exists():
            artifact_files.append(target_file)

        if artifact_files:
            data_list = [load_json(f) for f in artifact_files]
            merged_dict = {}
            for data in data_list:
                if isinstance(data, list):
                    for entry in data:
                        tvdb_id = entry.get("thetvdb")
                        if tvdb_id:
                            merged_dict[tvdb_id] = entry
            save_json(target_file, list(merged_dict.values()))

if __name__ == "__main__":
    main()
