#!/usr/bin/env python3
import argparse
import json
import shutil
from pathlib import Path
from typing import List, Union

JSONType = Union[dict, list]

def load_json(path: Path) -> JSONType:
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"Failed to load {path}: {e}")
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
    """Recursively collect all matching files from input_dir and subfolders."""
    return list(input_dir.rglob(pattern))

def merge_anime_data(input_dir: Path, repo_root: Path):
    """Move all anime_data JSONs into a single merged anime_data folder, preserving substructure."""
    merged_dir = repo_root / "anime_data"
    merged_dir.mkdir(parents=True, exist_ok=True)

    categories = ["series", "movie"]

    for category in categories:
        category_dir = merged_dir / category
        category_dir.mkdir(parents=True, exist_ok=True)

        source_files = collect_files(input_dir, f"anime_data/{category}/*.json")
        print(f"Merging {len(source_files)} {category} JSON files...")

        for src in source_files:
            dest = category_dir / src.name
            shutil.copy2(src, dest)

        count = len(list(category_dir.glob("*.json")))
        print(f"Final {category} count: {count}")

def merge_root_files(input_dir: Path, repo_root: Path):
    """Merge all mapped/unmapped files across all scraper artifacts."""
    patterns = [
        "mapped-tvdb-ids-series.json",
        "mapped-tvdb-ids-movie.json",
        "unmapped-series.json",
        "unmapped-seasons.json",
        "unmapped-episodes.json",
    ]

    for pattern in patterns:
        files = collect_files(input_dir, pattern)
        if not files:
            print(f"No files found for pattern {pattern}")
            continue

        data_list = [load_json(f) for f in files]
        merged_dict = {}

        for data in data_list:
            if isinstance(data, list):
                for entry in data:
                    tvdb_id = entry.get("thetvdb") or str(entry.get("TvdbId"))
                    if tvdb_id:
                        merged_dict[tvdb_id] = entry

        target_file = repo_root / pattern
        save_json(target_file, list(merged_dict.values()))
        print(f"Merged {len(files)} files into {target_file}")

def main():
    parser = argparse.ArgumentParser(description="Merge JSON files from multiple artifacts.")
    parser.add_argument("--input-dir", type=Path, required=True)
    args = parser.parse_args()

    input_dir = args.input_dir
    repo_root = Path.cwd()

    merge_anime_data(input_dir, repo_root)
    merge_root_files(input_dir, repo_root)

if __name__ == "__main__":
    main()
