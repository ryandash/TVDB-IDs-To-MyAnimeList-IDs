#!/usr/bin/env python3
import json
import os
from pathlib import Path
from typing import List, Dict

ARTIFACTS_DIR = Path("artifacts")
TARGET_FOLDERS = ["api", "anime_data"]
ROOT_FILES = ["mapped-tvdb-ids.json", "unmapped-tvdb-ids.json"]


def load_json(path: Path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def merge_json_files(target: Path, files: List[Path]):
    merged: List[Dict] = []

    if target.exists():
        try:
            merged.extend(load_json(target))
        except Exception:
            print(f"⚠ Skipping invalid JSON in {target}")

    for file in files:
        try:
            data = load_json(file)
            if isinstance(data, list):
                merged.extend(data)
            else:
                merged.append(data)
        except Exception:
            print(f"⚠ Skipping invalid JSON in {file}")

    # Deduplicate by tvdb_id if present
    seen = {}
    for item in merged:
        if isinstance(item, dict) and "tvdb_id" in item:
            seen[item["tvdb_id"]] = item
        else:
            seen[len(seen)] = item

    merged_unique = list(seen.values())
    merged_unique.sort(key=lambda x: x.get("tvdb_id", float("inf")))

    save_json(target, merged_unique)
    print(f"✅ Merged {len(files)} files → {target} ({len(merged_unique)} unique entries)")


def main():
    # Merge folder JSONs
    for folder in TARGET_FOLDERS:
        files = list(ARTIFACTS_DIR.rglob(f"{folder}/*.json"))
        if files:
            for name in {f.name for f in files}:
                target = Path(folder) / name
                group = [f for f in files if f.name == name]
                merge_json_files(target, group)

    # Merge root-level JSONs
    for root_file in ROOT_FILES:
        artifact_files = list(ARTIFACTS_DIR.rglob(root_file))
        if Path(root_file).exists():
            artifact_files.insert(0, Path(root_file))

        if artifact_files:
            merge_json_files(Path(root_file), artifact_files)


if __name__ == "__main__":
    main()
