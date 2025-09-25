#!/usr/bin/env python3
"""
merge_all_artifacts.py

Optimized: merges data from all folders named 'api-page-#-artifacts'
into consolidated root-level outputs.
"""

import json
import shutil
from pathlib import Path
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

FILES_TO_MERGE = ["mapped-tvdb-ids.json", "unmapped-tvdb-ids.json"]
PAGE_DIR_PATTERN = re.compile(r"^api-page-\d+-artifacts$")
ROOT = Path(".")
ANIME_DATA_OUT = ROOT / "anime_data"
API_OUT = ROOT / "api"


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


def merge_json(name: str, artifact_dirs: list[Path]):
    combined = []

    # Include existing root file if present
    root_file = ROOT / name
    if root_file.exists():
        combined.extend(load_json(root_file))

    # Merge from all api-page-#-artifacts folders
    for folder in artifact_dirs:
        file = folder / name
        if file.exists():
            combined.extend(load_json(file))

    # Write merged JSON
    with open(name, "w", encoding="utf-8") as out:
        json.dump(combined, out, indent=2, ensure_ascii=False)

    print(f"Merged {name} -> {len(combined)} items")


def copy_file(src: Path, dest: Path):
    """Copy one file, creating parent dirs if needed."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dest)


def merge_folders(src_name: str, dest: Path, artifact_dirs: list[Path], max_workers=8):
    """Merge all files from subfolders into one consolidated folder (parallelized)."""
    if not dest.exists():
        dest.mkdir(parents=True)

    tasks = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for folder in artifact_dirs:
            src = folder / src_name
            if src.exists() and src.is_dir():
                for file in src.rglob("*"):
                    if file.is_file():
                        rel_path = file.relative_to(src)
                        dest_file = dest / rel_path
                        tasks.append(executor.submit(copy_file, file, dest_file))

        # optional: progress tracking
        for future in as_completed(tasks):
            _ = future.result()

    print(f"Merged {src_name}/ into {dest}/ ({len(tasks)} files)")


if __name__ == "__main__":
    # Collect artifact dirs once
    artifact_dirs = [f for f in ROOT.iterdir() if f.is_dir() and PAGE_DIR_PATTERN.match(f.name)]

    # Merge JSON files
    for filename in FILES_TO_MERGE:
        merge_json(filename, artifact_dirs)

    # Merge folders in parallel
    merge_folders("anime_data", ANIME_DATA_OUT, artifact_dirs)
    merge_folders("api", API_OUT, artifact_dirs)
