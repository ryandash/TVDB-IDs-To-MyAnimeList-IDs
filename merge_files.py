#!/usr/bin/env python3
import argparse
import json
from pathlib import Path
from typing import List, Dict


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
        except Exception as e:
            print(f"⚠ Skipping invalid JSON in {target}: {e}")

    for file in files:
        try:
            data = load_json(file)
            if isinstance(data, list):
                merged.extend(data)
            else:
                merged.append(data)
        except Exception as e:
            print(f"⚠ Skipping invalid JSON in {file}: {e}")

    # Deduplicate by tvdb_id if present
    seen = {}
    for item in merged:
        if isinstance(item, dict) and "tvdb_id" in item:
            seen[item["tvdb_id"]] = item
        else:
            seen[len(seen)] = item  # fallback key

    merged_unique = list(seen.values())
    merged_unique.sort(key=lambda x: x.get("tvdb_id", float("inf")))

    save_json(target, merged_unique)
    print(f"✅ Merged {len(files)} files → {target} ({len(merged_unique)} unique entries)")


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
    target_folders = ["api", "anime_data"]
    root_files = ["mapped-tvdb-ids.json", "unmapped-tvdb-ids.json"]

    # Merge folder JSONs
    for folder in target_folders:
        folder_path = input_dir / folder
        if folder_path.is_dir():
            for file in folder_path.glob("*.json"):
                target = Path(folder) / file.name
                merge_json_files(target, [file])

    # Merge root-level JSONs
    for root_file in root_files:
        artifact_files = list(input_dir.rglob(root_file))
        if Path(root_file).exists():
            artifact_files.insert(0, Path(root_file))

        if artifact_files:
            merge_json_files(Path(root_file), artifact_files)


if __name__ == "__main__":
    main()
