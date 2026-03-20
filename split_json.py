import json
from pathlib import Path
import sys

# Input files (match your main script)
movies_json = Path("mapped-tvdb-ids-movie.json")
series_json = Path("mapped-tvdb-ids-series.json")
season_json = Path("mapped-tvdb-ids-seasons.json")
episode_json = Path("mapped-tvdb-ids-episodes.json")

# Check if anything exists
if not any(p.exists() for p in [movies_json, series_json, season_json, episode_json]):
    print("No mapped files found. Skipping split_json.py.")
    sys.exit(0)

# Directories
mal_dir = Path("api/myanimelist")
tvdb_series_dir = Path("api/thetvdb-series")
tvdb_seasons_dir = Path("api/thetvdb-seasons")
tvdb_episodes_dir = Path("api/thetvdb-episodes")
tvdb_movie_dir = Path("api/thetvdb-movie")

for d in [mal_dir, tvdb_series_dir, tvdb_seasons_dir, tvdb_episodes_dir, tvdb_movie_dir]:
    d.mkdir(parents=True, exist_ok=True)

# State
mal_entries = {}
tvdb_seen = set()

tvdb_count_series = 0
tvdb_count_movie = 0
tvdb_count_seasons = 0
tvdb_count_episodes = 0


def load_json(path: Path):
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def process_entries(data, tvdb_dir, counter_name):
    count = 0

    for entry in data:
        mal_id = entry.get("myanimelist")
        tvdb_id = entry.get("thetvdb")

        # MAL aggregation
        if mal_id is not None:
            mal_entries.setdefault(mal_id, []).append(entry)

        # TVDB split (avoid duplicates)
        if tvdb_id is not None and tvdb_id not in tvdb_seen:
            path = tvdb_dir / f"{tvdb_id}.json"
            with path.open("w", encoding="utf-8") as f:
                json.dump([entry], f, indent=4, ensure_ascii=False)

            tvdb_seen.add(tvdb_id)
            count += 1

    return count


# --- Process files ---
tvdb_count_movie = process_entries(load_json(movies_json), tvdb_movie_dir, "movie")
tvdb_count_series = process_entries(load_json(series_json), tvdb_series_dir, "series")
tvdb_count_seasons = process_entries(load_json(season_json), tvdb_seasons_dir, "season")
tvdb_count_episodes = process_entries(load_json(episode_json), tvdb_episodes_dir, "episode")


# --- Sorting ---
def sort_entries(entries):
    def entry_key(entry):
        season = entry.get("season")
        episode = entry.get("episode")

        try:
            season = int(season) if season is not None else None
        except ValueError:
            season = float('inf')

        try:
            episode = int(episode) if episode is not None else None
        except ValueError:
            episode = float('inf')

        season_sort = -1 if season is None else season
        episode_sort = -1 if episode is None else episode

        return (season_sort, episode_sort)

    return sorted(entries, key=entry_key)


# --- Write MAL files ---
mal_count = 0
for mal_id, entries in mal_entries.items():
    path = mal_dir / f"{mal_id}.json"
    with path.open("w", encoding="utf-8") as f:
        json.dump(sort_entries(entries), f, indent=4, ensure_ascii=False)
    mal_count += 1


print(
    f"Split complete.\n"
    f"MAL files: {mal_count}\n"
    f"TVDB movies: {tvdb_count_movie}\n"
    f"TVDB series: {tvdb_count_series}\n"
    f"TVDB seasons: {tvdb_count_seasons}\n"
    f"TVDB episodes: {tvdb_count_episodes}"
)