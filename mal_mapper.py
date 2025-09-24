#!/usr/bin/env python3
"""
mal_mapper.py

Loads anime-full.json and attempts to map TVDB series/seasons/episodes
to MyAnimeList URLs.

Outputs:
 - mapped-tvdb-ids.json   (per-episode mapping with MAL URL)
 - unmapped-tvdb-ids.json (per-episode entries that failed to map)

Does NOT modify anime-full.json.
"""

import argparse
import json
from pathlib import Path
import time
import random
import re
import httpx
from rapidfuzz import fuzz
from tqdm import tqdm

parser = argparse.ArgumentParser()
parser.add_argument("--page", type=int, default=None, help="Page number to scrape")
args = parser.parse_args()
page_to_scrape = args.page

MAPPED_OUT = "mapped-tvdb-ids.json"
UNMAPPED_OUT = "unmapped-tvdb-ids.json"
LOG_FILE = "mapping.log"
DATA_DIR = Path("anime_data")
DATA_DIR.mkdir(exist_ok=True)

_http_client = httpx.Client(timeout=30)
_last_request_time = 0.0

def log(message: str) -> None:
    """Append a message to the log file."""
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(message + "\n")

def safe_load_json(path: str) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        print(f"[WARN] JSON corrupted at pos {e.pos}, attempting salvage by truncating last anime...")
        with open(path, "r", encoding="utf-8") as f:
            lines = f.readlines()
        anime_start_pattern = re.compile(r'^ {4}"\d+"\s*:\s*{$')
        for i in range(len(lines)-1, -1, -1):
            line = lines[i].rstrip()
            if anime_start_pattern.match(line):
                if i > 0:
                    prev_line = lines[i-1].rstrip()
                    if prev_line.endswith("},"):
                        lines[i-1] = prev_line[:-2] + "}\n"
                    else:
                        lines[i-1] = prev_line + "\n"
                lines = lines[:i] + ["}\n"]
                break
        else:
            print("[ERROR] Could not salvage JSON. Returning empty dict.")
            return {}
        try:
            data = json.loads("".join(lines))
            with open(path, "w", encoding="utf-8") as f:
                f.writelines(lines)
            print("[INFO] Salvage successful, last anime truncated.")
            return data
        except Exception as e2:
            print(f"[ERROR] Salvage failed: {e2}")
            return {}

def load_data(page_num: int = None) -> dict: 
    anime_data = {}
    if page_num is not None:
        # Load only from the requested page directory
        page_dir = DATA_DIR / f"Page {page_num}"
        if not page_dir.exists():
            print(f"[WARN] Page directory {page_dir} does not exist.")
            return {}
        for file in page_dir.glob("*.json"):
            anime_info = safe_load_json(file)
            if anime_info:
                anime_data[file.stem] = anime_info
    else:
        # Fallback: load everything from all Page subdirs
        for page_dir in DATA_DIR.glob("Page *"):
            if not page_dir.is_dir():
                continue
            for file in page_dir.glob("*.json"):
                anime_info = safe_load_json(file)
                if anime_info:
                    anime_data[file.stem] = anime_info
    return anime_data

def fetch_json(url: str) -> dict | None:
    """Fetch JSON from a URL with error handling."""
    try:
        resp = _http_client.get(url)
        if resp.status_code == 200:
            return resp.json()
    except Exception as e:
        log(f"Error fetching {url}: {e}")
    return None


def rate_limited_get(url: str, min_interval: float = 0.45, max_retries: int = 3) -> dict | None:
    """Fetch JSON with retries and rate limiting, using fetch_json."""
    global _last_request_time

    for attempt in range(1, max_retries + 1):
        # Rate limiting
        now = time.time()
        wait_time = min_interval - (now - _last_request_time)
        if wait_time > 0:
            time.sleep(wait_time)
        _last_request_time = time.time()

        data = fetch_json(url)
        if data is not None:
            return data

        # Retry delay
        if attempt < max_retries:
            time.sleep(1 + random.random())
        else:
            log(f"Giving up on {url} after {max_retries} attempts.")

    return None

SEASON_REGEX = re.compile(r"(\s|\.)S[0-9]{1,2}")
ALT_NAME_REGEX = re.compile(r"\s*~(\w|[0-9]|\s)+~")
NATIVE_NAME_REGEX = re.compile(r"\((\w|[0-9]|\s)+\)$")
AMPERSAND_REGEX = re.compile(r"\s?&\s?")
HASH_REGEX = re.compile(r"#")
JELLYFIN_FOLDER_FORMAT_REGEX = re.compile(r"\([0-9]{4}\)\s*\[(\w|[0-9]|-)+\]$")
NORMALIZE_REGEX = re.compile(r"[:.!]")

EPISODE_CODE_REGEX = re.compile(r"S\d+E\d+")

def normalize_text(name: str) -> str:
    """Normalize anime title for better fuzzy matching."""
    if not name:
        return ""
    name = SEASON_REGEX.sub("", name)
    name = ALT_NAME_REGEX.sub("", name)
    name = NATIVE_NAME_REGEX.sub("", name)
    name = AMPERSAND_REGEX.sub(" and ", name)
    name = HASH_REGEX.sub(" ", name)
    name = JELLYFIN_FOLDER_FORMAT_REGEX.sub("", name)
    name = NORMALIZE_REGEX.sub("", name)
    return name.strip().lower()


# -------------------
# MAL Integration
# -------------------

def get_best_mal_id(search_term: str, anime_type: str, isSeason0: bool) -> int | None:
    """Search Jikan API and return the best matching MAL ID."""
    search_lower = search_term.lower()
    normalized_search = normalize_text(search_lower)

    split_normalized_search = None
    if ":" in search_term and anime_type == "movie":
        split_normalized_search = normalize_text(
            search_lower.split(":", 1)[1].strip()
        )

    base_url = "https://api.jikan.moe/v4/anime?limit=3"
    if anime_type:
        api_url = f"{base_url}&type={anime_type}&q={normalized_search}"
    else:
        api_url = f"{base_url}&q={normalized_search}"
    data = rate_limited_get(api_url)
    search_results = data.get("data", []) if data else []

    if not search_results:
        log(f"Failed to find data for {normalized_search}")
        return None

    for anime in search_results:
        all_titles = [
            t["title"].lower()
            for t in anime.get("titles", [])
            if "title" in t
        ]
        for title in all_titles:
            if isSeason0:
                similarity = fuzz.token_sort_ratio(
                    normalize_text(title), normalized_search.split('(')[0].strip()
                )
            else:
                similarity = fuzz.ratio(
                    normalize_text(title), normalized_search
                )
            if similarity >= 85:
                return anime["mal_id"]
            if split_normalized_search and anime_type == "movie":
                parts = title.split(":", 1)
                if len(parts) > 1:
                    split_title = normalize_text(parts[1].strip())
                    if fuzz.ratio(split_title, split_normalized_search) >= 90:
                        return anime["mal_id"]

    log(f"Failed to find MAL ID for {normalized_search}")
    return None


def get_mal_episode_count(mal_id: int) -> int | None:
    """Fetch total episode count for an anime ID."""
    url = f"https://api.jikan.moe/v4/anime/{mal_id}"
    data = rate_limited_get(url)
    if not data:
        return None
    episodes = data.get("data", {}).get("episodes")
    return episodes if isinstance(episodes, int) else None


def get_mal_relations(mal_id: int, offset_eps: int) -> int | None:
    """
    Fetch the next valid MAL sequel ID for a given anime ID.
    
    Skips 'Special' types and keeps recursing until a valid sequel
    with enough episodes is found. Returns (sequel_id, adjusted_offset),
    or None if no valid sequel exists.
    """
    url = f"https://api.jikan.moe/v4/anime/{mal_id}/relations"
    data = rate_limited_get(url)
    if not data:
        return None

    # Find first sequel that is not a "Special"
    sequel_id = next(
        (
            entry.get("mal_id")
            for rel in data.get("data", [])
            if rel.get("relation") == "Sequel"
            for entry in rel.get("entry", [])
            if entry.get("type") != "Special"
        ),
        None,
    )
    if not sequel_id:
        return None

    mal_eps = get_mal_episode_count(sequel_id)
    if not mal_eps:
        return None

    # mal_eps < offset_eps means mal season less episodes, if not movie, special, or ova this is fine OR offset_eps should be 1 ELSE skip to next MALID
    if mal_eps < offset_eps and mal_eps == 1:
        return get_mal_relations(sequel_id, offset_eps)

    return sequel_id

def get_cross_ids(mal_id: int, tvdb_id: str) -> dict | None:
    """Fetch cross-IDs for an anime from animeapi.my.id."""
    url = f"https://animeapi.my.id/myanimelist/{mal_id}"
    data = fetch_json(url)
    if not data:
        log(f"Failed to find other IDs for MAL ID {mal_id}")
        return None
    data["thetvdb"] = tvdb_id
    return dict(sorted(data.items()))

def load_mapped_lookup(mapped: list) -> dict[str, int | None]:
    lookup = {}
    for entry in mapped:
        tvdb_id = str(entry.get("thetvdb"))
        if not tvdb_id:
            continue

        if "myanimelist" in entry and entry["myanimelist"]:
            lookup[tvdb_id] = int(entry["myanimelist"])
        elif "myanimelist url" in entry and entry["myanimelist url"]:
            lookup[tvdb_id] = int(entry["myanimelist url"].replace("https://myanimelist.net/anime/", "").split('/')[0])
        else:
            lookup[tvdb_id] = None

    return lookup

# ----------------------
# Mapping
# ----------------------
def map_anime():
    # Load previously mapped data
    if Path(MAPPED_OUT).exists():
        with open(MAPPED_OUT, "r", encoding="utf-8") as f:
            oldmapped = json.load(f)
            lookup = load_mapped_lookup(oldmapped)
    else:
        lookup = {}

    mapped = []
    unmapped = []
    anime_data = load_data(page_to_scrape)
    total_series = len(anime_data)

    for series_id, series in tqdm(anime_data.items(), total=total_series, desc="Mapping series", unit="series"):
        series_title = series.get("TitleEnglish")
        aliases = series.get("Aliases") or []

        if series_id in lookup:
            # Already mapped series: populate only needed variables
            malid = lookup[series_id]
        else:
            malid = get_best_mal_id(series_title, None, False) if series_title else None
            if not malid:
                for alias in aliases:
                    candidate_id = get_best_mal_id(alias, None, False)
                    if candidate_id:
                        malid = candidate_id
                        break
            if malid:
                record = {"tvdb url": f"https://www.thetvdb.com/dereferrer/series/{series_id}", "myanimelist url":f"https://myanimelist.net/anime/{malid}"}
                cross_ids = get_cross_ids(malid, series_id) or {"thetvdb": series_id}
                if cross_ids:
                    record.update(cross_ids)
                mapped.append(record)
            else:
                unmapped.append({"tvdb url":f"https://www.thetvdb.com/dereferrer/series/{series_id}", "thetvdb": series_id, "myanimelist": None, "myanimelist url": None})

        # Initialize episode tracking
        SeasonMalID = malid
        episode_offset = 0
        mal_eps = 0
        seasons = series.get("Seasons") or {}
        total_seasons = len(seasons)

        for season_num, season_data in tqdm(seasons.items(), desc=f"  {series_title} seasons", unit="season", leave=False):
            season_id = season_data.get("ID")
            episodes = season_data.get("Episodes") or {}
            total_episodes = len(episodes)

            if season_id in lookup:
                SeasonMalID = lookup[season_id]
            else:
                if season_num != "0" and SeasonMalID:
                    if season_num == "1":
                        episode_offset = 0
                        mal_eps = get_mal_episode_count(SeasonMalID)

                    if SeasonMalID not in lookup:
                        record = {"season": season_num, "tvdb url": f"https://www.thetvdb.com/dereferrer/season/{season_id}", "myanimelist url": f"https://myanimelist.net/anime/{SeasonMalID}"}
                        cross_ids = get_cross_ids(SeasonMalID, season_id) or {"thetvdb": season_id}
                        if cross_ids:
                            record.update(cross_ids)
                        mapped.append(record)

            Season0Mal = None    
            for ep_num, ep_data in tqdm(episodes.items(), desc=f"    Season {season_num} episodes", unit="ep", leave=False):
                ep_id = ep_data.get("ID")
                ep_title = ep_data.get("TitleEnglish")
                if ep_id in lookup:
                    continue
                record = {"season": season_num, "episode": ep_num, "tvdb url": f"https://www.thetvdb.com/dereferrer/episode/{ep_id}"}

                if season_num == "0":
                    # Specials
                    type_mapping = {
                        "Movies": "movie",
                        # "Episodic Special": "special", MAL commonly has a different type so these commonly fail
                        # "OVA": "ova",
                        # "Pilots": "ova",
                        # "Season Recaps": "tv_special",
                    }
                    anime_type = type_mapping.get(ep_data.get("TYPE"))

                    EpisodeMALID = None
                    if Season0Mal:
                        EpisodeMALID = Season0Mal
                        print(f"Using {EpisodeMALID} for {series_title} {ep_title}")
                    elif ep_title:
                        search_term = ep_title
                        if anime_type != "movie" and series_title.lower() not in ep_title.lower():
                            search_term = f"{series_title} {ep_title}"

                        EpisodeMALID = get_best_mal_id(search_term, anime_type, True)
                        if EpisodeMALID is None:
                            print(f"Failed to get anime {search_term}")
                        else:
                            mal_eps = get_mal_episode_count(EpisodeMALID) + 1
                            if mal_eps > 2:
                                episode_offset = 1
                                Season0Mal = EpisodeMALID
                    if EpisodeMALID:
                        if Season0Mal:
                            record["myanimelist url"] = f"https://myanimelist.net/anime/{EpisodeMALID}/episodes/{episode_offset}"
                        else:
                            record["myanimelist url"] = f"https://myanimelist.net/anime/{EpisodeMALID}"
                        cross_ids = get_cross_ids(EpisodeMALID, ep_id)
                        if cross_ids:
                            record.update(cross_ids)
                        mapped.append(record)
                    else:
                        record["myanimelist url"] = None
                        unmapped.append(record)
                    
                    if mal_eps > episode_offset != 0:
                        episode_offset += 1
                        if mal_eps == episode_offset:
                            mal_eps = 0
                            episode_offset = 0
                            Season0Mal = None

                elif SeasonMalID:
                    # Regular episodes
                    episode_offset += 1
                    if mal_eps and mal_eps < episode_offset:
                        SeasonMalID = get_mal_relations(SeasonMalID, total_episodes - episode_offset + 1)
                        if SeasonMalID:
                            mal_eps = get_mal_episode_count(SeasonMalID)
                            episode_offset = 1

                    api_url = (
                        f"https://api.jikan.moe/v4/anime/{SeasonMalID}"
                        if total_episodes == 1
                        else f"https://api.jikan.moe/v4/anime/{SeasonMalID}/episodes/{episode_offset}"
                    )
                    data = rate_limited_get(api_url)
                    episodeMALURL = data.get("data", {}).get("url") if data else None
                    record["myanimelist url"] = episodeMALURL
                    record["thetvdb"] = ep_id

                    if episodeMALURL:
                        mapped.append(record)
                    else:
                        unmapped.append(record)
                        log(f"Missing MAL mapping for {api_url}")

        # Save progress after each series
        with open(MAPPED_OUT, "w", encoding="utf-8") as f:
            json.dump(mapped, f, indent=2, ensure_ascii=False)
        with open(UNMAPPED_OUT, "w", encoding="utf-8") as f:
            json.dump(unmapped, f, indent=2, ensure_ascii=False)

        print(f"Finished series {series_title}. Total mapped: {len(mapped)}, unmapped: {len(unmapped)}\n")

    print(f"Mapping complete! Total mapped: {len(mapped)}, total unmapped: {len(unmapped)}")

# ----------------------
# Run
# ----------------------
if __name__ == "__main__":
    map_anime()
