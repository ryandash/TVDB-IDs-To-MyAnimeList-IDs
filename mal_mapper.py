#!/usr/bin/env python3
"""
mal_mapper.py

Loads tvdb id json files and attempts to map TVDB series/seasons/episodes to MyAnimeList URLs.

Outputs:
 - mapped-tvdb-ids.json   (per-episode mapping with MAL URL)
 - unmapped-tvdb-ids.json (per-episode entries that failed to map)
"""

import json
import random
import re
import time
from pathlib import Path
from typing import Optional, Union

import httpx
from rapidfuzz import fuzz
from tqdm import tqdm

# ----------------------
# Config / Constants
# ----------------------

MAPPED_OUT = "mapped-tvdb-ids.json"
UNMAPPED_SERIES_OUT = "unmapped-series.json"
UNMAPPED_SEASONS_OUT = "unmapped-seasons.json"
UNMAPPED_EPISODES_OUT = "unmapped-episodes.json"
LOG_FILE = "mapping.log"
DATA_DIR = Path("anime_data")
DATA_DIR.mkdir(exist_ok=True)

HTTP_CLIENT = httpx.Client(timeout=30)
LAST_REQUEST_TIME = 0.0

# Regex patterns
NORMALIZE_REGEX = re.compile(r"[:.!]")

# ----------------------
# Helpers
# ----------------------

def safe_load_json(path: Path) -> dict:
    """Load JSON safely; try to salvage truncated files."""
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        print(f"\n[WARN] Corrupted JSON at {e.pos}, trying salvage...")
        lines = path.read_text(encoding="utf-8").splitlines(True)

        anime_start_pattern = re.compile(r'^ {4}"\d+"\s*:\s*{$')
        for i in range(len(lines) - 1, -1, -1):
            if anime_start_pattern.match(lines[i].rstrip()):
                if i > 0 and lines[i - 1].rstrip().endswith("},"):
                    lines[i - 1] = lines[i - 1].rstrip()[:-2] + "}\n"
                lines = lines[:i] + ["}\n"]
                try:
                    data = json.loads("".join(lines))
                    path.write_text("".join(lines), encoding="utf-8")
                    print("[INFO] Salvage successful (last anime truncated).")
                    return data
                except Exception as e2:
                    print(f"[ERROR] Salvage failed: {e2}")
                    return {}
        print("[ERROR] Could not salvage JSON.")
        return {}

def load_data() -> dict:
    """Load anime data from disk (all JSONs in DATA_DIR)."""
    anime_data = {}
    for file in DATA_DIR.glob("*.json"):
        anime_info = safe_load_json(file)
        if anime_info:
            anime_data[file.stem] = anime_info
    return anime_data

def fetch_json(url: str) -> dict | None:
    """Fetch JSON from a URL with error handling."""
    try:
        resp = HTTP_CLIENT.get(url)
        if resp.status_code == 200:
            return resp.json()
    except Exception as e:
        print(f"Error fetching {url}: {e}")
    return None


def rate_limited_get(url: str, min_interval: float = 0.45, retries: int = 3) -> dict | None:
    """Rate-limited GET with retries."""
    global LAST_REQUEST_TIME

    for attempt in range(1, retries + 1):
        wait = min_interval - (time.time() - LAST_REQUEST_TIME)
        if wait > 0:
            time.sleep(wait)
        LAST_REQUEST_TIME = time.time()

        data = fetch_json(url)
        if data:
            return data
        if attempt < retries:
            time.sleep(1 + random.random())
    print(f"Giving up on {url} after {retries} attempts.")
    return None

def normalize_text(name: str) -> str:
    """Normalize anime title for better fuzzy matching."""
    if not name:
        return ""
    name = NORMALIZE_REGEX.sub("", name)
    return name.strip().lower()


# -------------------
# MAL Integration
# -------------------

def get_best_mal_id(search_term: str, anime_type: str, isSeason0: bool) -> tuple[int | None, list[str]]:
    """Search Jikan API and return the best matching MAL ID and all_titles."""
    search_lower = search_term.lower()
    normalized_search = normalize_text(search_lower)

    split_normalized_search = None
    if ":" in search_term and anime_type == "movie":
        split_normalized_search = normalize_text(
            search_lower.split(":", 1)[1].strip()
        )

    base_url = "https://api.jikan.moe/v4/anime?limit=5"
    api_url = f"{base_url}&type={anime_type}&q={normalized_search}" if anime_type else f"{base_url}&q={normalized_search}"
    data = rate_limited_get(api_url)
    search_results = data.get("data", []) if data else []

    all_titles_seen = []
    best_match = (None, 0)

    for anime in search_results:
        titles = [t["title"].lower() for t in anime.get("titles", []) if "title" in t]
        all_titles_seen.extend(titles)

        for title in titles:
            if isSeason0:
                similarity = fuzz.ratio(
                    normalize_text(title), normalized_search.split('(')[0].strip()
                )
            else:
                similarity = fuzz.ratio(
                    normalize_text(title), normalized_search
                )
            if similarity >= 85 and similarity > best_match[1]:
                best_match = (anime["mal_id"], similarity)

            if split_normalized_search and anime_type == "movie":
                parts = title.split(":", 1)
                if len(parts) > 1:
                    split_title = normalize_text(parts[1].strip())
                    split_similarity = fuzz.ratio(split_title, split_normalized_search)
                    if split_similarity >= 90 and split_similarity > best_match[1]:
                        best_match = (anime["mal_id"], split_similarity)

    if best_match[0] is not None:
        return best_match[0], []

    return None, all_titles_seen


def get_mal_episode_count(mal_id: int) -> int | None:
    data = rate_limited_get(f"https://api.jikan.moe/v4/anime/{mal_id}")
    if data:
        eps = data.get("data", {}).get("episodes")
        return eps if isinstance(eps, int) else None
    return None

def get_mal_relations(mal_id: int, offset_eps: int, season_title: str, visited=None) -> int | None:
    """Find related MAL ID that matches season_title name first, then fallback to Sequel. Skips specials."""
    if visited is None:
        visited = set()
    if mal_id in visited:
        return None
    visited.add(mal_id)

    data = rate_limited_get(f"https://api.jikan.moe/v4/anime/{mal_id}/relations")
    if not data:
        return None

    relations = data.get("data", [])
    sequel_id = None

    if season_title is not None:
        # --- Step 1: Prefer relation entry whose name matches season_title ---
        normalized_title = normalize_text(season_title)
        for rel in relations:
            for e in rel.get("entry", []):
                name = e.get("name", "")
                if fuzz.ratio(normalize_text(name), normalized_title) >= 85:
                    sequel_id = e["mal_id"]
                    print(f"Matched season title '{season_title}' in relation '{e['name']}' (relation: {rel.get('relation')})")
                    break
            if sequel_id:
                break

    # --- Step 2: Fallback to Sequel if no name match found ---
    if not sequel_id:
        sequel_id = next(
            (e["mal_id"] for rel in relations
             if rel.get("relation") == "Sequel"
             for e in rel.get("entry", [])
             if e.get("type") != "Special"),
            None
        )

    if not sequel_id:
        return None

    # --- Step 3: Validate and possibly recurse ---
    mal_eps = get_mal_episode_count(sequel_id)
    if not mal_eps:
        return None

    print(f"New mal id {sequel_id} mal_eps: {mal_eps} offset_eps: {offset_eps}")
    if mal_eps < offset_eps and mal_eps == 1:
        return get_mal_relations(sequel_id, offset_eps, season_title, visited)

    return sequel_id

def get_mal_url(mal_id: int, episode_number: Union[int, None]) -> Optional[str]:
    """
    Get a MAL URL.
    - If episode_number is None, returns the anime's MAL page.
    - Otherwise, returns the base episode URL (ending in /episode/) so you can append numbers.
    """
    if episode_number is None:
        return f"https://myanimelist.net/anime/{mal_id}"

    data = rate_limited_get(f"https://api.jikan.moe/v4/anime/{mal_id}/episodes/{episode_number}")
    if not data:
        return None

    episode_data = data.get("data")
    if not episode_data:
        return None

    full_url = episode_data.get("url")
    if not full_url:
        return None

    base_url = full_url.rsplit("/", 1)[0]
    return f"{base_url}/"

def load_mapped_lookup(mapped: list) -> dict[str, tuple[int, str]]:
    lookup = {}
    for entry in mapped:
        tvdb_id = str(entry.get("thetvdb"))
        if not tvdb_id:
            continue
        mal_url: str = entry.get("myanimelist url")
        if mal_url:
            if "myanimelist.net/anime/" in mal_url:
                parts = mal_url.strip("/").split("/")
                mal_id = int(parts[4])
                if "episode" in parts:
                    base_url = mal_url.rsplit("/", 1)[0] + "/"
                    lookup[tvdb_id] = (mal_id, base_url)
                else:
                    lookup[tvdb_id] = (mal_id, f"https://myanimelist.net/anime/{mal_id}")
        else:
            print(f"bad entry: {entry}")
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
    unmapped_series = []
    unmapped_seasons = []
    unmapped_episodes = []
    anime_data = load_data()

    for series_id, series in tqdm(anime_data.items(), total=len(anime_data), desc=f"Mapping series", unit="series"):
        series_title = series.get("TitleEnglish")
        series_aliases = series.get("Aliases") or []

        malid = None
        all_titles: list[str] = []

        if series_id in lookup:
            malid = lookup[series_id][0]
        else:
            for anime_type in ["tv", "ona", "ova"]:
                if malid:
                    break
                titles_to_try = [series_title] + series_aliases
                # Try main title first
                for title in filter(None, titles_to_try):
                    mid, titles = get_best_mal_id(title, anime_type, False)
                    all_titles.extend(titles)
                    if mid:
                        malid = mid
                        break
            
            all_titles = list(dict.fromkeys(all_titles))
            
            if malid:
                mapped.append({
                    "thetvdb url": f"https://www.thetvdb.com/dereferrer/series/{series_id}",
                    "myanimelist url": get_mal_url(malid, None),
                    "myanimelist": int(malid),
                    "thetvdb": int(series_id)
                })
            else:
                unmapped_series.append({
                    "thetvdb url":f"https://www.thetvdb.com/dereferrer/series/{series_id}",
                    "thetvdb": int(series_id),
                    "search term": series_title,
                    "aliases": series_aliases,
                    "Jikan titles": all_titles
                })
                continue

        # Initialize episode tracking
        SeasonMalID = malid
        malurl = None
        mal_eps = None
        seasons = series.get("Seasons") or {}
        episode_offset = 0
        for season_num, season_data in tqdm(seasons.items(), desc=f"  {series_id} seasons", unit="season", leave=False):
            season_id = season_data.get("ID")
            season_title = season_data.get("TitleEnglish")
            episodes = season_data.get("Episodes") or {}
            total_episodes = len(episodes)
            
            if season_id in lookup:
                SeasonMalID = lookup[season_id][0]
                malurl = lookup[season_id][1]
            else:
                if season_num != "0":
                    if not SeasonMalID and season_title:
                        mid, _ = get_best_mal_id(season_title, None, False)
                        if mid:
                            SeasonMalID = mid
                    if season_num == "1":
                        episode_offset = 0
                        mal_eps = get_mal_episode_count(SeasonMalID)
                        malurl = get_mal_url(SeasonMalID, None if total_episodes == 1 else 1)

                    if mal_eps and mal_eps == episode_offset:
                        SeasonMalID = get_mal_relations(SeasonMalID, total_episodes, season_title)
                        if SeasonMalID:
                            episode_offset = 0
                            mal_eps = get_mal_episode_count(SeasonMalID)
                            malurl = get_mal_url(SeasonMalID, None if total_episodes == 1 else 1)
                        # else:
                        #     raise RuntimeError(f"This is a bug — logic failure in season mapping. Previous malid was {SeasonMalID}")
                    
                    if SeasonMalID and SeasonMalID not in lookup:
                        mapped.append({
                            "season": season_num, 
                            "thetvdb url": f"https://www.thetvdb.com/dereferrer/season/{season_id}", 
                            "myanimelist url": get_mal_url(SeasonMalID, None),
                            "myanimelist": int(SeasonMalID),
                            "thetvdb": int(season_id)
                        })
                    else:
                        unmapped_seasons.append({
                            "season": int(season_num), 
                            "thetvdb url": f"https://www.thetvdb.com/dereferrer/season/{season_id}",
                            "thetvdb": int(season_id),
                            "previous malid": int(SeasonMalID)
                        })
                        continue
 
            mal_episode_counter = {}
            for ep_num, ep_data in tqdm(episodes.items(), desc=f"    {season_id} Season {season_num} episodes", unit="ep", leave=False):
                ep_id = ep_data.get("ID")
                ep_title = ep_data.get("TitleEnglish")
                ep_aliases = ep_data.get("Aliases") or []
                if ep_id in lookup:
                    EpisodeMALID = lookup[ep_id][0]
                    mal_episode_counter[EpisodeMALID] = mal_episode_counter.get(EpisodeMALID, 0) + 1
                    malurl = lookup[ep_id][1]
                    continue
                record = {"season": int(season_num), "episode": int(ep_num), "thetvdb url": f"https://www.thetvdb.com/dereferrer/episode/{ep_id}"}

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
                    search_terms = None
                    all_titles = None
                    if ep_title:
                        search_terms = [ep_title]

                        for alias in ep_aliases:
                            search_terms.append(f"{alias}" if ep_title else alias)
                        
                        if series_title and series_title.lower() not in ep_title.lower():
                            search_terms.append(f"{series_title} {ep_title}")

                        EpisodeMALID, all_titles = None, None
                        for term in search_terms:
                            EpisodeMALID, all_titles = get_best_mal_id(term, anime_type, True)
                            if EpisodeMALID:
                                break
                        if EpisodeMALID:
                            mal_eps = get_mal_episode_count(EpisodeMALID)
                            if EpisodeMALID not in mal_episode_counter:
                                mal_episode_counter[EpisodeMALID] = 1
                            else:
                                mal_episode_counter[EpisodeMALID] += 1
                            if mal_eps and mal_eps == 1:
                                record["myanimelist url"] = f"{get_mal_url(EpisodeMALID, None)}"
                            else:
                                episode_number = mal_episode_counter[EpisodeMALID]
                                record["myanimelist url"] = f"{get_mal_url(EpisodeMALID, episode_number)}{episode_number}"
                    
                    if EpisodeMALID:
                        record["myanimelist"] = int(EpisodeMALID)
                        record["thetvdb"] = int(ep_id)
                        mapped.append(record)
                    else:
                        record["thetvdb"] = int(ep_id)
                        record["search terms"] = search_terms
                        record["Jikan titles"] = all_titles
                        unmapped_episodes.append(record)

                elif SeasonMalID:
                    # Regular episodes
                    episode_offset += 1
                    if mal_eps and mal_eps < episode_offset:
                        SeasonMalID = get_mal_relations(SeasonMalID, total_episodes - episode_offset + 1, None)
                        if SeasonMalID:
                            mal_eps = get_mal_episode_count(SeasonMalID)
                            episode_offset = 1
                            malurl = get_mal_url(SeasonMalID, None if total_episodes == 1 else episode_offset)
                        # else:
                        #     raise RuntimeError(f"This is a bug — logic failure in episode mapping. Previous malid was {SeasonMalID}")
                    
                    if SeasonMalID:
                        episodeMALURL = f"{malurl}{episode_offset}"

                    if episodeMALURL and malurl:
                        record["myanimelist url"] = episodeMALURL
                        record["myanimelist"] = int(SeasonMalID)
                        record["thetvdb"] = int(ep_id)
                        mapped.append(record)
                    else:
                        record["thetvdb"] = int(ep_id)
                        record["previous malid"] = int(SeasonMalID)
                        unmapped_episodes.append(record)

        # Save progress after each series
        with open(MAPPED_OUT, "w", encoding="utf-8") as f:
            json.dump(mapped, f, indent=2, ensure_ascii=False)
        with open(UNMAPPED_SERIES_OUT, "w", encoding="utf-8") as f:
            json.dump(unmapped_series, f, indent=2, ensure_ascii=False)
        with open(UNMAPPED_SEASONS_OUT, "w", encoding="utf-8") as f:
            json.dump(unmapped_seasons, f, indent=2, ensure_ascii=False)
        with open(UNMAPPED_EPISODES_OUT, "w", encoding="utf-8") as f:
            json.dump(unmapped_episodes, f, indent=2, ensure_ascii=False)

        print(f"\nFinished series {series_title}. Total mapped: {len(mapped)}, unmapped series: {len(unmapped_series)} unmapped seasons: {len(unmapped_seasons)} unmapped episodes: {len(unmapped_episodes)}")

    print(f"\nMapping complete! Total mapped: {len(mapped)}, unmapped series: {len(unmapped_series)} unmapped seasons: {len(unmapped_seasons)} unmapped episodes: {len(unmapped_episodes)}")

# ----------------------
# Run
# ----------------------

if __name__ == "__main__":
    try:
        map_anime()
    finally:
        HTTP_CLIENT.close()
