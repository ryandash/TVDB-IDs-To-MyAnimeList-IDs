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

import httpx
from rapidfuzz import fuzz
from tqdm import tqdm

# ----------------------
# Config / Constants
# ----------------------

MAPPED_OUT = "mapped-tvdb-ids.json"
UNMAPPED_OUT = "unmapped-tvdb-ids.json"
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

    print(f"Failed to find MAL ID for {normalized_search}")
    return None, all_titles_seen


def get_mal_episode_count(mal_id: int) -> int | None:
    data = rate_limited_get(f"https://api.jikan.moe/v4/anime/{mal_id}")
    if data:
        eps = data.get("data", {}).get("episodes")
        return eps if isinstance(eps, int) else None
    return None

def get_mal_relations(mal_id: int, offset_eps: int, visited = None) -> int | None:
    """Find valid sequel ID (skips specials)."""
    if visited is None:
        visited = set()
    
    if mal_id in visited:
        return None
    
    visited.add(mal_id)

    data = rate_limited_get(f"https://api.jikan.moe/v4/anime/{mal_id}/relations")
    if not data:
        return None

    sequel_id = next(
        (e["mal_id"] for rel in data.get("data", [])
         if rel.get("relation") == "Sequel"
         for e in rel.get("entry", []) if e.get("type") != "Special"),
        None,
    )
    if not sequel_id:
        return None

    mal_eps = get_mal_episode_count(sequel_id)
    if not mal_eps:
        return None
    if mal_eps < offset_eps and mal_eps == 1:
        return get_mal_relations(sequel_id, offset_eps, visited)
    return sequel_id

def get_cross_ids(mal_id: int, tvdb_id: str) -> dict | None:
    """Fetch cross-IDs for an anime from animeapi.my.id."""
    data = fetch_json(f"https://animeapi.my.id/myanimelist/{mal_id}")
    if not data:
        print(f"Missing cross IDs for MAL {mal_id}")
        return None
    data["thetvdb"] = tvdb_id
    return dict(sorted(data.items()))

def load_mapped_lookup(mapped: list) -> dict[str, int | None]:
    lookup = {}
    for entry in mapped:
        tvdb_id = str(entry.get("thetvdb"))
        if not tvdb_id:
            continue
        mal_url = entry.get("myanimelist url")
        mal_id = entry.get("myanimelist")
        if mal_id:
            lookup[tvdb_id] = int(mal_id)
        elif mal_url:
            lookup[tvdb_id] = int(mal_url.split("/anime/")[1].split("/")[0])
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
    anime_data = load_data()

    for series_id, series in tqdm(anime_data.items(), total=len(anime_data), desc=f"Mapping series", unit="series"):
        series_title = series.get("TitleEnglish")
        series_aliases = series.get("Aliases") or []

        malid = None
        all_titles: list[str] = []

        if series_id in lookup:
            malid = lookup[series_id]
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
                record = {
                    "tvdb url": f"https://www.thetvdb.com/dereferrer/series/{series_id}",
                    "myanimelist url":f"https://myanimelist.net/anime/{malid}"
                }
                cross_ids = get_cross_ids(malid, series_id)
                if cross_ids:
                    record.update(cross_ids)
                else:
                    record["thetvdb"] = series_id
                mapped.append(record)
            else:
                unmapped.append({
                    "tvdb url":f"https://www.thetvdb.com/dereferrer/series/{series_id}",
                    "thetvdb": series_id,
                    "search term": series_title,
                    "aliases": series_aliases,
                    "Jikan titles": all_titles
                })
                continue

        # Initialize episode tracking
        SeasonMalID = malid
        mal_eps = 0
        seasons = series.get("Seasons") or {}
        for season_num, season_data in tqdm(seasons.items(), desc=f"  {series_id} seasons", unit="season", leave=False):
            episode_offset = 0
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
                        cross_ids = get_cross_ids(SeasonMalID, season_id)
                        if cross_ids:
                            record.update(cross_ids)
                        else:
                            record["thetvdb"] = season_id
                        mapped.append(record)
 
            Season0Mal = None
            for ep_num, ep_data in tqdm(episodes.items(), desc=f"    {season_id} Season {season_num} episodes", unit="ep", leave=False):
                ep_id = ep_data.get("ID")
                ep_title = ep_data.get("TitleEnglish")
                ep_aliases = ep_data.get("Aliases") or []
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
                    search_terms = None
                    all_titles = None
                    if Season0Mal:
                        EpisodeMALID = Season0Mal
                        print(f"\nUsing {EpisodeMALID} for {series_title} {ep_title}")
                    elif ep_title:
                        search_terms = [ep_title]

                        for alias in ep_aliases:
                            search_terms.append(f"{alias}" if ep_title else alias)
                        
                        if series_title and series_title.lower() not in ep_title.lower():
                            search_terms.append(f"{series_title} {ep_title}")

                        EpisodeMALID, all_titles = None, None
                        for term in search_terms:
                            EpisodeMALID, all_titles = get_best_mal_id(term, anime_type, True)
                            if EpisodeMALID:
                                print(f"\nMatched using search term: {term}")
                                break

                        if EpisodeMALID is None:
                            print(f"\nFailed to get anime. Tried search terms: {search_terms}")
                        else:
                            mal_eps = get_mal_episode_count(EpisodeMALID)
                            if mal_eps is None:
                                # Series not finished → don’t reset Season0Mal, just reuse it
                                print(f"\n{EpisodeMALID} has unknown episode count, continuing with Season0Mal")
                                mal_eps = 0  # treat as unlimited
                                episode_offset = 1
                                Season0Mal = EpisodeMALID
                            elif mal_eps > 1:
                                # Finished or known episode count
                                episode_offset = 1
                                Season0Mal = EpisodeMALID
                                mal_eps += 1
                    if EpisodeMALID:
                        if Season0Mal:
                            record["myanimelist url"] = f"https://myanimelist.net/anime/{EpisodeMALID}/episodes/{episode_offset}"
                        else:
                            record["myanimelist url"] = f"https://myanimelist.net/anime/{EpisodeMALID}"
                        cross_ids = get_cross_ids(EpisodeMALID, ep_id)
                        if cross_ids:
                            record.update(cross_ids)
                        else:
                            record["thetvdb"] = ep_id
                        mapped.append(record)
                    else:
                        record["thetvdb"] = ep_id
                        record["search terms"] = search_terms
                        record["Jikan titles"] = all_titles
                        unmapped.append(record)
                    
                    # Only increment if MAL reports multiple episodes OR episode count is unknown (0)
                    if mal_eps != 1:
                        episode_offset += 1

                    if mal_eps and mal_eps == episode_offset:
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
                    
                    episodeMALURL = None
                    if SeasonMalID:
                        base = f"https://myanimelist.net/anime/{SeasonMalID}"
                        episodeMALURL = base if total_episodes == 1 else f"{base}/episodes/{episode_offset}"
                    
                    if episodeMALURL:
                        record["myanimelist url"] = episodeMALURL
                        record["thetvdb"] = ep_id
                        mapped.append(record)
                    else:
                        record["myanimelist url"] = None
                        record["thetvdb"] = ep_id
                        unmapped.append(record)
                        print(f"Missing MAL mapping for {ep_id}")

        # Save progress after each series
        with open(MAPPED_OUT, "w", encoding="utf-8") as f:
            json.dump(mapped, f, indent=2, ensure_ascii=False)
        with open(UNMAPPED_OUT, "w", encoding="utf-8") as f:
            json.dump(unmapped, f, indent=2, ensure_ascii=False)

        print(f"\nFinished series {series_title}. Total mapped: {len(mapped)}, unmapped: {len(unmapped)}")

    print(f"\nMapping complete! Total mapped: {len(mapped)}, total unmapped: {len(unmapped)}")

# ----------------------
# Run
# ----------------------

if __name__ == "__main__":
    try:
        map_anime()
    finally:
        HTTP_CLIENT.close()
