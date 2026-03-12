#!/usr/bin/env python3
"""
mal_mapper.py

Attempts to map TVDB series/seasons/episodes/movies to MyAnimeList URLs.
"""

import asyncio
import json
import re
from pathlib import Path
from typing import Optional, Union

from dataclasses import dataclass
from rapidfuzz import fuzz
from safe_jikan import SafeJikan
from tqdm import tqdm
# from googleSearch import GoogleSearch

# ----------------------
# Config / Constants
# ----------------------

LOG_FILE = "mapping.log"
DATA_DIR = Path("anime_data")
DATA_DIR.mkdir(exist_ok=True)

safe_jikan = SafeJikan()
# google_search = GoogleSearch()

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

# Regex patterns
NORMALIZE_REGEX = re.compile(r"\[.*?\]|\s*-\s*#?\d+.*|[:.!]")

def normalize_text(name: str) -> str:
    """Normalize anime title for better fuzzy matching."""
    if not name:
        return ""
    name = NORMALIZE_REGEX.sub("", name)
    return name.strip().lower()


# -------------------
# MAL Integration
# -------------------

async def get_best_mal_id(search_term: str, anime_type: str, isSeason0: bool,
                          ep_title_eng: str | None = None, ep_title_jpn: str | None = None) -> tuple[int | None, list[str]]:
    """Search Jikan API and return the best matching MAL ID and all_titles.
       If isSeason0 and ep_title_eng/jpn are provided, match against individual episodes of specials."""
    search_lower = search_term.lower()
    normalized_search = normalize_text(search_lower)
    season0_search = normalized_search.split('(')[0].strip() if isSeason0 else None

    split_normalized_search = None
    if anime_type == "movie" and ":" in search_term:
        split_normalized_search = normalize_text(
            search_lower.split(":", 1)[1].strip()
        )

    data = await safe_jikan.search_anime(query=normalized_search, type_=anime_type, limit=5)

    search_results = data.get("data", []) if data else []

    all_titles_seen = set()
    best_match = (None, 0)

    normalized_eng = normalize_text(ep_title_eng) if ep_title_eng else None
    normalized_jpn = normalize_text(ep_title_jpn) if ep_title_jpn else None

    for anime in search_results:
        mal_id = anime["mal_id"]
        titles = [
            (title := t["title"].lower(), normalize_text(title))
            for t in anime.get("titles", [])
            if "title" in t
        ]
        all_titles_seen.update(raw for raw, _ in titles)

        for raw_title, normalized_title in titles:
            if normalized_title == normalized_search:
                return mal_id, []
            if isSeason0:
                similarity = fuzz.ratio(
                    normalized_title, season0_search
                )
            else:
                similarity = fuzz.ratio(
                    normalized_title, normalized_search
                )
            if similarity >= 85 and similarity > best_match[1]:
                best_match = (mal_id, similarity)

            if split_normalized_search and anime_type == "movie":
                parts = raw_title.split(":", 1)
                if len(parts) > 1:
                    split_title = normalize_text(parts[1].strip())
                    split_similarity = fuzz.ratio(split_title, split_normalized_search)
                    if split_similarity >= 90 and split_similarity > best_match[1]:
                        best_match = (mal_id, split_similarity)
        
        if isSeason0 and anime_type != "movie" and (normalized_eng or normalized_jpn):
            episode_data  = await safe_jikan.get_anime_episodes(mal_id)
            
            for episode in episode_data["data"]:
                eng = normalize_text(episode.get("title"))
                jpn = normalize_text(episode.get("title_japanese"))

                if normalized_eng and eng and fuzz.ratio(normalized_eng, eng) >= 90:
                    return mal_id, []

                if normalized_jpn and jpn and fuzz.ratio(normalized_jpn, jpn) >= 90:
                    return mal_id, []

    if best_match[0] is not None:
        return best_match[0], []

    return None, list(all_titles_seen)


async def get_mal_episode_count(mal_id: int) -> int | None:
    data = await safe_jikan.get_anime(mal_id)
    if data:
        eps = data.get("data", {}).get("episodes")
        return eps if isinstance(eps, int) else None
    return None

async def get_mal_relations(mal_id: int, offset_eps: int, season_title: str, visited=None) -> int | None:
    """Find related MAL ID that matches season_title name first, then fallback to Sequel. Skips specials."""
    if visited is None:
        visited = set()
    if mal_id in visited:
        return None
    visited.add(mal_id)

    data = await safe_jikan.get_anime_relations(mal_id)
    relations = data.get("data", []) if data else []
    if not relations:
        return None
    
    normalized_title = normalize_text(season_title) if season_title else None
    anime_cache: dict[int, dict] = {}
    title_match_id = None
    sequel_candidates: list[tuple[int, str]] = []

    sequel_entries = [
        (int(e["mal_id"]), e.get("name", ""))
        for rel in relations if rel.get("relation") == "Sequel"
        for e in rel.get("entry", [])
    ]

    for rel_id, rel_name in sequel_entries:
        if rel_id not in anime_cache:
            anime_data = await safe_jikan.get_anime(rel_id)
            if not anime_data or "data" not in anime_data:
                continue
            anime_cache[rel_id] = anime_data["data"]

        anime_info = anime_cache[rel_id]
        anime_type = (anime_info.get("type") or "").lower()
        sequel_candidates.append((rel_id, anime_type))

        # Step 1: check for title match
        if normalized_title:
            titles = [normalize_text(t["title"]) for t in anime_info.get("titles", []) if "title" in t]
            if any(fuzz.ratio(t, normalized_title) >= 90 for t in titles):
                title_match_id = rel_id
                print(f"Matched season title '{season_title}' in relation '{rel_name}'")
                break

    if title_match_id:
        sequel_id = title_match_id
    else:
        type_priority = {"tv": 3, "ona": 2, "ova": 1}
        sequel_id = max(sequel_candidates, key=lambda x: type_priority.get(x[1], 0))[0] if sequel_candidates else None

    if not sequel_id:
        return None

    anime_info = anime_cache.get(sequel_id)
    if not anime_info:
        data = await safe_jikan.get_anime(sequel_id)
        anime_info = data.get("data", {}) if data else {}

    mal_eps = anime_info.get("episodes") if isinstance(anime_info.get("episodes"), int) else 0
    anime_type = anime_info.get("type")

    print(f"New mal id {sequel_id} mal_eps: {mal_eps} offset_eps: {offset_eps}")

    if (mal_eps < offset_eps and mal_eps == 1) or (anime_type == "Special"):
        return await get_mal_relations(sequel_id, offset_eps, season_title, visited)

    return sequel_id

async def get_mal_url(mal_id: int, ep_number: Union[int, None]) -> Optional[str]:
    """
    Get a MAL URL.
    - If episode_number is None, returns the anime's MAL page.
    - Otherwise, returns the base episode URL (ending in /episode/) so you can append numbers.
    """
    if ep_number is None:
        return f"https://myanimelist.net/anime/{mal_id}"

    data = await safe_jikan.get_anime(mal_id, episode_number=ep_number)
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

@dataclass
class TVDBMatches:
    TvdbId: int
    MalId: int
    Name: str
    Url: str

MIN_MAP_DIRS = {
    "series": Path("min_map_data/series"),
    "movie": Path("min_map_data/movie"),
}

def load_existing_malids(category: str) -> dict[str, int]:
    """Load pre-known MAL IDs from min_map_data/<category>/ directories."""
    existing_lookup = {}
    dirpath = MIN_MAP_DIRS[category]

    if not dirpath.exists():
        return existing_lookup

    for file in dirpath.glob("*.json"):
        try:
            data = safe_load_json(file)
            mal_id = data.get("MalId")
            if not mal_id:
                continue
            series_id = file.stem
            existing_lookup[series_id] = int(mal_id)
        except Exception as e:
            print(f"[WARN] Skipping {file.name} — no valid MAL ID ({e})")

    return existing_lookup

async def try_titles_for_mal_id(titles):
    for t in titles:
        mid, _ = await get_best_mal_id(t, None, False)
        if mid:
            return mid
    return None

_RE_TILDE = re.compile(r'\s*~[^~]*~\s*')

def clean_title(title: str | None) -> str | None:
    if not title:
        return None

    t = _RE_TILDE.sub("", title).strip()

    return t.lower()

def build_titles_to_try(main_eng, main_jpn, series_eng, series_jpn):
    main_eng  = clean_title(main_eng)
    main_jpn  = clean_title(main_jpn)
    series_eng = clean_title(series_eng)
    series_jpn = clean_title(series_jpn)

    if main_eng and series_eng and series_eng not in main_eng:
        main_eng = f"{series_eng} {main_eng}"
    if main_jpn and series_jpn and series_jpn not in main_jpn:
        main_jpn = f"{series_jpn} {main_jpn}"

    # Prioritize Japanese title first
    titles = []
    if main_jpn:
        titles.append(main_jpn)
    if main_eng:
        titles.append(main_eng)

    return titles

# ----------------------
# Mapping
# ----------------------

async def map_anime():
    all_unmapped_series = []
    all_unmapped_seasons = []
    all_unmapped_episodes = []

    for category in ["series", "movie"]:
        category_dir = DATA_DIR / category
        if not category_dir.exists():
            continue
        # Load previously mapped data
        mapped_out = f"mapped-tvdb-ids-{category}.json"

        anime_data = {}
        for file in category_dir.glob("*.json"):
            anime_info = safe_load_json(file)
            if anime_info:
                anime_data[file.stem] = anime_info

        if Path(mapped_out).exists():
            with open(mapped_out, "r", encoding="utf-8") as f:
                oldmapped = json.load(f)
                lookup = load_mapped_lookup(oldmapped)
        else:
            lookup = {}

        existing_malids = load_existing_malids(category)

        mapped = []
        unmapped_series = []
        unmapped_seasons = []
        unmapped_episodes = []

        for series_id, series in tqdm(anime_data.items(), total=len(anime_data), desc=f"Mapping series", unit="series"):
            titles = series.get("Titles", {})
            series_title_eng = titles.get("eng")
            series_title_jpn = titles.get("jpn")
            series_aliases = series.get("Aliases") or []

            malid = None
            all_titles: list[str] = []
            should_append = True

            if series_id in existing_malids:
                malid = existing_malids[series_id]
            elif series_id in lookup:
                malid = lookup[series_id][0]
                should_append = False
            else:
                if category == "movie":
                    types = ["movie"]
                else:
                    types = ["tv", "ona", "ova"]
                for anime_type in types:
                    if malid:
                        break
                    series_titles_to_try = [series_title_jpn, series_title_eng] + series_aliases
                    # Try main title first
                    for title in filter(None, series_titles_to_try):
                        mid, titles = await get_best_mal_id(title, anime_type, False)
                        all_titles.extend(titles)
                        if mid:
                            malid = mid
                            break
                
            all_titles = list(dict.fromkeys(all_titles))
            
            series_tvdb_url = f"https://www.thetvdb.com/dereferrer/{category}/{series_id}"

            if malid and should_append:
                mapped.append({
                    "thetvdb url": series_tvdb_url,
                    "myanimelist url": await get_mal_url(malid, None),
                    "myanimelist": int(malid),
                    "thetvdb": int(series_id)
                })
            elif not malid:
                unmapped_series.append({
                    "thetvdb url":series_tvdb_url,
                    "thetvdb": series_id,
                    "search term": series_titles_to_try,
                    "aliases": series_aliases,
                    "Jikan titles": all_titles
                })
                continue
            
            if category == "movie":
                continue
            
            # Initialize episode tracking
            SeasonMalID = malid
            malurl = None
            mal_eps = None
            seasons = series.get("Seasons") or {}
            episode_offset = 0
            changeSeason = True
            for season_num, season_data in tqdm(seasons.items(), desc=f"  {series_id} seasons", unit="season", leave=False):
                season_id = season_data.get("ID")
                season_titles = season_data.get("Titles", {})
                season_title_eng = season_titles.get("eng")
                season_title_jpn = season_titles.get("jpn")
                
                titles_to_try = build_titles_to_try(season_title_eng, season_title_jpn, series_title_eng, series_title_jpn)
                episodes = season_data.get("Episodes") or {}
                total_episodes = len(episodes)
                
                if season_id in lookup:
                    SeasonMalID = lookup[season_id][0]
                    malurl = lookup[season_id][1]
                else:
                    if season_num != "0":
                        previousSeasonMalID = SeasonMalID
                        
                        if season_num != "1":
                            if mal_eps and mal_eps == episode_offset:
                                changeSeason = True
                                SeasonMalID = await get_mal_relations(SeasonMalID, total_episodes, season_title_jpn or season_title_eng)
                        
                        if not SeasonMalID and titles_to_try:
                            for title in titles_to_try:
                                mid, _ = await get_best_mal_id(title, None, False)
                                if mid:
                                    SeasonMalID = mid
                                    changeSeason = True
                                    break
                        
                        if SeasonMalID and changeSeason:
                            episode_offset = 0
                            mal_eps = await get_mal_episode_count(SeasonMalID)
                            changeSeason = False
                            malurl = await get_mal_url(SeasonMalID, None if total_episodes == 1 else 1)

                        season_tvdb_url = f"https://www.thetvdb.com/dereferrer/season/{season_id}"
                        if SeasonMalID and SeasonMalID not in lookup:
                            mapped.append({
                                "season": int(season_num), 
                                "thetvdb url": season_tvdb_url, 
                                "myanimelist url": await get_mal_url(SeasonMalID, None),
                                "myanimelist": int(SeasonMalID),
                                "thetvdb": int(season_id)
                            })
                        else:
                            unmapped_seasons.append({
                                "season": int(season_num), 
                                "thetvdb url": season_tvdb_url,
                                "thetvdb": int(season_id),
                                "previous malid": previousSeasonMalID
                            })
                            continue
    
                mal_episode_counter = {}
                for ep_num, ep_data in tqdm(episodes.items(), desc=f"    {season_id} Season {season_num} episodes", unit="ep", leave=False):
                    ep_id = ep_data.get("ID")
                    ep_titles = ep_data.get("Titles", {})
                    ep_title_eng = ep_titles.get("eng")
                    ep_title_jpn = ep_titles.get("jpn")
                    ep_titles_to_try = build_titles_to_try(ep_title_eng, ep_title_jpn, series_title_eng, series_title_jpn)

                    ep_aliases = ep_data.get("Aliases") or []
                    episode_offset += 1
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

                        EpisodeMALID = None; all_titles = None
                        UseDuckSearch = False
                        
                        if ep_titles_to_try:
                            for alias in ep_aliases:
                                if alias:
                                    ep_titles_to_try.append(alias)

                            EpisodeMALID, all_titles = None, None
                            for term in ep_titles_to_try:
                                EpisodeMALID, all_titles = await get_best_mal_id(
                                    term, anime_type, True,
                                    ep_title_eng=ep_title_eng,
                                    ep_title_jpn=ep_title_jpn
                                )
                                if EpisodeMALID:
                                    break
                            
                            # if not EpisodeMALID and ep_title_eng:
                            #     data = await safe_jikan.get_anime_relations(SeasonMalID)
                            #     relations = [
                            #         int(e["mal_id"])
                            #         for rel in data.get("data", []) 
                            #         for e in rel.get("entry", [])
                            #     ]
                            #     if relations:
                            #         EpisodeMALID = await google_search.cached_search_mal_id(series_title_eng + " " + ep_title_eng, relations)

                            if EpisodeMALID:
                                mal_eps = await get_mal_episode_count(EpisodeMALID)
                                if EpisodeMALID not in mal_episode_counter:
                                    mal_episode_counter[EpisodeMALID] = 1
                                else:
                                    mal_episode_counter[EpisodeMALID] += 1
                                if mal_eps and mal_eps == 1:
                                    record["myanimelist url"] = await get_mal_url(EpisodeMALID, None)
                                else:
                                    episode_number = mal_episode_counter[EpisodeMALID]
                                    record["myanimelist url"] = f"{await get_mal_url(EpisodeMALID, episode_number)}{episode_number}"
                        
                        if EpisodeMALID and record["myanimelist url"]:
                            record["myanimelist"] = int(EpisodeMALID)
                            record["thetvdb"] = int(ep_id)
                            mapped.append(record)
                            
                        else:
                            record["thetvdb"] = ep_id
                            record["search terms"] = ep_titles_to_try
                            record["Jikan titles"] = all_titles
                            unmapped_episodes.append(record)

                    elif SeasonMalID:
                        # Regular episodes
                        previousSeasonMalID = SeasonMalID
                        if mal_eps and mal_eps < episode_offset:
                            SeasonMalID = await get_mal_relations(SeasonMalID, total_episodes - episode_offset + 1, None)
                            if SeasonMalID:
                                mal_eps = await get_mal_episode_count(SeasonMalID)
                                episode_offset = 1
                                malurl = await get_mal_url(SeasonMalID, None if total_episodes == 1 else 1)
                            # else:
                            #     raise RuntimeError(f"This is a bug — logic failure in episode mapping. Previous malid was {SeasonMalID}")

                        if SeasonMalID and malurl:
                            episodeMALURL = f"{malurl}{episode_offset}"
                            record["myanimelist url"] = episodeMALURL
                            record["myanimelist"] = int(SeasonMalID)
                            record["thetvdb"] = int(ep_id)
                            mapped.append(record)
                        else:
                            record["thetvdb"] = int(ep_id)
                            record["previous malid"] = previousSeasonMalID
                            unmapped_episodes.append(record)
                            continue

        # Save progress after each series
        with open(mapped_out, "w", encoding="utf-8") as f:
            json.dump(mapped, f, indent=2, ensure_ascii=False)
        all_unmapped_series.extend(unmapped_series)
        all_unmapped_seasons.extend(unmapped_seasons)
        all_unmapped_episodes.extend(unmapped_episodes)

        print(f"\nTotal mapped: {len(mapped)}, unmapped series: {len(unmapped_series)} unmapped seasons: {len(unmapped_seasons)} unmapped episodes: {len(unmapped_episodes)}")

    with open("unmapped-series.json", "w", encoding="utf-8") as f:
        json.dump(all_unmapped_series, f, indent=2, ensure_ascii=False)
    with open("unmapped-seasons.json", "w", encoding="utf-8") as f:
        json.dump(all_unmapped_seasons, f, indent=2, ensure_ascii=False)
    with open("unmapped-episodes.json", "w", encoding="utf-8") as f:
        json.dump(all_unmapped_episodes, f, indent=2, ensure_ascii=False)
    print(f"\nMapping complete!")

# ----------------------
# Run
# ----------------------

if __name__ == "__main__":
    async def main():
        try:
            await map_anime()
        finally:
            await safe_jikan.close()

    asyncio.run(main())
