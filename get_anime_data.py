import aiohttp
import asyncio
import json
import os
import re
from datetime import datetime
from pathlib import Path
from math import ceil
from typing import List, Optional
from rapidfuzz import fuzz
from safe_jikan import SafeJikan
from urllib.parse import quote
from datetime import datetime

# -----------------------------
# Data Classes
# -----------------------------
from dataclasses import dataclass, field

@dataclass
class TitleEntry:
    title: str
    type: str

@dataclass
class MinimalAnime:
    malId: int
    aniType: str
    year: int
    titles: List[TitleEntry] = field(default_factory=list)

@dataclass
class FetchMeta:
    totalFetchedFromJikan: int
    perPage: int
    lastUpdatedUtc: str

@dataclass
class TVDBMatches:
    TvdbId: int
    MalId: int
    Name: str
    Url: str

# -----------------------------
# Global HTTP client and semaphore
# -----------------------------
BASE_DIR = Path("min_map_data")
MOVIE_DIR = BASE_DIR / "movie"
SERIES_DIR = BASE_DIR / "series"
BASE_DIR.mkdir(parents=True, exist_ok=True)
MOVIE_DIR.mkdir(parents=True, exist_ok=True)
SERIES_DIR.mkdir(parents=True, exist_ok=True)

# For file locks to prevent race conditions
file_locks = {}

# -----------------------------
# Helpers
# -----------------------------
def get_file_lock(path: Path):
    if path not in file_locks:
        file_locks[path] = asyncio.Lock()
    return file_locks[path]

JIKAN = SafeJikan()
# -----------------------------
# Get Latest Algolia Key
# -----------------------------
async def get_latest_algolia_key():
    async with aiohttp.ClientSession(headers={"User-Agent": "Mozilla/5.0"}) as session:
        # Get HTML page
        async with session.get("https://www.thetvdb.com") as resp:
            resp.raise_for_status()
            html_text = await resp.text()

        # Find vendor JS path
        vendor_path_match = re.search(r'src="(/build/js/vendor-[^"]+\.js)"', html_text)
        if not vendor_path_match:
            raise Exception("Vendor JS not found.")
        vendor_path = vendor_path_match.group(1)

        # Get vendor JS content
        async with session.get(f"https://www.thetvdb.com{vendor_path}") as js_resp:
            js_resp.raise_for_status()
            js_text = await js_resp.text()

        # Extract Algolia key
        key_match = re.search(r'tvshowtime","([a-f0-9]{32})"', js_text)
        if not key_match:
            raise Exception("Algolia API key not found.")

        return key_match.group(1)

# -----------------------------
# Fetch New Anime
# -----------------------------
async def get_new_anime(json_file: str, type_: str) -> List[MinimalAnime]:
    json_file = BASE_DIR / json_file
    meta_file = json_file.with_suffix(".meta.json")

    # Load existing anime
    existing_anime: List[MinimalAnime] = []
    if os.path.exists(json_file):
        with open(json_file, "r", encoding="utf-8") as f:
            existing_anime = [
                MinimalAnime(
                    malId=x["malId"],
                    aniType=x["aniType"],
                    year=x["year"],
                    titles=[TitleEntry(**t) for t in x.get("titles", [])]
                )
                for x in json.load(f)
            ]
        print(f"Loaded {len(existing_anime)} saved entries from {json_file}.")

    # Load meta
    meta: Optional[FetchMeta] = None
    print(meta_file)
    if os.path.exists(meta_file):
        with open(meta_file, "r", encoding="utf-8") as f:
            meta = FetchMeta(**json.load(f))

    first_page = await JIKAN.search_anime(type_=type_, page=1)
    total_from_jikan = first_page["pagination"]["items"]["total"]
    per_page = first_page["pagination"]["items"]["per_page"]

    if total_from_jikan == 0:
        print("Could not read total count from Jikan pagination.")
        return []

    previously_fetched = getattr(meta, "totalFetchedFromJikan", 0) if meta else 0
    if previously_fetched >= total_from_jikan:
        print("No new entries from Jikan.")
        await update_meta(meta_file, total_from_jikan, per_page)
        # return existing_anime # For troubleshooting
        return []

    remaining = total_from_jikan - previously_fetched
    start_page = (previously_fetched // per_page) + 1
    pages_to_fetch = ceil(remaining / per_page)
    print(f"Fetching {pages_to_fetch} page(s) from page {start_page} onward...")

    newly_fetched: List[dict] = []
    for p in range(start_page, start_page + pages_to_fetch):
        page_data = await JIKAN.search_anime(type_=type_, page=p)
        data = page_data.get("data", [])
        if not data:
            print(f"Page {p} returned no data, breaking early.")
            break
        newly_fetched.extend(data)
    print(f"Fetched {len(newly_fetched)} entries.")

    # Convert to MinimalAnime
    new_entries: List[MinimalAnime] = []
    for a in newly_fetched:
        titles = [TitleEntry(title=t["title"], type=t["type"]) for t in a.get("titles", [])]
        english = next((t for t in titles if t.type.lower() == "english"), None)
        if english:
            titles.remove(english)
            titles.insert(0, english)
        aired_from = a.get("aired", {}).get("from")
        if aired_from:
            try:
                year = datetime.fromisoformat(aired_from.replace("Z", "+00:00")).year
            except Exception:
                year = None
        else:
            year = a.get("year")
        year = year or 0

        new_entries.append(MinimalAnime(
            malId=a["mal_id"],
            aniType=a["type"],
            year=year,
            titles=titles
        ))

    existing_ids = {x.malId for x in existing_anime}
    filtered_new_entries = [x for x in new_entries if x.malId not in existing_ids]
    print(f"After dedupe: {len(filtered_new_entries)} new entries.")

    if filtered_new_entries:
        existing_anime.extend(filtered_new_entries)
        with open(json_file, "w", encoding="utf-8") as f:
            json.dump([
                {
                    "malId": x.malId,
                    "aniType": x.aniType,
                    "year": x.year,
                    "titles": [t.__dict__ for t in x.titles]
                } for x in existing_anime
            ], f, indent=2)
        print(f"Saved {len(existing_anime)} entries to {json_file}.")

    await update_meta(meta_file, total_from_jikan, per_page)
    return filtered_new_entries

async def update_meta(meta_file: str, total: int, per_page: int):
    meta = FetchMeta(
        totalFetchedFromJikan=total, 
        perPage=per_page, 
        lastUpdatedUtc=datetime.utcnow().isoformat()
    )
    with open(meta_file, "w", encoding="utf-8") as f:
        json.dump(meta.__dict__, f, indent=2)

# -----------------------------
# Search TVDB and Save
# -----------------------------
async def search_and_save_tvdb_hits(key: str, anime_list: List[MinimalAnime]):
    async with aiohttp.ClientSession(headers={
        "X-Algolia-API-Key": key,
        "X-Algolia-Application-Id": "tvshowtime"
    }) as session:

        async def process_anime(anime: MinimalAnime):
            facet_type = "movie" if anime.aniType.lower() == "movie" else "series"
            output_dir = MOVIE_DIR if facet_type == "movie" else SERIES_DIR

            # Prepare two sets of facet filters: first with year, then without
            facet_filters_list = []
            if anime.year != 0:
                facet_filters_list.append(f'[[\"type:{facet_type}\"],[\"year:{anime.year}\"]]')
            facet_filters_list.append(f'[[\"type:{facet_type}\"]]')
            success = False

            for entry in anime.titles:
                if not entry.title:
                    continue
                query = entry.title
                encoded_query = quote(query, safe="")

                for facet_filters in facet_filters_list:
                    facet_filter_param = f"facetFilters={quote(facet_filters, safe='')}"
                    body = {
                        "requests": [
                            {
                                "indexName": "TVDB", 
                                "params": f"query={encoded_query}&{facet_filter_param}"
                            }
                        ]
                    }

                    try:
                        async with session.post(
                            "https://tvshowtime-dsn.algolia.net/1/indexes/*/queries",
                            json=body
                        ) as resp:
                            resp.raise_for_status()
                            data = await resp.json()
                        hits = data.get("results", [{}])[0].get("hits", [])
                        
                        if hits:  # If we got any hits, process them
                            for hit in hits:
                                output_path = output_dir / f"{hit['id']}.json"
                                names = set(hit.get("aliases", []))
                                translations = hit.get("translations", {})
                                names.update(translations.values())
                                if any(fuzz.ratio(name, query) >= 90 for name in names):
                                    match = TVDBMatches(
                                        TvdbId=hit["id"],
                                        MalId=anime.malId,
                                        Name=translations.get("eng") or hit["name"],
                                        Url=hit["url"]
                                    )
                                    lock = get_file_lock(output_path)
                                    async with lock:
                                        if not output_path.exists():
                                            with open(output_path, "w", encoding="utf-8") as f:
                                                json.dump(match.__dict__, f, indent=2)
                                    success = True
                                    break
                    except Exception as e:
                        print(f"Error processing {anime.malId} ({entry.title}): {e}")
                    
                    if (success):
                        break
                if (success):
                    break

        # Run tasks concurrently (up to 20 at once)
        tasks = [process_anime(a) for a in anime_list]
        await asyncio.gather(*tasks)
    print("\nAll matches saved to min_map_data/movie/ and min_map_data/series/ directories.")

# -----------------------------
# Main
# -----------------------------
async def main():
    key = await get_latest_algolia_key()
    new_movies = await get_new_anime("all_anime_movies.json", "movie")
    new_tvs = await get_new_anime("all_tv_anime.json", "tv")
    new_onas = await get_new_anime("all_ona_anime.json", "ona")
    new_ovas = await get_new_anime("all_ova_anime.json", "ova")
    new_specials = await get_new_anime("all_special_anime.json", "special")

    all_new = new_movies + new_tvs + new_onas + new_ovas + new_specials
    await search_and_save_tvdb_hits(key, all_new)

if __name__ == "__main__":
    asyncio.run(main())
