import aiohttp
import asyncio
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from math import ceil
from typing import List, Optional
from rapidfuzz import fuzz
from safe_jikan import SafeJikan
from urllib.parse import quote
from datetime import datetime
from tqdm.asyncio import tqdm_asyncio
from tqdm import tqdm

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
async def get_new_anime(existing_anime: List, meta_file: str | None, type_: str) -> List[MinimalAnime]:
    meta: Optional[FetchMeta] = None

    meta_path = BASE_DIR / Path(meta_file).with_suffix(".meta.json")
    if meta_path.exists():
        meta = FetchMeta(**json.loads(meta_path.read_text(encoding="utf-8")))

    first_page = await JIKAN.search_anime(type_=type_, page=1)
    pagination = first_page.get("pagination", {}).get("items", {})
    total_from_jikan = pagination.get("total", 0)
    per_page = pagination.get("per_page", 0)

    if total_from_jikan == 0:
        print("Could not read total count from Jikan pagination.")
        return []

    previously_fetched = getattr(meta, "totalFetchedFromJikan", 0) if meta else 0
    if previously_fetched >= total_from_jikan:
        print("No new entries from Jikan.")
        await update_meta(meta_path, total_from_jikan, per_page)
        # return existing_anime # For troubleshooting
        return []

    remaining = total_from_jikan - previously_fetched
    start_page = (previously_fetched // per_page) + 1
    pages_to_fetch = ceil(remaining / per_page)
    print(f"Fetching {pages_to_fetch} page(s) from page {start_page} onward...")

    newly_fetched: List[dict] = []
    for p in tqdm(range(start_page, start_page + pages_to_fetch), desc="Fetching pages from Jikan"):
        page_data = await JIKAN.search_anime(type_=type_, page=p)
        data = page_data.get("data", [])
        if not data:
            print(f"Page {p} returned no data, breaking early.")
            break
        newly_fetched.extend(data)
    print(f"Fetched {len(newly_fetched)} entries.")

    existing_ids = {int(x.malId) for x in existing_anime}
    seen_ids = set()
    filtered_new_entries = []

    for anime in newly_fetched:
        mal_id = int(anime.get("mal_id", -1))
        if mal_id > 0 and mal_id not in seen_ids and mal_id not in existing_ids:
            seen_ids.add(mal_id)
            filtered_new_entries.append(anime)

    # Convert to MinimalAnime
    new_entries: List[MinimalAnime] = []
    for a in filtered_new_entries:
        titles = [TitleEntry(title=t["title"], type=t["type"]) for t in a.get("titles", []) if t["type"].lower() != "synonym"]
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

    print(f"After dedupe: {len(new_entries)} new entries.")
    await update_meta(meta_path, total_from_jikan, per_page)
    return new_entries


async def update_meta(meta_path: Path, total: int, per_page: int):
    meta = FetchMeta(
        totalFetchedFromJikan=total, 
        perPage=per_page, 
        lastUpdatedUtc=datetime.now(timezone.utc).isoformat()
    )
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta.__dict__, f, indent=2)
    print(f"Updated meta file: {meta_path}")

async def preload_file_map() -> dict[int, Path]:
    file_map = {}
    for folder in (MOVIE_DIR, SERIES_DIR):
        for file in folder.glob("*.json"):
            try:
                with open(file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                mal_id = data.get("MalId")
                if mal_id:
                    file_map[int(mal_id)] = file
            except Exception as e:
                print(f"Failed to read {file}: {e}")
    return file_map

async def process_relations_worker(queue: asyncio.Queue, old_entries, file_map, lock):
    """Worker that continuously processes results from the queue."""
    mal_to_index = {a.malId: i for i, a in enumerate(old_entries)}
    new_entries_ordered = []  # This will hold new_entries with prequels before sequels

    while True:
        item = await queue.get()
        if item is None:
            queue.task_done()
            break

        new_anime, rel_data = item
        sequels_in_list = []

        if rel_data:
            for rel in rel_data.get("data", []):
                if rel["relation"].lower() == "sequel":
                    for e in rel["entry"]:
                        sequel_id = e["mal_id"]
                        if sequel_id in mal_to_index:
                            sequels_in_list.append(sequel_id)
                        if sequel_id in file_map:
                            path = file_map[sequel_id]
                            try:
                                path.unlink()
                                print(f"Deleted {path} (prequel {new_anime.malId} takes priority).")
                                del file_map[sequel_id]
                            except Exception as ex:
                                print(f"Failed to delete {path}: {ex}")

        async with lock:
            # Insert into old_entries
            if sequels_in_list:
                first_sequel_idx = min(mal_to_index[s] for s in sequels_in_list)
                old_entries.insert(first_sequel_idx, new_anime)
            else:
                old_entries.append(new_anime)

            # Insert into new_entries_ordered (only among new entries)
            if sequels_in_list:
                # Only consider sequels that are already in new_entries_ordered
                sequels_in_new = [s for s in sequels_in_list if any(a.malId == s for a in new_entries_ordered)]
                if sequels_in_new:
                    first_sequel_idx_new = min(
                        i for i, a in enumerate(new_entries_ordered) if a.malId in sequels_in_new
                    )
                    new_entries_ordered.insert(first_sequel_idx_new, new_anime)
                else:
                    new_entries_ordered.append(new_anime)
            else:
                new_entries_ordered.append(new_anime)

            # Rebuild index map for old_entries
            mal_to_index = {a.malId: i for i, a in enumerate(old_entries)}

        queue.task_done()

    return old_entries, new_entries_ordered


async def insert_new_entries_before_sequels(new_entries: List[MinimalAnime], old_entries: List[MinimalAnime]):
    """Sequentially fetch relations, but process I/O + CPU in background."""
    file_map = await preload_file_map()
    queue = asyncio.Queue()
    lock = asyncio.Lock()

    worker_task = asyncio.create_task(process_relations_worker(queue, old_entries, file_map, lock))

    for new_anime in tqdm(new_entries, desc="Fetching relations"):
        try:
            rel_data = await JIKAN.get_anime_relations(new_anime.malId)
        except Exception as e:
            print(f"Failed to fetch relations for {new_anime.malId}: {e}")
            rel_data = None
        await queue.put((new_anime, rel_data))

    await queue.put(None)
    await queue.join()
    merged_entries, new_entries_ordered = await worker_task

    return merged_entries, new_entries_ordered


# -----------------------------
# Search TVDB and Save
# -----------------------------
async def search_and_save_tvdb_hits(key: str, anime_list: list[MinimalAnime]):
    async with aiohttp.ClientSession(headers={
        "X-Algolia-API-Key": key,
        "X-Algolia-Application-Id": "tvshowtime"
    }) as session:
        
        for anime in tqdm(anime_list, desc="Processing TVDB hits sequentially"):
            facet_type = "movie" if anime.aniType.lower() == "movie" else "series"
            output_dir = MOVIE_DIR if facet_type == "movie" else SERIES_DIR

            if anime.year == 0:
                continue

            success = False
            for entry in anime.titles:
                if not entry.title:
                    continue

                title_variants = [entry.title]
                if ":" in entry.title:
                    title_variants.append(entry.title.split(":")[0].strip())
                
                for query in title_variants:
                    encoded_query = quote(query, safe="")

                    facet_filters = f'[[\"type:{facet_type}\"], [\"year:{anime.year}\"]]'
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

                        if hits:
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
                        print(f"Error processing {anime.malId} ({query}): {e}")

                    if success:
                        break

    print("\nAll matches saved to min_map_data/movie/ and min_map_data/series/ directories.")

async def load_anime_json(path: Path) -> List[MinimalAnime]:
    if not path.exists():
        return []
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return [
        MinimalAnime(
            malId=x["malId"],
            aniType=x["aniType"],
            year=x["year"],
            titles=[TitleEntry(**t) for t in x.get("titles", [])]
        )
        for x in data
    ]


async def save_anime_json(path: Path, anime_list: List[MinimalAnime]):
    with open(path, "w", encoding="utf-8") as f:
        json.dump([
            {
                "malId": x.malId,
                "aniType": x.aniType,
                "year": x.year,
                "titles": [t.__dict__ for t in x.titles]
            } for x in anime_list
        ], f, indent=2)
    print(f"Saved {len(anime_list)} entries to {path.name}.")

# -----------------------------
# Main
# -----------------------------
async def main():
    key = await get_latest_algolia_key()

    # --- MOVIES ---
    movie_json_path = BASE_DIR / "all_anime_movies.json"
    existing_movies = await load_anime_json(movie_json_path)
    print(f"Loaded {len(existing_movies)} movies from {movie_json_path.name}.")

    new_movies = await get_new_anime(existing_movies, "all_anime_movies", "movie")
    await save_anime_json(movie_json_path, existing_movies + new_movies)

    # --- SERIES (TV + ONA) ---
    series_json_path = BASE_DIR / "all_anime_series.json"
    old_series = await load_anime_json(series_json_path)
    print(f"Loaded {len(old_series)} series from {series_json_path.name}.")

    new_tvs = await get_new_anime(old_series, "all_tv_anime", "tv")
    new_onas = await get_new_anime(old_series, "all_ona_anime", "ona")
    new_ovas = await get_new_anime(old_series, "all_ova_anime", "ova")
    new_specials = await get_new_anime(old_series, "all_special_anime", "special")
    tv_specials = await get_new_anime(old_series, "all_tv_special_anime", "tv_special")
    all_entries, all_new_series = await insert_new_entries_before_sequels(new_tvs + new_onas + new_ovas + new_specials + tv_specials, old_series)
    await save_anime_json(series_json_path, all_entries)

    # --- SEARCH AND SAVE TO TVDB ---
    await search_and_save_tvdb_hits(key, all_new_series + new_movies)


if __name__ == "__main__":
    asyncio.run(main())
