import asyncio
import aiohttp
import argparse
import json
import networkx as nx
import re

from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from math import ceil
from pathlib import Path
from typing import List, Optional
from urllib.parse import quote

from rapidfuzz import fuzz
from tqdm import tqdm
from safe_jikan import SafeJikan

@dataclass
class TitleEntry:
    title: str
    type: str

@dataclass
class MinimalAnime:
    malId: int
    type: str
    year: int
    episodes: int = 0
    titles: List[TitleEntry] = field(default_factory=list)

@dataclass
class SortedListsOfMinimalAnime:
    groups: List[List[MinimalAnime]] = field(default_factory=list)

@dataclass
class FetchMeta:
    totalFetchedFromJikan: int
    perPage: int
    lastUpdatedUtc: str

@dataclass
class TVDBMatches:
    TvdbId: int
    MalIds: List[int]
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

# -----------------------------
# Helpers
# -----------------------------
from collections import defaultdict

file_locks: dict[Path, asyncio.Lock] = defaultdict(asyncio.Lock)
def get_file_lock(path: Path):
    return file_locks[path]

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
TITLE_PRIORITY = {
    "japanese": 0,
    "english": 1,
    "default": 2,
}

async def get_new_anime(existing_anime: List, type_: str) -> List[MinimalAnime]:
    meta: Optional[FetchMeta] = None

    meta_path = BASE_DIR / Path(type_).with_suffix(".meta.json")
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
        titles = [
            TitleEntry(title=t["title"], type=t["type"])
            for t in a.get("titles", [])
            if t["type"].lower() != "synonym"
        ]
        titles.sort(key=lambda t: TITLE_PRIORITY.get(t.type.lower(), 3))

        aired_from = a.get("aired", {}).get("from")
        try:
            year = datetime.fromisoformat(aired_from.replace("Z","+00:00")).year if aired_from else 0
        except Exception:
            year = 0

        new_entries.append(MinimalAnime(
            malId=a["mal_id"],
            type=a["type"],
            year=year,
            episodes=a.get("episodes") or 0,
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

REMOVE_TYPES = {"ova", "special", "tv_special"}
TARGET_TYPES = {"tv", "ona"}
def remove_unwanted_types_and_rewire(G: nx.DiGraph, mal_to_anime: dict[int, MinimalAnime]):
    nodes_to_remove = []

    for node in G.nodes:
        anime_type = mal_to_anime[node].type.lower()
        if anime_type not in REMOVE_TYPES:
            continue

        preds = list(G.predecessors(node))
        succs = list(G.successors(node))

        # Check if connected to at least one TV/ONA node
        connected_to_target = any(
            mal_to_anime[p].type.lower() in TARGET_TYPES for p in preds
        ) or any(
            mal_to_anime[s].type.lower() in TARGET_TYPES for s in succs
        )

        if connected_to_target:
            nodes_to_remove.append(node)

    for node in nodes_to_remove:
        preds = list(G.predecessors(node))
        succs = list(G.successors(node))

        # Rewire connections
        for p in preds:
            for s in succs:
                if p != s:
                    G.add_edge(p, s)

        G.remove_node(node)

REMOVE_IF_ISOLATED = {"special", "tv_special", "movie"}
UNWANTED_GROUP_TYPES = {"movie", "special", "tv_special"}

async def make_main_path_relations_map(
    new_entries: List[MinimalAnime],
    old_entries: List[MinimalAnime]
) -> SortedListsOfMinimalAnime:
    """
    Insert new entries in proper order using relations.
    Returns:
        - all_sorted_groups: SortedListsOfMinimalAnime containing all entries (old + new), grouped by franchise.
        - new_sorted_groups: SortedListsOfMinimalAnime containing only new entries, grouped similarly.
    """
    mal_to_anime = {a.malId: a for a in old_entries}
    mal_to_anime.update({a.malId: a for a in new_entries})
    all_entries = list(mal_to_anime.values())

    relations_map = {}

    async def fetch_relations_sequential(anime_list: List[MinimalAnime], desc: str):
        for anime in tqdm(anime_list, desc=desc):
            try:
                result = await JIKAN.get_anime_relations(anime.malId)
                relations_map[anime.malId] = result
            except Exception as e:
                relations_map[anime.malId] = e

    # Fetch new entries first
    await fetch_relations_sequential(new_entries, "Fetching relations (new entries)")

    # Then old entries
    await fetch_relations_sequential(old_entries, "Fetching relations (old entries)")
    
    # -----------------------------
    # Build relation graph
    # -----------------------------
    G_main = nx.DiGraph()
    for anime in all_entries:
        G_main.add_node(anime.malId)

    forward_edges = set()
    reverse_edges = set()

    for anime in all_entries:
        rel_data = relations_map.get(anime.malId)
        if isinstance(rel_data, Exception):
            continue
        for rel in rel_data.get("data", []):
            rel_type = rel.get("relation", "").lower()
            for e in rel.get("entry", []):
                other = e.get("mal_id")
                if not other:
                    continue
                if rel_type == "sequel":
                    forward_edges.add((anime.malId, other))
                elif rel_type == "prequel":
                    reverse_edges.add((other, anime.malId))

    # Only add edges that exist in BOTH directions -> confirmed main path
    for edge in forward_edges:
        if edge in reverse_edges:
            G_main.add_edge(*edge)

    nodes_to_remove = []

    for node in G_main.nodes:
        anime = mal_to_anime[node]
        if (
            anime.type.lower() in REMOVE_IF_ISOLATED
            and G_main.in_degree(node) == 0
            and G_main.out_degree(node) == 0
        ):
            nodes_to_remove.append(node)

    remove_unwanted_types_and_rewire(G_main, mal_to_anime)

    G_main.remove_nodes_from(nodes_to_remove)

    # Topological sort to linearize main path
    try:
        sorted_ids = list(nx.topological_sort(G_main))
    except nx.NetworkXUnfeasible:
        # Handle cycles: remove self-loops and 2-node cycles
        for node in list(G_main.nodes):
            if G_main.has_edge(node, node):
                G_main.remove_edge(node, node)
        for i, cycle in enumerate(nx.simple_cycles(G_main)):
            if len(cycle) == 2:
                a, b = cycle
                if G_main.has_edge(a, b) and a > b:
                    G_main.remove_edge(a, b)
                elif G_main.has_edge(b, a) and b > a:
                    G_main.remove_edge(b, a)
        try:
            sorted_ids = list(nx.topological_sort(G_main))
        except nx.NetworkXUnfeasible:
            print("❌ Cannot sort even after breaking cycles")

    # -----------------------------
    # Build groups along main path only
    # -----------------------------
    visited = set()
    all_sorted_groups: List[List[MinimalAnime]] = []

    def dfs_group(node_id: int, current_group: list[int]):
        if node_id in visited:
            return
        visited.add(node_id)
        current_group.append(node_id)
        for successor in G_main.successors(node_id):
            dfs_group(successor, current_group)

    for mal_id in sorted_ids:
        if mal_id not in visited:
            group_ids: List[int] = []
            dfs_group(mal_id, group_ids)
            if group_ids:
                group_anime = [mal_to_anime[m] for m in group_ids]
                all_sorted_groups.append(group_anime)

    all_sorted_groups = [
        group for group in all_sorted_groups
        if not all(a.type.lower() in UNWANTED_GROUP_TYPES for a in group)
    ]

    return SortedListsOfMinimalAnime(groups=all_sorted_groups)


# -----------------------------
# Search TVDB and Save
# -----------------------------
async def search_and_save_tvdb_hits(key: str, grouped: SortedListsOfMinimalAnime, max_concurrent_groups: int = 4):
    semaphore = asyncio.Semaphore(max_concurrent_groups)

    async with aiohttp.ClientSession(headers={
        "X-Algolia-API-Key": key,
        "X-Algolia-Application-Id": "tvshowtime"
    }) as session:

        async def worker(group: List[MinimalAnime]):
            async with semaphore:
                await process_group(session, group)

        tasks = [
            asyncio.create_task(worker(group))
            for group in grouped.groups
        ]

        for f in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Processing groups"):
            await f

    print("\nAll matches saved to min_map_data/movie/ and min_map_data/series/ directories.")

from bs4 import BeautifulSoup

async def fetch_html(session: aiohttp.ClientSession, url: str, retries=3, delay=3) -> str:
    for attempt in range(1, retries+1):
        try:
            async with session.get(url) as resp:
                if resp.status != 200:
                    raise RuntimeError(f"Status {resp.status}")
                return await resp.text()
        except Exception as e:
            if attempt < retries:
                await asyncio.sleep(delay * attempt)
            else:
                print(f"[FAIL] Could not fetch {url} after {retries} retries: {e}")
                return ""


async def get_tvdb_total_episodes(session: aiohttp.ClientSession, url: str) -> int:
    html = await fetch_html(session, url)
    if not html:
        return 0

    soup = BeautifulSoup(html, "html.parser")

    total_eps = 0

    season_rows = soup.select('#seasons-official table tbody tr')[2:-1]

    for s in season_rows:
        num_eps_elem = s.select_one('td:nth-child(4)')
        if not num_eps_elem:
            continue

        try:
            num_eps = int(num_eps_elem.get_text(strip=True))
            total_eps += num_eps
        except:
            continue

    return total_eps

def match_tvdb_to_mal(group: List[MinimalAnime], tvdb_total: int):
    remaining = tvdb_total
    matched = []

    for anime in group:
        if not anime.episodes or anime.episodes <= 0:
            continue

        if remaining >= anime.episodes:
            matched.append(anime)
            remaining -= anime.episodes
        else:
            break

    return matched, remaining

PAREN_REGEX = re.compile(r"\s*\([^)]*\)")
def remove_parentheses(s: str) -> str:
    """Remove anything between ( and ) including the parentheses."""
    return PAREN_REGEX.sub("", s).strip()

async def process_group(session: aiohttp.ClientSession, group: List[MinimalAnime]):
    remaining_group = group.copy()

    while remaining_group:
        anime = remaining_group[0]

        facet_type = "movie" if anime.type.lower() == "movie" else "series"
        output_dir = MOVIE_DIR if facet_type == "movie" else SERIES_DIR

        if anime.year == 0:
            remaining_group.pop(0)
            continue

        requests_payload = []
        query_map = {}
        idx = 0

        for entry in anime.titles:
            if not entry.title:
                continue

            clean_title = remove_parentheses(entry.title)
            title_variants = [clean_title]

            if ":" in clean_title:
                title_variants.append(clean_title.split(":")[0].strip())

            for query in title_variants:
                encoded_query = quote(query, safe="")
                facet_filters = f'[[\"type:{facet_type}\"], [\"year:{anime.year}\"]]'
                facet_filter_param = f"facetFilters={quote(facet_filters, safe='')}"

                requests_payload.append({
                    "indexName": "TVDB",
                    "params": f"query={encoded_query}&{facet_filter_param}"
                })

                query_map[idx] = query
                idx += 1

        if not requests_payload:
            remaining_group.pop(0)
            continue

        try:
            body = {"requests": requests_payload}
            async with session.post(
                "https://tvshowtime-dsn.algolia.net/1/indexes/*/queries",
                json=body
            ) as resp:
                resp.raise_for_status()
                data = await resp.json()
        except Exception as e:
            print(f"Error querying TVDB for {anime.malId}: {e}")
            remaining_group.pop(0)
            continue

        matched_any = False

        for i, result in enumerate(data.get("results", [])):
            hits = result.get("hits", [])
            query = query_map[i]

            for hit in hits:
                output_path = output_dir / f"{hit['id']}.json"

                names = set(hit.get("aliases", []))
                translations = hit.get("translations", {})
                names.update(translations.values())
                clean_names = [remove_parentheses(name) for name in names]

                if any(fuzz.ratio(clean_name, query) >= 90 for clean_name in clean_names):

                    if len(remaining_group) == 1 or anime.type.lower() == "movie":
                        matched_anime = [remaining_group[0]]
                    else:
                        tvdb_total_eps = await get_tvdb_total_episodes(session, hit["url"])
                        matched_anime, _ = match_tvdb_to_mal(remaining_group, tvdb_total_eps)

                    if not matched_anime:
                        continue
                    matched_ids = [a.malId for a in matched_anime]
                    remaining_group = [
                        a for a in remaining_group
                        if a.malId not in matched_ids
                    ]

                    match = TVDBMatches(
                        TvdbId=hit["id"],
                        MalIds=matched_ids,
                        Name=translations.get("eng") or hit["name"],
                        Url=hit["url"]
                    )

                    lock = get_file_lock(output_path)
                    async with lock:
                        if not output_path.exists():
                            with open(output_path, "w", encoding="utf-8") as f:
                                json.dump(asdict(match), f, indent=2)

                    matched_any = True
                    break

            if matched_any:
                break

        if not matched_any:
            remaining_group.pop(0)

# -----------------------------
# Load / Save Anime JSON (grouped)
# -----------------------------
async def load_anime_json(path: Path) -> SortedListsOfMinimalAnime:
    if not path.exists():
        return SortedListsOfMinimalAnime()

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    groups = [
        [
            MinimalAnime(
                **{**a, "titles": [TitleEntry(**t) for t in a.get("titles", [])]}
            )
            for a in group_data
        ]
        for group_data in data
    ]

    return SortedListsOfMinimalAnime(groups=groups)


async def save_anime_json(path: Path, anime_groups: SortedListsOfMinimalAnime):
    """
    Save grouped anime data to JSON.
    """
    with open(path, "w", encoding="utf-8") as f:
        json.dump(
            [
                [asdict(a) for a in group]
                for group in anime_groups.groups
            ],
            f,
            indent=2
        )
    total_count = sum(len(g) for g in anime_groups.groups)
    print(f"Saved {total_count} entries in {len(anime_groups.groups)} groups to {path.name}.")


# -----------------------------
# Main
# -----------------------------
async def main():
    global JIKAN
    JIKAN = SafeJikan()
    await JIKAN.preload_disk_cache()

    # Fetch Algolia key for TVDB
    key = await get_latest_algolia_key()

    # -----------------------------
    # Categories and JSON paths (all under BASE_DIR)
    # -----------------------------
    categories = {
        "movie": BASE_DIR / "movies.json",
        "tv": BASE_DIR / "tv.json",
        "ona": BASE_DIR / "ona.json",
        "ova": BASE_DIR / "ova.json",
        "special": BASE_DIR / "special.json",
        "tv_special": BASE_DIR / "tv_special.json",
    }

    # -----------------------------
    # Load existing entries per category
    # -----------------------------
    old_entries_per_category = {}
    for cat, path in categories.items():
        groups = await load_anime_json(path)
        existing_flat = [a for group in groups.groups for a in group]
        old_entries_per_category[cat] = existing_flat
        print(f"Loaded {len(existing_flat)} entries for category '{cat}' from {path.name}.")

    # -----------------------------
    # Fetch new entries per category
    # -----------------------------
    new_entries_per_category = {}
    for cat in categories.keys():
        print(f"\nFetching new entries for category: {cat}")
        entries = await get_new_anime(old_entries_per_category[cat], cat)
        new_entries_per_category[cat] = entries

    # -----------------------------
    # Append new entries to old and save per category
    # -----------------------------
    for cat, path in categories.items():
        combined = old_entries_per_category[cat] + new_entries_per_category[cat]
        # Remove duplicates
        seen = set()
        deduped = []
        for a in combined:
            if a.malId not in seen:
                deduped.append(a)
                seen.add(a.malId)
        await save_anime_json(path, SortedListsOfMinimalAnime(groups=[deduped]))

    # -----------------------------
    # Build relationships
    # -----------------------------
    all_new_entries = [a for entries in new_entries_per_category.values() for a in entries]
    all_old_entries = [a for entries in old_entries_per_category.values() for a in entries]
    main_path_groups = await make_main_path_relations_map(all_new_entries, all_old_entries)

    # Load old main_path_groups.json if it exists
    relations_path = BASE_DIR / "main_path_groups.json"
    old_main_path_groups = await load_anime_json(relations_path)

    # Build a set of MAL IDs from the old main path groups
    old_main_path_ids = {a.malId for group in old_main_path_groups.groups for a in group}

    # Filter out entries already in old main path
    filtered_main_path_groups = SortedListsOfMinimalAnime(
        groups=[
            [a for a in group if a.malId not in old_main_path_ids]
            for group in main_path_groups.groups
        ]
    )
    # Remove any empty groups after filtering
    filtered_main_path_groups.groups = [g for g in filtered_main_path_groups.groups if g]

    # Save the updated filtered main path groups
    await save_anime_json(relations_path, main_path_groups)
    print(f"Saved full relationship groups to {relations_path.name}.")

    # Wrap everything into groups for TVDB search (main path groups + new movies)
    all_groups_to_process = filtered_main_path_groups.groups + [[a] for a in new_entries_per_category.get("movie", [])]
    groups_for_tvdb = SortedListsOfMinimalAnime(groups=all_groups_to_process)
    
    # Search and save TVDB hits for both filtered main path + new movies
    await search_and_save_tvdb_hits(key, groups_for_tvdb)

    print("\n✅ All categories updated and new movies mapped to TVDB.")


if __name__ == "__main__":
    asyncio.run(main())
