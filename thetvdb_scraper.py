from urllib.parse import urljoin
from bs4 import BeautifulSoup
from dataclasses import dataclass
from datetime import datetime
import json
import os
import re
import argparse
import threading
import asyncio
from queue import Queue
from pathlib import Path
from copy import deepcopy
import traceback
from typing import List
import uuid
import aiohttp
from tqdm.asyncio import tqdm_asyncio

parser = argparse.ArgumentParser()
parser.add_argument("--worker", type=int, help="The worker number")
parser.add_argument("--delete-folder", action="store_true", help="Delete the anime_data folder before scraping to start fresh")
parser.add_argument("--save-interval", type=int, default=5, help="Save after this many anime")
args = parser.parse_args()

SAVE_INTERVAL = args.save_interval

# -----------------------------
# Config Paths
# -----------------------------

MIN_MAP_SERIES = Path("min_map_data/series")
MIN_MAP_MOVIE = Path("min_map_data/movie")

DATA_DIR_SERIES = Path("anime_data/series")
DATA_DIR_MOVIE = Path("anime_data/movie")
DATA_DIR_SERIES.mkdir(parents=True, exist_ok=True)
DATA_DIR_MOVIE.mkdir(parents=True, exist_ok=True)

MAX_ANIME_CONCURRENT = 5
MAX_SEASON_CONCURRENT = 10
SAVE_WORKERS = 2

# -----------------------------
# HTML Helpers
# -----------------------------
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

# -------------------
# Persistence
# -------------------

def safe_load_json(path: str) -> dict:
    p = Path(path)
    try:
        with p.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"[WARN] Could not load {p}: {e}")
        return {}

def build_lookup_table(category: str) -> dict:
    lookup = {}
    data_dir = DATA_DIR_SERIES if category == "series" else DATA_DIR_MOVIE
    for file in data_dir.glob("*.json"):
        try:
            data = safe_load_json(str(file))
            if data:
                lookup[file.stem] = data
        except Exception as e:
            print(f"[WARN] Failed to load {file}: {e}")
    return lookup

# -------------------
# Threaded Saving
# -------------------

save_queue = Queue()
stop_saver = threading.Event()

def save_anime(series_id: str, anime_info: dict, category: str):
    if not anime_info:
        return
    save_dir = DATA_DIR_MOVIE if category == "movie" else DATA_DIR_SERIES
    final_file = save_dir / f"{series_id}.json"
    tmp_file = save_dir / f"{series_id}.json.tmp.{uuid.uuid4().hex}"
    try:
        with tmp_file.open("w", encoding="utf-8") as f:
            json.dump(anime_info, f, indent=4, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_file, final_file)
    except Exception as e:
        print(f"[ERROR] Failed saving {category}/{series_id}: {e}")
        if tmp_file.exists():
            tmp_file.unlink(missing_ok=True)

def enqueue_save_anime(series_id: str, anime_info: dict, category: str):
    save_queue.put((series_id, deepcopy(anime_info), category))

def save_worker():
    """Consume save_queue until stop_saver set AND queue empty."""
    while True:
        if stop_saver.is_set() and save_queue.empty():
            break
        try:
            series_id, data_copy, category = save_queue.get(timeout=1)
        except Exception:
            continue
        try:
            save_anime(series_id, data_copy, category)
        except Exception as e:
            print(f"[ERROR] Unhandled error saving {category}/{series_id}: {e}\n{traceback.format_exc()}")
        finally:
            try:
                save_queue.task_done()
            except Exception:
                pass

def start_saver_threads():
    threads = []
    for _ in range(SAVE_WORKERS):
        t = threading.Thread(target=save_worker, daemon=True)
        t.start()
        threads.append(t)
    return threads

def stop_saver_threads(threads):
    stop_saver.set()
    for t in threads:
        t.join()

def parse_translations(soup: BeautifulSoup):
    translations = {"eng": {"title": None, "summary": None}, "jpn": {"title": None, "summary": None}}
    aliases = []
    divs = soup.select("#translations > div")
    for div in divs:
        lang = div.get("data-language")
        if lang not in translations:
            continue
        title = div.get("data-title")
        translations[lang]["title"] = title.strip() if title else None
        p_elem = div.find("p")
        translations[lang]["summary"] = p_elem.get_text(strip=True) if p_elem else None
        for li in div.select("ul li"):
            alias = li.get_text(strip=True)
            if alias and alias not in aliases:
                aliases.append(alias)
    return translations, aliases

def parse_season_translations(soup: BeautifulSoup):
    translations = {"eng": {"title": None, "summary": None}, "jpn": {"title": None, "summary": None}}
    base_selector = (
        "#app > div.container > div.row.mt-2 > "
        "div.col-xs-12.col-sm-8.col-md-8.col-lg-9.col-xl-10"
    )
    title_spans = soup.select(f"{base_selector} > h2 > span.change_translation_text")
    for span in title_spans:
        lang = span.get("data-language")
        text = span.get_text(strip=True) or None
        if not lang:
            continue
        if lang not in translations:
            translations[lang] = {"title": None, "summary": None}
        translations[lang]["title"] = text

    summary_divs = soup.select(f"{base_selector} > div.change_translation_text")
    for div in summary_divs:
        lang = div.get("data-language")
        p_elem = div.find("p")
        text = p_elem.get_text(strip=True) if p_elem else None
        if not lang:
            continue
        if lang not in translations:
            translations[lang] = {"title": None, "summary": None}
        translations[lang]["summary"] = text
    return translations

def parse_special_category(li):
    strong = li.find("strong")
    strong_text = strong.get_text(strip=True).upper() if strong else ""
    type_text = None
    if strong_text == "SPECIAL CATEGORY":
        a = li.find("span a")
        type_text = a.get_text(strip=True) if a else None
    elif strong_text == "NOTES":
        span = li.find("span")
        notes_text = span.get_text(strip=True).lower() if span else ""
        if "is a movie" in notes_text:
            type_text = "Movies"
    return type_text

# -------------------
# Episode / Season / Anime
# -------------------

async def scrape_episode(session: aiohttp.ClientSession, ep_info, season_eps: dict):
    ep_id, ep_url, ep_num = ep_info
    if ep_num in season_eps and season_eps.get("TitleEnglish") != None:
        return

    html = await fetch_html(session, ep_url)
    if not html:
        return

    soup = BeautifulSoup(html, "html.parser")
    translations, aliases = parse_translations(soup)
    titles = {lang: data.get("title") for lang, data in translations.items()}
    summaries = {lang: data.get("summary") for lang, data in translations.items()}

    if titles.get("eng", "") == "TBA":
        return

    eng_title = (titles.get("eng") or "").lower()
    type_text = None
    if "ova" in eng_title:
        type_text = "OVA"
    elif "movie" in eng_title:
        type_text = "Movies"
    else:
        for li in soup.select("#general > ul > li"):
            t = parse_special_category(li)
            if t:
                type_text = t
                break

    season_eps[ep_num] = {
        "ID": ep_id,
        "TYPE": type_text,
        "URL": ep_url,
        "Titles": titles,
        "Summaries": summaries,
        "Aliases": aliases
    }

async def scrape_season(session: aiohttp.ClientSession, season_url: str, numEpisodes: int, season_dict: dict, season_number: str):
    html = await fetch_html(session, season_url)
    if not html:
        return
    soup = BeautifulSoup(html, "html.parser")

    if not season_dict.get("ID"):
        soup = BeautifulSoup(html, "html.parser")
        
        season_id_elem = soup.select_one('#general ul li span')
        season_id = season_id_elem.get_text(strip=True) if season_id_elem else "N/A"

        translations = parse_season_translations(soup)
        titles = {lang: data.get("title") for lang, data in translations.items()}
        summaries = {lang: data.get("summary") for lang, data in translations.items()}
        
        season_dict.update({
            "ID": season_id,
            "URL": season_url,
            "Titles": titles,
            "Summaries": summaries,
            "# Episodes": int(numEpisodes)
        })
    
    
    ep_rows = []
    existing_eps = season_dict.setdefault("Episodes", {})

    if season_number == "0":
        special_categories = {"Episodic Special", "Movies", "OVAs", "Season Recaps", "Uncategorized"}
        for h3 in soup.select("#episodes > h3"):
            text = h3.get_text(strip=True)
            if any(cat.lower() in text.lower() for cat in special_categories):
                next_table = h3.find_next_sibling("table")
                if next_table:
                    ep_rows.extend(next_table.select("tbody tr"))
    else:
        ep_rows = soup.select("#episodes table tbody tr")
    
    ep_infos = []
    for erow in ep_rows or []:
        a_tag = erow.select_one("td:nth-child(2) a")
        code_td = erow.select_one("td:nth-child(1)")
        if not a_tag or not code_td:
            continue

        code_text = code_td.get_text(strip=True).upper()
        match = re.search(r"E(\d+)", code_text)
        ep_num = str(int(match.group(1))) if match else None
        if not ep_num or ep_num in existing_eps:
            continue

        href = a_tag.get("href")
        if not href:
            continue

        full_url = urljoin("https://www.thetvdb.com", href)
        ep_id = href.rstrip("/").split("/")[-1]
        ep_infos.append((ep_id, full_url, ep_num))

    if ep_infos:
        await asyncio.gather(*(
            scrape_episode(session, ep_info, existing_eps)
            for ep_info in ep_infos
        ))

    # --- Sort seasons by Season Number ---
    other_keys = {k: v for k, v in season_dict.items() if k != "Episodes"}
    season_dict.clear()
    season_dict.update(other_keys)
    season_dict["Episodes"] = dict(sorted(existing_eps.items(), key=lambda x: int(x[0])))

def parse_date(date_str: str):
    for fmt in ("%b %d, %Y", "%B %d, %Y"):  # abbreviated first, then full month
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"Could not parse date: {date_str}")

async def scrape_anime(session: aiohttp.ClientSession, url: str, category: str, lookup: dict):
    html = await fetch_html(session, url)
    if not html:
        return
    
    soup = BeautifulSoup(html, "html.parser")
    info_items = soup.select('#series_basic_info ul li')

    series_id = None
    modified_date = None
    genres, other_sites = [], []

    for li in info_items:
        label_elem = li.find("strong")
        label = label_elem.get_text(strip=True).upper() if label_elem else None
        if not label:
            continue

        if "ID" in label:
            span = li.find("span")
            series_id = span.get_text(strip=True) if span else None

        elif "MODIFIED" in label:
            span = li.find("span")
            modified_date_text = span.get_text(strip=True) if span else None
            if modified_date_text:
                date_str = modified_date_text.split("by")[0].strip()
                try:
                    modified_date = parse_date(date_str)
                except ValueError:
                    modified_date = None

        elif "GENRE" in label:
            genres = [g.get_text(strip=True) for g in li.select("span a")]

        elif "SITES" in label:
            other_sites = [s.get("href") for s in li.select("span a")]

    if not series_id:
        return

    existing  = lookup.get(series_id)    
    if not existing:
        translations, aliases = parse_translations(soup)
        titles = {lang: data.get("title") for lang, data in translations.items()}
        summaries = {lang: data.get("summary") for lang, data in translations.items()}

        if not titles.get("eng"):
            if not titles.get("jpn"):
                return
            titles["eng"], summaries["eng"] = titles.get("jpn"), summaries.get("jpn")
        elif "Abridged" in titles["eng"]:
            return
    
    anime_data = deepcopy(existing) if existing else {
        "URL": url,
        "Genres": genres,
        "Other Sites": other_sites,
        "Titles": titles,
        "Summaries": summaries,
        "Aliases": aliases,
        "Modified": modified_date.isoformat() if modified_date else None,
        "Seasons": {}
    }

    existing_date = None
    if existing and "Modified" in existing:
        existing_modified = existing.get("Modified")
        if existing_modified:
            try:
                existing_date = datetime.fromisoformat(existing_modified).date()
            except Exception:
                pass
    
    if existing_date and modified_date and modified_date <= existing_date:
        print(f"\nSkipped {series_id}")
        # enqueue_save_anime(series_id, anime_data, category)
        return

    if category != "movie":
        # --- Collect seasons ---
        season_rows = soup.select('#seasons-official table tbody tr')[1:-1]
        season_tasks = []

        for idx, s in enumerate(season_rows, start=1):
            season_number = str(idx - 1)
            num_eps_elem = s.select_one('td:nth-child(4)')
            num_eps = int(num_eps_elem.get_text(strip=True)) if num_eps_elem else 0
            if num_eps == 0:
                continue

            season_entry = anime_data["Seasons"].get(season_number)
            saved_num_eps = season_entry.get("# Episodes") if season_entry else None

            if isinstance(saved_num_eps, int) and saved_num_eps >= num_eps:
                continue

            a_elem = s.select_one('td:nth-child(1) a')
            href = a_elem.get("href") if a_elem else None
            if href:
                season_tasks.append(scrape_season(
                    session,
                    href,
                    num_eps,
                    anime_data["Seasons"].setdefault(season_number, {}),
                    season_number
                ))

        if season_tasks:
            for coro in tqdm_asyncio.as_completed(season_tasks, desc=f"{series_id} Seasons", total=len(season_tasks), leave=False):
                await coro

        anime_data["Seasons"] = dict(sorted(anime_data["Seasons"].items(), key=lambda x: int(x[0])))
    
    enqueue_save_anime(series_id, anime_data, category)

# -------------------
# Main Orchestration
# -------------------

@dataclass
class TVDBMatches:
    TvdbId: int
    MalId: int
    Name: str
    Url: str

async def scrape_all(matches_series: List[TVDBMatches], matches_movie: List[TVDBMatches]):
    sem = asyncio.Semaphore(MAX_ANIME_CONCURRENT)
    async with aiohttp.ClientSession() as session:

        lookup_series = build_lookup_table("series")
        lookup_movie = build_lookup_table("movie")

        if args.delete_folder:
            import shutil

            print("[INFO] Deleting anime_data folders for a fresh start...")
            for folder in [DATA_DIR_SERIES, DATA_DIR_MOVIE]:
                if folder.exists():
                    shutil.rmtree(folder)
                folder.mkdir(parents=True, exist_ok=True)

        async def process_match(match: TVDBMatches, category: str):
            async with sem:
                await scrape_anime(session, match.Url, category, lookup_series if category == "series" else lookup_movie)

        tasks = []
        for m in matches_series:
            tasks.append(process_match(m, "series"))
        for m in matches_movie:
            tasks.append(process_match(m, "movie"))

        for coro in tqdm_asyncio.as_completed(tasks, total=len(tasks), leave=True):
            await coro

# -----------------------------
# Load Input Data
# -----------------------------

def load_tvdb_matches(folder: Path) -> List[TVDBMatches]:
    matches = []
    for f in folder.glob("*.json"):
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
            matches.append(TVDBMatches(**data))
        except Exception as e:
            print(f"[WARN] Failed to parse {f}: {e}")
    return matches

# -------------------
# Entry Point
# -------------------

def split_list(lst, num_workers, worker_index):
    per_worker = len(lst) // num_workers
    remainder = len(lst) % num_workers
    start = worker_index * per_worker + min(worker_index, remainder)
    end = start + per_worker + (1 if worker_index < remainder else 0)
    return lst[start:end]

if __name__ == "__main__":
    series_matches = load_tvdb_matches(MIN_MAP_SERIES)
    movie_matches = load_tvdb_matches(MIN_MAP_MOVIE)

    num_workers = 20

    if args.worker is not None:
        worker_index = args.worker

        series_worker = split_list(series_matches, num_workers, worker_index)
        movie_worker = split_list(movie_matches, num_workers, worker_index)

        print(f"[INFO] Worker {worker_index} processing {len(series_worker)} series and {len(movie_worker)} movies")

    else:
        # If no worker specified, process all
        series_worker = series_matches
        movie_worker = movie_matches
        print(f"[INFO] No worker specified, processing all {len(series_worker)} series and {len(movie_worker)} movies")

    threads = start_saver_threads()
    asyncio.run(scrape_all(series_worker, movie_worker))
    stop_saver_threads(threads)
