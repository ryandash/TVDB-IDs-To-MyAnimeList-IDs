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
import time
import traceback
import shutil
import signal
from typing import Tuple
import uuid
from tqdm.asyncio import tqdm_asyncio
from playwright.async_api import Page, async_playwright

parser = argparse.ArgumentParser()
parser.add_argument("--page", type=int, default=None, help="Page number to scrape")
parser.add_argument("--delete-folder", action="store_true", help="Delete anime_data folder")
parser.add_argument("--save-interval", type=int, default=5, help="Save after this many anime")
args = parser.parse_args()
page_to_scrape = args.page
deleteFolder = args.delete_folder
SAVE_INTERVAL = args.save_interval

BASE_URL_TEMPLATE = "https://www.thetvdb.com/genres/anime?page={page_num}"
DATA_DIR = Path("anime_data")
DATA_DIR.mkdir(exist_ok=True)

# -------------------
# Persistence
# -------------------

def safe_load_json(path: str) -> dict:
    """
    Robust JSON loader that attempts to salvage truncated/corrupted JSON files.

    - Tries normal json.load first.
    - On JSONDecodeError or UnicodeDecodeError tries to read forgivingly (errors='ignore'),
      then truncates to the last '}' or ']' and tries to json.loads.
    - If that fails, falls back to your previous line-based truncated-anime approach.
    - If salvage succeeds it rewrites the file atomically.
    - Always returns a dict (empty dict on failure).
    """
    p = Path(path)
    try:
        with p.open("r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        print(f"[WARN] JSON corrupted at {path} ({exc}). Attempting salvage...")

        # Read forgivingly to avoid UnicodeDecodeError on truncated multibyte chars
        try:
            raw = p.read_text(encoding="utf-8", errors="ignore")
        except Exception as e:
            print(f"[ERROR] Could not read file forgivingly: {e}")
            return {}

        # Strategy 1: cut to last top-level close brace/bracket
        last_close = max(raw.rfind("}"), raw.rfind("]"))
        if last_close != -1:
            candidate = raw[: last_close + 1]
            try:
                data = json.loads(candidate)
                # write back salvaged content atomically
                tmp_file = p.with_suffix(p.suffix + ".salvage.tmp")
                try:
                    with tmp_file.open("w", encoding="utf-8") as tf:
                        tf.write(candidate)
                    os.replace(tmp_file, p)
                    print("[INFO] Salvage successful by truncating to last '}' or ']'.")
                except Exception as e:
                    print(f"[WARN] Could not write salvaged file atomically: {e}")
                return data
            except Exception:
                # fall through to next strategy
                pass

        # Strategy 2: fallback to original line-based truncation (search for start of last anime)
        try:
            lines = raw.splitlines(True)
            anime_start_pattern = re.compile(r'^\s*"\d+"\s*:\s*{\s*$')  # looser whitespace
            for i in range(len(lines) - 1, -1, -1):
                if anime_start_pattern.match(lines[i].rstrip()):
                    # try to close previous object's trailing comma / bracket
                    if i > 0:
                        prev_line = lines[i - 1].rstrip()
                        if prev_line.endswith("},"):
                            lines[i - 1] = prev_line[:-1] + "\n"  # replace "}," -> "}"
                        else:
                            lines[i - 1] = prev_line + "\n"
                    # Keep everything up to the start of the problematic object, then close JSON
                    truncated = "".join(lines[:i]) + "}\n"
                    try:
                        data = json.loads(truncated)
                        # persist truncated JSON atomically
                        tmp_file = p.with_suffix(p.suffix + ".salvage.tmp")
                        try:
                            with tmp_file.open("w", encoding="utf-8") as tf:
                                tf.write(truncated)
                            os.replace(tmp_file, p)
                            print("[INFO] Salvage successful by truncating last object.")
                        except Exception as e:
                            print(f"[WARN] Could not write truncated file atomically: {e}")
                        return data
                    except Exception:
                        # can't salvage here, continue
                        break
        except Exception as e:
            print(f"[WARN] Line-based salvage attempt failed: {e}")

        print("[ERROR] Could not salvage JSON. Returning empty dict.")
        return {}

def build_lookup_table() -> dict:
    """
    Loads all existing JSONs from DATA_DIR/*.json into a flat lookup table keyed by series ID.
    If a file cannot be salvaged, skip it and continue.
    """
    lookup = {}
    for file in DATA_DIR.glob("*.json"):
        try:
            anime_info = safe_load_json(str(file))
            if anime_info:
                lookup[file.stem] = anime_info
            else:
                # empty dict indicates salvage/read failure; skip but warn
                print(f"[WARN] Skipping {file} due to unreadable/corrupted content.")
        except Exception as e:
            print(f"[WARN] Unexpected error loading {file}: {e}")
    return lookup


# -------------------
# Threaded Saving
# -------------------

save_queue = Queue()
stop_saver = threading.Event()

def save_anime(series_id: str, anime_info: dict, max_replace_attempts: int = 3):
    """Atomically save anime_info to DATA_DIR/{series_id}.json using a unique tmp file and fsync."""
    if not anime_info:
        return
    final_file = DATA_DIR / f"{series_id}.json"
    # create a unique temporary file name in same dir (important for os.replace atomicity)
    tmp_name = f"{series_id}.json.tmp.{uuid.uuid4().hex}"
    tmp_file = DATA_DIR / tmp_name

    try:
        # Use open() and write, flush and fsync to avoid partial writes
        with tmp_file.open("w", encoding="utf-8") as f:
            json.dump(anime_info, f, indent=4, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())

        # Attempt atomic replace (retry on rare transient issues)
        for attempt in range(1, max_replace_attempts + 1):
            try:
                os.replace(tmp_file, final_file)
                break
            except Exception as e:
                if attempt >= max_replace_attempts:
                    raise
                time.sleep(0.1 * attempt)
        # Optionally: verify file is readable right after replace (quick sanity)
        try:
            with final_file.open("r", encoding="utf-8") as vf:
                json.load(vf)
        except Exception as verify_exc:
            print(f"[WARN] Verification failed for {final_file}: {verify_exc}")
    except Exception as e:
        print(f"[ERROR] Failed saving anime {series_id}: {e}")
        # try to cleanup orphan tmp file (best-effort)
        try:
            if tmp_file.exists():
                tmp_file.unlink()
        except Exception:
            pass

def enqueue_save_anime(series_id: str, anime_info: dict):
    """Put a deepcopy on the queue (like before) but guard against exploding memory."""
    try:
        save_queue.put((series_id, deepcopy(anime_info)))
    except Exception as e:
        print(f"[ERROR] enqueue_save_anime failed for {series_id}: {e}")

def save_worker():
    """Consume save_queue until stop_saver set AND queue empty."""
    while True:
        # exit condition: stop flag set and queue empty
        if stop_saver.is_set() and save_queue.empty():
            break
        try:
            series_id, data_copy = save_queue.get(timeout=1)
        except Exception:
            continue
        try:
            save_anime(series_id, data_copy)
        except Exception as e:
            print(f"[ERROR] Unhandled error saving {series_id}: {e}\n{traceback.format_exc()}")
        finally:
            try:
                save_queue.task_done()
            except Exception:
                pass

def _signal_handler(signum, frame):
    print(f"[INFO] Received signal {signum}. Stopping saver after draining queue...")
    stop_saver.set()

signal.signal(signal.SIGINT, _signal_handler)
try:
    signal.signal(signal.SIGTERM, _signal_handler)
except Exception:
    pass

async def create_page_pool(context, pool_size: int) -> asyncio.Queue: 
    pages: list[Page] = [await context.new_page() for _ in range(pool_size)]
    available = asyncio.Queue()
    for p in pages:
        await available.put(p)
    return available

async def with_page(available, fn, *args, **kwargs):
    page = await available.get()
    try:
        await fn(page, *args, available=available, **kwargs)
    finally:
        await available.put(page)
# -------------------
# Async Helpers
# -------------------

async def async_safe_goto(page: Page, url: str, retries=3, delay=3):
    for attempt in range(1, retries + 1):
        try:
            await page.goto(url, timeout=60000, wait_until="domcontentloaded")
            return
        except Exception as e:
            if attempt > 2:
                print(f"[Retry {attempt}/{retries}] Failed {page.url}: {e}")
            if attempt < retries:
                await asyncio.sleep(delay * attempt)
                await page.reload(wait_until="domcontentloaded")
                await asyncio.sleep(delay * attempt)
            else:
                print(f"Failed on page {getattr(page, 'url', 'unknown')}")
                raise

async def async_wait_for_selector(page: Page, selector: str, retries=3, delay=5) -> bool:
    for attempt in range(1, retries + 1):
        try:
            content = await page.content()
            if "Whoops, looks like something went wrong." in content:
                return False
            await page.wait_for_selector(selector, state="attached")
            return True
        except Exception as e:
            if attempt > 2:
                print(f"[Retry {attempt}/{retries}] Failed {page.url}: {e}")
            if attempt < retries:
                await asyncio.sleep(delay * attempt)
                await page.reload(wait_until="domcontentloaded")
                await asyncio.sleep(delay * attempt)
            else:
                print(f"Failed on page {getattr(page, 'url', 'unknown')}")
                raise

async def get_total_pages(page: Page) -> int:
    if not await async_wait_for_selector(page, 'xpath=//*[@id="app"]/div[3]/div[3]/div[1]/ul'):
        return 1
    li_elements = await page.query_selector_all('xpath=//*[@id="app"]/div[3]/div[3]/div[1]/ul/li')
    if len(li_elements) < 2:
        return 1
    last_li = li_elements[-2]
    a_tag = await last_li.query_selector("a")
    if a_tag:
        href = await a_tag.get_attribute("href")
        if href and "?page=" in href:
            try:
                return int(href.split("?page=")[1].split("&")[0])
            except ValueError:
                pass
    return 1

async def first_selector(page, selectors):
    for sel in selectors:
        elems = await page.query_selector_all(sel)
        if elems:
            return elems
    return []

async def extract_translations_async(page: Page) -> Tuple[dict[str, dict[str, str | None]], list[str]]:
    translations = {
        "eng": {"title": None, "summary": None},
        "jpn": {"title": None, "summary": None},
    }
    aliases: set[str] = set()

    divs = await page.query_selector_all("#translations > div")
    for div in divs:
        lang = await div.get_attribute("data-language")
        if lang not in translations:
            continue

        # Title
        title = await div.get_attribute("data-title")
        translations[lang]["title"] = title.strip() if title else None

        # Summary
        p_elem = await div.query_selector("p")
        if p_elem:
            text = (await p_elem.inner_text()).strip()
            translations[lang]["summary"] = text or None

        # Aliases (flat list, not per language)
        alias_items = await div.query_selector_all("ul li")
        alias_texts = await asyncio.gather(*[li.text_content() for li in alias_items])
        aliases.update(a.strip() for a in alias_texts if a and a.strip())

    return translations, sorted(aliases, key=str.lower)

# -------------------
# Episode / Season / Anime
# -------------------

async def scrape_episode_async(page: Page, ep_info, season_eps: dict, available: Queue):
    ep_id, ep_url, ep_num = ep_info
    if ep_num in season_eps:
        return

    await async_safe_goto(page, ep_url)
    if not await async_wait_for_selector(page, "#translations"):
        print(f"[SKIP] Skipping {ep_id} due to error page")
        return

    translations, aliases = await extract_translations_async(page)
    titles = {lang: data.get("title") for lang, data in translations.items()}
    summaries = {lang: data.get("summary") for lang, data in translations.items()}

    # Fallback English
    if not titles.get("eng"):
        titles["eng"], summaries["eng"] = titles.get("jpn"), summaries.get("jpn")

    eng_title = (titles.get("eng") or "").lower()
    type_text = None
    if "ova" in eng_title:
        type_text = "OVA"
    elif "movie" in eng_title:
        type_text = "Movies"

    if type_text is None:
        li_elements = await first_selector(page, [
            "#general > ul > li",
            "#app > div.container > div.row > div.col-xs-12.col-sm-12.col-md-8.col-lg-8 > div:nth-child(4) > ul > li"
        ])
        for li in li_elements:
            strong_elem = await li.query_selector("strong")
            strong_text = (await strong_elem.inner_text()).strip().upper() if strong_elem else None
            if strong_text == "SPECIAL CATEGORY":
                type_elem = await li.query_selector("span a")
                type_text = (await type_elem.inner_text()).strip() if type_elem else None
                break
            elif strong_text == "NOTES":
                type_elem = await li.query_selector("span")
                notes_text = (await type_elem.inner_text()).strip().lower() if type_elem else ""
                if "is a movie" in notes_text:
                    type_text = "Movies"
                    break

    season_eps[ep_num] = {
        "ID": ep_id,
        "TYPE": type_text,
        "URL": ep_url,
        "TitleEnglish": titles.get("eng"),
        "SummaryEnglish": summaries.get("eng"),
        "Aliases": aliases
    }

async def scrape_season_async(page:Page, season_url: str, numEpisodes: int, season_dict: dict, season_number: str, available: Queue):
    existing_eps = season_dict.setdefault("Episodes", {})
    await async_safe_goto(page, season_url)

    if not season_dict.get("ID"):
        if not await async_wait_for_selector(page, "#general"):
            print(f"[SKIP] Skipping Season: {season_url} due to error page")
            return

        season_id_elem = await page.query_selector('#general ul li span')
        season_dict.update({
            "ID": (await season_id_elem.inner_text() if season_id_elem else "N/A"),
            "URL": season_url,
            "# Episodes": int(numEpisodes)
        })

    if not await async_wait_for_selector(page, "#episodes"):
        print(f"[SKIP] Skipping Season: {season_url} due to error page")
        return
    
    ep_rows = []
    if season_number == "0":
        special_categories = {"Episodic Special", "Movies", "OVAs", "Season Recaps", "Uncategorized"}
        h3_elems = await page.query_selector_all("#episodes > h3")
        for h3 in h3_elems:
            text = (await h3.inner_text()).strip()
            if any(cat.lower() in text.lower() for cat in special_categories):
                next_table = await h3.evaluate_handle(
                    "(node) => node.nextElementSibling && node.nextElementSibling.tagName === 'TABLE' ? node.nextElementSibling : null"
                )
                if next_table:
                    rows = await next_table.query_selector_all("tbody tr")
                    ep_rows.extend(rows)
    else:
        ep_rows = await page.query_selector_all("#episodes table tbody tr")
    
    ep_infos = []
    for erow in ep_rows or []:
        ep_href_elem = await erow.query_selector('td:nth-child(2) a')
        ep_code_elem = await erow.query_selector('td:nth-child(1)')
        if not ep_href_elem or not ep_code_elem:
            continue
        match = re.search(r'E(\d+)', (await ep_code_elem.inner_text()).strip().upper())
        ep_num = str(int(match.group(1))) if match else None
        if ep_num in existing_eps:
            continue
        ep_href = await ep_href_elem.get_attribute('href')
        ep_infos.append((ep_href.rstrip('/').split('/')[-1], "https://www.thetvdb.com" + ep_href, ep_num))

    if ep_infos:
        await asyncio.gather(*(with_page(available, scrape_episode_async, ep_info, existing_eps) for ep_info in ep_infos))

    # --- Sort seasons by Season Number ---
    other_keys = {k: v for k, v in season_dict.items() if k != "Episodes"}
    season_dict.clear()
    season_dict.update(other_keys)
    season_dict["Episodes"] = dict(sorted(existing_eps.items(), key=lambda x: int(x[0])))


lookup = build_lookup_table()
MAX_SEASON_CONCURRENT = None

def parse_date(date_str: str):
    for fmt in ("%b %d, %Y", "%B %d, %Y"):  # abbreviated first, then full month
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"Could not parse date: {date_str}")

async def scrape_anime_page_async(page: Page, anime_url: str, available: Queue):
    await async_safe_goto(page, anime_url)
    if not await async_wait_for_selector(page, "#series_basic_info"):
        print(f"[SKIP] Skipping Anime: {anime_url} due to error page")
        return

    series_id = None
    modified_date = None
    genres, other_sites = [], []
    info_items = await page.query_selector_all('#series_basic_info ul li')
    for li in info_items:
        label_elem = await li.query_selector("strong")
        label = (await label_elem.inner_text()).strip().upper() if label_elem else None
        if not label:
            continue
        if "ID" in label:
            span = await li.query_selector("span")
            series_id = await span.inner_text() if span else None
        elif "MODIFIED" in label:
            span = await li.query_selector("span")
            modified_date = await span.inner_text() if span else None
            if modified_date:
                date_str = modified_date.split("by")[0].strip()
                modified_date = parse_date(date_str)
        elif "GENRE" in label:
            genres = [await g.inner_text() for g in await li.query_selector_all("span a")]
        elif "SITES" in label:
            other_sites = [await s.get_attribute("href") for s in await li.query_selector_all("span a")]

    if not series_id:
        return

    existing  = lookup.get(series_id)    
    if not existing:
        translations, aliases = await extract_translations_async(page)
        titles = {lang: data.get("title") for lang, data in translations.items()}
        summaries = {lang: data.get("summary") for lang, data in translations.items()}
    
    anime_data = deepcopy(existing) if existing else {
        "URL": anime_url,
        "Genres": genres,
        "Other Sites": other_sites,
        "TitleEnglish": titles.get("eng"),
        "SummaryEnglish": summaries.get("eng"),
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
        print(f"Skipped {series_id}")
        enqueue_save_anime(series_id, anime_data)
        return

    # --- Collect seasons ---
    season_rows = (await page.query_selector_all('#seasons-official table tbody tr'))[1:-1]
    season_info = []

    for idx, s in enumerate(season_rows, start=1):
        season_number = str(idx - 1)
        num_eps_elem = await s.query_selector('td:nth-child(4)')
        num_eps = int(await num_eps_elem.inner_text()) if num_eps_elem else 0
        if (num_eps == 0):
            continue
        season_entry = anime_data["Seasons"].get(season_number)
        saved_num_eps = season_entry.get("# Episodes") if season_entry else None

        if isinstance(saved_num_eps, int) and saved_num_eps >= num_eps:
            # Season already fully scraped; skip it
            continue

        a_elem = await s.query_selector('td:nth-child(1) a')
        href = await a_elem.get_attribute('href') if a_elem else None
        if href:
            season_info.append((season_number, href, num_eps))

    async def limited_scrape_season(season_url: str, num_eps: int, anime_data:dict, season_number: str):
        async with MAX_SEASON_CONCURRENT:
            await with_page(available, scrape_season_async, season_url, num_eps, anime_data["Seasons"].setdefault(season_number, {}), season_number)

    if season_info:
        season_tasks = [
            limited_scrape_season(season_url, num_eps, anime_data, season_number)
            for season_number, season_url, num_eps in season_info
        ]
        for coro in tqdm_asyncio.as_completed(season_tasks, desc=f"{series_id} Seasons", total=len(season_tasks), leave=False):
            await coro

    anime_data["Seasons"] = dict(sorted(anime_data["Seasons"].items(), key=lambda x: int(x[0])))
    
    enqueue_save_anime(series_id, anime_data)


# -------------------
# Main Orchestration
# -------------------

async def scrape_all_async():
    # create semaphores inside the event loop so they're bound to the correct loop
    global MAX_SEASON_CONCURRENT
    MAX_ANIME_CONCURRENT = asyncio.Semaphore(2)
    MAX_SEASON_CONCURRENT = asyncio.Semaphore(2)
    MAX_PAGES = 10

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()

        page_pool_available = await create_page_pool(context, pool_size=MAX_PAGES)
        try:
            page = await context.new_page()
            await async_safe_goto(page, BASE_URL_TEMPLATE.format(page_num=1))
            total_pages = await get_total_pages(page) if not page_to_scrape else page_to_scrape
            page_nums = [page_to_scrape] if page_to_scrape else range(1, total_pages + 1)
            await page.close()

            for page_num in page_nums:
                if deleteFolder and DATA_DIR.exists():
                    shutil.rmtree(DATA_DIR)
                    DATA_DIR.mkdir(exist_ok=True)

                page = await context.new_page()
                await async_safe_goto(page, BASE_URL_TEMPLATE.format(page_num=page_num))
                if not await async_wait_for_selector(page, "table tbody tr"):
                    print(f"[SKIP] Failed to find anime on page: {page_num} due to error page")
                    return
                rows = (await page.query_selector_all("table tbody tr"))[1:]
                anime_urls = []
                for r in rows:
                    a_elem = await r.query_selector('td a')
                    if not a_elem:
                        continue
                    href = await a_elem.get_attribute("href")
                    if href:
                        anime_urls.append("https://www.thetvdb.com" + href)

                await page.close()

                async def limited_scrape_anime(url):
                    async with MAX_ANIME_CONCURRENT:
                        try:
                            await with_page(page_pool_available, scrape_anime_page_async, url)
                        except Exception as e:
                            print(f"[ERROR] Failed scraping {url}: {e}")
                            raise

                # Launch all tasks concurrently
                tasks = [asyncio.create_task(limited_scrape_anime(anime_url)) for anime_url in anime_urls]
                for coro in tqdm_asyncio.as_completed(tasks, desc=f"Page {page_num}/{total_pages}", total=len(tasks)):
                    await coro

                if not page_to_scrape:
                    # polite pause between pages (don't block the event loop)
                    await asyncio.sleep(30)

            print("Scraping complete!")
        finally:
            await browser.close()

# -------------------
# Entry Point
# -------------------

if __name__ == "__main__":
    saver_thread = threading.Thread(target=save_worker, daemon=True)
    saver_thread.start()

    while True:
        try:
            asyncio.run(scrape_all_async())
            break
        except Exception as e:
            print(f"[FATAL] Scraper crashed: {e}\n{traceback.format_exc()}")
            print("Restarting in 5 minutes...")
            time.sleep(300)
        finally:
            pass

    # signal the saver to stop, wait for queue to drain
    stop_saver.set()
    print("[INFO] Waiting for save queue to drain...")
    save_queue.join()
    saver_thread.join(timeout=10)
    print("Saver thread stopped, exiting.")
