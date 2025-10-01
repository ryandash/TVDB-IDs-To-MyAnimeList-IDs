from datetime import datetime
import json
import os
import re
import argparse
import asyncio
from queue import Queue
from pathlib import Path
import time
from typing import Tuple
import uuid
from playwright.async_api import Page, async_playwright

parser = argparse.ArgumentParser()
parser.add_argument("--episode", type=int, default=None, help="TVDB episode id")
args = parser.parse_args()
page_to_scrape = args.episode

BASE_URL_TEMPLATE = "https://www.thetvdb.com"
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

def save_anime(series_id: str, anime_info: dict, max_replace_attempts: int = 3):
    """Atomically save anime_info to DATA_DIR/{series_id}.json using a unique tmp file and fsync."""
    if not anime_info:
        return
    final_file = DATA_DIR / f"{series_id}.json"
    tmp_name = f"{series_id}.json.tmp.{uuid.uuid4().hex}"
    tmp_file = DATA_DIR / tmp_name

    try:
        with tmp_file.open("w", encoding="utf-8") as f:
            json.dump(anime_info, f, indent=4, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())

        for attempt in range(1, max_replace_attempts + 1):
            try:
                os.replace(tmp_file, final_file)
                break
            except Exception as e:
                if attempt >= max_replace_attempts:
                    raise
                time.sleep(0.1 * attempt)

        # Optional verification
        try:
            with final_file.open("r", encoding="utf-8") as vf:
                json.load(vf)
        except Exception as verify_exc:
            print(f"[WARN] Verification failed for {final_file}: {verify_exc}")
    except Exception as e:
        print(f"[ERROR] Failed saving anime {series_id}: {e}")
        try:
            if tmp_file.exists():
                tmp_file.unlink()
        except Exception:
            pass
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

async def scrape_episode_async(page: Page, ep_url):

    episode_data = {}

    await async_safe_goto(page, ep_url)

    breadcrumb_div = await page.query_selector("#app > div.container > div.page-toolbar > div.crumbs")
    if not breadcrumb_div:
        raise ValueError("Breadcrumb container missing")

    # Extract all <a> elements
    a_elems = await breadcrumb_div.query_selector_all("a")
    hrefs = [await a.get_attribute("href") for a in a_elems]
    
    series_href = next((h for h in hrefs if "/series/" in h), None)
    series_url = f"{BASE_URL_TEMPLATE}{series_href}" if series_href else None

    season_href = next((h for h in hrefs if "seasons" in h), None)
    season_url = f"{BASE_URL_TEMPLATE}{season_href}" if season_href else None

    text_nodes = await breadcrumb_div.evaluate(
        "el => Array.from(el.childNodes).map(n => n.textContent.trim()).filter(t => t.length > 0)"
    )

    # Extract episode number from the first node that matches
    episode_number = next(
        (re.search(r"Episode\s+(\d+)", t, re.IGNORECASE).group(1)
        for t in text_nodes if re.search(r"Episode\s+(\d+)", t, re.IGNORECASE)),
        None
    )

    if episode_number is None:
        print(f"[WARN] Could not find episode number in breadcrumbs: {text_nodes}")

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

    episode_data[episode_number] = {
        "ID": ep_url.rstrip('/').split('/')[-1],
        "TYPE": type_text,
        "URL": ep_url,
        "TitleEnglish": titles.get("eng"),
        "SummaryEnglish": summaries.get("eng"),
        "Aliases": aliases
    }

    return episode_data, series_url, season_url


async def scrape_season_async(page:Page, season_url: str):
    season_dict = {}
    await async_safe_goto(page, season_url)

    if not await async_wait_for_selector(page, "#general"):
        print(f"[SKIP] Skipping Season: {season_url} due to error page")
        return

    season_id_elem = await page.query_selector('#general ul li span')
    season_dict.update({
        "ID": (await season_id_elem.inner_text() if season_id_elem else "N/A"),
        "URL": season_url
    })

    return season_dict


lookup = build_lookup_table()

def parse_date(date_str: str):
    for fmt in ("%b %d, %Y", "%B %d, %Y"):  # abbreviated first, then full month
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"Could not parse date: {date_str}")

async def scrape_anime_page_async(page: Page, anime_url: str, season_number: str):
    await async_safe_goto(page, anime_url)

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
    
    translations, aliases = await extract_translations_async(page)
    titles = {lang: data.get("title") for lang, data in translations.items()}
    summaries = {lang: data.get("summary") for lang, data in translations.items()}
    
    anime_data = {
        "URL": anime_url,
        "Genres": genres,
        "Other Sites": other_sites,
        "TitleEnglish": titles.get("eng"),
        "SummaryEnglish": summaries.get("eng"),
        "Aliases": aliases,
        "Modified": modified_date.isoformat() if modified_date else None,
        "Seasons": {season_number:{}}
    }

    season_rows = (await page.query_selector_all('#seasons-official table tbody tr'))[1:-1]
    num_eps_elem = await season_rows[int(season_number)].query_selector('td:nth-child(4)')
    num_eps = int(await num_eps_elem.inner_text()) if num_eps_elem else 0
    
    return series_id, anime_data, num_eps

async def scrape_single_episode(thetvdbid: str):
    url = f"https://www.thetvdb.com/dereferrer/episode/{thetvdbid}"

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()

        page_episode, page_series, page_season = await asyncio.gather(
            context.new_page(), context.new_page(), context.new_page()
        )

        episode_data, series_url, season_url = await scrape_episode_async(page_episode, url)

        season_number = str(re.findall(r"\d+", season_url)[-1]) if season_url else None

        anime_task, season_data = await asyncio.gather(
            scrape_anime_page_async(page_series, series_url, season_number),
            scrape_season_async(page_season, season_url)
        )

        series_id, anime_data, num_eps = anime_task

        season_data["# Episodes"] = num_eps
        season_data["Episodes"] = episode_data
        anime_data["Seasons"][season_number] = season_data

        # Save at the end
        save_anime(series_id, anime_data)

        print(f"[INFO] Scraped episode {thetvdbid}")

        await browser.close()


# -------------------
# Entry Point
# -------------------

if __name__ == "__main__":
    asyncio.run(scrape_single_episode(args.episode))