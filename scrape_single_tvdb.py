from datetime import datetime
import json
import os
import re
import argparse
import asyncio
from queue import Queue
from pathlib import Path
import shutil
import time
from typing import Tuple
import uuid
from playwright.async_api import Page, async_playwright

parser = argparse.ArgumentParser()
parser.add_argument("--episode", type=int, default=None, help="TVDB episode id")
parser.add_argument("--delete-folder", action="store_true", help="Delete anime_data folder")
args = parser.parse_args()

BASE_URL_TEMPLATE = "https://www.thetvdb.com"
DATA_DIR = Path("anime_data")
DATA_DIR.mkdir(exist_ok=True)

# -------------------
# Persistence
# -------------------

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

async def extract_season_translations_async(page: Page) -> dict[str, dict[str, str | None]]:
    """Extracts title and summary translations for season pages."""
    translations = {
        "eng": {"title": None, "summary": None},
        "jpn": {"title": None, "summary": None},
    }

    base = (
        "#app > div.container > div.row.mt-2 > "
        "div.col-xs-12.col-sm-8.col-md-8.col-lg-9.col-xl-10"
    )

    title_spans = await page.query_selector_all(f"{base} > h2 > span.change_translation_text")
    for span in title_spans:
        lang = await span.get_attribute("data-language")
        if lang not in translations:
            continue

        text = (await span.inner_text()).strip()
        translations[lang]["title"] = text or None

    # --- Summaries ---
    summary_divs = await page.query_selector_all(f"{base} > div.change_translation_text")
    for div in summary_divs:
        lang = await div.get_attribute("data-language")
        if lang not in translations:
            continue

        p_elem = await div.query_selector("p")
        if not p_elem:
            continue

        text = (await p_elem.inner_text()).strip()
        translations[lang]["summary"] = text or None

    return translations

# -------------------
# Episode / Season / Anime
# -------------------

async def scrape_episode_async(page: Page):

    episode_data = {}

    translations, aliases = await extract_translations_async(page)
    titles = {lang: data.get("title") for lang, data in translations.items()}
    summaries = {lang: data.get("summary") for lang, data in translations.items()}

    print(titles.get("eng"))
    # Fallback English
    if not titles.get("eng"):
        print(titles.get("jpn"))
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

    episode_data = {
        "ID": page.url.rstrip('/').split('/')[-1],
        "TYPE": type_text,
        "URL": page.url,
        "TitleEnglish": titles.get("eng"),
        "SummaryEnglish": summaries.get("eng"),
        "Aliases": aliases
    }

    return episode_data


async def scrape_season_async(page:Page):
    season_dict = {}

    if not await async_wait_for_selector(page, "#general"):
        print(f"[SKIP] Skipping Season: {page.url} due to error page")
        return

    season_id_elem = await page.query_selector('#general ul li span')

    translations = await extract_season_translations_async(page)

    # Extract into your season dict
    titles = {lang: data.get("title") for lang, data in translations.items()}
    summaries = {lang: data.get("summary") for lang, data in translations.items()}
    season_dict.update({
        "ID": (await season_id_elem.inner_text() if season_id_elem else "N/A"),
        "URL": page.url,
        "TitleEnglish": titles.get("eng", ""),
        "SummaryEnglish": summaries.get("eng", ""),
        "Episodes": {}
    })

    return season_dict

def parse_date(date_str: str):
    for fmt in ("%b %d, %Y", "%B %d, %Y"):  # abbreviated first, then full month
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"Could not parse date: {date_str}")

async def scrape_anime_page_async(page: Page, season_number: str):
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
            if "Anime" not in genres:
                return None, None, None
        elif "SITES" in label:
            other_sites = [await s.get_attribute("href") for s in await li.query_selector_all("span a")]

    if not series_id:
        return
    
    translations, aliases = await extract_translations_async(page)
    titles = {lang: data.get("title") for lang, data in translations.items()}
    summaries = {lang: data.get("summary") for lang, data in translations.items()}

    if not titles.get("eng"):
        if not titles.get("jpn"):
            return
        titles["eng"], summaries["eng"] = titles.get("jpn"), summaries.get("jpn")
    elif ["Abridged", "DC Heroes United"] in titles["eng"]:
        return
    
    anime_data = {
        "URL": page.url,
        "Genres": genres,
        "Other Sites": other_sites,
        "TitleEnglish": titles.get("eng"),
        "SummaryEnglish": summaries.get("eng"),
        "Aliases": aliases,
        "Modified": modified_date.isoformat() if modified_date else None,
        "Seasons": {}
    }
    
    num_eps = None
    if season_number:
        season_rows = (await page.query_selector_all('#seasons-official table tbody tr'))[1:-1]
        num_eps_elem = await season_rows[int(season_number)].query_selector('td:nth-child(4)')
        num_eps = int(await num_eps_elem.inner_text()) if num_eps_elem else 0
    
    return series_id, anime_data, num_eps

def merge_anime(existing: dict, new: dict) -> dict:
    merged = existing.copy()
    # Simple dict update for top-level fields
    for k, v in new.items():
        if k == "Seasons" and "Seasons" in merged:
            for sn, sdata in v.items():
                if sn not in merged["Seasons"]:
                    merged["Seasons"][sn] = sdata
                else:
                    merged["Seasons"][sn]["Episodes"].update(sdata.get("Episodes", {}))
                    # keep "# Episodes" if present
                    if "# Episodes" in sdata:
                        merged["Seasons"][sn]["# Episodes"] = sdata["# Episodes"]
        else:
            merged[k] = v
    return merged

async def scrape_single_tvdb(thetvdbid: str):
    url_episode = f"https://www.thetvdb.com/dereferrer/episode/{thetvdbid}"
    url_season  = f"https://www.thetvdb.com/dereferrer/season/{thetvdbid}"
    url_series  = f"https://www.thetvdb.com/dereferrer/series/{thetvdbid}"

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()

        if args.delete_folder and DATA_DIR.exists():
            shutil.rmtree(DATA_DIR)
            DATA_DIR.mkdir(exist_ok=True)

        page_episode, page_season, page_series = await asyncio.gather(
            context.new_page(), context.new_page(), context.new_page()
        )

        async def is_valid_page(page, url: str) -> bool:
            try:
                await page.goto(url, timeout=30000)
                body_text = await page.inner_text("body")
                return "404" not in body_text
            except:
                return False

        chosen_page, page_type = None, None
        if await is_valid_page(page_episode, url_episode):
            chosen_page, page_type = page_episode, "episode"
        elif await is_valid_page(page_season, url_season):
            chosen_page, page_type = page_season, "season"
        elif await is_valid_page(page_series, url_series):
            chosen_page, page_type = page_series, "series"

        if not chosen_page:
            print(f"[ERROR] Could not determine page type for {thetvdbid}")
            await browser.close()
            return

        breadcrumb_div = await chosen_page.query_selector("#app > div.container > div.page-toolbar > div.crumbs")
        if breadcrumb_div:
            a_elems = await breadcrumb_div.query_selector_all("a")
            hrefs = [await a.get_attribute("href") for a in a_elems]

        if page_type == "season":
            if not breadcrumb_div:
                print(f"[ERROR] Breadcrumb not found for season {thetvdbid}")
                await browser.close()
                return

            series_href = next((h for h in hrefs if "/series/" in h), None)
            if not series_href:
                return
            series_url = f"{BASE_URL_TEMPLATE}{series_href}" if series_href else None
            season_number = str(re.findall(r"\d+", chosen_page.url)[-1])

            await async_safe_goto(page_series, series_url)
            series_id, anime_data, num_eps = await scrape_anime_page_async(page_series, season_number)
            if series_id is None:
                return
            season_data = await scrape_season_async(chosen_page)
            season_data["# Episodes"] = num_eps
            anime_data["Seasons"][season_number] = season_data
            save_anime(series_id, anime_data)
            print(f"[INFO] Scraped episode {thetvdbid}")
            await browser.close()
            return

        if page_type == "episode":
            if not breadcrumb_div:
                print(f"[ERROR] Breadcrumb not found for episode {thetvdbid}")
                await browser.close()
                return
            
            series_href = next((h for h in hrefs if "/series/" in h), None)
            if not series_href:
                return
            series_url = f"{BASE_URL_TEMPLATE}{series_href}" if series_href else None
            
            season_href = next((h for h in hrefs if "seasons" in h), None)
            if not season_href:
                return

            season_url = f"{BASE_URL_TEMPLATE}{season_href}"
            season_number = str(re.findall(r"\d+", season_url)[-1])

            text_nodes = await breadcrumb_div.evaluate(
                "el => Array.from(el.childNodes).map(n => n.textContent.trim()).filter(t => t.length > 0)"
            )
            episode_number = next(
                (re.search(r"Episode\s+(\d+)", t, re.IGNORECASE).group(1)
                for t in text_nodes if re.search(r"Episode\s+(\d+)", t, re.IGNORECASE)),
                None
            )

            episode_data = await scrape_episode_async(chosen_page)

            await async_safe_goto(page_series, series_url)
            await async_safe_goto(page_season, season_url)
            anime_task, season_data = await asyncio.gather(
                scrape_anime_page_async(page_series, season_number),
                scrape_season_async(page_season)
            )

            series_id, anime_data, num_eps = anime_task
            if series_id is None:
                return
            season_data["# Episodes"] = num_eps
            season_data["Episodes"][episode_number] = episode_data
            anime_data["Seasons"][season_number] = season_data
            save_anime(series_id, anime_data)
            print(f"[INFO] Scraped episode {thetvdbid}")
            await browser.close()
            return
            
        series_id, anime_data, _ = await scrape_anime_page_async(chosen_page, None)
        if series_id is None:
            return
        save_anime(series_id, anime_data)
        print(f"[INFO] Scraped episode {thetvdbid}")
        await browser.close()
        return

# -------------------
# Entry Point
# -------------------

if __name__ == "__main__":
    asyncio.run(scrape_single_tvdb(args.episode))