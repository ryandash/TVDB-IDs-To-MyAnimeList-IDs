import json
import os
import random
import re
import time

import httpx
from playwright.sync_api import sync_playwright
from rapidfuzz import fuzz

BASE_URL_TEMPLATE = "https://www.thetvdb.com/genres/anime?page={page_num}"
LOG_FILE = "anime_scrape.log"

# -------------------
# HTTP Helpers
# -------------------

_last_request_time = 0
_http_client = httpx.Client(timeout=30)


def log(message: str) -> None:
    """Append a message to the log file."""
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(message + "\n")


def fetch_json(url: str) -> dict | None:
    """Fetch JSON from a URL with error handling."""
    try:
        resp = _http_client.get(url)
        if resp.status_code == 200:
            return resp.json()
    except Exception as e:
        log(f"Error fetching {url}: {e}")
    return None


def rate_limited_get(url: str, min_interval: float = 0.4, max_retries: int = 3) -> dict | None:
    """Fetch JSON with retries and rate limiting, using fetch_json."""
    global _last_request_time

    for attempt in range(1, max_retries + 1):
        # Rate limiting
        now = time.time()
        wait_time = min_interval - (now - _last_request_time)
        if wait_time > 0:
            time.sleep(wait_time)
        _last_request_time = time.time()

        data = fetch_json(url)
        if data is not None:
            return data

        # Retry delay
        if attempt < max_retries:
            time.sleep(1 + random.random())
        else:
            log(f"Giving up on {url} after {max_retries} attempts.")

    return None


def safe_goto(page, url: str, max_retries: int = 3) -> bool:
    """Navigate to a URL with retries and exponential backoff."""
    for attempt in range(1, max_retries + 1):
        try:
            response = page.goto(url)
            if response and response.status == 502:
                raise Exception("502 Bad Gateway")
            return True
        except Exception as e:
            print(f"[Retry {attempt}/{max_retries}] Failed to load {url}: {e}")
            if attempt < max_retries:
                sleep_time = 20 + random.randint(-5, 5)
                print(f"Waiting {sleep_time} seconds before retry...")
                time.sleep(sleep_time)
            else:
                print(f"Giving up on {url} after {max_retries} attempts.")
                return False


# -------------------
# Regex Patterns
# -------------------

SEASON_REGEX = re.compile(r"(\s|\.)S[0-9]{1,2}")
ALT_NAME_REGEX = re.compile(r"\s*~(\w|[0-9]|\s)+~")
NATIVE_NAME_REGEX = re.compile(r"\((\w|[0-9]|\s)+\)$")
AMPERSAND_REGEX = re.compile(r"\s?&\s?")
HASH_REGEX = re.compile(r"#")
JELLYFIN_FOLDER_FORMAT_REGEX = re.compile(r"\([0-9]{4}\)\s*\[(\w|[0-9]|-)+\]$")
NORMALIZE_REGEX = re.compile(r"[:.!]")

EPISODE_CODE_REGEX = re.compile(r"S\d+E\d+")


# -------------------
# Scraping Helpers
# -------------------

def get_inner_text(elem) -> str | None:
    """Extract trimmed inner text from an element safely."""
    return elem.inner_text().strip() if elem else None


def get_total_pages(page) -> int:
    """Return total number of pages from pagination links."""
    page.wait_for_selector('xpath=//*[@id="app"]/div[3]/div[3]/div[1]/ul')
    li_elements = page.query_selector_all(
        'xpath=//*[@id="app"]/div[3]/div[3]/div[1]/ul/li'
    )
    if len(li_elements) < 2:
        return 1
    last_li = li_elements[-2]
    a_tag = last_li.query_selector("a")
    if a_tag:
        href = a_tag.get_attribute("href")
        if href and "?page=" in href:
            try:
                return int(href.split("?page=")[1].split("&")[0])
            except ValueError:
                pass
    return 1


def extract_summaries(page) -> dict[str, str | None]:
    """Extract English and Japanese summaries from the page."""
    summaries = {"eng": None, "jpn": None}
    summary_divs = page.query_selector_all('xpath=//*[@id="translations"]/div')
    for div in summary_divs:
        lang = div.get_attribute("data-language")
        if lang not in summaries:
            continue
        p_elem = div.query_selector("p")
        if not p_elem:
            continue
        text = p_elem.inner_text().strip() or p_elem.text_content().strip()
        summaries[lang] = text
    return summaries


def extract_titles(page) -> dict[str, str | None]:
    """Extract English and Japanese titles from translation spans."""
    titles = {"eng": None, "jpn": None}
    spans = page.query_selector_all(
        'xpath=//*[@id="translations"]/div[last()]/span'
    )
    for span in spans:
        lang = span.get_attribute("data-language")
        if lang not in titles:
            continue
        span.click()
        title_elem = (
            page.query_selector('xpath=//*[@id="app"]/div[3]/div[3]/div[2]/h1')
            or page.query_selector('xpath=//*[@id="app"]/div[3]/div[3]/div[1]/h1')
            or page.query_selector("#series_title")
        )
        titles[lang] = get_inner_text(title_elem)
    return titles


def normalize_text(name: str) -> str:
    """Normalize anime title for better fuzzy matching."""
    if not name:
        return ""
    name = SEASON_REGEX.sub("", name)
    name = ALT_NAME_REGEX.sub("", name)
    name = NATIVE_NAME_REGEX.sub("", name)
    name = AMPERSAND_REGEX.sub(" and ", name)
    name = HASH_REGEX.sub(" ", name)
    name = JELLYFIN_FOLDER_FORMAT_REGEX.sub("", name)
    name = NORMALIZE_REGEX.sub("", name)
    return name.strip().lower()


# -------------------
# MAL Integration
# -------------------

def get_best_mal_id(search_term: str, movie: bool) -> int | None:
    """Search Jikan API and return the best matching MAL ID."""
    search_lower = search_term.lower()
    normalized_search = normalize_text(search_lower)

    split_normalized_search = None
    if ":" in search_term and movie:
        split_normalized_search = normalize_text(
            search_lower.split(":", 1)[1].strip()
        )

    api_url = f"https://api.jikan.moe/v4/anime?limit=10&q={normalized_search}"
    data = rate_limited_get(api_url)
    search_results = data.get("data", []) if data else []

    if not search_results:
        log(f"Failed to find data for {normalized_search}")
        return None

    for anime in search_results:
        all_titles = [
            t["title"].lower()
            for t in anime.get("titles", [])
            if "title" in t
        ]
        for title in all_titles:
            similarity = fuzz.ratio(
                normalize_text(title), normalized_search
            )
            if similarity >= 85:
                return anime["mal_id"]
            if split_normalized_search and movie:
                parts = title.split(":", 1)
                if len(parts) > 1:
                    split_title = normalize_text(parts[1].strip())
                    if fuzz.ratio(split_title, split_normalized_search) >= 90:
                        return anime["mal_id"]

    log(f"Failed to find MAL ID for {normalized_search}")
    return None


def get_mal_episode_count(mal_id: int) -> int | None:
    """Fetch total episode count for an anime ID."""
    url = f"https://api.jikan.moe/v4/anime/{mal_id}"
    data = rate_limited_get(url)
    if not data:
        return None
    episodes = data.get("data", {}).get("episodes")
    return episodes if isinstance(episodes, int) else None


def get_mal_relations(mal_id: int, offset_eps: int) -> tuple[int | None, int]:
    """
    Fetch the next valid MAL sequel ID for a given anime ID.
    
    Skips 'Special' types and keeps recursing until a valid sequel
    with enough episodes is found. Returns (sequel_id, adjusted_offset),
    or None if no valid sequel exists.
    """
    url = f"https://api.jikan.moe/v4/anime/{mal_id}/relations"
    data = rate_limited_get(url)
    if not data:
        return None, 0

    # Find first sequel that is not a "Special"
    sequel_id = next(
        (
            entry.get("mal_id")
            for rel in data.get("data", [])
            if rel.get("relation") == "Sequel"
            for entry in rel.get("entry", [])
            if entry.get("type") != "Special"
        ),
        None,
    )
    if not sequel_id:
        return None, 0

    mal_eps = get_mal_episode_count(sequel_id)
    if not mal_eps:
        return None, 0

    if mal_eps < offset_eps and mal_eps == 1: # mal_eps < offset_eps means mal season less episodes, if not movie, special, or ova this is fine OR offset_eps should be 1 ELSE skip to next MALID
        return get_mal_relations(sequel_id, offset_eps)

    return sequel_id, mal_eps

def get_cross_ids(mal_id: int, tvdb_id: str) -> dict | None:
    """Fetch cross-IDs for an anime from animeapi.my.id."""
    url = f"https://animeapi.my.id/myanimelist/{mal_id}"
    data = fetch_json(url)
    if not data:
        log(f"Failed to find other IDs for MAL ID {mal_id}")
        return None
    data["thetvdb"] = tvdb_id
    return dict(sorted(data.items()))


# -------------------
# Data Persistence
# -------------------

def save_data(anime_data: dict, mapped_ids: list[dict]) -> None:
    """Save anime data and mapped IDs to JSON files."""
    with open("anime-full.json", "w", encoding="utf-8") as f:
        json.dump(anime_data, f, indent=4, ensure_ascii=False)
    with open("mapped-tvdb-ids.json", "w", encoding="utf-8") as f:
        json.dump(mapped_ids, f, indent=4, ensure_ascii=False)


def load_data() -> tuple[dict, list]:
    """Load existing anime data and mapped IDs from disk."""
    anime_data, mapped_ids = {}, []
    if os.path.exists("anime-full.json"):
        with open("anime-full.json", "r", encoding="utf-8") as f:
            anime_data = json.load(f)
    if os.path.exists("mapped-tvdb-ids.json"):
        with open("mapped-tvdb-ids.json", "r", encoding="utf-8") as f:
            mapped_ids = json.load(f)
    return anime_data, mapped_ids

if __name__ == "__main__":
    open(LOG_FILE, "w", encoding="utf-8").close()
    anime_data, mapped_ids = load_data()

    test_url = "https://www.thetvdb.com/series/bleach"

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, slow_mo=100)
        page = browser.new_page()
        page2 = browser.new_page()

        if safe_goto(page, test_url):
            page.wait_for_selector('#series_basic_info', state='attached')

            # Extract basic info
            series_id, genres, other_sites = "N/A", [], []
            info_items = page.query_selector_all('#series_basic_info ul li')
            for li in info_items:
                label = get_inner_text(li.query_selector('strong'))
                if not label:
                    continue
                label = label.upper()
                if "ID" in label:
                    series_id = get_inner_text(li.query_selector('span'))
                elif "GENRE" in label:
                    genres = [get_inner_text(g) for g in li.query_selector_all("span a")]
                elif "SITES" in label:
                    other_sites = [s.get_attribute('href') for s in li.query_selector_all("span a")]

            # Skip Playwright scraping if series exists
            if series_id in anime_data:
                existing_series = anime_data[series_id]
                malid = existing_series.get("MAL ID")
                if malid is None:
                    log(f"Missing MAL ID for Series {anime_data[series_id]['TitleEnglish']}")
            else:
                print(f"Getting Series: {series_id}")
                titles = extract_titles(page)
                summaries = extract_summaries(page)

                malid = get_best_mal_id(titles["eng"], False)
                if malid:
                    cross_ids = get_cross_ids(malid, series_id)
                    if cross_ids:
                        mapped_ids.append(cross_ids)
                    malurl = f"https://myanimelist.net/anime/{str(malid)}"
                else:
                    malurl = None

                anime_data[series_id] = {
                    "MAL ID": malid,
                    "MAL URL": malurl,
                    "Genres": genres,
                    "Other Sites": other_sites,
                    "TitleEnglish": titles["eng"],
                    "SummaryEnglish": summaries["eng"],
                    "TitleJapanese": titles["jpn"],
                    "SummaryJapanese": summaries["jpn"],
                    "Seasons": {}
                }

            # Seasons
            season_rows = (page.query_selector_all('#seasons-official table tbody tr'))[1:-1]
            season_info = []
            for s in season_rows:
                a_elem = s.query_selector('td:nth-child(1) a')
                href = a_elem.get_attribute('href') if a_elem else None
                num_eps = get_inner_text(s.query_selector('td:nth-child(4)'))
                if href:
                    season_info.append((href, num_eps))

            SeasonMalID = None
            episode_offset = 0
            mal_eps = 0

            for season_idx, (season_url, numEpisodes) in enumerate(season_info, start=1):
                season_number = str(season_idx - 1)
                safe_goto(page, season_url)
                page.wait_for_selector('#general', state='attached')

                # Skip already scraped seasons
                if season_number in anime_data[series_id]["Seasons"]:
                    season_dict = anime_data[series_id]["Seasons"][season_number]
                    SeasonMalID = season_dict.get("MAL ID")
                    if SeasonMalID is None and season_number != "0":
                        log(f"Missing MALURL for Season: {season_number}")
                else:
                    print(f"Getting Season #{season_number}")
                    season_id = get_inner_text(page.query_selector('#general ul li span')) or "N/A"
                    numEpisodes = int(numEpisodes)

                    log(f"Season #{season_number}")

                    if season_number == "0":  # Specials do not exist on MAL
                        SeasonMalID = None
                        episode_offset = 0
                    else:
                        if season_number == "1":
                            alt_title = get_inner_text(
                                page.query_selector('xpath=//*[@id="app"]/div[3]/div[3]/div[2]/h2/span[1]')
                            ) or None

                            if malid:
                                SeasonMalID = malid
                            elif alt_title:
                                search_title = f"{anime_data[series_id]['TitleEnglish']} {alt_title}"
                                SeasonMalID = get_best_mal_id(search_title, False)
                                if SeasonMalID:
                                    malid = SeasonMalID
                                    anime_data[series_id]["MAL ID"] = malid
                                    anime_data[series_id]["MAL URL"] = f"https://myanimelist.net/anime/{str(malid)}"
                            mal_eps = get_mal_episode_count(SeasonMalID)
                            if SeasonMalID is None:
                                print(f"This needs to be fixed for {anime_data[series_id]['TitleEnglish']} {alt_title}")

                    if SeasonMalID:
                        seasonMalURL = f"https://myanimelist.net/anime/{SeasonMalID}"
                    else:
                        seasonMalURL = None

                    anime_data[series_id]["Seasons"][season_number] = {
                        "ID": season_id,
                        "URL": season_url,
                        "MAL ID": SeasonMalID,
                        "MAL URL": seasonMalURL,
                        "# Episodes": numEpisodes,
                        "Episodes": {}
                    }

                # Episodes
                episode_rows = page.query_selector_all('#episodes table tbody tr')
                episode_list = []
                for erow in episode_rows:
                    ep_href_elem = erow.query_selector('td:nth-child(2) a')
                    if not ep_href_elem:
                        continue
                    ep_code_elem = erow.query_selector('td:nth-child(1)')
                    raw_code = (ep_code_elem.inner_text()).strip().upper()
                    ep_num = str(int(raw_code.split('E')[1])) if "E" in raw_code else None
                    ep_href = ep_href_elem.get_attribute('href')
                    ep_id = ep_href.rstrip('/').split('/')[-1]
                    ep_url = "https://www.thetvdb.com" + ep_href
                    episode_list.append((ep_id, ep_url, ep_num))

                episodes_dict = anime_data[series_id]["Seasons"][season_number]["Episodes"]

                for _, (ep_id, ep_url, ep_num) in enumerate(episode_list, start=1):
                    if season_number != "0" :
                        episode_offset+=1
                    if ep_num in episodes_dict:
                        episodeMALURL = anime_data[series_id]["Seasons"][season_number]["Episodes"][ep_num]["MALURL"]
                        if episodeMALURL is None:
                            log(f"Missing MALURL for Season {season_number}, Episode {ep_num}")
                        continue
                    print(f"Getting Episode #{ep_num}")
                    safe_goto(page, ep_url)
                    page.wait_for_selector('#translations', state='attached')

                    titles = extract_titles(page)
                    summaries = extract_summaries(page)

                    episodeMALURL = None
                    # Specials (season 0)
                    if season_number == "0" and titles["eng"]:
                        search_term = titles["eng"]
                        movie = False
                        li_elements = page.query_selector_all('xpath=//*[@id="app"]/div[3]/div[3]/div[1]/div[3]/ul/li')
                        for li in li_elements:
                            strong_elem = li.query_selector('strong')
                            strong_text = (strong_elem.inner_text()).strip() if strong_elem else None
                            if strong_text and strong_text.upper() == "SPECIAL CATEGORY":
                                type_elem = li.query_selector('xpath=span/a')
                                type_text = (type_elem.inner_text()).strip() if type_elem else None
                                if type_text == "Movies":
                                    movie = True
                                    
                        if movie is False:
                            combined_title = f"{anime_data[series_id]['TitleEnglish']} {titles['eng']}"
                            if anime_data[series_id]['TitleEnglish'].lower() not in search_term.lower():
                                search_term = combined_title
                        EpisodeMALID = get_best_mal_id(search_term, movie)
                        if EpisodeMALID:
                            episodeMALURL = f"https://myanimelist.net/anime/{str(EpisodeMALID)}"
                            cross_ids = get_cross_ids(EpisodeMALID, ep_id)
                            if cross_ids:
                                mapped_ids.append(cross_ids)

                    # Regular episodes
                    elif SeasonMalID and ep_num:
                        api_url = f"https://api.jikan.moe/v4/anime/{SeasonMalID}/episodes/{episode_offset}"
                        if mal_eps == episode_offset:
                            SeasonMalID, _ = get_mal_relations(SeasonMalID, mal_eps - episode_offset)
                            episode_offset = 0
                            mal_eps = get_mal_episode_count(SeasonMalID)
                        data = rate_limited_get(api_url)
                        if data:
                            episodeMALURL = data.get("data", {}).get("url")
                            print(episodeMALURL)

                    # Save episode data
                    episodes_dict[ep_num] = {
                        "ID": ep_id,
                        "URL": ep_url,
                        "MALURL": episodeMALURL,
                        "TitleEnglish": titles["eng"],
                        "SummaryEnglish": summaries["eng"],
                        "TitleJapanese": titles["jpn"],
                        "SummaryJapanese": summaries["jpn"]
                    }
                
            # save_data(anime_data, mapped_ids)
            print(f"Finished scraping {test_url}")

        browser.close()
