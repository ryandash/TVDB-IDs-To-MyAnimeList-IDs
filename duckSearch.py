from playwright.async_api import async_playwright, TimeoutError
from playwright_stealth import Stealth
from bs4 import BeautifulSoup
from urllib.parse import quote, urlparse, parse_qs, unquote
import re
import asyncio
import random

class DuckSearch:
    def __init__(self):
        self.search_cache = {}
        self.browser = None
        self.page = None
        self.playwright = None

    async def init_session(self):
        self.playwright = await async_playwright().start()

        self.browser = await self.playwright.chromium.launch(
            headless=True
        )

        context = await self.browser.new_context()
        self.page = await context.new_page()

        stealth = Stealth()
        await stealth.apply_stealth_async(self.page)

    async def close(self):
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()

    async def safe_goto(self, url: str, retries: int = 5, wait_dom: str = "domcontentloaded"):
        """
        Navigate with retries + exponential backoff to handle network failures.
        """
        for attempt in range(1, retries + 1):
            try:
                await self.page.goto(url, wait_until=wait_dom)
                return
            except (TimeoutError, Exception) as e:
                if attempt == retries:
                    raise  # Give up on last attempt
                backoff = (2 ** attempt) + random.uniform(0, 1)
                print(f"[safe_goto] Attempt {attempt}/{retries} failed: {e}. Retrying in {backoff:.1f}s...")
                await asyncio.sleep(backoff)

    async def cached_search_mal_id(self, query: str, relations: list) -> int | None:
        if self.page is None:
            await self.init_session()

        key = (query, tuple(relations))
        if key in self.search_cache:
            return self.search_cache[key]

        mal_id = await self.search_mal_id(query, relations)
        self.search_cache[key] = mal_id

        await asyncio.sleep(random.uniform(0.5, 1.5))
        return mal_id

    async def search_mal_id(self, query: str, relations: list) -> int | None:
        query = f"{query} site:myanimelist.net/anime"
        url = f"https://lite.duckduckgo.com/lite/?q={quote(query)}"

        await self.safe_goto(url)

        html = await self.page.content()
        soup = BeautifulSoup(html, "lxml")
        pattern = re.compile(r"^https?://myanimelist\.net/anime/(\d+)/[^/]+/?$")

        for a in soup.select("a.result-link"):
            href = a.get("href", "")
            if "/l/?" not in href:
                continue

            parsed = urlparse(href)
            qs = parse_qs(parsed.query)
            if "uddg" not in qs:
                continue

            real_url = unquote(qs["uddg"][0])
            match = pattern.match(real_url)
            if match:
                malid = int(match.group(1))
                if malid in relations:
                    return malid

        return None