import re
import requests
from urllib.parse import quote_plus
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from playwright_stealth import Stealth


class GoogleSearch:
    """
    Google search using:
    FlareSolverr (session bootstrap)
    + Playwright
    + Stealth
    """

    def __init__(self):
        self.search_cache = {}
        self.initialized = False

        self.browser = None
        self.context = None
        self.page = None
        self.playwright_ctx = None

        self.byparr_url = "http://localhost:8191/v1"

    def get_flaresolverr_session(self, url):
        payload = {
            "cmd": "request.get",
            "url": url,
            "maxTimeout": 60000
        }

        resp = requests.post(self.byparr_url, json=payload, timeout=60)
        data = resp.json()

        solution = data["solution"]

        return solution["cookies"], solution["userAgent"]

    async def init(self):
        if self.initialized:
            return

        stealth = Stealth(
            navigator_languages_override=("en-US", "en"),
            init_scripts_only=True
        )

        self.playwright_ctx = stealth.use_async(async_playwright())
        self.p = await self.playwright_ctx.__aenter__()

        # Solve challenge once
        cookies, user_agent = self.get_flaresolverr_session(f"https://lite.duckduckgo.com/lite/?q={quote_plus("fullmetal alchemist brotherhood myanimelist")}")

        self.browser = await self.p.chromium.launch(
            headless=True
        )

        self.context = await self.browser.new_context(
            user_agent=user_agent
        )

        # Inject cookies
        await self.context.add_cookies([
            {
                "name": c["name"],
                "value": c["value"],
                "domain": c["domain"],
                "path": c["path"],
                "httpOnly": c.get("httpOnly", False),
                "secure": c.get("secure", False)
            }
            for c in cookies
        ])

        # Block heavy resources
        await self.context.route(
            "**/*",
            lambda route, request: (
                route.abort()
                if request.resource_type in ["image", "font", "stylesheet"]
                else route.continue_()
            )
        )

        self.page = await self.context.new_page()

        self.initialized = True

    async def close(self):
        if self.browser:
            await self.browser.close()

        if self.playwright_ctx:
            await self.playwright_ctx.__aexit__(None, None, None)

    async def cached_search_mal_id(self, query: str, relations: list[int]) -> int | None:
        await self.init()

        key = (query, tuple(relations))
        if key in self.search_cache:
            return self.search_cache[key]

        malid_found = None

        search_query = f"{query} site:myanimelist.net/anime"
        url = f"https://lite.duckduckgo.com/lite/?q={quote_plus(search_query)}"

        try:
            await self.page.goto(
                url,
                timeout=20000,
                wait_until="domcontentloaded"
            )

            html = await self.page.content()
            soup = BeautifulSoup(html, "html.parser")

            links = []

            for a in soup.select("#search a"):
                link = a.get("href")

                if not link:
                    continue

                if "myanimelist.net/anime/" in link:
                    links.append(link)

            pattern = re.compile(r"^https?://myanimelist\.net/anime/(\d+)/[^/]+/?$")

            for link in links:
                print("[GoogleSearch] Found:", link)

                match = pattern.match(real_url)
                if match:
                    malid = int(match.group(1))
                    if malid in relations:
                        return malid

        except Exception as e:
            print("[GoogleSearch] Search failed:", e)

        self.search_cache[key] = malid_found
        return malid_found