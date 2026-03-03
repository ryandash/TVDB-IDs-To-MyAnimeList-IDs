import asyncio
import time
from typing import Callable, Any, List
from jikanpy import AioJikan, exceptions

# -----------------------------
# Task Limiter
# -----------------------------
class TaskLimiterConfiguration:
    def __init__(self, max_tasks: int, period_sec: float):
        self.max_tasks = max_tasks
        self.period_sec = period_sec
        self._timestamps: List[float] = []

    async def wait_for_slot(self):
        now = time.monotonic()
        # keep only timestamps within the window
        self._timestamps = [t for t in self._timestamps if now - t < self.period_sec]
        if len(self._timestamps) >= self.max_tasks:
            wait_time = self.period_sec - (now - self._timestamps[0])
            await asyncio.sleep(wait_time)
        self._timestamps.append(time.monotonic())


class TaskLimiter:
    def __init__(self, configs: List[TaskLimiterConfiguration]):
        self.configs = configs
        self._lock = asyncio.Lock()

    async def acquire(self):
        async with self._lock:
            for cfg in self.configs:
                await cfg.wait_for_slot()


# -----------------------------
# SafeJikan
# -----------------------------
class SafeJikan:
    def __init__(self, request_delay: float = 0.5, max_concurrent: int = 10):
        self.request_delay = request_delay
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.aio_jikan = AioJikan()
        self._last_request = 0.0
        self._lock = asyncio.Lock()

        self.limiter = TaskLimiter([
            TaskLimiterConfiguration(3, 1.0),   # max 3 requests per second
            TaskLimiterConfiguration(4, 4.0),   # baseline limit (60/min)
        ])

    async def _wait_for_slot(self):
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_request
            if elapsed < self.request_delay:
                await asyncio.sleep(self.request_delay - elapsed)
            self._last_request = time.monotonic()

    async def _retry_on_failure(self, func: Callable[..., Any], *args, **kwargs):
        delay = 1.0
        max_delay = 60.0  # cap backoff at 1 minute
        attempt = 0

        while True:
            try:
                async with self.semaphore:
                    await self.limiter.acquire()
                    await self._wait_for_slot()
                    return await func(*args, **kwargs)

            except exceptions.APIException as e:
                # Handle Jikan rate limit gracefully
                code = getattr(e, "status_code", getattr(e, "code", None))
                if code in (429, 500):
                    attempt += 1
                    reason = "Rate-limited" if code == 429 else "Server error 500"
                    print(f"[Jikan] {reason} (attempt {attempt}). Retrying in {delay:.1f}s...")
                    await asyncio.sleep(delay)
                    delay = min(delay * 1.5, max_delay)
                    continue
                elif code == 404:
                    print(f"[Jikan] Resource not found (404). Returning None.")
                    return None
                else:
                    print(f"[Jikan] Non-retryable API error {code}: {e}")
                    raise

            except (asyncio.TimeoutError, Exception) as e:
                # Handle network or temporary failures
                attempt += 1
                print(f"[Jikan] Request error: {e} (attempt {attempt}). Retrying in {delay:.1f}s...")
                await asyncio.sleep(delay)
                delay = min(delay * 1.5, max_delay)
                continue

    # -----------------------------
    # Public Jikan API methods
    # -----------------------------
    async def search_anime(
        self,
        query: str | None = None,
        type_: str | None = None,
        page: int | None = None,
        limit: int | None = None
    ):
        """
        Perform a safe Jikan anime search with automatic rate limiting and retries.
        """
        if not any([query, type_, page]):
            raise ValueError(
                "search_anime() requires at least one of: query, type_, or page."
            )
        
        params: dict[str, int | str] = {}

        if type_:
            params["type"] = type_
        if limit:
            params["limit"] = limit

        # Add `page` argument only if explicitly provided
        kwargs = {"search_type": "anime", "parameters": params}
        if query is not None:
            kwargs["query"] = query
        else:
            kwargs["query"] = ""
        if page is not None:
            kwargs["page"] = page

        return await self._retry_on_failure(self.aio_jikan.search, **kwargs)

    async def get_anime(self, mal_id: int, episode_number: int | None = None):
        if not isinstance(mal_id, int) or mal_id <= 0:
            raise ValueError("mal_id must be a positive integer.")

        if episode_number:
            return await self._retry_on_failure(self.aio_jikan.anime_episode_by_id, mal_id, episode_number)

        return await self._retry_on_failure(self.aio_jikan.anime, mal_id)

    async def get_anime_relations(self, mal_id: int):
        data = await self._retry_on_failure(
            self.aio_jikan.anime, mal_id, extension="relations"
        )
        if not data:
            return None

        # Filter out any relation entries that are manga
        return {
            "data": [
                {
                    "relation": rel["relation"],
                    "entry": [e for e in rel["entry"] if e["type"].lower() != "manga"]
                }
                for rel in data.get("data", [])
                if any(e["type"].lower() != "manga" for e in rel["entry"])
            ]
        }

    async def get_anime_episodes(self, mal_id: int) -> dict:
        """
        Fetch all episodes for a given anime ID, handling pagination.
        Returns a dict with 'data': list of episodes, each containing
        'title', 'title_japanese', 'mal_id', 'url', etc.
        """
        episodes = []
        page = 1

        while True:
            data = await self._retry_on_failure(
                self.aio_jikan.anime_episodes_by_id, mal_id, page
            )
            if not data or "data" not in data:
                break

            eps = data["data"]
            # normalize title fields for easier matching later
            for ep in eps:
                ep["title"] = ep.get("title") or ""
                ep["title_japanese"] = ep.get("title_japanese") or ""
            episodes.extend(eps)

            pagination = data.get("pagination", {})
            if not pagination.get("has_next_page", False):
                break
            page += 1

        return {"data": episodes}

    async def close(self):
        await self.aio_jikan.close()
