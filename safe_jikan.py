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

        # Multi-tier limiter like the C# one
        self.limiter = TaskLimiter([
            TaskLimiterConfiguration(1, 0.3),   # at least 300ms between requests
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
                if getattr(e, "status_code", getattr(e, "code", None)) == 429:
                    attempt += 1
                    print(f"[Jikan] Rate-limited (attempt {attempt}). Sleeping {delay:.1f}s...")
                    await asyncio.sleep(delay)
                    delay = min(delay * 1.5, max_delay)
                    continue
                else:
                    raise  # other APIException (e.g., 404, 500)

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

    async def get_anime(
        self,
        mal_id: int,
        extension: str | None = None,
        episode_number: int | None = None
    ):
        """
        Retrieve anime data from Jikan safely with automatic rate limiting and retries.
        """
        
        if not isinstance(mal_id, int) or mal_id <= 0:
            raise ValueError("mal_id must be a positive integer.")

        if episode_number is not None and not extension:
            # You can't specify an episode number without the 'episodes' extension
            extension = "episodes"

        # Build extension string properly
        ext_path = None
        if extension and episode_number is not None:
            ext_path = f"{extension}/{episode_number}"
        elif extension:
            ext_path = extension

        return await self._retry_on_failure(self.aio_jikan.anime, mal_id, extension=ext_path)

    async def get_anime_relations(self, mal_id: int):
        return await self._retry_on_failure(
            self.aio_jikan.anime, mal_id, extension="relations"
        )

    async def close(self):
        await self.aio_jikan.close()
