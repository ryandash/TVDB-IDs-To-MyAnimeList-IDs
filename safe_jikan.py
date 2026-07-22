import asyncio
import time
from typing import Callable, Any, List, Optional
from jikanpy import AioJikan, exceptions
from pathlib import Path
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field, asdict
import glob
import orjson

@dataclass
class TitleEntry:
    title: str
    type: str

@dataclass
class MinimalAnime:
    malId: int
    type: str
    titles: List[TitleEntry] = field(default_factory=list)
    relations: List[dict] = field(default_factory=list)
    episodes: int = 0
    url: Optional[str] = None
    aired: dict[str, Any] = field(default_factory=dict)

# -----------------------------
# Task Limiter
# -----------------------------
class TaskLimiterConfiguration:
    def __init__(self, max_tasks: int, period_sec: float):
        self.max_tasks = max_tasks
        self.period_sec = period_sec
        self._timestamps: List[float] = []

    async def wait_for_slot(self) -> None:
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
        self._cache: dict[tuple, Any] = {}
        self._inflight_tasks: dict[tuple, asyncio.Task] = {}
        self._cache_lock = asyncio.Lock()
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.aio_jikan = AioJikan(selected_base="https://api.tenrai.org/v1")
        self._last_request = 0.0
        self._lock = asyncio.Lock()
        self.disk_cache_dir = Path("anime_cache")
        self.disk_cache_dir.mkdir(exist_ok=True)
        self.disk_cache_lock = asyncio.Lock()

        self.limiter = TaskLimiter([
            TaskLimiterConfiguration(3, 1.0),   # max 3 requests per second
            TaskLimiterConfiguration(4, 4.0),   # baseline limit (60/min)
        ])

    async def preload_disk_cache(self) -> None:
        files = list(self.disk_cache_dir.glob("*.json"))

        async def load_file(path: Path):
            try:
                content = await asyncio.to_thread(path.read_bytes)
                cached = orjson.loads(content)
                mal_id = cached.get("malId")
                if not mal_id:
                    return

                anime = MinimalAnime(
                    malId=mal_id,
                    type=cached.get("type", ""),
                    titles=[TitleEntry(**t) for t in cached.get("titles", [])],
                    relations=cached.get("relations", []),
                    episodes=cached.get("episodes"),
                    url=cached.get("url"),
                    aired=cached.get("aired", {})
                )

                key = ("anime_minimal", mal_id)
                async with self._cache_lock:
                    self._cache[key] = anime
            except Exception as e:
                print(f"[SafeJikan] Failed to preload {path.name}: {e}")

        await asyncio.gather(*(load_file(path) for path in files))

    async def clear_disk_cache(self) -> None:
        """Delete all JSON cache files on disk."""
        async with self.disk_cache_lock:
            try:
                for file in self.disk_cache_dir.glob("*.json"):
                    await asyncio.to_thread(file.unlink)
                print(f"[SafeJikan] Cleared {self.disk_cache_dir} cache files.")
            except Exception as e:
                print(f"[SafeJikan] Failed to clear cache: {e}")

    async def _wait_for_slot(self) -> None:
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_request
            if elapsed < self.request_delay:
                await asyncio.sleep(self.request_delay - elapsed)
            self._last_request = time.monotonic()

    async def _retry_on_failure(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
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
                code = (
                    getattr(e, "status", None)
                    or getattr(e, "status_code", None)
                    or getattr(e, "code", None)
                )
                if code in (429, 500, 502, 503, 504):
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

            except (
                asyncio.TimeoutError,
                aiohttp.ClientError,
                OSError,
            ) as e:
                # Handle network or temporary failures
                attempt += 1
                print(f"[Jikan] Request error: {e} (attempt {attempt}). Retrying in {delay:.1f}s...")
                await asyncio.sleep(delay)
                delay = min(delay * 1.5, max_delay)
                continue
    
    def _freeze(self, value: Any) -> Any:
        if isinstance(value, dict):
            return tuple(sorted((k, self._freeze(v)) for k, v in value.items()))
        elif isinstance(value, list):
            return tuple(self._freeze(v) for v in value)
        elif isinstance(value, set):
            return tuple(sorted(self._freeze(v) for v in value))
        return value

    def _should_persist(self, aired: dict) -> bool:
        aired_to = aired.get("to")
        if not aired_to:
            return False

        try:
            aired_date = datetime.fromisoformat(aired_to.replace("Z", "+00:00"))
            return datetime.now(timezone.utc) - aired_date > timedelta(days=1)
        except Exception as e:
            print(f"Failed to parse aired.to: {e}")
            return False
    
    def _disk_cache_path(self, mal_id: int) -> Path:
        return self.disk_cache_dir / f"{mal_id}.json"

    async def _write_disk_cache(self, mal_id: int, data: dict):
        path = self._disk_cache_path(mal_id)
        data_bytes = orjson.dumps(data)
        async with self.disk_cache_lock:
            try:
                await asyncio.to_thread(path.write_bytes, data_bytes)
            except Exception as e:
                print(f"[SafeJikan] Failed to write cache for {mal_id}: {e}")

    async def _invalidate_cache(self, mal_id: int):
        """Remove from memory and disk cache if exists."""
        key = ("anime_minimal", mal_id)
        async with self._cache_lock:
            self._cache.pop(key, None)

        path = self._disk_cache_path(mal_id)
        if path.exists():
            try:
                await asyncio.to_thread(path.unlink)
                print(f"[SafeJikan] Invalidated disk cache for {mal_id}")
            except Exception as e:
                print(f"[SafeJikan] Failed to remove cache for {mal_id}: {e}")

    async def _cached_call(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        key = (
            func.__name__,
            self._freeze(args),
            self._freeze(kwargs),
        )

        cached = self._cache.get(key)
        if cached is not None:
            return cached

        async with self._cache_lock:
            cached = self._cache.get(key)
            if cached is not None:
                return cached

            if key in self._inflight_tasks:
                task = self._inflight_tasks[key]
            else:
                task = asyncio.create_task(self._retry_on_failure(func, *args, **kwargs))
                self._inflight_tasks[key] = task

        try:
            result = await task
        finally:
            # Remove inflight task
            async with self._cache_lock:
                self._inflight_tasks.pop(key, None)

        async with self._cache_lock:
            self._cache[key] = result

        return result

    # -----------------------------
    # Public Jikan API methods
    # -----------------------------
    async def anime_list(self, **kwargs) -> dict:
        url = f"{self.aio_jikan.base}/anime"

        session = await self.aio_jikan._get_session()

        response = await session.get(
            url,
            params=kwargs
        )

        return await self.aio_jikan._wrap_response(
            response,
            str(response.url)
        )

    async def search_anime(self, query: str | None = None, type_: str | None = None, page: int | None = None, limit: int | None = None) -> dict:
        """
        Perform a safe Jikan anime search with automatic rate limiting and retries.
        """

        if query is None and type_ is None and page is None:
            raise ValueError(
                "search_anime() requires at least one of: query, type_, or page."
            )

        kwargs = {
            **({"q": query} if query else {}),
            **({"type": type_} if type_ else {}),
            **({"limit": limit} if limit else {}),
            **({"page": page} if page else {}),
        }

        result = await self._cached_call(
            self.anime_list,
            **kwargs
        )

        return {
            **result,
            "data": [
                a for a in result.get("data", [])
                if not (a.get("rating") or "").lower().startswith("rx")
            ]
        }

    async def get_anime(self, mal_id: int, episode_number: Optional[int] = None) -> Optional[MinimalAnime]:
        if not isinstance(mal_id, int) or mal_id <= 0:
            return None

        if episode_number is not None:
            return await self._cached_call(self.aio_jikan.anime_episode_by_id, mal_id, episode_number)

        return await self._get_anime_full(mal_id)

    async def get_anime_relations(self, mal_id: int) -> Optional[dict]:
        if not isinstance(mal_id, int) or mal_id <= 0:
            return None
        
        data = await self._get_anime_full(mal_id)
        if not data:
            return None
        
        return {"data": data.relations}

    async def _get_anime_full(self, mal_id: int, visited: bool = False) -> Optional[MinimalAnime]:
        """
        Fetch anime full data from Jikan, reduce it to minimal fields, and cache to disk.
        """
        key = ("anime_minimal", mal_id)

        # Check in-memory cache
        async with self._cache_lock:
            if key in self._cache:
                return self._cache[key]

        # Fetch full anime from Jikan
        data = await self._cached_call(self.aio_jikan.anime, mal_id, extension="full")
        if not data:
            return None

        node = data.get("data", {})

        animeType = (node.get("type") or "").lower()
        titles = [TitleEntry(title=t["title"], type=t["type"]) for t in node.get("titles", [])]
        relations = [
            {"relation": rel.get("relation", ""), "entry": entries}
            for rel in node.get("relations", [])
            if (entries := [e for e in rel.get("entry", []) if e.get("type", "").lower() != "manga"])
        ]

        episodes = node.get("episodes") or 0
        url = node.get("url")
        aired = node.get("aired", {})

        anime = MinimalAnime(
            malId=mal_id,
            type=animeType,
            titles=titles,
            relations=relations,
            episodes=episodes,
            url=url,
            aired=aired
        )
        if not visited:
            for rel in relations:
                if rel["relation"].lower() == "prequel":
                    for prequel_entry in rel["entry"]:
                        preq_id = prequel_entry.get("mal_id")
                        if not preq_id:
                            continue

                        preq_key = ("anime_minimal", preq_id)
                        async with self._cache_lock:
                            cached_prequel: Optional[MinimalAnime] = self._cache.get(preq_key)

                        if cached_prequel:
                            # Check if current mal_id exists in any relation
                            found = any(
                                e.get("mal_id") == mal_id
                                for r in cached_prequel.relations
                                for e in r.get("entry", [])
                            )
                            if not found:
                                # Invalidate prequel and refetch
                                await self._invalidate_cache(preq_id)
                                await self._get_anime_full(preq_id, True)

        # Write current anime cache
        await self._write_disk_cache(mal_id, asdict(anime))
        async with self._cache_lock:
            self._cache[key] = anime

        return anime

    async def get_anime_episodes(self, mal_id: int) -> dict:
        """
        Fetch all episodes for a given anime ID, handling pagination.
        Results are cached in-memory for the lifetime of SafeJikan.
        """
        if not isinstance(mal_id, int) or mal_id <= 0:
            return None

        episodes = []
        page = 1

        while True:
            data = await self._cached_call(
                self.aio_jikan.anime,
                mal_id,
                extension="episodes",
                page=page
            )

            if not data or "data" not in data:
                break

            eps = data.get("data") or []
            if not eps:
                break

            for ep in eps:
                ep["title"] = ep.get("title") or ""
                ep["title_japanese"] = ep.get("title_japanese") or ""

            episodes.extend(eps)

            if not data.get("pagination", {}).get("has_next_page", False):
                break

            page += 1

        result = {"data": episodes}

        return result

    async def close(self) -> None:
        await self.aio_jikan.close()
