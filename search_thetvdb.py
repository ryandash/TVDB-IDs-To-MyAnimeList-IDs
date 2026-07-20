from curl_cffi import requests
import asyncio
import argparse

TVDB_URL = "https://api4.thetvdb.com/web/search/queries"


# -------------------------
# SINGLE REQUEST BUILDER
# -------------------------
def build_single_request(query: str, animeType: str, year: int, page: int = 0):
    facet_filters = []

    if animeType:
        facet_filters.append([f"type:{animeType}"])

    if year:
        facet_filters.append([f"year:{year}"])

    return {
        "indexName": "TVDB",
        "params": {
            "query": query,
            "page": page,
            "facetFilters": facet_filters,
            "maxValuesPerFacet": 10
        }
    }


# -------------------------
# BATCH BUILDER (IMPORTANT)
# -------------------------
def build_batch_requests(requests: list[dict]):
    return {
        "requests": requests
    }


# -------------------------
# HTTP CALL (WORKS FOR BOTH)
# -------------------------
async def search_tvdb(session: requests.AsyncSession, body: dict) -> dict:
    resp = await session.post(TVDB_URL, json=body)
    if resp.status_code != 200:
        raise RuntimeError(f"TVDB search failed: {resp.status_code}")
    return await resp.json()


def extract_hits(data: dict):
    hits = []
    for result in data.get("results", []):
        hits.extend(result.get("hits", []))
    return hits


# -------------------------
# CLI TEST (SINGLE MODE)
# -------------------------
async def main():
    headers = {
        "X-Algolia-Application-Id": "tvshowtime",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 "
            "(KHTML, like Gecko) "
            "Chrome/150.0.0.0 Safari/537.36"
        )
    }

    async with requests.AsyncSession(
        impersonate="chrome",
        headers=headers
    ) as session:

        body = build_batch_requests([
            build_single_request("dragon ball z", "series", 1986),
            build_single_request("naruto", "series", 2002),
        ])

        data = await search_tvdb(session, body)

        print("\n=== RAW RESULTS ===\n")

        for i, result in enumerate(data.get("results", [])):
            print(f"\n--- Query {i} ---")
            for hit in result.get("hits", []):
                print(f"- {hit.get('name')} ({hit.get('year')}) | id={hit.get('id')}")


if __name__ == "__main__":
    asyncio.run(main())