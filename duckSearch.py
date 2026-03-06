from duckduckgo_api_haystack import DuckduckgoApiWebSearch

class DuckSearch:
    def __init__(self):
        self.search_cache = {}
        self.websearch = DuckduckgoApiWebSearch(
            top_k=3,
            timeout=10,
            max_search_frequency=1
        )

    async def cached_search_mal_id(self, query: str, relations: list) -> int | None:
        key = (query, tuple(relations))
        if key in self.search_cache:
            return self.search_cache[key]

        # Perform search
        results = self.websearch.run(query=f"{query} site:myanimelist.net/anime")
        for link in results["links"]:
            if "myanimelist.net/anime/" in link:
                malid = int(link.split("/anime/")[1].split("/")[0])
                if malid in relations:
                    self.search_cache[key] = malid
                    return malid

        self.search_cache[key] = None
        return None