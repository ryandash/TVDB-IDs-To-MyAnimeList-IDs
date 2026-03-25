export default {
  async fetch(request, env) {
    const cache = caches.default;

    try {
      const url = new URL(request.url);
      const path = url.pathname.slice(1);
      const { GITHUB_OWNER: owner, GITHUB_REPO: repo } = env;

      if (!owner || !repo) {
        return new Response("Server not configured", { status: 500 });
      }

      if (!path) {
        return new Response(
          `Welcome to the API! Here's how to use it:

Valid paths:
  /myanimelist
  /thetvdb-series
  /thetvdb-movie
  /thetvdb-seasons
  /thetvdb-episodes

Required Query Parameter:
  ?id=YOUR_ID

Optional Query Parameters:
  &crossIDs   (Use to fetch cross-referenced IDs from animeAPI NOTE: this uses another api so results will vary and it will take longer)

  &season=SEASON_NUMBER (For series paths, fetch a specific season for a series)

  &episode=EPISODE_NUMBER (For series/season paths, fetch a specific episode for a series or season)

Example usage:
  - Fetch the mapped data for episode 5 of season 2 for Naruto:
  /thetvdb-series?id=78857&season=2&episode=5

  - Fetch the mapped data for One Piece with cross-referenced IDs for the MyAnimeList id #21
  /myanimelist?id=21&crossIDs`,
          { status: 200, headers: { "Content-Type": "text/plain" } }
        );
      }

      const pathRules = {
        "myanimelist": ["id"],
        "thetvdb-movie": ["id"],
        "thetvdb-series": ["id", "season", "episode"],
        "thetvdb-seasons": ["id", "episode"],
        "thetvdb-episodes": ["id"]
      };

      const id = url.searchParams.get("id");
      if (!id) {
        return new Response(
          `Missing ?id=YOUR_ID for ${path}.

Example usage:
  /${path}?id=YOUR_ID
  /${path}?id=YOUR_ID&crossIDs`,
          { status: 400, headers: { "Content-Type": "text/plain" } }
        );
      }

      const season = url.searchParams.get("season");
      const episode = url.searchParams.get("episode");
      
      // Validate params
      const allowedParams = pathRules[path];
      if ((season && !allowedParams.includes("season")) || (episode && !allowedParams.includes("episode"))) {
        return new Response(
          `Invalid parameters for ${path}. Allowed query parameters: ${allowedParams.join(", ")}`,
          { status: 400, headers: { "Content-Type": "text/plain" } }
        );
      }

      const crossIDs = url.searchParams.has("crossIDs");

      async function fetchCrossID(malId) {
        const cacheKey = new Request(`https://animeapi-cache.local/myanimelist/${malId}`);
        let cached = await cache.match(cacheKey);
        if (cached) {
          try { return await cached.json(); } catch {}
        }

        const resp = await fetch(`https://animeapi.my.id/myanimelist/${malId}`, {
          cf: { cacheTtl: 3600, cacheEverything: true },
        });

        if (!resp.ok) return null;
        const data = await resp.json();

        // Cache for 1 hour
        cache.put(cacheKey, new Response(JSON.stringify(data), {
          headers: { "Content-Type": "application/json", "Cache-Control": "public, max-age=3600, immutable" },
        })).catch(() => {});

        return data;
      }

      async function enrichWithCrossIDs(dataArray) {
        if (!Array.isArray(dataArray) || dataArray.length === 0) return dataArray;

        const malIds = [...new Set(dataArray.map(d => d.myanimelist).filter(Boolean))];
        if (malIds.length === 0) return dataArray;

        const fetches = await Promise.allSettled(malIds.map(fetchCrossID));
        const crossMap = new Map();
        fetches.forEach((res, i) => {
          if (res.status === "fulfilled" && res.value) {
            crossMap.set(malIds[i], res.value);
          }
        });

        return dataArray.map(item => {
          const malId = item.myanimelist;
          const crossData = crossMap.get(malId) || {};
          const tvdbUrl = item["tvdb url"];
          let tvdbId = null;

          if (tvdbUrl) {
            const match = /series\/(\d+)|season\/(\d+)/.exec(tvdbUrl);
            tvdbId = match ? match[1] || match[2] : null;
          }

          return {
            ...item,
            ...(crossIDs ? crossData : {}),
            thetvdb: tvdbId || null,
          };
        });
      }

      async function fetchGithubJSON(basePath, id, season, episode) {
        const baseUrl = `https://${owner}.github.io/${repo}/api/${basePath}`;
      
        switch (basePath) {
          case "thetvdb-series":
            if (!season) return fetch(`${baseUrl}/${encodeURIComponent(id)}.json`);
            if (season && !episode) return fetch(`${baseUrl}/${encodeURIComponent(id)}/${encodeURIComponent(season)}.json`);
            if (season && episode) return fetch(`${baseUrl}/${encodeURIComponent(id)}/${encodeURIComponent(season)}/${encodeURIComponent(episode)}.json`);
            break;
      
          case "thetvdb-seasons":
            if (!episode) return fetch(`${baseUrl}/${encodeURIComponent(id)}.json`);
            if (episode) return fetch(`${baseUrl}/${encodeURIComponent(id)}/${encodeURIComponent(episode)}.json`);
            break;
      
          case "thetvdb-movie":
          case "myanimelist":
          case "thetvdb-episodes":
            return fetch(`${baseUrl}/${encodeURIComponent(id)}.json`);
      
          default:
            return null;
        }
      }

      // --- Check cache first ---
      let cachedResp = await cache.match(request);
      let githubData = null;

      if (cachedResp && !crossIDs) {
        return new Response(cachedResp.body, cachedResp);
      }

      if (cachedResp && crossIDs) {
        try { githubData = await cachedResp.json(); } catch { githubData = null; }
      }

      // --- Fetch from GitHub Pages ---
      if (!githubData) {
        const season = url.searchParams.get("season");
        const episode = url.searchParams.get("episode");

        // --- Fetch GitHub JSON dynamically based on parameters ---
        const ghResp = await fetchGithubJSON(path, id, season, episode);

        // If the fetch failed (404, 403, etc.) or returned HTML, return a clear message
        if (!ghResp || !ghResp.ok) {
          return new Response(
            JSON.stringify({
              message: `No data found for ${path}/${id}${season ? `/${season}` : ""}${episode ? `/${episode}` : ""}`
            }),
            {
              status: 404,
              headers: { "Content-Type": "application/json" },
            }
          );
        }

        try {
          githubData = await ghResp.json();
        } catch {
          return new Response(
            JSON.stringify({
              message: `No data found for ${path}/${id}${season ? `/${season}` : ""}${episode ? `/${episode}` : ""}`
            }),
            {
              status: 404,
              headers: { "Content-Type": "application/json" },
            }
          );
        }

        cache.put(request, new Response(JSON.stringify(githubData), {
          headers: { "Content-Type": "application/json", "Cache-Control": "public, max-age=86400, immutable", "X-Source": "github_pages" },
        })).catch(() => {});
      }

      // --- Enrich with crossIDs if requested ---
      const finalData = crossIDs && ["thetvdb-series", "thetvdb-movie", "myanimelist", "thetvdb-seasons", "thetvdb-episodes"].includes(path)
        ? await enrichWithCrossIDs(githubData)
        : githubData;

      return new Response(JSON.stringify(finalData), {
        headers: { "Content-Type": "application/json", "Cache-Control": "no-store", "X-Source": cachedResp ? "cache" : "github_pages" },
      });

    } catch (err) {
      return new Response(`Worker error: ${err.message}`, { status: 500 });
    }
  },
};