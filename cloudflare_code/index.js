export default {
  async fetch(request, env) {
    const cache = caches.default;

    try {
      const url = new URL(request.url);
      const path = url.pathname.slice(1); // e.g. "thetvdb-series"

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

Required query parameter:
  ?id=YOUR_ID

Optional parameter:
  &crossIDs   (Use this if you want to fetch cross-referenced IDs from animeAPI)
          
Example usage:
  /thetvdb-series?id=361957&crossIDs
  /myanimelist?id=21`,
          { status: 200, headers: { "Content-Type": "text/plain" } }
        );
      }

      if (!["myanimelist", "thetvdb-series", "thetvdb-movie"].includes(path)) {
        return new Response(
          "Invalid path. Use /myanimelist or /thetvdb-series or /thetvdb-movie",
          { status: 400 }
        );
      }

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

      const crossIDs = url.searchParams.has("crossIDs");

      async function fetchCrossID(malId) {
        const cacheKey = new Request(`https://animeapi-cache.local/myanimelist/${malId}`);
        let cached = await cache.match(cacheKey);
        if (cached) {
          try {
            return await cached.json();
          } catch {
            // fall through to refetch
          }
        }

        const resp = await fetch(`https://animeapi.my.id/myanimelist/${malId}`, {
          cf: { cacheTtl: 3600, cacheEverything: true },
        });

        if (!resp.ok) return null;
        const data = await resp.json();

        // Cache for 1 hour
        cache.put(
          cacheKey,
          new Response(JSON.stringify(data), {
            headers: {
              "Content-Type": "application/json",
              "Cache-Control": "public, max-age=3600, immutable",
            },
          })
        ).catch(() => {});

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

      // Return cached version if available
      const cachedResp = await cache.match(request);
      if (cachedResp && !crossIDs) {
        return new Response(cachedResp.body, cachedResp);
      }

      let githubData = null;
      if (cachedResp && crossIDs) {
        try {
          githubData = await cachedResp.json();
        } catch {
          githubData = null;
        }
      }

      // Fetch from GitHub Pages
      if (!githubData) {
        const ghResp = await fetch(
          `https://${owner}.github.io/${repo}/api/${path}/${encodeURIComponent(id)}.json`,
          { cf: { cacheTtl: 3600, cacheEverything: false } }
        );

        const contentType = ghResp.headers.get("content-type") || "";
        if (ghResp.ok && contentType.includes("application/json")) {
          githubData = await ghResp.json();

          // Cache it
          cache.put(
            request,
            new Response(JSON.stringify(githubData), {
              headers: {
                "Content-Type": "application/json",
                "Cache-Control": "public, max-age=86400, immutable",
                "X-Source": "github_pages",
              },
            })
          ).catch(() => {});
        } else {
          return new Response(
            JSON.stringify({
              message: `Data not found for ${path}/${id}`,
            }),
            {
              status: 404,
              headers: { "Content-Type": "application/json" },
            }
          );
        }
      }

      // Enrich with cross IDs if requested
      const finalData =
        crossIDs && ["thetvdb-series", "thetvdb-movie", "myanimelist"].includes(path)
          ? await enrichWithCrossIDs(githubData)
          : githubData;

      return new Response(JSON.stringify(finalData), {
        headers: {
          "Content-Type": "application/json",
          "Cache-Control": "no-store",
          "X-Source": cachedResp ? "cache" : "github_pages",
        },
      });
    } catch (err) {
      return new Response(`Worker error: ${err.message}`, { status: 500 });
    }
  },
};
