# TVDB-IDs-To-MyAnimeList-IDs

### To use for myanimelist to thetvdb
https://ryandash.github.io/TVDB-IDs-To-MyAnimeList-IDs/api/myanimelist/{myanimelist-id}.json

Example: https://ryandash.github.io/TVDB-IDs-To-MyAnimeList-IDs/api/myanimelist/20.json

### To use for thetvdb series, season, or episode ids to myanimelist
https://ryandash.github.io/TVDB-IDs-To-MyAnimeList-IDs/api/thetvdb-series/{thetvdb-id}.json

Example: https://ryandash.github.io/TVDB-IDs-To-MyAnimeList-IDs/api/thetvdb-series/78857.json

### To use for thetvdb movie ids to myanimelist
https://ryandash.github.io/TVDB-IDs-To-MyAnimeList-IDs/api/thetvdb-movie/{thetvdb-id}.json

Example: https://ryandash.github.io/TVDB-IDs-To-MyAnimeList-IDs/api/thetvdb-movie/197.json

Only supports thetvdb Series, Season, Episode, and movie ids.\
Will not support TheTVDB.com List IDs or any other IDs really

Cloudflare worker that uses the above information and allows mapping with [nattadasu/animeApi](https://github.com/nattadasu/animeApi). \
https://github-checker-worker.ryandash0.workers.dev/

<!---counts-start--->
### TVDB → MAL Mapping Stats

- ✅ Mapped Movies: **1622**
- ✅ Mapped Series: **5343**
- ✅ Mapped Seasons: **7785**
- ✅ Mapped Episodes: **175183**
- ❌ Unmapped Series/Movies: **0**
- ❌ Unmapped Seasons: **84**
- ❌ Unmapped Episodes: **64330**
<!---counts-end--->
