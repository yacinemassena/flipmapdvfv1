# main.py
import asyncio
import logging
from fastapi import FastAPI, Query, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from contextlib import asynccontextmanager

from fastapi.responses import FileResponse
import orjson
from import_data import import_data
from cache import redis_client
from precompute_tiles import precompute, get_zoom_resolution, ZOOM_TO_H3
from utils import tile_to_bbox, bounds_to_tiles, cluster_by_h3
import polars as pl

logging.basicConfig(level=logging.INFO)

PROPERTIES_DF = None  # global dataframe


@asynccontextmanager
async def lifespan(app: FastAPI):
    global PROPERTIES_DF
    PROPERTIES_DF = import_data()
    if PROPERTIES_DF is None or PROPERTIES_DF.is_empty():
        raise RuntimeError("No data loaded!")

    # Only one worker precomputes
    if redis_client:
        try:
            await precompute(PROPERTIES_DF)
        except Exception as e:
            logging.error(f"Precompute failed: {e}")
    yield


app = FastAPI(lifespan=lifespan)

app.add_middleware(GZipMiddleware, minimum_size=1000)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def compute_tile_on_fly(df: pl.DataFrame, x: int, y: int, z: int, h3_res: int):
    """Compute a single tile cluster"""
    min_lat, max_lat, min_lon, max_lon = tile_to_bbox(x, y, z)
    tile_df = df.filter(
        (pl.col("latitude") >= min_lat)
        & (pl.col("latitude") <= max_lat)
        & (pl.col("longitude") >= min_lon)
        & (pl.col("longitude") <= max_lon)
    )

    if tile_df.is_empty():
        return []

    clusters = cluster_by_h3(tile_df, h3_res)

    # lat_idx/lon_idx for compatibility
    lat_diff = max_lat - min_lat or 0.0001
    lon_diff = max_lon - min_lon or 0.0001
    for c in clusters:
        c["lat_idx"] = int((c["latitude"] - min_lat) / lat_diff)
        c["lon_idx"] = int((c["longitude"] - min_lon) / lon_diff)

    return clusters


def get_viewport_cache_key(min_lat, max_lat, min_lon, max_lon, zoom) -> str:
    """Generate a unique cache key for the viewport"""
    key_str = f"{round(min_lat,3)}:{round(max_lat,3)}:{round(min_lon,3)}:{round(max_lon,3)}:{int(zoom)}"
    import hashlib

    return "viewport:" + hashlib.md5(key_str.encode()).hexdigest()
FRONTEND_DIR = "."


@app.get("/")
async def root():
    return FileResponse(f"{FRONTEND_DIR}/index.html")


@app.get("/api/markers")
async def markers(
    min_lat: float = Query(...),
    max_lat: float = Query(...),
    min_lon: float = Query(...),
    max_lon: float = Query(...),
    zoom: int = Query(10),
    response: Response = None,
):
    """Return clusters in viewport using async Redis and thread-safe tile computation."""
    global PROPERTIES_DF
    response.headers["Cache-Control"] = "public, max-age=60"
    h3_res = get_zoom_resolution(zoom)

    # --- 1. Viewport cache
    viewport_key = get_viewport_cache_key(min_lat, max_lat, min_lon, max_lon, zoom)
    cached_view = await redis_client.get(viewport_key)
    if cached_view:
        return orjson.loads(cached_view)

    # --- 2. Determine tiles to fetch
    tiles = bounds_to_tiles(min_lat, max_lat, min_lon, max_lon, zoom)
    cache_keys = [f"tile:{zoom}:{tx}:{ty}" for tx, ty in tiles]

    # --- 3. MGET from Redis
    raw_tiles = await redis_client.mget(*cache_keys)
    all_clusters = []

    # --- 4. Deserialize cached tiles
    for raw in raw_tiles:
        if raw:
            all_clusters.extend(orjson.loads(raw))

    # --- 5. Missing tiles
    missing_tiles = [
        (tiles[i], cache_keys[i]) for i, r in enumerate(raw_tiles) if r is None
    ]

    # --- 6. Compute missing tiles in thread pool
    if missing_tiles:
        loop = asyncio.get_running_loop()

        async def compute_tile(tx, ty, key):
            # Run the CPU-bound computation in thread pool
            result = await loop.run_in_executor(
                None, lambda: compute_tile_on_fly(PROPERTIES_DF, tx, ty, zoom, h3_res)
            )
            # Cache result
            if result:
                try:
                    await redis_client.set(key, orjson.dumps(result))
                except Exception:
                    pass
            return result

        tasks = [compute_tile(tx, ty, key) for (tx, ty), key in missing_tiles]
        results = await asyncio.gather(*tasks)

        # Merge results
        for tile_res in results:
            if tile_res:
                all_clusters.extend(tile_res)

    # --- 7. Cache viewport
    if all_clusters:
        try:
            await redis_client.setex(viewport_key, 300, orjson.dumps(all_clusters))
        except Exception:
            pass

    return all_clusters
