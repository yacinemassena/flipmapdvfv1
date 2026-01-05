import logging
import threading
from fastapi.responses import FileResponse
import orjson
import polars as pl
from fastapi import FastAPI
from cache import redis_async_client
from import_data import load_dataframe
from precompute_tiles import precompute_all
from utils import cluster_by_h3, clamp_res
from contextlib import asynccontextmanager
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("api")


DATAFRAME: pl.DataFrame | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global DATAFRAME
    DATAFRAME = load_dataframe()

    # Run precompute in background (non-blocking)
    threading.Thread(
        target=precompute_all,
        args=(DATAFRAME,),
        daemon=True,
    ).start()

    yield

app = FastAPI(lifespan=lifespan)
FRONTEND_DIR = "."


@app.get("/")
async def root():
    return FileResponse(f"{FRONTEND_DIR}/index.html")


@app.get("/api/markers")
async def markers(
    min_lat: float,
    max_lat: float,
    min_lon: float,
    max_lon: float,
    zoom: int,
):
    res = clamp_res(zoom)

    cache_key = f"viewport:{min_lat}:{max_lat}:{min_lon}:{max_lon}:{res}"
    cached = await redis_async_client.get(cache_key)
    if cached:
        return orjson.loads(cached)

    df_view = DATAFRAME.filter(
        (pl.col("latitude") >= min_lat)
        & (pl.col("latitude") <= max_lat)
        & (pl.col("longitude") >= min_lon)
        & (pl.col("longitude") <= max_lon)
    )

    result = cluster_by_h3(df_view, res)
    await redis_async_client.set(cache_key, orjson.dumps(result))
    return result
