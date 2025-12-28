import polars as pl
import math
import orjson
import hashlib
import asyncio
from concurrent.futures import ThreadPoolExecutor
from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import FileResponse
import os
import time
import logging
import threading
import sys
from contextlib import asynccontextmanager
from database import SessionLocal
from cache import redis_client_sync, redis_client_async
from import_data import import_data
from precompute_tiles import precompute_all_tiles
from utils import tile_to_bbox, bounds_to_tiles, perform_clustering

# Configure logging
# Create a custom logger
logger = logging.getLogger("api_logger")
logger.setLevel(logging.INFO)

# Create handlers
c_handler = logging.StreamHandler(sys.stdout)
c_handler.setLevel(logging.INFO)

# Create formatters and add it to handlers
log_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
c_handler.setFormatter(log_format)

# Add handlers to the logger
if not logger.hasHandlers():
    logger.addHandler(c_handler)

FRONTEND_DIR = os.path.dirname(os.path.abspath(__file__))
PROPERTIES_DF = None

# Thread pool for CPU-bound work
executor = ThreadPoolExecutor(max_workers=8)

# Global Status for Precompute
PRECOMPUTE_STATUS = {
    "running": False,
    "completed": False,
    "error": None
}

def get_properties_df():
    """Get the properties DataFrame, loading it if necessary."""
    global PROPERTIES_DF
    if PROPERTIES_DF is None:
        load_db_to_memory_sync()
    return PROPERTIES_DF

def load_db_to_memory_sync():
    """Load properties from DB to RAM. Does NOT trigger precompute."""
    global PROPERTIES_DF
    
    # Import data if needed
    try:
        import_data()
    except Exception as e:
        print(f"Error importing data: {e}")

    print("Loading all properties from DB to memory with Polars...")
    db = SessionLocal()
    try:
        query = "SELECT * FROM properties"
        PROPERTIES_DF = pl.read_database(query, db.bind)
        print(f"✅ Loaded {len(PROPERTIES_DF)} properties to memory.")
    except Exception as e:
        print(f"❌ Error loading data: {e}")
        raise
    finally:
        db.close()

def background_precompute():
    """Lance le pré-calcul dans un thread séparé pour ne pas bloquer l'API."""
    global PROPERTIES_DF
    global PRECOMPUTE_STATUS
    
    PRECOMPUTE_STATUS["running"] = True
    
    try:
        print("🔄 Background pre-computation started...")
        start = time.time()
        
        # Ensure we have data
        if PROPERTIES_DF is None:
             load_db_to_memory_sync()

        # Pass the dataframe to avoid reloading in the thread if possible
        precompute_all_tiles(PROPERTIES_DF, redis_client_sync)
        
        elapsed = time.time() - start
        print(f"✅ Background pre-computation completed in {elapsed:.2f}s")
        PRECOMPUTE_STATUS["completed"] = True
        
    except Exception as e:
        print(f"❌ Background pre-computation failed: {e}")
        PRECOMPUTE_STATUS["error"] = str(e)
        import traceback
        traceback.print_exc()
    finally:
        PRECOMPUTE_STATUS["running"] = False

def prewarm_initial_tiles():
    """Ensure low-zoom tiles for France are cached immediately."""
    if get_properties_df() is None:
        return
        
    print("🔥 Pre-warming cache for zoom 6-8...")
    # Approximate France bounds
    min_lat, max_lat = 41.0, 51.0
    min_lon, max_lon = -5.0, 10.0
    
    for z in range(6, 9):
        tiles = bounds_to_tiles(min_lat, max_lat, min_lon, max_lon, z)
        for tx, ty in tiles:
            cache_key = f"tile:{z}:{tx}:{ty}"
            try:
                if not redis_client_sync.get(cache_key):
                    res = compute_tile_on_fly(tx, ty, z)
                    if res:
                        redis_client_sync.setex(cache_key, 86400, orjson.dumps(res))
            except Exception as e:
                logger.error(f"Error prewarming tile {z}/{tx}/{ty}: {e}")
    print("✅ Pre-warming complete.")

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    print("🚀 Starting application...")
    
    # 1. Charger la DB en RAM (bloquant, nécessaire)
    if get_properties_df() is None:
        print("❌ Failed to load initial data")
    
    # 1b. Pre-warm common tiles (quick)
    prewarm_thread = threading.Thread(target=prewarm_initial_tiles, daemon=True)
    prewarm_thread.start()

    # 2. Lancer le pré-calcul en BACKGROUND thread
    if os.getenv("SKIP_PRECOMPUTE", "false").lower() != "true":
        print("📊 Starting tile pre-computation in background...")
        background_thread = threading.Thread(
            target=background_precompute,
            daemon=True  # Important: daemon pour que Railway puisse kill proprement
        )
        background_thread.start()
    
    print("✅ API ready to accept requests")
    
    yield  # L'app tourne ici
    
    # Shutdown (si besoin de cleanup)
    print("🛑 Shutting down...")

app = FastAPI(lifespan=lifespan)

@app.middleware("http")
async def log_requests(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    logger.info(f"{request.method} {request.url} - Status: {response.status_code} - Duration: {process_time:.4f}s")
    return response

app.add_middleware(GZipMiddleware, minimum_size=1000)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def read_root():
    return FileResponse(os.path.join(FRONTEND_DIR, "index.html"))

@app.get("/api/status")
def get_status():
    """Retourne le status du pré-calcul."""
    return {
        "api_ready": PROPERTIES_DF is not None,
        "precompute": PRECOMPUTE_STATUS
    }

def compute_tile_on_fly(x, y, z):
    """Calcule un tile à la volée si pas en cache."""
    df = get_properties_df()
    if df is None:
        return []
    
    min_lat, max_lat, min_lon, max_lon = tile_to_bbox(x, y, z)
    
    df_filtered = df.filter(
        (pl.col('latitude') >= min_lat) & 
        (pl.col('latitude') <= max_lat) & 
        (pl.col('longitude') >= min_lon) & 
        (pl.col('longitude') <= max_lon)
    )
    
    return perform_clustering(df_filtered, min_lat, max_lat, min_lon, max_lon, z)

def get_cache_key(min_lat, max_lat, min_lon, max_lon, zoom):
    """Generate a cache key for a viewport request."""
    precision = 3 if zoom < 10 else 4
    key = f"{round(min_lat,precision)}:{round(max_lat,precision)}:{round(min_lon,precision)}:{round(max_lon,precision)}:{int(zoom)}"
    return f"viewport:{hashlib.md5(key.encode()).hexdigest()}"

@app.get("/api/markers")
async def get_markers(
    min_lat: float,
    max_lat: float,
    min_lon: float,
    max_lon: float,
    zoom: float,
    response: Response,
):
    """
    Smart endpoint: utilise les tiles pré-calculés si disponibles,
    sinon fallback sur calcul dynamique.
    """
    # ADD HTTP CACHING
    response.headers["Cache-Control"] = "public, max-age=60"

    # 0. Check Viewport Cache
    viewport_key = get_cache_key(min_lat, max_lat, min_lon, max_lon, zoom)
    try:
        cached_view = await redis_client_async.get(viewport_key)
        if cached_view:
            return orjson.loads(cached_view)
    except Exception as e:
        logger.error(f"Viewport cache error: {e}")

    z = int(zoom)
    req_z = min(max(z, 6), 14)  # Clamp between 6-14
    
    tiles_to_fetch = bounds_to_tiles(min_lat, max_lat, min_lon, max_lon, req_z)
    
    # PARALLEL Redis fetch
    cache_keys = [f"tile:{req_z}:{tx}:{ty}" for tx, ty in tiles_to_fetch]
    
    try:
        # Use MGET for faster batch retrieval
        cached_values = await redis_client_async.mget(cache_keys)
    except Exception as e:
        logger.error(f"Batch Redis error: {e}")
        cached_values = [None] * len(cache_keys)
    
    all_results = []
    missing_tiles = []
    
    for i, cached in enumerate(cached_values):
        if cached:
            all_results.extend(orjson.loads(cached))
        else:
            missing_tiles.append((tiles_to_fetch[i], cache_keys[i]))
    
    # Compute missing tiles in thread pool (parallel)
    if missing_tiles:
        loop = asyncio.get_event_loop()
        compute_tasks = [
            loop.run_in_executor(executor, compute_tile_on_fly, tx, ty, req_z)
            for (tx, ty), _ in missing_tiles
        ]
        
        try:
            computed_results = await asyncio.gather(*compute_tasks)
            
            for i, tile_result in enumerate(computed_results):
                if tile_result:
                    all_results.extend(tile_result)
                    try:
                        # Cache the result
                        await redis_client_async.setex(missing_tiles[i][1], 86400, orjson.dumps(tile_result))
                    except Exception:
                        pass
        except Exception as e:
            logger.error(f"Error computing missing tiles: {e}")
    
    # Cache the final result for this viewport
    if all_results:
        try:
             await redis_client_async.setex(viewport_key, 300, orjson.dumps(all_results)) # Cache for 5 mins
        except Exception as e:
             logger.error(f"Viewport cache write error: {e}")
    
    return all_results


@app.get("/api/tiles/{z}/{x}/{y}")
async def get_tile(z: int, x: int, y: int, response: Response):
    # Set cache headers
    response.headers["Cache-Control"] = "public, max-age=86400"
    
    # Try Redis first
    cache_key = f"tile:{z}:{x}:{y}"
    try:
        cached = await redis_client_async.get(cache_key)
        if cached:
            logger.debug(f"Cache HIT: {cache_key}")
            return orjson.loads(cached)
        else:
            logger.info(f"Cache MISS: {cache_key} - computing on-the-fly")
    except Exception as e:
        logger.error(f"Redis error for {cache_key}: {e}")
    
    # Fallback to on-the-fly calculation
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(executor, compute_tile_on_fly, x, y, z)
        
    # Store in Redis
    try:
        if result:
            await redis_client_async.setex(cache_key, 86400, orjson.dumps(result))
    except Exception as e:
        logger.error(f"Redis write error: {e}")
        
    return result

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
