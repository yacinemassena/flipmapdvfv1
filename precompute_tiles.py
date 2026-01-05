# precompute_tiles.py
import asyncio
import logging
import time
import orjson
from cache import redis_client
from utils import cluster_by_h3, assign_h3

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("precompute")

# Example zoom-to-H3 resolution mapping
ZOOM_TO_H3 = {6: 5, 7: 6, 8: 6, 9: 7, 10: 7, 11: 8, 12: 8, 13: 9, 14: 9}


async def precompute(df):
    """
    Precompute clusters for all zoom levels and store in Redis using a pipeline.
    """
    if df is None or df.is_empty():
        logger.warning("No data to precompute!")
        return

    start_time = time.time()
    pipe = redis_client.pipeline()

    total_tiles = 0

    for zoom in range(6, 15):
        h3_res = ZOOM_TO_H3[zoom]
        clusters = cluster_by_h3(df, h3_res)

        # Skip empty clusters
        if not clusters:
            continue

        # Compute Redis key per zoom (storing all clusters per zoom)
        key = f"zoom:{zoom}:all"

        # Use pipeline to store in Redis
        pipe.set(key, orjson.dumps(clusters))
        total_tiles += 1

        # Optional: flush every N zooms or N tiles
        if total_tiles % 5 == 0:
            await pipe.execute()

        logger.info(f"Zoom {zoom}: {len(clusters)} clusters queued for Redis.")

    # Final flush
    await pipe.execute()
    elapsed = time.time() - start_time
    logger.info(
        f"Precompute complete. Total zooms: {total_tiles}. Time: {elapsed:.2f}s"
    )


def get_zoom_resolution(zoom: int) -> int:
    return ZOOM_TO_H3.get(int(zoom), 9)
