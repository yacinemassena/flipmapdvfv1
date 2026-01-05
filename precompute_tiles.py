import logging
import orjson
from concurrent.futures import ThreadPoolExecutor
from cache import redis_sync
from utils import cluster_by_h3, MIN_RES, MAX_RES

logger = logging.getLogger("precompute")
executor = ThreadPoolExecutor(max_workers=8)

LOCK_KEY = "h3:precompute:lock"


def precompute_all(df):
    lock = redis_sync.lock(LOCK_KEY, timeout=3600, blocking=False)

    if not lock.acquire(blocking=False):
        logger.info("Another worker is precomputing. Skipping.")
        return

    try:
        logger.info("Precompute lock acquired")
        futures = []

        for res in range(MIN_RES, MAX_RES + 1):
            futures.append(executor.submit(_compute_resolution, df, res))

        for f in futures:
            f.result()

        redis_sync.set("h3:precompute:done", b"1")
        logger.info("Precompute completed")

    finally:
        lock.release()


def _compute_resolution(df, res: int):
    clusters = cluster_by_h3(df, res)

    pipe = redis_sync.pipeline(transaction=False)
    for item in clusters:
        key = f"h3:{res}:{item['h3']}"
        pipe.set(key, orjson.dumps(item))
    pipe.execute()

    logger.info(f"Resolution {res}: {len(clusters)} cells stored")
