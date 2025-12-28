import sys
import os
import time
import math
import json
import logging
import polars as pl
from database import SessionLocal
from utils import tile_to_bbox, perform_clustering
from cache import redis_client

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def get_db_data():
    """Load all properties from Postgres to memory using Polars."""
    logger.info("Loading all properties from DB to memory with Polars...")
    db = SessionLocal()
    try:
        # Check if we can read directly
        query = "SELECT * FROM properties"
        df = pl.read_database(query, db.bind)
        logger.info(f"Loaded {len(df)} properties.")
        return df
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        return None
    finally:
        db.close()

def precompute_all_tiles(df=None, r_client=None):
    start_global = time.time()
    
    # 1. Load Data
    if df is None:
        df = get_db_data()
    
    if df is None or df.is_empty():
        logger.error("No data found. Exiting.")
        return

    # Ensure we have valid lat/lon
    df = df.drop_nulls(subset=['latitude', 'longitude'])
    
    # 2. Setup Redis
    if r_client is None:
        r_client = redis_client

    pipe = r_client.pipeline()
    total_tiles_generated = 0
    
    # 2. Iterate Zoom Levels
    for zoom in range(6, 15): # 6 to 14 inclusive
        logger.info(f"Processing zoom level {zoom}...")
        
        n = 2.0 ** zoom
        
        # Calculate tile coordinates for all points using Polars expressions
        # x = floor((lon + 180) / 360 * 2^zoom)
        df_zoom = df.with_columns([
            ((pl.col("longitude") + 180.0) / 360.0 * n).floor().cast(pl.Int32).alias("tile_x")
        ])
        
        # y calculation involves tan/log/cos
        df_zoom = df_zoom.with_columns(
            (pl.col("latitude") * math.pi / 180.0).alias("lat_rad")
        )
        
        df_zoom = df_zoom.with_columns(
            ((1.0 - (
                (pl.col("lat_rad").tan() + (1.0 / pl.col("lat_rad").cos())).log()
            ) / math.pi) / 2.0 * n).floor().cast(pl.Int32).alias("tile_y")
        )
        
        # Filter for relevant area (e.g. France) to avoid generating empty tiles far away
        # Bounds: lat 41-51, lon -5 to 10
        tiles_dict = df_zoom.filter(
            (pl.col("latitude") >= 41) & (pl.col("latitude") <= 51) &
            (pl.col("longitude") >= -5) & (pl.col("longitude") <= 10)
        ).partition_by(["tile_x", "tile_y"], as_dict=True)
        
        logger.info(f"Zoom {zoom}: {len(tiles_dict)} tiles to process.")
        
        # Process tiles sequentially (Polars is already fast)
        count = 0
        for (tx, ty), tile_df in tiles_dict.items():
            min_lat, max_lat, min_lon, max_lon = tile_to_bbox(tx, ty, zoom)
            result = perform_clustering(tile_df, min_lat, max_lat, min_lon, max_lon, zoom)
            
            if result:
                key = f"tile:{zoom}:{tx}:{ty}"
                pipe.setex(key, 2592000, json.dumps(result)) # 30 days
                count += 1
                
            if count % 1000 == 0:
                pipe.execute()
                
        pipe.execute()
        
        total_tiles_generated += count
        logger.info(f"Zoom {zoom}: Finished. {count} tiles stored.")
        
    end_global = time.time()
    logger.info(f"Pre-computation complete. Total tiles: {total_tiles_generated}. Time: {end_global - start_global:.2f}s")

if __name__ == "__main__":
    precompute_all_tiles()
