import pandas as pd
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import FileResponse
from typing import List, Union
import os
from database import SessionLocal
from schemas import PropertySchema, ClusterSchema
from cache import redis_client
from import_data import import_data

# Create tables if they don't exist (handled in import_data but good to have)
# Base.metadata.create_all(bind=engine)

app = FastAPI()

app.add_middleware(GZipMiddleware, minimum_size=1000)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Get absolute path to frontend directory
FRONTEND_DIR = os.path.dirname(os.path.abspath(__file__))

ALL_PROPERTIES_KEY = "all_properties"
PROPERTIES_DF = None

@app.on_event("startup")
def load_db_to_redis():
    """Load all properties from Postgres to Redis and memory on startup."""
    global PROPERTIES_DF
    
    # Ensure data is imported
    try:
        import_data()
    except Exception as e:
        print(f"Error importing data: {e}")

    print("Loading all properties from DB to memory...")
    db = SessionLocal()
    try:
        # Fetch all properties
        # Use pandas to read directly from sql for efficiency
        query = "SELECT * FROM properties"
        PROPERTIES_DF = pd.read_sql(query, db.bind)
        
        # Convert to JSON and store in Redis (backup)
        # orient='records' creates a list of dicts: [{col: val, ...}, ...]
        json_data = PROPERTIES_DF.to_json(orient="records")
        
        # Store with 30 days TTL (2592000 seconds)
        redis_client.setex(ALL_PROPERTIES_KEY, 2592000, json_data)
        print(f"Loaded {len(PROPERTIES_DF)} properties to memory and Redis.")
    except Exception as e:
        print(f"Error loading data to Redis: {e}")
    finally:
        db.close()

@app.get("/")
def read_root():
    return FileResponse(os.path.join(FRONTEND_DIR, "index.html"))

@app.get("/api/markers", response_model=Union[List[PropertySchema], List[ClusterSchema]])
def get_markers(
    min_lat: float,
    max_lat: float,
    min_lon: float,
    max_lon: float,
    zoom: float,
):
    global PROPERTIES_DF

    if PROPERTIES_DF is None:
        # Fallback: Reload from DB if memory is empty
        print("Memory cache miss (PROPERTIES_DF), reloading...")
        load_db_to_redis()
        if PROPERTIES_DF is None:
            return [] # Should not happen if DB has data

    # Filter by bounding box directly on DataFrame in memory
    df_filtered = PROPERTIES_DF[
        (PROPERTIES_DF['latitude'] >= min_lat) & 
        (PROPERTIES_DF['latitude'] <= max_lat) & 
        (PROPERTIES_DF['longitude'] >= min_lon) & 
        (PROPERTIES_DF['longitude'] <= max_lon)
    ]

    if zoom >= 14:
        # Return individual properties (limit 2000)
        return df_filtered.head(2000).to_dict(orient="records")
    else:
        # Return clusters
        if df_filtered.empty:
            return []

        lat_diff = max_lat - min_lat
        lon_diff = max_lon - min_lon
        
        # Avoid division by zero
        if lat_diff == 0: lat_diff = 0.0001
        if lon_diff == 0: lon_diff = 0.0001
        
        resolution = 10
        lat_step = lat_diff / resolution
        lon_step = lon_diff / resolution
        
        # Create grid indices
        # We need to copy because we are modifying the filtered slice
        # Use assign to avoid SettingWithCopyWarning if possible, but copy is safer for indices
        df_grid = df_filtered.copy()
        df_grid['lat_idx'] = ((df_grid['latitude'] - min_lat) / lat_step).astype(int)
        df_grid['lon_idx'] = ((df_grid['longitude'] - min_lon) / lon_step).astype(int)
        
        grouped = df_grid.groupby(['lat_idx', 'lon_idx'])
        
        agg_funcs = {
            'latitude': 'mean',
            'longitude': 'mean',
            'id': 'max', # Use max id as representative
            'margin': 'max',
            'type_local': 'max',
            'address': 'max'
        }
        
        # Calculate count separately
        counts = grouped.size().reset_index(name='count')
        
        # Calculate other aggregates
        agged = grouped.agg(agg_funcs).reset_index()
        
        # Merge
        result = pd.merge(agged, counts, on=['lat_idx', 'lon_idx'])
        
        # Convert to list of dicts
        return result.to_dict(orient="records")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
