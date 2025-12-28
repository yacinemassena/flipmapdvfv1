import math
import polars as pl

def tile_to_bbox(x, y, zoom):
    """Calculate the bounding box for a tile."""
    n = 2.0 ** zoom
    lon_min = x / n * 360.0 - 180.0
    lon_max = (x + 1) / n * 360.0 - 180.0
    
    lat_rad_min = math.atan(math.sinh(math.pi * (1 - 2 * (y + 1) / n)))
    lat_min = math.degrees(lat_rad_min)
    
    lat_rad_max = math.atan(math.sinh(math.pi * (1 - 2 * y / n)))
    lat_max = math.degrees(lat_rad_max)
    
    return lat_min, lat_max, lon_min, lon_max

def bounds_to_tiles(min_lat, max_lat, min_lon, max_lon, zoom, limit=200):
    """Convertit des bounds géographiques en liste de tiles (x, y)."""
    n = 2.0 ** zoom
    
    def lat_lon_to_tile(lat, lon):
        x = int((lon + 180.0) / 360.0 * n)
        # Handle potential edge cases for lat
        lat_rad = math.radians(lat)
        try:
            val = math.tan(lat_rad) + 1.0/math.cos(lat_rad)
            if val <= 0:
                y = 0 # Fallback
            else:
                y = int((1.0 - math.log(val) / math.pi) / 2.0 * n)
        except ValueError:
            y = 0
        return x, y
    
    min_tile_x, max_tile_y = lat_lon_to_tile(min_lat, min_lon)
    max_tile_x, min_tile_y = lat_lon_to_tile(max_lat, max_lon)
    
    # Normalize coordinates if they are flipped or crossed
    start_x, end_x = min(min_tile_x, max_tile_x), max(min_tile_x, max_tile_x)
    start_y, end_y = min(min_tile_y, max_tile_y), max(min_tile_y, max_tile_y)

    tiles = []
    for x in range(start_x, end_x + 1):
        for y in range(start_y, end_y + 1):
            tiles.append((x, y))
            if limit and len(tiles) >= limit:
                return tiles
    
    return tiles

def perform_clustering(df, min_lat, max_lat, min_lon, max_lon, zoom):
    """
    Performs clustering on a DataFrame within a bounding box.
    Returns a list of dicts.
    """
    if df.is_empty():
        return []

    if zoom >= 14:
        return df.head(500).to_dicts()
    
    # Dynamic resolution: fewer clusters at low zoom
    if zoom <= 6:
        resolution = 3      # 9 clusters per tile max
    elif zoom <= 8:
        resolution = 5      # 25 clusters per tile max
    elif zoom <= 10:
        resolution = 7      # 49 clusters per tile max
    else:
        resolution = 10     # 100 clusters per tile max
        
    lat_diff = max_lat - min_lat
    lon_diff = max_lon - min_lon
    
    if lat_diff == 0: lat_diff = 0.0001
    if lon_diff == 0: lon_diff = 0.0001
    
    lat_step = lat_diff / resolution
    lon_step = lon_diff / resolution
    
    result = (
        df
        .with_columns([
            ((pl.col('latitude') - min_lat) / lat_step).cast(pl.Int32).alias('lat_idx'),
            ((pl.col('longitude') - min_lon) / lon_step).cast(pl.Int32).alias('lon_idx')
        ])
        .group_by(['lat_idx', 'lon_idx'])
        .agg([
            pl.col('latitude').mean().alias('latitude'),
            pl.col('longitude').mean().alias('longitude'),
            pl.len().alias('count'),
            pl.col('id').first(),
            pl.col('margin').max(),
            pl.col('type_local').first(),
            pl.col('address').first()
        ])
    ).to_dicts()
    
    return result
