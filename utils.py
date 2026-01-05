# utils.py
import math
import polars as pl
from h3 import latlng_to_cell


# -------------------------
# CSV clustering
# -------------------------


def assign_h3(df: pl.DataFrame, res: int) -> pl.DataFrame:
    """Polars-safe H3 assignment using map_elements (NOT apply)"""
    return df.with_columns(
        pl.struct(["latitude", "longitude"])
        .map_elements(lambda p: latlng_to_cell(p["latitude"], p["longitude"], res))
        .alias("h3")
    )


def cluster_by_h3(df: pl.DataFrame, res: int) -> list[dict]:
    """Cluster points by H3"""
    df = assign_h3(df, res)
    grouped = (
        df.group_by("h3")
        .agg(
            pl.count().alias("count"),
            pl.mean("latitude").alias("latitude"),
            pl.mean("longitude").alias("longitude"),
            pl.first("id").alias("id"),
            pl.max("margin").alias("margin"),
            pl.first("type_local").alias("type_local"),
            pl.first("address").alias("address"),
        )
        .sort("count", descending=True)
    )
    return grouped.to_dicts()


# -------------------------
# Tile utilities
# -------------------------
def tile_to_bbox(x: int, y: int, z: int):
    """Convert Web Mercator tile x,y,z to lat/lon bounds"""
    n = 2.0**z
    lon_min = x / n * 360.0 - 180.0
    lon_max = (x + 1) / n * 360.0 - 180.0

    lat_rad_max = math.atan(math.sinh(math.pi * (1 - 2 * y / n)))
    lat_rad_min = math.atan(math.sinh(math.pi * (1 - 2 * (y + 1) / n)))
    lat_min = math.degrees(lat_rad_min)
    lat_max = math.degrees(lat_rad_max)
    return lat_min, lat_max, lon_min, lon_max


def bounds_to_tiles(min_lat, max_lat, min_lon, max_lon, zoom):
    """Return all x,y tile coordinates covering a bounding box at zoom"""

    def latlon_to_tile(lat, lon, z):
        n = 2**z
        xtile = int((lon + 180.0) / 360.0 * n)
        ytile = int(
            (
                1
                - math.log(
                    math.tan(math.radians(lat)) + 1 / math.cos(math.radians(lat))
                )
                / math.pi
            )
            / 2
            * n
        )
        return xtile, ytile

    x0, y0 = latlon_to_tile(max_lat, min_lon, zoom)
    x1, y1 = latlon_to_tile(min_lat, max_lon, zoom)

    tiles = []
    for x in range(x0, x1 + 1):
        for y in range(y0, y1 + 1):
            tiles.append((x, y))
    return tiles
