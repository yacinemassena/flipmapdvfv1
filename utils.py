import polars as pl
from h3 import latlng_to_cell

MIN_RES = 6
MAX_RES = 14


def clamp_res(res: int) -> int:
    return max(MIN_RES, min(MAX_RES, res))


def assign_h3(df: pl.DataFrame, res: int) -> pl.DataFrame:
    """
    Polars-safe H3 assignment using map_elements (NOT apply)
    """
    return df.with_columns(
        pl.struct(["latitude", "longitude"])
        .map_elements(lambda p: latlng_to_cell(p["latitude"], p["longitude"], res))
        .alias("h3")
    )


def cluster_by_h3(df: pl.DataFrame, res: int) -> list[dict]:
    if df.is_empty():
        return []

    df = assign_h3(df, res)

    grouped = df.group_by("h3").agg(
        [
            # center
            pl.col("latitude").mean().alias("latitude"),
            pl.col("longitude").mean().alias("longitude"),
            # bounds (CRITICAL)
            pl.col("latitude").min().alias("min_lat"),
            pl.col("latitude").max().alias("max_lat"),
            pl.col("longitude").min().alias("min_lon"),
            pl.col("longitude").max().alias("max_lon"),
            # count
            pl.len().alias("count"),
            # property fields (safe: FE uses only when count == 1)
            pl.col("id").first().alias("id"),
            pl.col("margin").max().alias("margin"),
            pl.col("type_local").first().alias("type_local"),
            pl.col("address").first().alias("address"),
            pl.col("days_on_market").first().alias("days_on_market"),
        ]
    )

    return grouped.to_dicts()
