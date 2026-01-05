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
    df = assign_h3(df, res)

    grouped = (
        df.group_by("h3")
        .agg(
            pl.count().alias("count"),
            pl.mean("latitude").alias("latitude"),
            pl.mean("longitude").alias("longitude"),
        )
        .sort("count", descending=True)
    )

    return grouped.to_dicts()
