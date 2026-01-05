import os
import tempfile
import requests
import polars as pl

CSV_URL = os.getenv(
    "CSV_URL",
    "https://pub-ecf2cacf42304db4aff89b230d889189.r2.dev/source_data.csv",
)
CSV_FILE = os.path.join(tempfile.gettempdir(), "source_data.csv")


def load_dataframe() -> pl.DataFrame:
    if not os.path.exists(CSV_FILE):
        response = requests.get(CSV_URL, timeout=30)
        response.raise_for_status()
        with open(CSV_FILE, "wb") as f:
            f.write(response.content)

    df = pl.read_csv(CSV_FILE)

    if "property_id" in df.columns:
        df = df.rename({"property_id": "id"})

    df = df.drop_nulls(["latitude", "longitude"])

    if df.is_empty():
        raise RuntimeError("No data loaded!")

    return df
