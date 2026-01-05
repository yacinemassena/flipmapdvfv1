# import_data.py
import polars as pl
import os
import tempfile
import requests

CSV_URL = os.getenv(
    "CSV_URL", "https://pub-ecf2cacf42304db4aff89b230d889189.r2.dev/source_data.csv"
)
CSV_FILE = os.path.join(tempfile.gettempdir(), "source_data.csv")


def import_data() -> pl.DataFrame:
    """Download CSV if missing and load as Polars DataFrame."""
    if not os.path.exists(CSV_FILE):
        print(f"Downloading CSV from {CSV_URL}...")
        response = requests.get(CSV_URL, stream=True)
        response.raise_for_status()
        with open(CSV_FILE, "wb") as f:
            for chunk in response.iter_content(8192):
                f.write(chunk)
        print("Download complete.")

    df = pl.read_csv(CSV_FILE)
    if "property_id" in df.columns:
        df = df.rename({"property_id": "id"})

    df = df.drop_nulls(subset=["latitude", "longitude"])
    print(f"Loaded {len(df)} properties.")
    return df
