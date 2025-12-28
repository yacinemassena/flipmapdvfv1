import polars as pl
import sys
import os
import requests
from sqlalchemy import text
from database import engine, Base, SQLALCHEMY_DATABASE_URL
import tempfile

# Ensure models are loaded so tables can be created
from models import Property

# Create tables
Base.metadata.create_all(bind=engine)

CSV_URL = os.getenv("CSV_URL", "https://pub-ecf2cacf42304db4aff89b230d889189.r2.dev/source_data.csv")
CSV_FILE = os.path.join(tempfile.gettempdir(), 'source_data.csv')

def import_data():
    # Download CSV from R2 if not exists locally
    if not os.path.exists(CSV_FILE):
        print(f"Downloading CSV from {CSV_URL}...")
        try:
            response = requests.get(CSV_URL, stream=True)
            response.raise_for_status()
            
            with open(CSV_FILE, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            print("Download completed.")
        except Exception as e:
            print(f"Error downloading CSV: {e}")
            return
    
    if not os.path.exists(CSV_FILE):
        print(f"File {CSV_FILE} not found after download.")
        return

    # Check if data already exists
    with engine.connect() as connection:
        try:
            result = connection.execute(text("SELECT COUNT(*) FROM properties"))
            count = result.scalar()
            if count and count > 0:
                print(f"Database already contains {count} records. Skipping import.")
                return
        except Exception as e:
            print(f"Error checking database: {e}")
            # If table doesn't exist, we continue to import
            pass

    print("Reading CSV and importing to DB with Polars...")
    
    try:
        # Read CSV with Polars
        # Polars is much faster and handles types well
        df = pl.read_csv(CSV_FILE)
        
        # Rename property_id to id if it exists
        if "property_id" in df.columns:
            df = df.rename({"property_id": "id"})
            
        # Drop rows with missing lat/lon
        df = df.drop_nulls(subset=['latitude', 'longitude'])
        
        print(f"Read {len(df)} rows from CSV. Writing to database...")
        
        # Write to DB
        # We use the connection string (URL) if possible for better performance with connectorx/adbc if installed
        # But we'll fallback to engine if needed. 
        # For compatibility with sqlalchemy engine in write_database:
        df.write_database(
            table_name="properties",
            connection=engine,
            if_table_exists="append"
        )
        
        print("Import completed.")
        
    except Exception as e:
        print(f"Error importing data: {e}")

if __name__ == "__main__":
    import_data()
