import pandas as pd
import sys
import os
import requests
from sqlalchemy.orm import Session
from database import SessionLocal, engine, Base
from models import Property

# Create tables
Base.metadata.create_all(bind=engine)

CSV_URL = os.getenv("CSV_URL", "https://pub-ecf2cacf42304db4aff89b230d889189.r2.dev/source_data.csv")
CSV_FILE = '/tmp/source_data.csv'

def import_data():
    # Download CSV from R2 if not exists locally
    if not os.path.exists(CSV_FILE):
        print(f"Downloading CSV from {CSV_URL}...")
        response = requests.get(CSV_URL, stream=True)
        response.raise_for_status()
        
        with open(CSV_FILE, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print("Download completed.")
    
    if not os.path.exists(CSV_FILE):
        print(f"File {CSV_FILE} not found after download.")
        return

    print("Reading CSV and importing to DB...")
    
    # Read CSV in chunks to handle memory efficiently
    chunk_size = 10000
    
    for i, chunk in enumerate(pd.read_csv(CSV_FILE, chunksize=chunk_size)):
        print(f"Processing chunk {i+1}...")
        
        # Rename property_id to id
        chunk = chunk.rename(columns={'property_id': 'id'})
        
        # Drop rows with missing lat/lon
        chunk = chunk.dropna(subset=['latitude', 'longitude'])
        
        # Write to DB
        chunk.to_sql('properties', con=engine, if_exists='append', index=False, method='multi')
        
    print("Import completed.")

if __name__ == "__main__":
    import_data()
