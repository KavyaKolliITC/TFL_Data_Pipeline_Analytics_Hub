import requests
import pandas as pd
import time
import os
from datetime import datetime

# TfL API URLs
urls = {
    "Piccadilly": "https://api.tfl.gov.uk/Line/Piccadilly/Arrivals?app_id=92293faa428041caad3dd647d39753a0&app_key=ba72936a3db54b4ba5792dc8f7acc043",
    "Northern": "https://api.tfl.gov.uk/Line/Northern/Arrivals?app_id=92293faa428041caad3dd647d39753a0&app_key=ba72936a3db54b4ba5792dc8f7acc043",
    "Central": "https://api.tfl.gov.uk/Line/Central/Arrivals?app_id=92293faa428041caad3dd647d39753a0&app_key=ba72936a3db54b4ba5792dc8f7acc043",
    "Bakerloo": "https://api.tfl.gov.uk/Line/Bakerloo/Arrivals?app_id=92293faa428041caad3dd647d39753a0&app_key=ba72936a3db54b4ba5792dc8f7acc043",
    "Metropolitan": "https://api.tfl.gov.uk/Line/Metropolitan/Arrivals?app_id=92293faa428041caad3dd647d39753a0&app_key=ba72936a3db54b4ba5792dc8f7acc043",
    "Victoria": "https://api.tfl.gov.uk/Line/Victoria/Arrivals?app_id=92293faa428041caad3dd647d39753a0&app_key=ba72936a3db54b4ba5792dc8f7acc043"
}

# Save inside: TFL_Batch Processing/data/raw
BASE_DIR = os.path.dirname(os.path.dirname(__file__))  # go from src → batch folder
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
os.makedirs(RAW_DIR, exist_ok=True)

print(f"Saving files to: {RAW_DIR}")

while True:
    for name, url in urls.items():
        print(f"Fetching {name} line...")

        try:
            response = requests.get(url, timeout=30)
            data = response.json()

            if not data:
                print(f"No data for {name}")
                continue

            # Convert JSON → DataFrame (keeps all columns)
            df_new = pd.json_normalize(data)

            # Add ingestion timestamp
            df_new["api_fetch_time"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

            file_path = os.path.join(RAW_DIR, f"{name}.csv")

            # If file exists → append, dedupe
            if os.path.exists(file_path):
                df_old = pd.read_csv(file_path)

                df_combined = pd.concat([df_old, df_new], ignore_index=True)

                # Keep unique rows using ID
                df_combined.drop_duplicates(subset=["id"], inplace=True)

                df_combined.to_csv(file_path, index=False)
                print(f"Updated {file_path} → {len(df_combined)} rows")

            else:
                # First time save
                df_new.to_csv(file_path, index=False)
                print(f"Created {file_path} → {len(df_new)} rows")

        except Exception as e:
            print(f" Error fetching {name}: {e}")

    print("Waiting 60 seconds...\n")
    time.sleep(60)
