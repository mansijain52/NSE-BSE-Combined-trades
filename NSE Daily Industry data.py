import os
import io
import time
import zipfile
import requests
import pandas as pd
from datetime import datetime, timedelta

# CONFIGURATION
START_DATE = datetime.strptime("02-06-2025", "%d-%m-%Y")
END_DATE = datetime.today()
BASE_URL = "https://nsearchives.nseindia.com/archives/fo/mkt/fo{}.zip"
CACHE_DIR = "nse_zip_cache"
RETRIES = 3
TIMEOUT = 30  # seconds

# Create cache directory if not exists
os.makedirs(CACHE_DIR, exist_ok=True)

# Retry logic for downloading
def download_with_retries(url, retries=RETRIES, timeout=TIMEOUT, backoff=5):
    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=timeout)
            if response.status_code == 200:
                return response.content
            else:
                print(f"Attempt {attempt+1}: HTTP {response.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"Attempt {attempt+1} failed: {e}")
        time.sleep(backoff * (attempt + 1))  # exponential backoff
    return None

# Main processing loop
total_trades = 0
current_date = START_DATE

while current_date <= END_DATE:
    date_str = current_date.strftime("%d%m%Y")
    zip_filename = f"fo{date_str}.zip"
    inner_file_name = f"op{date_str}.dat"
    zip_path = os.path.join(CACHE_DIR, zip_filename)

    print(f"\nProcessing {date_str}...")

    # Download if not cached
    if not os.path.exists(zip_path):
        url = BASE_URL.format(date_str)
        content = download_with_retries(url)
        if not content:
            print(f"Failed to download {zip_filename}. Skipping...")
            current_date += timedelta(days=1)
            continue

        # Save to cache
        with open(zip_path, "wb") as f:
            f.write(content)
    else:
        print(f"Using cached file: {zip_filename}")

    # Extract and process the file
    try:
        with zipfile.ZipFile(zip_path, "r") as z:
            if inner_file_name not in z.namelist():
                print(f"{inner_file_name} not found in ZIP. Skipping...")
                current_date += timedelta(days=1)
                continue

            with z.open(inner_file_name) as file:
                df = pd.read_csv(file)

                if "NO_OF_TRADE" in df.columns:
                    day_sum = df["NO_OF_TRADE"].sum()
                    total_trades += day_sum
                    print(f"{date_str}: {day_sum} trades")
                else:
                    print(f"'NO_OF_TRADE' column missing in {inner_file_name}")

    except Exception as e:
        print(f"Error processing {zip_filename}: {e}")

    current_date += timedelta(days=1)

# Final Output
print("\n✅ Total NO_OF_TRADE from 02-Jun-2025 to today:", total_trades)
