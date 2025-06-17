import os
import time
import zipfile
import requests
import pandas as pd
from datetime import datetime, timedelta

# CONFIGURATION
START_DATE = datetime.strptime("02-06-2025", "%d-%m-%Y")
END_DATE = datetime.today() - timedelta(days=1)
BASE_URL = "https://nsearchives.nseindia.com/archives/fo/mkt/fo{}.zip"
CACHE_DIR = "nse_zip_cache"
CSV_OUTPUT = r"C:\Users\rachit.jain\Desktop\Python projects\daily_trade_summary.csv"
RETRIES = 3
TIMEOUT = 30  # seconds

# NSE Trading holidays in 2025 (example list — update if needed)
TRADING_HOLIDAYS_2025 = {
    "2025-01-01",
    "2025-01-14",
    "2025-03-29",
    "2025-04-01",
    "2025-04-14",
    "2025-04-18",
    "2025-05-01",
    "2025-08-15",
    "2025-09-17",
    "2025-10-02",
    "2025-11-01",
    "2025-11-04",
    "2025-12-25",
}

# Browser-like headers to avoid blocking
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36"
}

# Create cache directory if not exists
os.makedirs(CACHE_DIR, exist_ok=True)

# Load existing summary CSV if it exists
if os.path.exists(CSV_OUTPUT):
    summary_df = pd.read_csv(CSV_OUTPUT, parse_dates=["Date"])
    processed_dates = set(summary_df["Date"].dt.strftime("%Y-%m-%d"))
else:
    summary_df = pd.DataFrame(columns=["Date", "NO_OF_TRADE"])
    processed_dates = set()

def download_with_retries(url, retries=RETRIES, timeout=TIMEOUT, backoff=5):
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=HEADERS, timeout=timeout)
            if response.status_code == 200:
                return response.content
            else:
                print(f"Attempt {attempt+1}: HTTP {response.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"Attempt {attempt+1} failed: {e}")
        time.sleep(backoff * (attempt + 1))  # exponential backoff
    return None

total_trades = 0
current_date = START_DATE
new_rows = []

while current_date <= END_DATE:
    iso_date = current_date.strftime("%Y-%m-%d")

    # Skip weekends
    if current_date.weekday() >= 5:  # 5=Sat, 6=Sun
        print(f"Skipping weekend: {iso_date}")
        current_date += timedelta(days=1)
        continue

    # Skip trading holidays
    if iso_date in TRADING_HOLIDAYS_2025:
        print(f"Skipping holiday: {iso_date}")
        current_date += timedelta(days=1)
        continue

    if iso_date in processed_dates:
        print(f"Skipping already processed date: {iso_date}")
        current_date += timedelta(days=1)
        continue

    date_str = current_date.strftime("%d%m%Y")
    zip_filename = f"fo{date_str}.zip"
    inner_file_name = f"op{date_str}.csv"
    zip_path = os.path.join(CACHE_DIR, zip_filename)

    print(f"\nProcessing {iso_date}...")

    # Download if not cached
    if not os.path.exists(zip_path):
        url = BASE_URL.format(date_str)
        content = download_with_retries(url)
        if not content:
            print(f"Failed to download {zip_filename}. Skipping...")
            current_date += timedelta(days=1)
            continue

        with open(zip_path, "wb") as f:
            f.write(content)
    else:
        print(f"Using cached file: {zip_filename}")

    try:
        with zipfile.ZipFile(zip_path, "r") as z:
            if inner_file_name not in z.namelist():
                print(f"{inner_file_name} not found in ZIP. Skipping...")
                current_date += timedelta(days=1)
                continue

            with z.open(inner_file_name) as file:
                df = pd.read_csv(file)
                df.columns = df.columns.str.strip()

                if "NO_OF_TRADE" in df.columns:
                    day_sum = df["NO_OF_TRADE"].sum()
                    total_trades += day_sum
                    new_rows.append({"Date": iso_date, "NO_OF_TRADE": day_sum})
                    print(f"{iso_date}: {day_sum} trades")
                else:
                    print(f"'NO_OF_TRADE' column missing in {inner_file_name}")

    except Exception as e:
        print(f"Error processing {zip_filename}: {e}")

    current_date += timedelta(days=1)

# Append new data and save CSV
if new_rows:
    new_df = pd.DataFrame(new_rows)
    combined_df = pd.concat([summary_df, new_df], ignore_index=True)
    combined_df.sort_values("Date", inplace=True)
    combined_df.to_csv(CSV_OUTPUT, index=False)
    print(f"\n✅ Updated {CSV_OUTPUT} with {len(new_rows)} new entries.")
else:
    print("\n✅ No new data to update.")

print(f"\nTotal NO_OF_TRADE from {START_DATE.strftime('%d-%b-%Y')} to {END_DATE.strftime('%d-%b-%Y')}: {total_trades}")
