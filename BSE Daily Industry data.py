import os
import time
import requests
import pandas as pd
from datetime import datetime, timedelta

# CONFIGURATION
START_DATE = datetime.strptime("2025-06-02", "%Y-%m-%d")
END_DATE = datetime.today() - timedelta(days=1)  # Up to yesterday
BASE_URL = "https://www.bseindia.com/download/Bhavcopy/Derivative/MS_{}.csv"
CACHE_DIR = "bse_csv_cache"
CSV_OUTPUT = r"C:\Users\rachit.jain\Desktop\Python projects\bse_daily_trade_summary.csv"
RETRIES = 3
TIMEOUT = 30  # seconds

# Column to sum from the CSV - update as needed
COLUMN_TO_SUM = "No. of Trades"  # Replace with actual column name from BSE CSV

# Trading holidays in 2025 (example list — update if needed)
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

# Browser-like headers to mimic a real user
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/114.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.bseindia.com/",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
}

# Create cache directory if not exists
os.makedirs(CACHE_DIR, exist_ok=True)

# Load existing summary CSV if it exists
if os.path.exists(CSV_OUTPUT):
    summary_df = pd.read_csv(CSV_OUTPUT, parse_dates=["Date"])
    processed_dates = set(summary_df["Date"].dt.strftime("%Y-%m-%d"))
else:
    summary_df = pd.DataFrame(columns=["Date", COLUMN_TO_SUM])
    processed_dates = set()

def download_with_retries(url, retries=RETRIES, timeout=TIMEOUT, backoff=5):
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=HEADERS, timeout=timeout)
            if response.status_code == 200:
                return response.content
            else:
                print(f"Attempt {attempt+1}: HTTP {response.status_code} for {url}")
        except requests.exceptions.RequestException as e:
            print(f"Attempt {attempt+1} failed: {e}")
        time.sleep(backoff * (attempt + 1))  # exponential backoff
    return None

total_sum = 0
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

    date_str = current_date.strftime("%Y%m%d")
    filename = f"MS_{date_str}-01.csv"
    url = BASE_URL.format(f"{date_str}-01")
    cache_path = os.path.join(CACHE_DIR, filename)

    print(f"\nProcessing {iso_date}...")

    # Download if not cached
    if not os.path.exists(cache_path):
        content = download_with_retries(url)
        if not content:
            print(f"Failed to download {filename} from {url}. Skipping...")
            current_date += timedelta(days=1)
            continue

        with open(cache_path, "wb") as f:
            f.write(content)
    else:
        print(f"Using cached file: {filename}")

    try:
        df = pd.read_csv(cache_path)
        df.columns = df.columns.str.strip()

        if COLUMN_TO_SUM in df.columns:
            day_sum = df[COLUMN_TO_SUM].sum()
            total_sum += day_sum
            new_rows.append({"Date": iso_date, COLUMN_TO_SUM: day_sum})
            print(f"{iso_date}: {day_sum} total {COLUMN_TO_SUM}")
        else:
            print(f"Column '{COLUMN_TO_SUM}' not found in {filename}")

    except Exception as e:
        print(f"Error processing {filename}: {e}")

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

print(f"\nTotal {COLUMN_TO_SUM} from {START_DATE.strftime('%Y-%m-%d')} to {END_DATE.strftime('%Y-%m-%d')}: {total_sum}")
