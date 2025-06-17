import os
import time
import requests
import pandas as pd
from datetime import datetime, timedelta
import zipfile
import pytz  # <--- NEW IMPORT

# ------------- CONFIGURATION ------------------

IST = pytz.timezone("Asia/Kolkata")

START_DATE = datetime.strptime("2025-06-02", "%Y-%m-%d")
START_DATE = IST.localize(START_DATE)  # make aware

now_ist = datetime.now(IST)

# Determine if we passed 5:30 PM IST or not
end_date_candidate = now_ist.replace(hour=19, minute=30, second=0, microsecond=0)

if now_ist >= end_date_candidate:
    # After 5:30 PM IST, we can fetch today's data
    END_DATE = now_ist.replace(hour=0, minute=0, second=0, microsecond=0)
else:
    # Before 5:30 PM IST, fetch only up to yesterday
    END_DATE = (now_ist - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

# Convert both to naive datetime for consistent comparison and iteration
START_DATE = START_DATE.replace(tzinfo=None)
END_DATE = END_DATE.replace(tzinfo=None)

# NSE (DDMMYYYY)
NSE_BASE_URL = "https://nsearchives.nseindia.com/archives/fo/mkt/fo{}.zip"
NSE_CACHE_DIR = "nse_fo_cache"
NSE_COLUMN = "NO_OF_TRADE"

# BSE (YYYYMMDD)
BSE_BASE_URL = "https://www.bseindia.com/download/Bhavcopy/Derivative/MS_{}.csv"
BSE_CACHE_DIR = "bse_csv_cache"
BSE_COLUMN = "No. of Trades"

# Combined output file
OUTPUT_CSV = r"C:\Users\rachit.jain\Desktop\Python projects\Exisitng project\NSE BSE Combined\Combined\combined_nse_bse_trade_summary.csv"


RETRIES = 3
TIMEOUT = 60  # seconds

# Common trading holidays 2025 (adjust as needed)
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

# Browser-like headers for all requests
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/114.0.0.0 Safari/537.36"
    ),
}

BSE_HEADERS = {
    **DEFAULT_HEADERS,
    "Referer": "https://www.bseindia.com/",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
}

# Create cache directories if not exist
os.makedirs(NSE_CACHE_DIR, exist_ok=True)
os.makedirs(BSE_CACHE_DIR, exist_ok=True)

# Load existing combined summary
if os.path.exists(OUTPUT_CSV):
    combined_df = pd.read_csv(OUTPUT_CSV, parse_dates=["Date"])
    processed_dates = set(combined_df["Date"].dt.strftime("%Y-%m-%d"))
else:
    combined_df = pd.DataFrame(columns=["Date", "NSE_NO_OF_TRADE", "BSE_No_of_Trades"])
    processed_dates = set()


def download_with_retries(url, headers=None, retries=RETRIES, timeout=TIMEOUT, backoff=10):
    merged_headers = {**DEFAULT_HEADERS, **(headers or {})}
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=merged_headers, timeout=timeout)
            if response.status_code == 200:
                return response.content
            else:
                print(f"Attempt {attempt+1}: HTTP {response.status_code} for {url}")
        except requests.exceptions.RequestException as e:
            print(f"Attempt {attempt+1} failed: {e}")
        wait_time = backoff * (2 ** attempt)
        print(f"Waiting {wait_time}s before retrying...")
        time.sleep(wait_time)
    return None


def is_trading_day(date_obj):
    iso_date = date_obj.strftime("%Y-%m-%d")
    if date_obj.weekday() >= 5:  # Saturday or Sunday
        return False
    if iso_date in TRADING_HOLIDAYS_2025:
        return False
    return True


def process_nse_day(date_obj):
    iso_date = date_obj.strftime("%Y-%m-%d")
    date_str = date_obj.strftime("%d%m%Y")
    filename = f"fo{date_str}.zip"
    url = NSE_BASE_URL.format(date_str)
    cache_path = os.path.join(NSE_CACHE_DIR, filename)

    print(f"\nNSE: Processing {iso_date}...")

    if not os.path.exists(cache_path):
        content = download_with_retries(url)
        if not content:
            print(f"NSE: Failed to download {filename} from {url}. Skipping...")
            return None
        with open(cache_path, "wb") as f:
            f.write(content)
    else:
        print(f"NSE: Using cached file: {filename}")

    try:
        with zipfile.ZipFile(cache_path, 'r') as z:
            inner_file = f"op{date_str}.csv"
            if inner_file not in z.namelist():
                inner_file = f"op{date_str}.dat"  # fallback
            if inner_file not in z.namelist():
                print(f"NSE: {inner_file} not found in ZIP. Skipping...")
                return None
            with z.open(inner_file) as f:
                df = pd.read_csv(f)
                df.columns = df.columns.str.strip()
                if NSE_COLUMN in df.columns:
                    day_sum = df[NSE_COLUMN].sum()
                    print(f"NSE: {iso_date}: {day_sum} total {NSE_COLUMN}")
                    return day_sum
                else:
                    print(f"NSE: Column '{NSE_COLUMN}' not found in {inner_file}")
                    return None
    except Exception as e:
        print(f"NSE: Error processing {filename}: {e}")
        return None


def process_bse_day(date_obj):
    iso_date = date_obj.strftime("%Y-%m-%d")
    date_str = date_obj.strftime("%Y%m%d")
    filename = f"MS_{date_str}-01.csv"
    url = BSE_BASE_URL.format(f"{date_str}-01")
    cache_path = os.path.join(BSE_CACHE_DIR, filename)

    print(f"\nBSE: Processing {iso_date}...")

    if not os.path.exists(cache_path):
        content = download_with_retries(url, headers=BSE_HEADERS)
        if not content:
            print(f"BSE: Failed to download {filename} from {url}. Skipping...")
            return None
        with open(cache_path, "wb") as f:
            f.write(content)
    else:
        print(f"BSE: Using cached file: {filename}")

    try:
        df = pd.read_csv(cache_path)
        df.columns = df.columns.str.strip()
        if BSE_COLUMN in df.columns:
            day_sum = df[BSE_COLUMN].sum()
            print(f"BSE: {iso_date}: {day_sum} total {BSE_COLUMN}")
            return day_sum
        else:
            print(f"BSE: Column '{BSE_COLUMN}' not found in {filename}")
            return None
    except Exception as e:
        print(f"BSE: Error processing {filename}: {e}")
        return None


# ----------------- MAIN PROCESS ------------------

new_rows = []

current_date = START_DATE
while current_date <= END_DATE:
    iso_date = current_date.strftime("%Y-%m-%d")
    if not is_trading_day(current_date):
        print(f"Skipping weekend/holiday: {iso_date}")
        current_date += timedelta(days=1)
        continue

    if iso_date in processed_dates:
        print(f"Skipping already processed date: {iso_date}")
        current_date += timedelta(days=1)
        continue

    nse_total = process_nse_day(current_date)
    bse_total = process_bse_day(current_date)

    new_rows.append({
        "Date": iso_date,
        "NSE_NO_OF_TRADE": nse_total if nse_total is not None else pd.NA,
        "BSE_No_of_Trades": bse_total if bse_total is not None else pd.NA,
    })

    processed_dates.add(iso_date)
    current_date += timedelta(days=1)
    time.sleep(1)  # polite wait between requests

# Append and save combined data
if new_rows:
    new_df = pd.DataFrame(new_rows)
    combined_df = pd.concat([combined_df, new_df], ignore_index=True)
    combined_df.sort_values("Date", inplace=True)
    combined_df.to_csv(OUTPUT_CSV, index=False)
    print(f"\n✅ Updated {OUTPUT_CSV} with {len(new_rows)} new entries.")
else:
    print("\n✅ No new data to update.")

print("\nProcess completed.")
