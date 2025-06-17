import time
import requests

# Test NSE file URL
test_date = "02062025"
test_url = f"https://nsearchives.nseindia.com/archives/fo/mkt/fo{test_date}.zip"

# Standard browser headers to avoid being blocked
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36"
}

print(f"Testing download from: {test_url}")

try:
    start_time = time.time()
    response = requests.get(test_url, headers=HEADERS, timeout=30)
    elapsed = time.time() - start_time

    print(f"\nHTTP Status Code: {response.status_code}")
    print(f"Time taken: {elapsed:.2f} seconds")

    if response.status_code == 200:
        file_size_kb = len(response.content) / 1024
        print(f"Downloaded file size: {file_size_kb:.2f} KB")

        # Save it for inspection
        with open("test_download.zip", "wb") as f:
            f.write(response.content)
        print("✅ File saved as 'test_download.zip'")
    else:
        print("❌ Failed to download. Possibly blocked or file missing.")

except requests.exceptions.RequestException as e:
    print(f"❌ Request failed with error:\n{e}")
