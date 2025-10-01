# ==================================================
# REVISION: rev.0-debug
# فایل تست برای بررسی مراحل داده GSC و تولید stable_key
# ==================================================

from google.oauth2 import service_account
from googleapiclient.discovery import build
import pandas as pd
from datetime import datetime, timedelta
import hashlib
import time
import os
import json
import sys

# ---------- CONFIG ----------
SITE_URL = 'https://bamtabridsazan.com/'
ROW_LIMIT = 25000
START_DATE = (datetime.utcnow() - timedelta(days=480)).strftime('%Y-%m-%d')  # 16 months ago
END_DATE = (datetime.utcnow() - timedelta(days=1)).strftime('%Y-%m-%d')       # yesterday
RETRY_DELAY = 60  # seconds

# ---------- CREDENTIALS ----------
service_account_file = os.environ.get("SERVICE_ACCOUNT_FILE", "gcp-key.json")
with open(service_account_file, "r") as f:
    sa_info = json.load(f)

credentials = service_account.Credentials.from_service_account_info(sa_info)

# Build Search Console service
service = build('searchconsole', 'v1', credentials=credentials)

# --- Check access ---
try:
    response = service.sites().get(siteUrl=SITE_URL).execute()
    print(f"✅ Service Account has access to {SITE_URL}", flush=True)
except Exception as e:
    print(f"❌ Service Account does NOT have access to {SITE_URL}", flush=True)
    print("Error details:", e)
    sys.exit(1)

# ---------- HELPER: create deterministic unique key ----------
def stable_key(row):
    # ۱. Normalize Query
    query = (row.get('Query') or '').strip().lower()
    
    # ۲. Normalize Page / URL
    page = (row.get('Page') or '').strip().lower().rstrip('/')  # حذف trailing slash
    
    # ۳. Normalize Date به YYYY-MM-DD
    date_raw = row.get('Date')
    if isinstance(date_raw, str):
        date = date_raw[:10]  # فقط yyyy-mm-dd
    elif isinstance(date_raw, datetime):
        date = date_raw.strftime("%Y-%m-%d")
    else:
        date = str(date_raw)[:10]  # fallback
    
    # ۴. بساز یک tuple deterministic از فیلدهای normalized
    det_tuple = (query, page, date)
    
    # ۵. Join و هش
    s = "|".join(det_tuple)
    key = hashlib.sha256(s.encode('utf-8')).hexdigest()
    
    print(f"[DEBUG] RAW ROW: {row}")
    print(f"[DEBUG] NORMALIZED: query={query}, page={page}, date={date}")
    print(f"[DEBUG] GENERATED KEY: {key}")
    
    return key

# ---------- FETCH GSC DATA WITH DEBUG ----------
def fetch_gsc_data_debug(start_date, end_date):
    all_rows = []
    start_row = 0
    batch_index = 1

    while True:
        request = {
            'startDate': start_date,
            'endDate': end_date,
            'dimensions': ['date','query','page'],
            'rowLimit': ROW_LIMIT,
            'startRow': start_row
        }

        try:
            resp = service.searchanalytics().query(siteUrl=SITE_URL, body=request).execute()
        except Exception as e:
            print(f"[ERROR] Timeout or error: {e}, retrying in {RETRY_DELAY} sec...", flush=True)
            time.sleep(RETRY_DELAY)
            continue

        rows = resp.get('rows', [])
        if not rows:
            print("[INFO] No more rows returned from GSC.", flush=True)
            break

        batch_count = 0
        for r in rows[:5]:  # فقط ۵ رکورد اول هر batch برای تست
            date = r['keys'][0]
            query_text = r['keys'][1]
            page = r['keys'][2]
            clicks = r.get('clicks',0)
            impressions = r.get('impressions',0)
            ctr = r.get('ctr',0)
            position = r.get('position',0)

            key = stable_key({
                'Query': query_text,
                'Page': page,
                'Date': date
            })

            all_rows.append({
                'Date': date,
                'Query': query_text,
                'Page': page,
                'Clicks': clicks,
                'Impressions': impressions,
                'CTR': ctr,
                'Position': position,
                'unique_key': key
            })
            batch_count += 1

        print(f"[INFO] Batch {batch_index}: Fetched {len(rows)} rows, {batch_count} debug rows processed.", flush=True)
        batch_index += 1

        if len(rows) < ROW_LIMIT:
            break
        start_row += len(rows)

        # توقف بعد از اولین batch برای تست
        if batch_index > 2:
            print("[INFO] Stopping after first batch for debug test.", flush=True)
            break

    df_debug = pd.DataFrame(all_rows)
    print(f"[INFO] Total debug rows collected: {len(df_debug)}", flush=True)
    return df_debug

# ---------- MAIN ----------
if __name__ == "__main__":
    df_debug = fetch_gsc_data_debug(START_DATE, END_DATE)
    print("[INFO] DEBUG fetch finished. Showing first 5 rows:")
    print(df_debug.head())
