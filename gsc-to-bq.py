# REVISION: rev.1
from google.oauth2 import service_account
from googleapiclient.discovery import build
from google.cloud import bigquery
import pandas as pd
from datetime import datetime, timedelta
import hashlib
import time
import os
import json
import sys

# ---------- CONFIG ----------
SITE_URL = 'https://bamtabridsazan.com/'
ROW_LIMIT = 5  # برای debug، هر batch فقط 5 رکورد
START_DATE = (datetime.utcnow() - timedelta(days=480)).strftime('%Y-%m-%d')
END_DATE = (datetime.utcnow() - timedelta(days=1)).strftime('%Y-%m-%d')
RETRY_DELAY = 5  # ثانیه در صورت timeout

# ---------- CREDENTIALS ----------
service_account_file = os.environ.get("SERVICE_ACCOUNT_FILE", "gcp-key.json")
with open(service_account_file, "r") as f:
    sa_info = json.load(f)
credentials = service_account.Credentials.from_service_account_info(sa_info)
service = build('searchconsole', 'v1', credentials=credentials)

# ---------- BIGQUERY CLIENT ----------
bq_client = bigquery.Client(credentials=credentials, project=credentials.project_id)

# ---------- HELPER: create deterministic key ----------
def stable_key(row):
    query = (row.get('Query') or '').strip().lower()
    page = (row.get('Page') or '').strip().lower().rstrip('/')
    date_raw = row.get('Date')
    if isinstance(date_raw, str):
        date = date_raw[:10]
    elif isinstance(date_raw, datetime):
        date = date_raw.strftime("%Y-%m-%d")
    else:
        date = str(date_raw)[:10]
    s = f"{query}|{page}|{date}"
    print(f"[DEBUG] STRING TO HASH: '{s}'", flush=True)
    key = hashlib.sha256(s.encode('utf-8')).hexdigest()
    print(f"[DEBUG] GENERATED KEY: {key}", flush=True)
    return key

# ---------- FETCH EXISTING KEYS FROM BIGQUERY ----------
def get_existing_keys():
    # برای debug، می‌تونیم خالی فرض کنیم یا key های واقعی رو برگردونیم
    try:
        query = f"SELECT unique_key FROM `bamtabridsazan.seo_reports.bamtabridsazan__gsc__raw_data`"
        df = bq_client.query(query).to_dataframe()
        print(f"[INFO] Retrieved {len(df)} existing keys from BigQuery.", flush=True)
        return set(df['unique_key'].astype(str).tolist())
    except Exception as e:
        print(f"[WARN] Failed to fetch existing keys: {e}", flush=True)
        return set()

# ---------- FETCH GSC DATA ----------
def fetch_gsc_data(start_date, end_date):
    all_rows = []
    start_row = 0
    existing_keys = get_existing_keys()
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
        for r in rows:
            date = r['keys'][0]
            query_text = r['keys'][1]
            page = r['keys'][2]
            clicks = r.get('clicks',0)
            impressions = r.get('impressions',0)
            ctr = r.get('ctr',0)
            position = r.get('position',0)

            print(f"[DEBUG] RAW ROW: {r}", flush=True)
            key = stable_key({'Query': query_text, 'Page': page, 'Date': date})

            if key not in existing_keys:
                existing_keys.add(key)
                all_rows.append([date, query_text, page, clicks, impressions, ctr, position, key])
                batch_count += 1

        print(f"[INFO] Batch {batch_index}: Fetched {len(rows)} rows, {batch_count} new rows.", flush=True)
        batch_index += 1
        start_row += len(rows)

        # برای debug، توقف قبل از insert
        if batch_index > 3:  # فقط 3 batch اول بررسی شود
            print("[INFO] Stopping after 3 batches for debug.", flush=True)
            break

    return pd.DataFrame(all_rows, columns=['Date','Query','Page','Clicks','Impressions','CTR','Position','unique_key'])

# ---------- MAIN ----------
if __name__ == "__main__":
    df = fetch_gsc_data(START_DATE, END_DATE)
    print("[INFO] DEBUG fetch finished. Showing first 10 rows:", flush=True)
    print(df.head(10))
