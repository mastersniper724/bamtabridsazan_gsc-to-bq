import pandas as pd
from googleapiclient.discovery import build
from google.oauth2 import service_account
from datetime import datetime, timedelta, date
import hashlib
import time
import os
import json
import sys
import warnings

# ---------- CONFIG ----------
SITE_URL = 'https://bamtabridsazan.com/'
ROW_LIMIT = 25000
RETRY_DELAY = 60  # seconds
CSV_OUTPUT = f'gsc_fullfetch_test_{date.today()}.csv'

# ---------- CREDENTIALS ----------
service_account_file = os.environ.get("SERVICE_ACCOUNT_FILE", "gcp-key.json")
with open(service_account_file, "r") as f:
    sa_info = json.load(f)
credentials = service_account.Credentials.from_service_account_info(sa_info)
service = build('searchconsole', 'v1', credentials=credentials)

# ---------- DATE RANGE ----------
START_DATE = (datetime.utcnow() - timedelta(days=3)).strftime('%Y-%m-%d')
END_DATE = datetime.utcnow().strftime('%Y-%m-%d')

# ---------- DIMENSIONS ----------
DIMENSIONS = ["query", "page", "country", "device", "searchAppearance", "date"]

# ---------- HELPER ----------
def stable_key(record, dimension):
    value = record.get('value','').strip().lower()
    date_val = record.get('date', START_DATE)
    s = f"{dimension}|{value}|{date_val}"
    return hashlib.sha256(s.encode('utf-8')).hexdigest()

# ---------- FETCH GSC DATA ----------
def fetch_gsc_data(start_date, end_date, dimension):
    all_rows = []
    start_row = 0
    batch_index = 1

    while True:
        request = {
            'startDate': start_date,
            'endDate': end_date,
            'dimensions': [dimension],
            'rowLimit': ROW_LIMIT,
            'startRow': start_row
        }
        try:
            resp = service.searchanalytics().query(siteUrl=SITE_URL, body=request).execute()
        except Exception as e:
            print(f"[ERROR] Timeout or error: {e}, retrying in {RETRY_DELAY} sec...")
            time.sleep(RETRY_DELAY)
            continue

        rows = resp.get('rows', [])
        if not rows:
            break

        batch_new_rows = []
        for r in rows:
            value = r['keys'][0] if r.get('keys') else ""
            record_date = r['keys'][0] if dimension=="date" else start_date
            batch_new_rows.append({
                "date": record_date,
                "dimension_type": dimension,
                "dimension_value": value,
                "clicks": r.get('clicks',0),
                "impressions": r.get('impressions',0),
                "ctr": r.get('ctr',0),
                "position": r.get('position',0),
                "unique_key": stable_key({'value': value,'date':record_date}, dimension)
            })

        print(f"[INFO] Dimension '{dimension}', Batch {batch_index}: Fetched {len(rows)} rows", flush=True)
        all_rows.extend(batch_new_rows)
        batch_index += 1
        if len(rows) < ROW_LIMIT:
            break
        start_row += len(rows)

    return all_rows

# ---------- MAIN ----------
if __name__ == "__main__":
    all_data = []
    for dim in DIMENSIONS:
        print(f"[INFO] Fetching data for dimension: {dim}", flush=True)
        dim_rows = fetch_gsc_data(START_DATE, END_DATE, dim)
        all_data.extend(dim_rows)

    df = pd.DataFrame(all_data)
    df.to_csv(CSV_OUTPUT, index=False)
    print(f"[INFO] Exported {len(df)} rows to {CSV_OUTPUT}", flush=True)
