# =================================================
# FILE: gsc_to_bq_rev6.3_fullfetch.py
# REV: 6.3
# PURPOSE: Full Fetch GSC → BigQuery loader with all key dimensions and searchAppearance fix
# =================================================

from google.oauth2 import service_account
from googleapiclient.discovery import build
from google.cloud import bigquery
import pandas as pd
from datetime import datetime, timedelta
import hashlib
import time
import os
import json
import argparse
import warnings

# ---------- CONFIG ----------
SITE_URL = 'https://bamtabridsazan.com/'
BQ_PROJECT = 'bamtabridsazan'
BQ_DATASET = 'seo_reports'
BQ_TABLE = 'bamtabridsazan__gsc__raw_data_fullfetch'
ROW_LIMIT = 25000
RETRY_DELAY = 60  # seconds
MAX_RETRIES = 3   # max retries for API errors

# ---------- ARGUMENT PARSER ----------
parser = argparse.ArgumentParser(description="GSC to BigQuery Full Fetch Loader")
parser.add_argument("--start-date", type=str, help="Start date YYYY-MM-DD for full fetch")
parser.add_argument("--end-date", type=str, help="End date YYYY-MM-DD")
parser.add_argument("--debug", action="store_true", help="Enable debug mode (skip BQ insert)")
parser.add_argument("--csv-test", type=str, default="gsc_fullfetch_test.csv", help="CSV test output file")
args = parser.parse_args()

START_DATE = args.start_date or (datetime.utcnow() - timedelta(days=365*1)).strftime('%Y-%m-%d')
END_DATE = args.end_date or datetime.utcnow().strftime('%Y-%m-%d')
DEBUG_MODE = args.debug
CSV_TEST_FILE = args.csv_test

# ---------- CREDENTIALS ----------
service_account_file = os.environ.get("SERVICE_ACCOUNT_FILE", "gcp-key.json")
with open(service_account_file, "r") as f:
    sa_info = json.load(f)
credentials = service_account.Credentials.from_service_account_info(sa_info)

# Build Search Console service
service = build('searchconsole', 'v1', credentials=credentials)

# ---------- BIGQUERY CLIENT ----------
bq_client = bigquery.Client(credentials=credentials, project=credentials.project_id)
table_ref = bq_client.dataset(BQ_DATASET).table(BQ_TABLE)

# ---------- ENSURE TABLE EXISTS ----------
def ensure_table():
    try:
        bq_client.get_table(table_ref)
        print(f"[INFO] Table {BQ_TABLE} exists.", flush=True)
    except:
        print(f"[INFO] Table {BQ_TABLE} not found. Creating...", flush=True)
        schema = [
            bigquery.SchemaField("Date", "DATE"),
            bigquery.SchemaField("Query", "STRING"),
            bigquery.SchemaField("Page", "STRING"),
            bigquery.SchemaField("Country", "STRING"),
            bigquery.SchemaField("Device", "STRING"),
            bigquery.SchemaField("SearchAppearance", "STRING"),
            bigquery.SchemaField("Clicks", "INTEGER"),
            bigquery.SchemaField("Impressions", "INTEGER"),
            bigquery.SchemaField("CTR", "FLOAT"),
            bigquery.SchemaField("Position", "FLOAT"),
            bigquery.SchemaField("unique_key", "STRING"),
        ]
        table = bigquery.Table(table_ref, schema=schema)
        table.clustering_fields = ["Date", "Query"]
        bq_client.create_table(table)
        print(f"[INFO] Table {BQ_TABLE} created.", flush=True)

# ---------- HELPER: create unique key ----------
def stable_key(row):
    keys = [
        (row.get('Query') or '').strip().lower(),
        (row.get('Page') or '').strip().lower().rstrip('/'),
        (row.get('Country') or '').strip().lower(),
        (row.get('Device') or '').strip().lower(),
        (row.get('SearchAppearance') or '').strip().lower(),
        row.get('Date')[:10] if isinstance(row.get('Date'), str) else row.get('Date').strftime("%Y-%m-%d")
    ]
    s = "|".join(keys)
    return hashlib.sha256(s.encode('utf-8')).hexdigest()

# ---------- FETCH EXISTING KEYS FROM BIGQUERY ----------
def get_existing_keys():
    try:
        query = f"SELECT unique_key FROM `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}`"
        try:
            from google.cloud import bigquery_storage
            bqstorage_client = bigquery_storage.BigQueryReadClient()
            use_bqstorage = True
        except Exception:
            bqstorage_client = None
            use_bqstorage = False

        if use_bqstorage:
            df = bq_client.query(query).to_dataframe(bqstorage_client=bqstorage_client)
        else:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                df = bq_client.query(query).to_dataframe()

        print(f"[INFO] Retrieved {len(df)} existing keys from BigQuery.", flush=True)
        return set(df['unique_key'].astype(str).tolist())
    except Exception as e:
        print(f"[WARN] Failed to fetch existing keys: {e}", flush=True)
        return set()

# ---------- UPLOAD TO BIGQUERY ----------
def upload_to_bq(df):
    if df.empty:
        print("[INFO] No new rows to insert.", flush=True)
        return
    df['Date'] = pd.to_datetime(df['Date'])
    if DEBUG_MODE:
        print(f"[DEBUG] Debug mode ON: skipping insert of {len(df)} rows to BigQuery")
        return
    try:
        job = bq_client.load_table_from_dataframe(df, table_ref)
        job.result()
        print(f"[INFO] Inserted {len(df)} rows to BigQuery.", flush=True)
    except Exception as e:
        print(f"[ERROR] Failed to insert rows: {e}", flush=True)

# ---------- FETCH GSC DATA ----------
def fetch_gsc_data(start_date, end_date):
    all_rows = []
    existing_keys = get_existing_keys()
    batch_index = 1

    # List of dimensions
    dimensions_list = [
        ['date','query','page'],
        ['date','query'],
        ['date','page'],
        ['date','country'],
        ['date','device'],
        ['searchAppearance']  # Single-dimension only
    ]

    for dims in dimensions_list:
        start_row = 0
        retries = 0
        while True:
            request = {
                'startDate': start_date,
                'endDate': end_date,
                'dimensions': dims,
                'rowLimit': ROW_LIMIT,
                'startRow': start_row
            }

            try:
                resp = service.searchanalytics().query(siteUrl=SITE_URL, body=request).execute()
            except Exception as e:
                retries += 1
                if retries > MAX_RETRIES:
                    print(f"[ERROR] Max retries reached for dims {dims}. Skipping.", flush=True)
                    break
                print(f"[ERROR] Timeout or error: {e}, retrying in {RETRY_DELAY} sec...", flush=True)
                time.sleep(RETRY_DELAY)
                continue

            rows = resp.get('rows', [])
            if not rows:
                break

            batch_new_rows = []
            for r in rows:
                row_data = {
                    'Date': r['keys'][0] if len(r['keys']) > 0 else None,
                    'Query': r['keys'][1] if 'query' in dims and len(r['keys']) > 1 else None,
                    'Page': r['keys'][2] if 'page' in dims and len(r['keys']) > 2 else None,
                    'Country': r['keys'][1] if 'country' in dims and len(r['keys']) > 1 else None,
                    'Device': r['keys'][1] if 'device' in dims and len(r['keys']) > 1 else None,
                    'SearchAppearance': r['keys'][0] if 'searchAppearance' in dims and len(r['keys']) > 0 else None,
                    'Clicks': r.get('clicks',0),
                    'Impressions': r.get('impressions',0),
                    'CTR': r.get('ctr',0),
                    'Position': r.get('position',0)
                }
                key = stable_key(row_data)
                if key not in existing_keys:
                    existing_keys.add(key)
                    batch_new_rows.append({**row_data, 'unique_key': key})

            print(f"[INFO] Batch {batch_index}, dims {dims}: Fetched {len(rows)} rows, {len(batch_new_rows)} new rows.", flush=True)
            if batch_new_rows:
                df_batch = pd.DataFrame(batch_new_rows)
                upload_to_bq(df_batch)
                all_rows.extend(batch_new_rows)

            if len(rows) < ROW_LIMIT:
                break
            start_row += ROW_LIMIT

        batch_index += 1

    # Write CSV for test
    df_all = pd.DataFrame(all_rows)
    df_all.to_csv(CSV_TEST_FILE, index=False)
    print(f"[INFO] CSV test output written: {CSV_TEST_FILE}", flush=True)
    return df_all

# ---------- MAIN ----------
if __name__ == "__main__":
    ensure_table()
    df = fetch_gsc_data(START_DATE, END_DATE)
    print(f"[INFO] Finished fetching all data. Total rows: {len(df)}", flush=True)
