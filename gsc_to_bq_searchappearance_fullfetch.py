# =================================================
# FILE: gsc_to_bq_searchappearance_fullfetch.py
# REV: 6.5.8
# PURPOSE: Full fetch SearchAppearance data from GSC to BigQuery
#          with duplicate prevention using unique_key
#          + fetch metadata (fetch_date, fetch_id)
#          + allocation functions (direct, sample-driven, proportional)
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
import sys
import argparse
import warnings
import uuid

# ---------- CONFIG ----------
SITE_URL = "sc-domain:bamtabridsazan.com"
BQ_PROJECT = 'bamtabridsazan'
BQ_DATASET = 'seo_reports'
BQ_TABLE = 'bamtabridsazan__gsc__raw_domain_data_searchappearance'
ROW_LIMIT = 25000
RETRY_DELAY = 60  # seconds in case of timeout

# ---------- ARGUMENT PARSER ----------
parser = argparse.ArgumentParser(description="GSC SearchAppearance to BigQuery Full Fetch")
parser.add_argument("--start-date", type=str, help="Start date YYYY-MM-DD for full fetch")
parser.add_argument("--end-date", type=str, help="End date YYYY-MM-DD")
parser.add_argument("--debug", action="store_true", help="Enable debug mode (skip BQ insert)")
# <<< FIX: ignore any --csv-test argument passed
parser.add_argument("--csv-test", type=str, help=argparse.SUPPRESS)

args = parser.parse_args()

START_DATE = args.start_date or (datetime.utcnow() - timedelta(days=3)).strftime('%Y-%m-%d')
END_DATE = args.end_date or datetime.utcnow().strftime('%Y-%m-%d')
DEBUG_MODE = args.debug

# ---------- FETCH METADATA ----------
FETCH_DATE = datetime.utcnow().strftime('%Y-%m-%d')
FETCH_ID = f"{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"

# ---------- CREDENTIALS ----------
SERVICE_ACCOUNT_FILE = os.environ.get("SERVICE_ACCOUNT_FILE", "gcp-key.json")
with open(SERVICE_ACCOUNT_FILE, "r") as f:
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
            bigquery.SchemaField("SearchAppearance", "STRING"),
            bigquery.SchemaField("Clicks", "INTEGER"),
            bigquery.SchemaField("Impressions", "INTEGER"),
            bigquery.SchemaField("CTR", "FLOAT"),
            bigquery.SchemaField("Position", "FLOAT"),
            bigquery.SchemaField("unique_key", "STRING"),
            bigquery.SchemaField("fetch_date", "DATE"),
            bigquery.SchemaField("fetch_id", "STRING"),
        ]
        table = bigquery.Table(table_ref, schema=schema)
        table.clustering_fields = ["SearchAppearance"]
        bq_client.create_table(table)
        print(f"[INFO] Table {BQ_TABLE} created.", flush=True)

# ---------- HELPER: create unique key ----------
def stable_key(row):
    sa = (row.get('SearchAppearance') or '').strip().lower()
    det_tuple = (sa,)
    s = "|".join(det_tuple)
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
    if DEBUG_MODE:
        print(f"[DEBUG] Debug mode ON: skipping insert of {len(df)} rows to BigQuery")
        return
    df['Clicks'] = pd.to_numeric(df['Clicks'], errors='coerce').fillna(0).astype(int)
    df['Impressions'] = pd.to_numeric(df['Impressions'], errors='coerce').fillna(0).astype(int)
    df['CTR'] = pd.to_numeric(df['CTR'], errors='coerce').fillna(0.0).astype(float)
    df['Position'] = pd.to_numeric(df['Position'], errors='coerce').fillna(0.0).astype(float)
    try:
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        bq_client.load_table_from_dataframe(df, table_ref, job_config=job_config).result()
        print(f"[INFO] Inserted {len(df)} rows to BigQuery.", flush=True)
    except Exception as e:
        print(f"[ERROR] Failed to insert rows: {e}", flush=True)

# ---------- FETCH GSC DATA ----------
def fetch_searchappearance_data(start_date, end_date):
    all_rows = []
    existing_keys = get_existing_keys()
    print(f"[INFO] Fetching SearchAppearance data from {start_date} to {end_date}", flush=True)
    request = {
        'startDate': start_date,
        'endDate': end_date,
        'dimensions': ['searchAppearance'],
        'rowLimit': ROW_LIMIT,
    }
    try:
        resp = service.searchanalytics().query(siteUrl=SITE_URL, body=request).execute()
    except Exception as e:
        print(f"[ERROR] Failed fetching SearchAppearance data: {e}", flush=True)
        return pd.DataFrame()
    rows = resp.get('rows', [])
    print(f"[INFO] Fetched {len(rows)} rows from GSC.", flush=True)
    batch_new_rows = []
    for r in rows:
        sa = r['keys'][0]
        clicks = r.get('clicks', 0)
        impressions = r.get('impressions', 0)
        ctr = r.get('ctr', 0.0)
        position = r.get('position', 0.0)
        key = stable_key({'SearchAppearance': sa})
        if key not in existing_keys:
            existing_keys.add(key)
            batch_new_rows.append([sa, clicks, impressions, ctr, position, key, FETCH_DATE, FETCH_ID])
    df_batch = pd.DataFrame(batch_new_rows, columns=['SearchAppearance','Clicks','Impressions','CTR','Position','unique_key','fetch_date','fetch_id'])
    # تبدیل fetch_date به datetime.date برای BigQuery
    df_batch['fetch_date'] = pd.to_datetime(df_batch['fetch_date']).dt.date
    print(f"[INFO] {len(df_batch)} new rows to insert. {len(rows)-len(df_batch)} duplicate rows skipped.", flush=True)
    return df_batch

# ---------- ALLOCATION FUNCTIONS ----------
def direct_mapping(df_raw, mapping_df):
    df = df_raw.merge(mapping_df, on='SearchAppearance', how='left')
    df['AllocationMethod'] = 'direct'
    df['AllocationWeight'] = 1.0
    df['Clicks_alloc'] = df['Clicks'] * df['AllocationWeight']
    df['Impressions_alloc'] = df['Impressions'] * df['AllocationWeight']
    df['CTR_alloc'] = df['Clicks_alloc'] / df['Impressions_alloc'].replace(0,1)
    df['Position_alloc'] = df['Position']
    df['fetch_id'] = FETCH_ID
    return df

def sample_driven_mapping(df_raw, mapping_df):
    df = df_raw.merge(mapping_df, on='SearchAppearance', how='left')
    df['AllocationMethod'] = 'sample-driven'
    df['AllocationWeight'] = df['sample_ratio'].fillna(0)
    df['Clicks_alloc'] = df['Clicks'] * df['AllocationWeight']
    df['Impressions_alloc'] = df['Impressions'] * df['AllocationWeight']
    df['CTR_alloc'] = df['Clicks_alloc'] / df['Impressions_alloc'].replace(0,1)
    df['Position_alloc'] = df['Position'] * df['AllocationWeight']
    df['fetch_id'] = FETCH_ID
    return df

def proportional_allocation(df_raw, mapping_df):
    df = df_raw.merge(mapping_df, on='SearchAppearance', how='left')
    df['AllocationMethod'] = 'proportional'
    df['AllocationWeight'] = df['prop_share'].fillna(0)
    df['Clicks_alloc'] = df['Clicks'] * df['AllocationWeight']
    df['Impressions_alloc'] = df['Impressions'] * df['AllocationWeight']
    df['CTR_alloc'] = df['Clicks_alloc'] / df['Impressions_alloc'].replace(0,1)
    df['Position_alloc'] = df['Position'] * df['AllocationWeight']
    df['fetch_id'] = FETCH_ID
    return df

# ---------- MAIN ----------
def main():
    ensure_table()
    df_new = fetch_searchappearance_data(START_DATE, END_DATE)
    upload_to_bq(df_new)
    print("[INFO] Finished processing SearchAppearance data.", flush=True)

if __name__ == "__main__":
    main()
