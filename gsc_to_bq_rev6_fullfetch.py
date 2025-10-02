# ============================================================
# File: gsc_to_bq_rev6.5_fullfetch.py
# Description: Full Fetch from Google Search Console API
# Author: MasterSniper
# Revision: Rev6.5 - 2025-10-02 (Duplicate prevention restored, searchAppearance removed)
# ============================================================

import os
import sys
import time
import hashlib
import argparse
import pandas as pd
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from google.cloud import bigquery
import warnings
import json

# ============================================================
# Configurations
# ============================================================
SITE_URL = "https://bamtabridsazan.com/"
BQ_PROJECT = "bamtabridsazan"
BQ_DATASET = "seo_reports"
BQ_TABLE = "bamtabridsazan__gsc__raw_data_fullfetch"
ROW_LIMIT = 25000
RETRY_DELAY = 60  # seconds
SERVICE_ACCOUNT_FILE = os.environ.get("SERVICE_ACCOUNT_FILE", "gcp-key.json")

# ============================================================
# Argument Parser
# ============================================================
parser = argparse.ArgumentParser()
parser.add_argument("--start-date", required=True)
parser.add_argument("--end-date", required=True)
parser.add_argument("--debug", action="store_true")
args = parser.parse_args()

START_DATE = args.start_date
END_DATE = args.end_date
DEBUG_MODE = args.debug

# ============================================================
# BigQuery Client
# ============================================================
def get_bq_client():
    creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)
    return bigquery.Client(credentials=creds, project=creds.project_id)

bq_client = get_bq_client()
table_ref = bq_client.dataset(BQ_DATASET).table(BQ_TABLE)

# ============================================================
# Ensure Table Exists
# ============================================================
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

# ============================================================
# Unique Key Generator
# ============================================================
def generate_unique_key(row):
    query = (row.get("Query") or "").strip().lower()
    page = (row.get("Page") or "").strip().lower().rstrip("/")
    date_raw = row.get("Date")
    if isinstance(date_raw, str):
        date = date_raw[:10]
    elif isinstance(date_raw, datetime):
        date = date_raw.strftime("%Y-%m-%d")
    else:
        date = str(date_raw)[:10]
    key_str = f"{date}|{query}|{page}"
    return hashlib.sha256(key_str.encode("utf-8")).hexdigest()

# ============================================================
# Fetch existing unique keys from BigQuery
# ============================================================
def get_existing_keys():
    try:
        query = f"SELECT unique_key FROM `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}`"
        try:
            from google.cloud import bigquery_storage
            bqstorage_client = bigquery_storage.BigQueryReadClient()
            use_bqstorage = True
        except:
            bqstorage_client = None
            use_bqstorage = False

        if use_bqstorage:
            df = bq_client.query(query).to_dataframe(bqstorage_client=bqstorage_client)
        else:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                df = bq_client.query(query).to_dataframe()

        print(f"[INFO] Retrieved {len(df)} existing keys from BigQuery.", flush=True)
        return set(df["unique_key"].astype(str).tolist())
    except Exception as e:
        print(f"[WARN] Failed to fetch existing keys: {e}", flush=True)
        return set()

# ============================================================
# Upload to BigQuery
# ============================================================
def upload_to_bq(df):
    if df.empty:
        print("[INFO] No new rows to insert.", flush=True)
        return

    df["Date"] = pd.to_datetime(df["Date"])
    if DEBUG_MODE:
        print(f"[DEBUG] Debug mode ON: skipping insert of {len(df)} rows to BigQuery")
        return
    try:
        job = bq_client.load_table_from_dataframe(df, table_ref)
        job.result()
        print(f"[INFO] Inserted {len(df)} rows to BigQuery.", flush=True)
    except Exception as e:
        print(f"[ERROR] Failed to insert rows: {e}", flush=True)

# ============================================================
# Fetch GSC Data with duplicate prevention
# ============================================================
def fetch_gsc_data(start_date, end_date):
    service = build("searchconsole", "v1", credentials=service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=["https://www.googleapis.com/auth/webmasters.readonly"]))
    existing_keys = get_existing_keys()
    all_new_rows = []
    start_row = 0
    batch_index = 1

    while True:
        request = {
            "startDate": start_date,
            "endDate": end_date,
            "dimensions": ["date", "query", "page"],
            "rowLimit": ROW_LIMIT,
            "startRow": start_row
        }

        try:
            resp = service.searchanalytics().query(siteUrl=SITE_URL, body=request).execute()
        except Exception as e:
            print(f"[ERROR] Timeout or error: {e}, retrying in {RETRY_DELAY} sec...", flush=True)
            time.sleep(RETRY_DELAY)
            continue

        rows = resp.get("rows", [])
        if not rows:
            print("[INFO] No more rows returned from GSC.", flush=True)
            break

        batch_new_rows = []
        for r in rows:
            date = r["keys"][0]
            query_text = r["keys"][1]
            page = r["keys"][2]
            clicks = r.get("clicks", 0)
            impressions = r.get("impressions", 0)
            ctr = r.get("ctr", 0)
            position = r.get("position", 0)
            key = generate_unique_key({"Date": date, "Query": query_text, "Page": page})

            if key not in existing_keys:
                existing_keys.add(key)
                batch_new_rows.append([date, query_text, page, clicks, impressions, ctr, position, key])

        print(f"[INFO] Batch {batch_index}: Fetched {len(rows)} rows, {len(batch_new_rows)} new rows.", flush=True)

        if batch_new_rows:
            df_batch = pd.DataFrame(batch_new_rows, columns=["Date","Query","Page","Clicks","Impressions","CTR","Position","unique_key"])
            upload_to_bq(df_batch)
            all_new_rows.extend(df_batch.values.tolist())

        batch_index += 1
        if len(rows) < ROW_LIMIT:
            break
        start_row += len(rows)

    return pd.DataFrame(all_new_rows, columns=["Date","Query","Page","Clicks","Impressions","CTR","Position","unique_key"])

# ============================================================
# Main
# ============================================================
if __name__ == "__main__":
    ensure_table()
    df = fetch_gsc_data(START_DATE, END_DATE)

    if df.empty:
        print("[INFO] No new rows fetched from GSC.", flush=True)
    else:
        print(f"[INFO] Total new rows fetched and inserted: {len(df)}", flush=True)
