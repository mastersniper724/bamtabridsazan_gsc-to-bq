# ============================================================
# File: gsc_to_bq_searchappearance_fullfetch.py
# Description: Full Fetch from Google Search Console API (SearchAppearance Only)
# Author: MasterSniper
# Revision: Rev 6.5.3 - 2025-10-03 (SearchAppearance Edition with Duplicate Prevention)
# ============================================================

import os
import sys
import time
import json
import hashlib
import argparse
import pandas as pd
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from google.cloud import bigquery

# ============================================================
# ✅ Configurations
# ============================================================
SCOPES = ["https://www.googleapis.com/auth/webmasters.readonly"]
SITE_URL = "https://bamtabridsazan.com/"
TABLE_ID = "bamtabridsazan.seo_reports.bamtabridsazan__gsc__raw_data_searchappearance"
SERVICE_ACCOUNT_FILE = "gcp-key.json"
ROW_LIMIT = 25000

# ============================================================
# ✅ Helper: Unique Key Generator
# ============================================================
def generate_unique_key(row):
    key_str = str(row.get("SearchAppearance", "")) + "|" + str(row.get("Date", ""))
    return hashlib.sha256(key_str.encode()).hexdigest()

# ============================================================
# ✅ BigQuery Connection
# ============================================================
def get_bq_client():
    creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)
    return bigquery.Client(credentials=creds, project=creds.project_id)

# ============================================================
# ✅ Ensure Table Exists
# ============================================================
def ensure_table(bq_client):
    try:
        bq_client.get_table(TABLE_ID)
        print(f"[INFO] Table {TABLE_ID} exists.", flush=True)
    except Exception:
        print(f"[INFO] Table {TABLE_ID} not found. Creating...", flush=True)
        schema = [
            bigquery.SchemaField("Date", "DATE"),
            bigquery.SchemaField("SearchAppearance", "STRING"),
            bigquery.SchemaField("Clicks", "INTEGER"),
            bigquery.SchemaField("Impressions", "INTEGER"),
            bigquery.SchemaField("CTR", "FLOAT"),
            bigquery.SchemaField("Position", "FLOAT"),
            bigquery.SchemaField("unique_key", "STRING"),
        ]
        table = bigquery.Table(TABLE_ID, schema=schema)
        table.clustering_fields = ["SearchAppearance"]
        bq_client.create_table(table)
        print(f"[INFO] Table {TABLE_ID} created.", flush=True)

# ============================================================
# ✅ Fetch existing keys from BigQuery
# ============================================================
def get_existing_keys(bq_client):
    try:
        query = f"SELECT unique_key FROM `{TABLE_ID}`"
        df = bq_client.query(query).to_dataframe()
        keys = set(df['unique_key'].astype(str).tolist())
        print(f"[INFO] Retrieved {len(keys)} existing keys from BigQuery.", flush=True)
        return keys
    except Exception as e:
        print(f"[WARN] Failed to fetch existing keys: {e}", flush=True)
        return set()

# ============================================================
# ✅ Fetch Data from GSC
# ============================================================
def fetch_gsc_data(start_date, end_date):
    service = build("searchconsole", "v1", credentials=service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES))
    all_rows = []
    batch_index = 1

    request = {
        "startDate": start_date,
        "endDate": end_date,
        "dimensions": ["searchAppearance"],
        "rowLimit": ROW_LIMIT,
    }

    try:
        print(f"[INFO] Fetching SearchAppearance data from {start_date} to {end_date}", flush=True)
        response = service.searchanalytics().query(siteUrl=SITE_URL, body=request).execute()
        rows = response.get("rows", [])
        print(f"[INFO] Fetched {len(rows)} rows from GSC.", flush=True)

        for r in rows:
            keys = r.get("keys", [])
            row_data = {
                "SearchAppearance": keys[0] if len(keys) > 0 else None,
                "Date": start_date,  # GSC API does not provide date column in SearchAppearance-only export
                "Clicks": r.get("clicks", 0),
                "Impressions": r.get("impressions", 0),
                "CTR": r.get("ctr", 0.0),
                "Position": r.get("position", 0.0),
            }
            row_data["unique_key"] = generate_unique_key(row_data)
            all_rows.append(row_data)

    except Exception as e:
        print(f"[ERROR] Failed to fetch SearchAppearance data: {e}", flush=True)

    return pd.DataFrame(all_rows)

# ============================================================
# ✅ Upload to BigQuery with Duplicate Prevention
# ============================================================
def upload_to_bq(df, bq_client):
    if df.empty:
        print("[INFO] No data to upload.", flush=True)
        return

    existing_keys = get_existing_keys(bq_client)
    new_rows = df[~df['unique_key'].isin(existing_keys)]
    print(f"[INFO] {len(new_rows)} new rows to insert. {len(df)-len(new_rows)} duplicate rows skipped.", flush=True)

    if new_rows.empty:
        print("[INFO] No new rows to insert.", flush=True)
        return

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema=[
            bigquery.SchemaField("Date", "DATE"),
            bigquery.SchemaField("SearchAppearance", "STRING"),
            bigquery.SchemaField("Clicks", "INTEGER"),
            bigquery.SchemaField("Impressions", "INTEGER"),
            bigquery.SchemaField("CTR", "FLOAT"),
            bigquery.SchemaField("Position", "FLOAT"),
            bigquery.SchemaField("unique_key", "STRING"),
        ],
        clustering_fields=["SearchAppearance"]
    )

    bq_client.load_table_from_dataframe(new_rows, TABLE_ID, job_config=job_config).result()
    print(f"[INFO] Inserted {len(new_rows)} new rows to BigQuery.", flush=True)

# ============================================================
# ✅ Main
# ============================================================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--csv-test", required=False)
    args = parser.parse_args()

    START_DATE = args.start_date
    END_DATE = args.end_date

    bq_client = get_bq_client()
    ensure_table(bq_client)

    df = fetch_gsc_data(START_DATE, END_DATE)

    if args.csv_test:
        df.to_csv(args.csv_test, index=False)
        print(f"[INFO] CSV test output written: {args.csv_test}", flush=True)

    upload_to_bq(df, bq_client)
    print(f"[INFO] Finished fetching and uploading SearchAppearance data. Total rows fetched: {len(df)}", flush=True)

if __name__ == "__main__":
    main()
