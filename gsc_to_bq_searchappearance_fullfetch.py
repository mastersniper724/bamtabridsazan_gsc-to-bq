# ============================================================
# File: gsc_to_bq_searchappearance_fullfetch.py
# Description: Full Fetch for SearchAppearance data from Google Search Console API
# Author: MasterSniper
# Revision: Rev6.5.1 (SearchAppearance Edition) - 2025-10-03
# ============================================================

import os
import sys
import time
import json
import argparse
import pandas as pd
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

# ============================================================
# ✅ BigQuery Connection
# ============================================================
def get_bq_client():
    creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)
    return bigquery.Client(credentials=creds)

# ============================================================
# ✅ GSC API Connection
# ============================================================
def get_gsc_service():
    creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    return build("searchconsole", "v1", credentials=creds)

# ============================================================
# ✅ Ensure Table Exists
# ============================================================
def ensure_table():
    bq = get_bq_client()
    try:
        bq.get_table(TABLE_ID)
        print(f"[INFO] Table {TABLE_ID} exists.")
    except:
        print(f"[INFO] Table {TABLE_ID} not found. Creating...")
        schema = [
            bigquery.SchemaField("SearchAppearance", "STRING"),
            bigquery.SchemaField("Clicks", "INTEGER"),
            bigquery.SchemaField("Impressions", "INTEGER"),
            bigquery.SchemaField("CTR", "FLOAT"),
            bigquery.SchemaField("Position", "FLOAT"),
        ]
        table = bigquery.Table(TABLE_ID, schema=schema)
        bq.create_table(table)
        print(f"[INFO] Table {TABLE_ID} created.")

# ============================================================
# ✅ Fetch SearchAppearance Data
# ============================================================
def fetch_searchappearance_data(start_date, end_date):
    service = get_gsc_service()
    request = {
        "startDate": start_date,
        "endDate": end_date,
        "dimensions": ["searchAppearance"],
        "rowLimit": 25000
    }
    all_rows = []
    try:
        response = service.searchanalytics().query(siteUrl=SITE_URL, body=request).execute()
        rows = response.get("rows", [])
        for r in rows:
            keys = r.get("keys", [])
            all_rows.append({
                "SearchAppearance": keys[0] if keys else None,
                "Clicks": r.get("clicks", 0),
                "Impressions": r.get("impressions", 0),
                "CTR": r.get("ctr", 0.0),
                "Position": r.get("position", 0.0),
            })
        print(f"[INFO] Fetched {len(all_rows)} rows for SearchAppearance.")
    except Exception as e:
        print(f"[ERROR] Failed to fetch SearchAppearance data: {e}")
        sys.exit(1)
    return pd.DataFrame(all_rows)

# ============================================================
# ✅ Upload to BigQuery
# ============================================================
def upload_to_bq(df):
    if df.empty:
        print("[INFO] No new data to upload.")
        return
    bq = get_bq_client()
    try:
        job = bq.load_table_from_dataframe(df, TABLE_ID)
        job.result()
        print(f"[INFO] Inserted {len(df)} rows to BigQuery.")
    except Exception as e:
        print(f"[ERROR] Failed to insert rows: {e}")
        sys.exit(1)

# ============================================================
# ✅ Main
# ============================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    args = parser.parse_args()

    START_DATE = args.start_date
    END_DATE = args.end_date

    print(f"[INFO] Fetching SearchAppearance data from {START_DATE} to {END_DATE}")
    ensure_table()
    df = fetch_searchappearance_data(START_DATE, END_DATE)
    upload_to_bq(df)
    print(f"[INFO] Finished fetching all SearchAppearance data. Total rows: {len(df)}")
