# ============================================================
# File: gsc_to_bq_searchappearance_fullfetch.py
# Description: Full Fetch for GSC SearchAppearance dimension only
# Author: MasterSniper
# Revision: Rev6.5.4 (SearchAppearance Edition)
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

# ============================================================
# ✅ Helper: Unique Key Generator
# ============================================================
def generate_unique_key(row):
    key_parts = [
        str(row.get("Date", "")),
        str(row.get("SearchAppearance", "")),
    ]
    key_str = "|".join(key_parts)
    return hashlib.sha256(key_str.encode()).hexdigest()

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
# ✅ Ensure table exists
# ============================================================
def ensure_table(bq_client):
    try:
        bq_client.get_table(TABLE_ID)
        print(f"[INFO] Table {TABLE_ID} exists.")
    except:
        print(f"[INFO] Table {TABLE_ID} not found. Creating...")
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
        print(f"[INFO] Table {TABLE_ID} created.")

# ============================================================
# ✅ Fetch existing keys from BQ
# ============================================================
def get_existing_keys(bq_client):
    try:
        query = f"SELECT unique_key FROM `{TABLE_ID}`"
        df = bq_client.query(query).to_dataframe()
        print(f"[INFO] Retrieved {len(df)} existing keys from BigQuery.")
        return set(df['unique_key'].astype(str).tolist())
    except Exception as e:
        print(f"[WARN] Failed to fetch existing keys: {e}")
        return set()

# ============================================================
# ✅ Fetch GSC data for searchAppearance
# ============================================================
def fetch_gsc_data(start_date, end_date, existing_keys):
    service = get_gsc_service()
    all_data = []

    dims = ["searchAppearance"]
    request = {
        "startDate": start_date,
        "endDate": end_date,
        "dimensions": dims,
        "rowLimit": 25000,
    }

    try:
        response = service.searchanalytics().query(siteUrl=SITE_URL, body=request).execute()
        rows = response.get("rows", [])
        print(f"[INFO] Fetched {len(rows)} rows from GSC.")
        for r in rows:
            row_data = {
                "Date": start_date,  # برای هر row تاریخ start_date قرار داده می‌شود
                "SearchAppearance": r["keys"][0] if len(r.get("keys", [])) > 0 else None,
                "Clicks": r.get("clicks", 0),
                "Impressions": r.get("impressions", 0),
                "CTR": r.get("ctr", 0.0),
                "Position": r.get("position", 0.0),
            }
            row_data["unique_key"] = generate_unique_key(row_data)
            all_data.append(row_data)
    except Exception as e:
        print(f"[ERROR] Fetching GSC data failed: {e}")

    df = pd.DataFrame(all_data)
    df["Date"] = pd.to_datetime(df["Date"]).dt.date  # تبدیل به datetime.date برای BQ
    if not df.empty:
        new_rows = df[~df["unique_key"].isin(existing_keys)]
        duplicate_rows = df[df["unique_key"].isin(existing_keys)]
        print(f"[INFO] {len(new_rows)} new rows to insert. {len(duplicate_rows)} duplicate rows skipped.")
    else:
        new_rows = pd.DataFrame(columns=df.columns)
        print("[INFO] No rows fetched from GSC.")

    return new_rows

# ============================================================
# ✅ Upload to BigQuery
# ============================================================
def upload_to_bq(df, bq_client):
    if df.empty:
        print("[INFO] Nothing to insert to BQ.")
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
    bq_client.load_table_from_dataframe(df, TABLE_ID, job_config=job_config).result()
    print(f"[INFO] Inserted {len(df)} rows to BigQuery.")

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

    print(f"[INFO] Fetching SearchAppearance data from {START_DATE} to {END_DATE}")

    bq_client = get_bq_client()
    ensure_table(bq_client)
    existing_keys = get_existing_keys(bq_client)

    new_rows = fetch_gsc_data(START_DATE, END_DATE, existing_keys)
    upload_to_bq(new_rows, bq_client)

    if args.csv_test:
        new_rows.to_csv(args.csv_test, index=False)
        print(f"[INFO] CSV test output written: {args.csv_test}")

    print(f"[INFO] Finished fetching all data. Total new rows: {len(new_rows)}")

# ============================================================
if __name__ == "__main__":
    main()
