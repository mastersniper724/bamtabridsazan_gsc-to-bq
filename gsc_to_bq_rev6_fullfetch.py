# ============================================================
# File: gsc_to_bq_rev6.5_fullfetch.py
# Description: Full Fetch from Google Search Console API
# Author: MasterSniper
# Revision: Rev6.5 - 2025-10-02 (Updated for searchAppearance+date batch)
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
TABLE_ID = "bamtabridsazan.seo_reports.bamtabridsazan__gsc__raw_data_fullfetch"
SERVICE_ACCOUNT_FILE = "gcp-key.json"

# ============================================================
# ✅ Helper: Unique Key Generator
# ============================================================
def generate_unique_key(row):
    key_parts = [
        str(row.get("Date", "")),
        str(row.get("Query", "")),
        str(row.get("Page", "")),
        str(row.get("Country", "")),
        str(row.get("Device", "")),
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
# ✅ Fetch Data Function
# ============================================================
def fetch_gsc_data(start_date, end_date):
    service = get_gsc_service()
    all_data = []

    # ✅ Batchهای ترکیبی
    DIMENSION_BATCHES = [
        ["date", "query", "page"],
        ["date", "query", "country"],
        ["date", "query", "device"],
        ["date", "searchAppearance"],  # ✅ فقط date+searchAppearance
    ]

    for i, dims in enumerate(DIMENSION_BATCHES, start=1):
        print(f"[INFO] Batch {i}, dims {dims}: fetching data...")

        request = {
            "startDate": start_date,
            "endDate": end_date,
            "dimensions": dims,
            "rowLimit": 25000,
        }

        try:
            response = service.searchanalytics().query(siteUrl=SITE_URL, body=request).execute()
            rows = response.get("rows", [])
            print(f"[INFO] Batch {i}, dims {dims}: fetched {len(rows)} rows")

            for r in rows:
                keys = r.get("keys", [])
                row_data = {
                    "Date": keys[0] if len(keys) > 0 else None,
                    "Query": keys[1] if "query" in dims and len(keys) > 1 else None,
                    "Page": keys[2] if "page" in dims and len(keys) > 2 else None,
                    "Country": keys[2] if "country" in dims and len(keys) > 2 else None,
                    "Device": keys[2] if "device" in dims and len(keys) > 2 else None,
                    "SearchAppearance": keys[1] if "searchAppearance" in dims and len(keys) > 1 else None,
                    "Clicks": r.get("clicks", 0),
                    "Impressions": r.get("impressions", 0),
                    "CTR": r.get("ctr", 0.0),
                    "Position": r.get("position", 0.0),
                }
                all_data.append(row_data)

        except Exception as e:
            print(f"[ERROR] Batch {i} failed: {e}")
            time.sleep(60)

    df = pd.DataFrame(all_data)
    if not df.empty:
        df["unique_key"] = df.apply(generate_unique_key, axis=1)
    return df

# ============================================================
# ✅ Upload to BigQuery
# ============================================================
def upload_to_bq(df):
    if df.empty:
        print("[INFO] No new data to upload.")
        return

    bq = get_bq_client()

    # 🔹 حذف Unknown (در صورت وجود)
    df.replace("Unknown", None, inplace=True)

    # 🔹 تبدیل تاریخ‌ها
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema=[
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
        ],
        clustering_fields=["Query", "Page"]  # ✅ کلاستر کردن همان نسخه اصلی
    )

    print(f"[INFO] Uploading {len(df)} rows to BigQuery...")
    bq.load_table_from_dataframe(df, TABLE_ID, job_config=job_config).result()
    print(f"[INFO] Inserted {len(df)} rows to BigQuery.")

# ============================================================
# ✅ Main
# ============================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--csv-test", required=False)
    args = parser.parse_args()

    START_DATE = args.start_date
    END_DATE = args.end_date

    print(f"[INFO] Fetching data from {START_DATE} to {END_DATE}")

    df = fetch_gsc_data(START_DATE, END_DATE)

    if df.empty:
        print("[INFO] No data fetched from GSC.")
        sys.exit(0)

    upload_to_bq(df)

    # CSV test optional
    if args.csv_test:
        df.to_csv(args.csv_test, index=False)
        print(f"[INFO] CSV test output written: {args.csv_test}")

    print(f"[INFO] Finished fetching all data. Total rows: {len(df)}")
