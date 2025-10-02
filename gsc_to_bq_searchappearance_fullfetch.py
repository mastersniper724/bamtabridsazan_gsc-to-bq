# ============================================================
# File: gsc_to_bq_searchappearance_fullfetch.py
# Description: Full Fetch from Google Search Console API (SearchAppearance Only)
# Author: MasterSniper
# Revision: Rev6.5.2 (SearchAppearance Edition)
# ============================================================

import os
import sys
import time
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
# ✅ Fetch SearchAppearance Data
# ============================================================
def fetch_searchappearance_data(start_date, end_date):
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
        print(f"[INFO] Fetched {len(rows)} rows for SearchAppearance")

        for r in rows:
            keys = r.get("keys", [])
            row_data = {
                "SearchAppearance": keys[0] if len(keys) > 0 else None,
                "Clicks": r.get("clicks", 0),
                "Impressions": r.get("impressions", 0),
                "CTR": r.get("ctr", 0.0),
                "Position": r.get("position", 0.0),
            }
            all_data.append(row_data)

    except Exception as e:
        print(f"[ERROR] Fetch failed: {e}")
        time.sleep(60)

    df = pd.DataFrame(all_data)
    return df

# ============================================================
# ✅ Upload to BigQuery
# ============================================================
def upload_to_bq(df):
    if df.empty:
        print("[INFO] No new data to upload.")
        return

    bq = get_bq_client()

    # حذف Unknown (در صورت وجود)
    df.replace("Unknown", None, inplace=True)

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema=[
            bigquery.SchemaField("SearchAppearance", "STRING"),
            bigquery.SchemaField("Clicks", "INTEGER"),
            bigquery.SchemaField("Impressions", "INTEGER"),
            bigquery.SchemaField("CTR", "FLOAT"),
            bigquery.SchemaField("Position", "FLOAT"),
        ]
    )

    print(f"[INFO] Uploading {len(df)} rows to BigQuery...")
    bq.load_table_from_dataframe(df, TABLE_ID, job_config=job_config).result()
    print(f"[INFO] Inserted {len(df)} rows to BigQuery.")

# ============================================================
# ✅ Main
# ============================================================
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--csv-test", required=False)  # ✅ اضافه شد
    args = parser.parse_args()

    START_DATE = args.start_date
    END_DATE = args.end_date

    print(f"[INFO] Fetching SearchAppearance data from {START_DATE} to {END_DATE}")

    df = fetch_searchappearance_data(START_DATE, END_DATE)

    if df.empty:
        print("[INFO] No data fetched from GSC.")
        sys.exit(0)

    upload_to_bq(df)

    # CSV test optional
    if args.csv_test:
        df.to_csv(args.csv_test, index=False)
        print(f"[INFO] CSV test output written: {args.csv_test}")

    print(f"[INFO] Finished fetching all data. Total rows: {len(df)}")
