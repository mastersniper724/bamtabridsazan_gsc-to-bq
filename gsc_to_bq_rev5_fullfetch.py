#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import os
from datetime import datetime, timedelta
from google.oauth2 import service_account
from googleapiclient.discovery import build
from google.cloud import bigquery

# ===========================
# Argument parsing
# ===========================
parser = argparse.ArgumentParser(description="Full fetch from GSC to BigQuery")
parser.add_argument("--start-date", type=str, required=True, help="Start date in YYYY-MM-DD")
parser.add_argument("--end-date", type=str, required=True, help="End date in YYYY-MM-DD")
parser.add_argument("--debug", action="store_true", help="Enable debug mode")
parser.add_argument("--csv-test", type=str, default=None, help="Optional CSV output for testing")
args = parser.parse_args()

START_DATE = args.start_date
END_DATE = args.end_date
DEBUG = args.debug
CSV_TEST = args.csv_test

if DEBUG:
    print(f"[DEBUG] Start: {START_DATE}, End: {END_DATE}, CSV: {CSV_TEST}")

# ===========================
# Service account & clients
# ===========================
SERVICE_ACCOUNT_FILE = os.environ.get("GCP_KEY_PATH", "gcp-key.json")

credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE,
    scopes=["https://www.googleapis.com/auth/webmasters.readonly"]
)

# GSC client
webmasters_service = build("searchconsole", "v1", credentials=credentials)

# BigQuery client
bq_client = bigquery.Client(credentials=credentials, project=credentials.project_id)

# ===========================
# BigQuery dataset & table
# ===========================
BQ_DATASET = "seo_reports"
BQ_TABLE = "bamtabridsazan__gsc__raw_data_fullfetch"

# ===========================
# Helper functions
# ===========================
def fetch_gsc_data(start_date, end_date):
    """Fetch GSC data for all dimensions (query, page, country, device, searchAppearance)"""
    site_url = "https://bamtabridsazan.com/"
    body = {
        "startDate": start_date,
        "endDate": end_date,
        "dimensions": ["date", "query", "page", "country", "device", "searchAppearance"],
        "rowLimit": 25000
    }
    response = webmasters_service.searchanalytics().query(siteUrl=site_url, body=body).execute()
    rows = response.get("rows", [])
    data = []
    for row in rows:
        record = {
            "date": row.get("keys", [None])[0],
            "query": row.get("keys", [None, None])[1] if len(row.get("keys", [])) > 1 else None,
            "page": row.get("keys", [None, None, None])[2] if len(row.get("keys", [])) > 2 else None,
            "country": row.get("keys", [None, None, None, None])[3] if len(row.get("keys", [])) > 3 else None,
            "device": row.get("keys", [None, None, None, None, None])[4] if len(row.get("keys", [])) > 4 else None,
            "searchAppearance": row.get("keys", [None, None, None, None, None, None])[5] if len(row.get("keys", [])) > 5 else None,
            "clicks": row.get("clicks", 0),
            "impressions": row.get("impressions", 0),
            "ctr": row.get("ctr", 0),
            "position": row.get("position", 0),
        }
        # Generate unique_key for deduplication
        record["unique_key"] = (record["date"] or "") + (record["query"] or "") + (record["page"] or "")
        data.append(record)
    return data

def upload_to_bq(data):
    """Upload GSC data to BigQuery"""
    table_id = f"{bq_client.project}.{BQ_DATASET}.{BQ_TABLE}"
    errors = bq_client.insert_rows_json(table_id, data)
    if errors:
        print(f"[ERROR] BigQuery insert errors: {errors}")
    elif DEBUG:
        print(f"[DEBUG] Inserted {len(data)} rows to {table_id}")

# ===========================
# Main execution
# ===========================
if __name__ == "__main__":
    if DEBUG:
        print(f"[DEBUG] Fetching data from {START_DATE} to {END_DATE}")
    batch_data = fetch_gsc_data(START_DATE, END_DATE)
    if DEBUG:
        print(f"[DEBUG] Fetched {len(batch_data)} rows")
    if CSV_TEST:
        import csv
        keys = batch_data[0].keys() if batch_data else []
        with open(CSV_TEST, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=keys)
            writer.writeheader()
            writer.writerows(batch_data)
        print(f"[INFO] CSV exported to {CSV_TEST}")
    # Upload to BigQuery
    if batch_data:
        upload_to_bq(batch_data)
        print(f"[INFO] Data uploaded to BigQuery table {BQ_TABLE}")
