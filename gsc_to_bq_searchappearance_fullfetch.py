#!/usr/bin/env python3
# ================================================================
#  Project: BamTabridSazan - GSC Full Fetch System
#  File: gsc_searchappearance_fullfetch.py
#  Description: Fetches Search Appearance data from Google Search Console API
#               and inserts into BigQuery table (raw_data_searchappearance).
#  Version: Rev 6.5 (SearchAppearance Edition)
#  Author: MasterSniper (AI Co-Pilot)
#  Created: 2025-10-03
#  Last Updated: 2025-10-03
# ================================================================
#  ✅ Purpose:
#    - Extract all searchAppearance performance data (Clicks, Impressions, CTR, Position)
#      across given date range.
#    - Store in BigQuery table: bamtabridsazan__gsc__raw_data_searchappearance
#    - Prevent duplicate rows (using date + searchAppearance as unique key).
#
#  ⚙️ Table Schema:
#    - date (DATE)
#    - searchAppearance (STRING)
#    - clicks (FLOAT)
#    - impressions (FLOAT)
#    - ctr (FLOAT)
#    - position (FLOAT)
#
#  ⚡ Notes:
#    - This script is based on rev6 architecture with auto table creation.
#    - Dimensions allowed: ["searchAppearance", "date"] (UI restriction)
#    - Other dimensions (query/page/device/country) are not supported.
#
#  🧩 Example Usage:
#    python gsc_searchappearance_fullfetch.py --start-date 2025-09-01 --end-date 2025-09-30
#    python gsc_searchappearance_fullfetch.py --start-date 2025-09-01 --end-date 2025-09-30 --csv-test gsc_fullfetch_searchappearance_test.csv
# ================================================================

import argparse
import csv
import os
import sys
from datetime import datetime, timedelta
from google.cloud import bigquery
from googleapiclient.discovery import build
from google.oauth2 import service_account

# ================================================================
# 🔧 CONFIGURATION
# ================================================================
PROJECT_ID = "bamtabridsazan"
DATASET_ID = "seo_reports"
TABLE_ID = "bamtabridsazan__gsc__raw_data_searchappearance"
FULL_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

# Path to your service account key
SERVICE_ACCOUNT_FILE = "service_account.json"

# ================================================================
# 🧠 FUNCTIONS
# ================================================================
def init_gsc_service():
    """Initialize Google Search Console API service."""
    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=["https://www.googleapis.com/auth/webmasters.readonly"]
    )
    service = build("searchconsole", "v1", credentials=credentials)
    return service


def init_bq_client():
    """Initialize BigQuery client."""
    credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
    return client


def create_table_if_not_exists(bq_client):
    """Create the BigQuery table if it doesn't exist."""
    schema = [
        bigquery.SchemaField("date", "DATE"),
        bigquery.SchemaField("searchAppearance", "STRING"),
        bigquery.SchemaField("clicks", "FLOAT"),
        bigquery.SchemaField("impressions", "FLOAT"),
        bigquery.SchemaField("ctr", "FLOAT"),
        bigquery.SchemaField("position", "FLOAT"),
    ]
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    try:
        bq_client.get_table(table_ref)
        print(f"✅ Table exists: {table_ref}")
    except Exception:
        table = bigquery.Table(table_ref, schema=schema)
        bq_client.create_table(table)
        print(f"🆕 Created table: {table_ref}")


def fetch_gsc_searchappearance_data(service, site_url, start_date, end_date):
    """Fetch Search Appearance data with pagination."""
    all_rows = []
    page_row_limit = 25000
    start_row = 0

    while True:
        request = {
            "startDate": start_date,
            "endDate": end_date,
            "dimensions": ["searchAppearance", "date"],
            "rowLimit": page_row_limit,
            "startRow": start_row,
        }

        response = service.searchanalytics().query(siteUrl=site_url, body=request).execute()
        rows = response.get("rows", [])
        if not rows:
            break

        for row in rows:
            keys = row.get("keys", [])
            if len(keys) < 2:
                continue

            search_appearance = keys[0]
            date_value = keys[1]

            clicks = row.get("clicks", 0)
            impressions = row.get("impressions", 0)
            ctr = row.get("ctr", 0)
            position = row.get("position", 0)

            all_rows.append({
                "date": date_value,
                "searchAppearance": search_appearance,
                "clicks": clicks,
                "impressions": impressions,
                "ctr": ctr,
                "position": position
            })

        start_row += len(rows)
        print(f"📦 Fetched {len(rows)} rows (total: {start_row})")

        if len(rows) < page_row_limit:
            break

    print(f"✅ Total rows fetched: {len(all_rows)}")
    return all_rows


def insert_to_bigquery(bq_client, data):
    """Insert data into BigQuery with duplicate check."""
    if not data:
        print("⚠️ No data to insert.")
        return

    table = bq_client.get_table(FULL_TABLE_ID)
    unique_rows = []

    # Fetch existing unique keys
    query = f"""
        SELECT CONCAT(CAST(date AS STRING), '_', searchAppearance) as unique_key
        FROM `{FULL_TABLE_ID}`
        WHERE date BETWEEN '{min([r['date'] for r in data])}' AND '{max([r['date'] for r in data])}'
    """
    existing_keys = {row["unique_key"] for row in bq_client.query(query).result()}

    for r in data:
        key = f"{r['date']}_{r['searchAppearance']}"
        if key not in existing_keys:
            unique_rows.append(r)

    if not unique_rows:
        print("🟡 All rows already exist. Nothing new to insert.")
        return

    errors = bq_client.insert_rows_json(table, unique_rows)
    if errors:
        print("❌ Errors while inserting:", errors)
    else:
        print(f"✅ Inserted {len(unique_rows)} new rows.")


def save_to_csv(data, filename):
    """Save output to CSV for testing."""
    keys = ["date", "searchAppearance", "clicks", "impressions", "ctr", "position"]
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(data)
    print(f"💾 CSV saved: {filename}")


# ================================================================
# 🚀 MAIN
# ================================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--csv-test", required=False, help="Optional CSV filename for test output")
    parser.add_argument("--debug", action="store_true", help="Enable debug mode")
    args = parser.parse_args()

    start_date = args.start_date
    end_date = args.end_date

    # 🔹 Init clients
    bq_client = init_bq_client()
    gsc_service = init_gsc_service()
    create_table_if_not_exists(bq_client)

    # 🔹 Replace with your verified site
    SITE_URL = "https://bamtabridsazan.com/"

    print(f"🚀 Fetching SearchAppearance data from {start_date} to {end_date} ...")
    data = fetch_gsc_searchappearance_data(gsc_service, SITE_URL, start_date, end_date)

    if args.csv_test:
        save_to_csv(data, args.csv_test)

    insert_to_bigquery(bq_client, data)
    print("🎯 Done.")
