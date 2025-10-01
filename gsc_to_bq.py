# ==========================================================
# File: gsc_to_bq.py
# Rev: 4 (Incremental Mode - Fixed Duplicates & Logs)
# ==========================================================

import os
import sys
import json
import argparse
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import bigquery
from googleapiclient.discovery import build
from google.oauth2 import service_account

# ==========================================================
# 🔧 SITE CONFIGURATION (Fixed Values)
# ==========================================================
SITE_URL = "https://bamtabridsazan.com/"
PROJECT_ID = "bamtabridsazan"
DATASET_ID = "seo_reports"
TABLE_ID = "bamtabridsazan__gsc__raw_data"
KEY_FILE = "gcp-key.json"

# ==========================================================
# 🔧 INITIALIZATION
# ==========================================================
SCOPES = ["https://www.googleapis.com/auth/webmasters.readonly"]
credentials = service_account.Credentials.from_service_account_file(KEY_FILE, scopes=SCOPES)
webmasters_service = build("searchconsole", "v1", credentials=credentials)
bq_client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

print(f"✅ Service Account has access to {SITE_URL}")

# ==========================================================
# 🧠 ARGUMENT PARSER
# ==========================================================
parser = argparse.ArgumentParser(description="Fetch GSC data incrementally and push to BigQuery")
parser.add_argument("--start-date", required=False, help="Start date (YYYY-MM-DD)")
parser.add_argument("--end-date", required=False, help="End date (YYYY-MM-DD)")
args = parser.parse_args()

# اگر تاریخ ندادیم، سه روز گذشته تا دیروز رو بگیریم
end_date = args.end_date or (datetime.utcnow().date() - timedelta(days=1)).isoformat()
start_date = args.start_date or (datetime.utcnow().date() - timedelta(days=3)).isoformat()

# ==========================================================
# 🧱 TABLE SCHEMA
# ==========================================================
TABLE_REF = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

SCHEMA = [
    bigquery.SchemaField("site_url", "STRING"),
    bigquery.SchemaField("date", "DATE"),
    bigquery.SchemaField("query", "STRING"),
    bigquery.SchemaField("page", "STRING"),
    bigquery.SchemaField("country", "STRING"),
    bigquery.SchemaField("device", "STRING"),
    bigquery.SchemaField("clicks", "FLOAT"),
    bigquery.SchemaField("impressions", "FLOAT"),
    bigquery.SchemaField("ctr", "FLOAT"),
    bigquery.SchemaField("position", "FLOAT"),
]

# ==========================================================
# ⚙️ TABLE CREATION IF NOT EXISTS
# ==========================================================
def ensure_table():
    try:
        bq_client.get_table(TABLE_REF)
        print(f"[INFO] Table {TABLE_ID} exists.")
    except Exception:
        print(f"[INFO] Table {TABLE_ID} not found. Creating...")
        table = bigquery.Table(TABLE_REF, schema=SCHEMA)
        bq_client.create_table(table)
        print(f"[INFO] Table {TABLE_ID} created.")

# ==========================================================
# 📦 FETCH EXISTING KEYS
# ==========================================================
def get_existing_keys():
    query = f"""
        SELECT CONCAT(site_url, '|', CAST(date AS STRING), '|', query, '|', page, '|', country, '|', device) AS key
        FROM `{TABLE_REF}`
        WHERE date BETWEEN '{start_date}' AND '{end_date}'
    """
    try:
        df = bq_client.query(query).result().to_dataframe()
        print(f"[INFO] Retrieved {len(df)} existing keys from BigQuery.")
        return set(df["key"].values)
    except Exception as e:
        print(f"[WARN] Failed to fetch existing keys: {e}")
        return set()

# ==========================================================
# 💾 INSERT INTO BIGQUERY
# ==========================================================
def insert_rows_to_bigquery(df):
    if df.empty:
        return 0
    bq_client.load_table_from_dataframe(df, TABLE_REF).result()
    print(f"[INFO] Inserted {len(df)} rows to BigQuery.")
    return len(df)

# ==========================================================
# 🚀 FETCH GSC DATA
# ==========================================================
def fetch_gsc_data():
    all_rows = []
    row_limit = 25000
    start_row = 0
    total_new_rows = 0
    existing_keys = get_existing_keys()

    while True:
        request = {
            "startDate": start_date,
            "endDate": end_date,
            "dimensions": ["query", "page", "country", "device"],
            "rowLimit": row_limit,
            "startRow": start_row,
        }
        response = webmasters_service.searchanalytics().query(siteUrl=SITE_URL, body=request).execute()
        rows = response.get("rows", [])
        if not rows:
            break

        batch_data = []
        for row in rows:
            keys = row["keys"]
            record = {
                "site_url": SITE_URL,
                "date": start_date,
                "query": keys[0],
                "page": keys[1],
                "country": keys[2],
                "device": keys[3],
                "clicks": row.get("clicks", 0),
                "impressions": row.get("impressions", 0),
                "ctr": row.get("ctr", 0),
                "position": row.get("position", 0),
            }
            record_key = f"{SITE_URL}|{record['date']}|{record['query']}|{record['page']}|{record['country']}|{record['device']}"
            if record_key not in existing_keys:
                batch_data.append(record)
                existing_keys.add(record_key)

        if batch_data:
            df = pd.DataFrame(batch_data)
            total_new_rows += insert_rows_to_bigquery(df)
            print(f"[INFO] Batch {start_row // row_limit + 1}: Fetched {len(rows)} rows, {len(batch_data)} new rows.")
        else:
            print(f"[INFO] Batch {start_row // row_limit + 1}: Fetched {len(rows)} rows, no new rows.")

        if len(rows) < row_limit:
            break
        start_row += row_limit

    print(f"[INFO] Finished fetching all data. Total new rows: {total_new_rows}")

# ==========================================================
# 🏁 MAIN
# ==========================================================
if __name__ == "__main__":
    ensure_table()
    fetch_gsc_data()
