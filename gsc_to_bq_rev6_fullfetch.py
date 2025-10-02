import argparse
import csv
import os
import sys
import time
from datetime import datetime, timedelta

from googleapiclient.discovery import build
from google.oauth2 import service_account
from google.cloud import bigquery

# ==========================
# CONFIG
# ==========================
SCOPES = ["https://www.googleapis.com/auth/webmasters.readonly"]
KEY_FILE = "gcp-key.json"
SITE_URL = "https://bamtabridsazan.com/"
BQ_PROJECT = "bamtabridsazan"
BQ_DATASET = "seo_reports"
BQ_TABLE = "bamtabridsazan__gsc__raw_data_fullfetch"

# ==========================
# FUNCTIONS
# ==========================
def get_gsc_service():
    creds = service_account.Credentials.from_service_account_file(KEY_FILE, scopes=SCOPES)
    service = build("searchconsole", "v1", credentials=creds)
    return service

def get_bq_client():
    return bigquery.Client()

def generate_unique_key(row):
    dims = ["date", "query", "page", "country", "device", "searchAppearance"]
    return "_".join([str(row.get(dim, "") or "Unknown") for dim in dims])

def fetch_gsc_data(service, start_date, end_date, dimensions, row_limit=25000):
    all_rows = []
    start_row = 0

    while True:
        request = {
            "startDate": start_date,
            "endDate": end_date,
            "dimensions": dimensions,
            "rowLimit": row_limit,
            "startRow": start_row
        }

        try:
            response = service.searchanalytics().query(siteUrl=SITE_URL, body=request).execute()
        except Exception as e:
            raise RuntimeError(e)

        rows = response.get("rows", [])
        if not rows:
            break

        for row in rows:
            keys = row.get("keys", [])
            record = {
                "date": None,
                "query": None,
                "page": None,
                "country": None,
                "device": None,
                "searchAppearance": None,
                "clicks": row.get("clicks", 0),
                "impressions": row.get("impressions", 0),
                "ctr": row.get("ctr", 0),
                "position": row.get("position", 0)
            }
            for i, dim in enumerate(dimensions):
                record[dim] = keys[i] if i < len(keys) else None

            all_rows.append(record)

        if len(rows) < row_limit:
            break
        start_row += row_limit

    return all_rows

def ensure_table(client):
    dataset_ref = client.dataset(BQ_DATASET)
    table_ref = dataset_ref.table(BQ_TABLE)
    try:
        client.get_table(table_ref)
        print(f"[INFO] Table {BQ_TABLE} exists.")
    except Exception:
        schema = [
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("query", "STRING"),
            bigquery.SchemaField("page", "STRING"),
            bigquery.SchemaField("country", "STRING"),
            bigquery.SchemaField("device", "STRING"),
            bigquery.SchemaField("searchAppearance", "STRING"),
            bigquery.SchemaField("clicks", "FLOAT"),
            bigquery.SchemaField("impressions", "FLOAT"),
            bigquery.SchemaField("ctr", "FLOAT"),
            bigquery.SchemaField("position", "FLOAT"),
            bigquery.SchemaField("unique_key", "STRING")
        ]
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)
        print(f"[INFO] Created table {BQ_TABLE}.")

def load_existing_keys(client):
    query = f"SELECT unique_key FROM `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}`"
    try:
        rows = client.query(query).result()
        return {row.unique_key for row in rows}
    except Exception:
        return set()

def insert_rows(client, rows):
    if not rows:
        return 0
    table_ref = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    errors = client.insert_rows_json(table_ref, rows)
    if errors:
        print(f"[ERROR] Failed inserts: {errors}")
    return len(rows)

def write_csv_test(rows, csv_path):
    if not rows:
        return
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    print(f"[INFO] CSV test output written: {csv_path}")

# ==========================
# MAIN
# ==========================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--csv-test", required=False)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    start_date = args.start_date
    end_date = args.end_date
    csv_path = args.csv_test
    debug = args.debug

    print(f"[INFO] Fetching data from {start_date} to {end_date}")

    service = get_gsc_service()
    bq_client = get_bq_client()
    ensure_table(bq_client)
    existing_keys = load_existing_keys(bq_client)
    print(f"[INFO] Retrieved {len(existing_keys)} existing keys from BigQuery.")

    all_new_rows = []
    batches = [
        ["date", "query", "page"],
        ["date", "query", "country"],
        ["date", "query", "device"],
        ["date", "searchAppearance", "page"],  # 🔹 تست ترکیب جدید
        ["date"]
    ]

    for i, dims in enumerate(batches, start=1):
        print(f"[INFO] Batch {i}, dims {dims}: fetching data...")
        try:
            rows = fetch_gsc_data(service, start_date, end_date, dims)
        except RuntimeError as e:
            print(f"Error:  Batch {i} failed: {e}")
            continue

        print(f"[INFO] Batch {i}, dims {dims}: fetched {len(rows)} rows")
        new_rows = []
        for r in rows:
            r["unique_key"] = generate_unique_key(r)
            if r["unique_key"] not in existing_keys:
                existing_keys.add(r["unique_key"])
                new_rows.append(r)
        if new_rows:
            count = insert_rows(bq_client, new_rows)
            print(f"[INFO] Inserted {count} rows to BigQuery.")
            all_new_rows.extend(new_rows)

    if csv_path:
        write_csv_test(all_new_rows, csv_path)

    print(f"[INFO] Finished fetching all data. Total rows: {len(all_new_rows)}")

if __name__ == "__main__":
    main()
