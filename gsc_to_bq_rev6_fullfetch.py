#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ============================================================
# File: gsc_to_bq_rev6_fullfetch.py
# Revision: Rev6.5 (final) — duplicate prevention restored, searchAppearance removed
# Purpose: Full fetch from GSC -> BigQuery with duplicate prevention and --csv-test support
# ============================================================

import os
import sys
import time
import hashlib
import argparse
import warnings
import pandas as pd
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from google.cloud import bigquery

# ---------- CONFIG ----------
SITE_URL = "https://bamtabridsazan.com/"
BQ_PROJECT = "bamtabridsazan"
BQ_DATASET = "seo_reports"
BQ_TABLE = "bamtabridsazan__gsc__raw_data_fullfetch"
ROW_LIMIT = 25000
RETRY_DELAY = 60  # seconds
# SERVICE_ACCOUNT_FILE can be provided via env SERVICE_ACCOUNT_FILE or defaults to gcp-key.json
SERVICE_ACCOUNT_FILE = os.environ.get("SERVICE_ACCOUNT_FILE", "gcp-key.json")

# ---------- ARGUMENTS ----------
parser = argparse.ArgumentParser(description="GSC to BigQuery Full Fetch (Rev6.5 final)")
parser.add_argument("--start-date", required=True, help="Start date YYYY-MM-DD")
parser.add_argument("--end-date", required=True, help="End date YYYY-MM-DD")
parser.add_argument("--debug", action="store_true", help="Debug: skip BQ insert (still creates CSV if requested)")
parser.add_argument("--csv-test", required=False, help="Optional CSV test output filename")
args = parser.parse_args()

START_DATE = args.start_date
END_DATE = args.end_date
DEBUG_MODE = args.debug
CSV_TEST_FILE = args.csv_test

# ---------- CLIENTS ----------
def get_credentials():
    # use service account json file path (workflow writes gcp-key.json)
    creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=["https://www.googleapis.com/auth/webmasters.readonly"])
    return creds

def get_bq_client():
    creds = get_credentials()
    return bigquery.Client(credentials=creds, project=creds.project_id)

def get_gsc_service():
    creds = get_credentials()
    return build("searchconsole", "v1", credentials=creds)

bq_client = get_bq_client()
table_ref = bq_client.dataset(BQ_DATASET).table(BQ_TABLE)

# ---------- ENSURE TABLE EXISTS ----------
def ensure_table():
    try:
        bq_client.get_table(table_ref)
        print(f"[INFO] Table {BQ_TABLE} exists.", flush=True)
    except Exception:
        print(f"[INFO] Table {BQ_TABLE} not found. Creating...", flush=True)
        schema = [
            bigquery.SchemaField("Date", "DATE"),
            bigquery.SchemaField("Query", "STRING"),
            bigquery.SchemaField("Page", "STRING"),
            bigquery.SchemaField("Country", "STRING"),
            bigquery.SchemaField("Device", "STRING"),
            bigquery.SchemaField("Clicks", "INTEGER"),
            bigquery.SchemaField("Impressions", "INTEGER"),
            bigquery.SchemaField("CTR", "FLOAT"),
            bigquery.SchemaField("Position", "FLOAT"),
            bigquery.SchemaField("unique_key", "STRING"),
        ]
        table = bigquery.Table(table_ref, schema=schema)
        # keep clustering similar to previous versions
        table.clustering_fields = ["Query", "Page"]
        bq_client.create_table(table)
        print(f"[INFO] Table {BQ_TABLE} created.", flush=True)

# ---------- UNIQUE KEY (duplicate prevention) ----------
def generate_unique_key(row):
    # compose key from date, query, page, country, device (searchAppearance intentionally excluded)
    q = (row.get("Query") or "").strip().lower()
    p = (row.get("Page") or "").strip().lower().rstrip("/")
    c = (row.get("Country") or "").strip().lower()
    d = (row.get("Device") or "").strip().lower()
    date_raw = row.get("Date")
    if isinstance(date_raw, str):
        date = date_raw[:10]
    elif isinstance(date_raw, datetime):
        date = date_raw.strftime("%Y-%m-%d")
    else:
        date = str(date_raw)[:10]
    key_str = "|".join([date, q, p, c, d])
    return hashlib.sha256(key_str.encode("utf-8")).hexdigest()

# ---------- GET EXISTING KEYS FROM BQ ----------
def get_existing_keys():
    try:
        query = f"SELECT unique_key FROM `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}`"
        # try BigQuery Storage client for faster download
        try:
            from google.cloud import bigquery_storage
            bqstorage_client = bigquery_storage.BigQueryReadClient()
            df = bq_client.query(query).to_dataframe(bqstorage_client=bqstorage_client)
        except Exception:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                df = bq_client.query(query).to_dataframe()
        print(f"[INFO] Retrieved {len(df)} existing keys from BigQuery.", flush=True)
        return set(df["unique_key"].astype(str).tolist())
    except Exception as e:
        print(f"[WARN] Failed to fetch existing keys: {e}", flush=True)
        return set()

# ---------- UPLOAD TO BIGQUERY (Batch Load) ----------
def upload_to_bq(df):
    if df.empty:
        print("[INFO] No new rows to insert.", flush=True)
        return 0
    # ensure Date column is datetime
    df["Date"] = pd.to_datetime(df["Date"])
    if DEBUG_MODE:
        print(f"[DEBUG] Debug mode ON: skipping insert of {len(df)} rows to BigQuery", flush=True)
        return len(df)
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema=[
            bigquery.SchemaField("Date", "DATE"),
            bigquery.SchemaField("Query", "STRING"),
            bigquery.SchemaField("Page", "STRING"),
            bigquery.SchemaField("Country", "STRING"),
            bigquery.SchemaField("Device", "STRING"),
            bigquery.SchemaField("Clicks", "INTEGER"),
            bigquery.SchemaField("Impressions", "INTEGER"),
            bigquery.SchemaField("CTR", "FLOAT"),
            bigquery.SchemaField("Position", "FLOAT"),
            bigquery.SchemaField("unique_key", "STRING"),
        ],
    )
    try:
        job = bq_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        print(f"[INFO] Inserted {len(df)} rows to BigQuery.", flush=True)
        return len(df)
    except Exception as e:
        print(f"[ERROR] Failed to insert rows: {e}", flush=True)
        return 0

# ---------- FETCH GSC DATA (three valid batches only) ----------
def fetch_gsc_data(start_date, end_date):
    service = get_gsc_service()
    existing_keys = get_existing_keys()
    all_new_rows = []
    total_new_count = 0

    # batches we keep (searchAppearance removed completely)
    DIMENSION_BATCHES = [
        ["date", "query", "page"],
        ["date", "query", "country"],
        ["date", "query", "device"],
    ]

    for i, dims in enumerate(DIMENSION_BATCHES, start=1):
        start_row = 0
        batch_index = 1
        while True:
            print(f"[INFO] Batch {i}, dims {dims}: fetching data (startRow={start_row})...", flush=True)
            request = {
                "startDate": start_date,
                "endDate": end_date,
                "dimensions": dims,
                "rowLimit": ROW_LIMIT,
                "startRow": start_row,
            }
            try:
                resp = service.searchanalytics().query(siteUrl=SITE_URL, body=request).execute()
            except Exception as e:
                print(f"[ERROR] Timeout or error: {e}, retrying in {RETRY_DELAY} sec...", flush=True)
                time.sleep(RETRY_DELAY)
                continue

            rows = resp.get("rows", [])
            if not rows:
                print(f"[INFO] Batch {i} no more rows (startRow={start_row}).", flush=True)
                break

            batch_new = []
            for r in rows:
                keys = r.get("keys", [])
                # mapping keys safely by checking dims
                date = keys[0] if len(keys) > 0 else None
                query = keys[1] if ("query" in dims and len(keys) > 1) else None
                # dimension at index 2 differs by batch meaning: page/country/device
                third = keys[2] if len(keys) > 2 else None
                page = third if "page" in dims else None
                country = third if "country" in dims else None
                device = third if "device" in dims else None

                row = {
                    "Date": date,
                    "Query": query,
                    "Page": page,
                    "Country": country,
                    "Device": device,
                    "Clicks": r.get("clicks", 0),
                    "Impressions": r.get("impressions", 0),
                    "CTR": r.get("ctr", 0.0),
                    "Position": r.get("position", 0.0),
                }

                unique_key = generate_unique_key(row)
                if unique_key not in existing_keys:
                    existing_keys.add(unique_key)
                    row["unique_key"] = unique_key
                    batch_new.append(row)

            print(f"[INFO] Batch {i} (page {batch_index}): Fetched {len(rows)} rows, {len(batch_new)} new rows.", flush=True)

            # if have new rows -> upload
            if batch_new:
                df_batch = pd.DataFrame(batch_new)
                inserted = upload_to_bq(df_batch)
                total_new_count += inserted
                # also keep a copy for optional CSV output
                all_new_rows.extend(batch_new)

            batch_index += 1
            # pagination: if fewer than ROW_LIMIT then done
            if len(rows) < ROW_LIMIT:
                break
            start_row += len(rows)

    # return DataFrame of all new rows (for CSV or reporting)
    df_all_new = pd.DataFrame(all_new_rows)
    return df_all_new, total_new_count

# ---------- MAIN ----------
def main():
    ensure_table()
    print(f"[INFO] Fetching data from {START_DATE} to {END_DATE}", flush=True)
    df_new, total_inserted = fetch_gsc_data(START_DATE, END_DATE)

    if df_new.empty:
        print("[INFO] No new rows fetched from GSC.", flush=True)
    else:
        print(f"[INFO] Total new rows fetched: {len(df_new)}", flush=True)
        print(f"[INFO] Total rows inserted to BigQuery: {total_inserted}", flush=True)

    # optional CSV output (useful for debug / artifact)
    if CSV_TEST_FILE:
        try:
            df_new.to_csv(CSV_TEST_FILE, index=False)
            print(f"[INFO] CSV test output written: {CSV_TEST_FILE}", flush=True)
        except Exception as e:
            print(f"[WARN] Failed to write CSV test file: {e}", flush=True)

    print("[INFO] Finished.", flush=True)

if __name__ == "__main__":
    main()
