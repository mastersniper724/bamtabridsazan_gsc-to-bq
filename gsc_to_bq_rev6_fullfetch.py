#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ============================================================
# File: gsc_to_bq_rev6_fullfetch.py
# Revision: Rev6.6.4 — Batch 7 = UN-KNOWN pages added
# Purpose: Full fetch from GSC -> BigQuery with duplicate prevention and sitewide total batch
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
SERVICE_ACCOUNT_FILE = os.environ.get("SERVICE_ACCOUNT_FILE", "gcp-key.json")

# ---------- ARGUMENTS ----------
parser = argparse.ArgumentParser(description="GSC to BigQuery Full Fetch (Rev6.6)")
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
    creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=["https://www.googleapis.com/auth/webmasters.readonly"])
    return creds

def get_bq_client():
    creds_for_bq = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)
    return bigquery.Client(credentials=creds_for_bq, project=creds_for_bq.project_id)

def get_gsc_service():
    creds_for_gsc = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/webmasters.readonly"]
    )
    return build("searchconsole", "v1", credentials=creds_for_gsc)

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
        table.clustering_fields = ["Query", "Page"]
        bq_client.create_table(table)
        print(f"[INFO] Table {BQ_TABLE} created.", flush=True)

# ---------- UNIQUE KEY ----------
def generate_unique_key(row):
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

# ---------- GET EXISTING KEYS ----------
def get_existing_keys():
    try:
        query = f"SELECT unique_key FROM `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}`"
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

# ---------- UPLOAD TO BIGQUERY ----------
def upload_to_bq(df):
    if df.empty:
        print("[INFO] No new rows to insert.", flush=True)
        return 0
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

# ---------- FETCH GSC DATA ----------
def fetch_gsc_data(start_date, end_date):
    service = get_gsc_service()
    existing_keys = get_existing_keys()
    all_new_rows = []
    total_new_count = 0

    DIMENSION_BATCHES = [
        ["date", "query", "page"],
        ["date", "query", "country"],
        ["date", "query", "device"],
        ["date", "query"],              # Batch 6 جدید
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
                date = keys[0] if len(keys) > 0 else None
                query = keys[1] if ("query" in dims and len(keys) > 1) else None
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

            if batch_new:
                df_batch = pd.DataFrame(batch_new)
                inserted = upload_to_bq(df_batch)
                total_new_count += inserted
                all_new_rows.extend(batch_new)

            batch_index += 1
            if len(rows) < ROW_LIMIT:
                break
            start_row += len(rows)

    df_all_new = pd.DataFrame(all_new_rows)
    return df_all_new, total_new_count

# ---------- A: FETCH SITEWIDE BATCH (ISOLATED) ----------
def fetch_sitewide_batch(start_date, end_date):
    print("[INFO] Running sitewide batch ['date']...", flush=True)
    service = get_gsc_service()
    existing_keys = get_existing_keys()
    all_new_rows = []
    total_new_count = 0

    # ---------- Step 1: fetch actual GSC rows for ['date'] ----------
    start_row = 0
    batch_index = 1
    while True:
        request = {
            "startDate": start_date,
            "endDate": end_date,
            "dimensions": ["date"],
            "rowLimit": ROW_LIMIT,
            "startRow": start_row,
        }
        try:
            resp = service.searchanalytics().query(siteUrl=SITE_URL, body=request).execute()
        except Exception as e:
            print(f"[ERROR] Sitewide batch error: {e}, retrying in {RETRY_DELAY} sec...", flush=True)
            time.sleep(RETRY_DELAY)
            continue

        rows = resp.get("rows", [])
        if not rows:
            print(f"[INFO] Sitewide batch: no more rows (startRow={start_row}).", flush=True)
            break

        batch_new = []
        for r in rows:
            keys = r.get("keys", [])
            date = keys[0] if len(keys) > 0 else None

            row = {
                "Date": date,
                "Query": "__SITE_TOTAL__",
                "Page": "__SITE_TOTAL__",
                "Country": None,
                "Device": None,
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

        print(f"[INFO] Sitewide batch page {batch_index}: fetched {len(rows)} rows, {len(batch_new)} new.", flush=True)

        if batch_new:
            df_batch = pd.DataFrame(batch_new)
            inserted = upload_to_bq(df_batch)
            total_new_count += inserted
            all_new_rows.extend(batch_new)

        batch_index += 1
        if len(rows) < ROW_LIMIT:
            break
        start_row += len(rows)

    # ---------- Step 2: add placeholder rows for missing dates ----------
    date_range = pd.date_range(start=start_date, end=end_date)
    for dt in date_range:
        date_str = dt.strftime("%Y-%m-%d")
        # فقط برای تاریخ‌هایی که هیچ row واقعی نداشته‌اند
        if not any(row["Date"] == date_str for row in all_new_rows):
            placeholder_row = {
                "Date": date_str,
                "Query": "__SITE_TOTAL__",
                "Page": "__SITE_TOTAL__",
                "Country": None,
                "Device": None,
                "Clicks": None,
                "Impressions": None,
                "CTR": None,
                "Position": None,
            }
            unique_key = generate_unique_key(placeholder_row)
            if unique_key not in existing_keys:
                existing_keys.add(unique_key)
                placeholder_row["unique_key"] = unique_key
                all_new_rows.append(placeholder_row)
                print(f"[INFO] Sitewide batch: adding placeholder for missing date {date_str}", flush=True)

    # Insert all placeholders at once (داده واقعی قبلاً Insert شده)
    placeholders_only = [row for row in all_new_rows if row["Clicks"] is None]
    if placeholders_only:
        df_placeholders = pd.DataFrame(placeholders_only)
        inserted = upload_to_bq(df_placeholders)
        total_new_count += inserted

    print(f"[INFO] Sitewide batch done: {total_new_count} new rows inserted.", flush=True)
    return pd.DataFrame(all_new_rows), total_new_count


# ----------------------------
# B. Fetch Batch 4: Date + Page (Page IS NOT NULL)
# ----------------------------
logger.info("Fetching Batch 4 (Date + Page, excluding NULL pages)...")

batch4_rows = []
try:
    data = fetch_gsc_data(
        start_date=start_date,
        end_date=end_date,
        dimensions=["date", "page"],
        row_limit=25000
    )
    for row in data:
        # فقط ردیف‌هایی که page پر دارند
        if "keys" in row and len(row["keys"]) == 2 and row["keys"][1]:
            batch4_rows.append(row)
except Exception as e:
    logger.error(f"Error fetching Batch 4 (Date+Page): {e}")

logger.info(f"Batch 4 fetched rows: {len(batch4_rows)}")

# افزودن به خروجی نهایی
if batch4_rows:
    all_rows.extend(batch4_rows)

# ----------------------------
# C. Fetch Batch 7: Unknown Page Data (page IS NULL)
# ----------------------------
logger.info("Fetching Batch 7 (Unknown Page Data, where page is NULL)...")

batch7_rows = []
try:
    data = fetch_gsc_data(
        start_date=start_date,
        end_date=end_date,
        dimensions=["date", "page"],
        row_limit=25000
    )

    for row in data:
        # فقط ردیف‌هایی که page خالی یا NULL هستن
        if "keys" in row and len(row["keys"]) == 2 and not row["keys"][1]:
            # جایگزین کردن مقدار خالی با placeholder
            row["keys"][1] = "__UNKNOWN_PAGE__"
            batch7_rows.append(row)

except Exception as e:
    logger.error(f"Error fetching Batch 7 (Unknown Page): {e}")

logger.info(f"Batch 7 fetched rows: {len(batch7_rows)}")

# افزودن به خروجی نهایی
if batch7_rows:
    all_rows.extend(batch7_rows)


# ---------- MAIN ----------
def main():
    ensure_table()
    print(f"[INFO] Fetching data from {START_DATE} to {END_DATE}", flush=True)
    df_new, total_inserted = fetch_gsc_data(START_DATE, END_DATE)

    # --- run isolated sitewide batch ---
    df_site, total_site = fetch_sitewide_batch(START_DATE, END_DATE)

    total_all = total_inserted + total_site

    if df_new.empty and df_site.empty:
        print("[INFO] No new rows fetched from GSC.", flush=True)
    else:
        print(f"[INFO] Total new rows fetched: {total_all}", flush=True)

    if CSV_TEST_FILE:
        try:
            df_combined = pd.concat([df_new, df_site], ignore_index=True)
            df_combined.to_csv(CSV_TEST_FILE, index=False)
            print(f"[INFO] CSV test output written: {CSV_TEST_FILE}", flush=True)
        except Exception as e:
            print(f"[WARN] Failed to write CSV test file: {e}", flush=True)

    print("[INFO] Finished.", flush=True)

if __name__ == "__main__":
    main()
