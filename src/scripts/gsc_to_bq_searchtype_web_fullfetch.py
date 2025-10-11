#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ============================================================
# File: gsc_to_bq_fullfetch.py
# Revision: Rev6.9 — Converting ISO 3166 Alpha-2 Codes country values to full Country Name.
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
from utils.gsc_country_utils import load_country_map, robust_map_country_column

# ---------- CONFIG ----------
SITE_URL = "sc-domain:bamtabridsazan.com"
BQ_PROJECT = "bamtabridsazan"
BQ_DATASET = "seo_reports"
BQ_TABLE = "bamtabridsazan__gsc__raw_domain_data_fullfetch"
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

# ---------- COUNTRY MAPPING ----------
client = bigquery.Client()
query = """
    SELECT country_code_alpha3 AS country_code, country_name
    FROM `bamtabridsazan.seo_reports.gsc_dim_country`
"""
df_country = client.query(query).to_dataframe()
df_country["country_code"] = df_country["country_code"].str.upper()
COUNTRY_MAP = dict(zip(df_country["country_code"], df_country["country_name"]))

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
            bigquery.SchemaField("SearchAppearance", "STRING"),
            bigquery.SchemaField("Clicks", "INTEGER"),
            bigquery.SchemaField("Impressions", "INTEGER"),
            bigquery.SchemaField("CTR", "FLOAT"),
            bigquery.SchemaField("Position", "FLOAT"),
            bigquery.SchemaField("SearchType", "STRING"),
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
            bigquery.SchemaField("SearchAppearance", "STRING"),
            bigquery.SchemaField("Clicks", "INTEGER"),
            bigquery.SchemaField("Impressions", "INTEGER"),
            bigquery.SchemaField("CTR", "FLOAT"),
            bigquery.SchemaField("Position", "FLOAT"),
            bigquery.SchemaField("SearchType", "STRING"),
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
def fetch_gsc_data(start_date, end_date, existing_keys):
    """
    Main batches (keeps original DIMENSION_BATCHES from 6.6.11).
    existing_keys: set passed from main() to avoid re-fetching keys repeatedly.
    Returns (df_all_new, total_inserted)
    """
    service = get_gsc_service()
    all_new_rows = []
    total_inserted = 0

    DIMENSION_BATCHES = [
        ["date", "query", "page"],
        ["date", "query", "country"],
        ["date", "query", "device"],
        ["date", "query"],
    ]

    total_fetched_overall = 0
    total_new_candidates_overall = 0

    for i, dims in enumerate(DIMENSION_BATCHES, start=1):
        start_row = 0
        batch_index = 1
        fetched_total_for_batch = 0
        new_candidates_for_batch = 0
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

            fetched_total_for_batch += len(rows)
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
                    "SearchAppearance": None,  #Null
                    "Clicks": r.get("clicks", 0),
                    "Impressions": r.get("impressions", 0),
                    "CTR": r.get("ctr", 0.0),
                    "Position": r.get("position", 0.0),
                    "SearchType": "web",
                }

                unique_key = generate_unique_key(row)
                if unique_key not in existing_keys:
                    existing_keys.add(unique_key)
                    row["unique_key"] = unique_key
                    batch_new.append(row)

            new_candidates_for_batch += len(batch_new)
            print(f"[INFO] Batch {i} (page {batch_index}): Fetched {len(rows)} rows, {len(batch_new)} new rows.", flush=True)

            if batch_new:
                df_batch = pd.DataFrame(batch_new)

                # ---------- APPLY COUNTRY MAPPING FOR THIS BATCH (if applicable) ----------
                # only attempt mapping for batches that requested the 'country' dimension
                if "country" in [d.lower() for d in dims]:
                    # find actual country column name in df_batch (case-insensitive)
                    country_col = next((c for c in df_batch.columns if c.lower() == "country"), None)

                    if country_col is None:
                        print(f"[DEBUG] Batch {i}: expected 'country' column but none found in columns. Skipping country mapping.", flush=True)
                    else:
                        # quick samples to inspect incoming codes
                        sample_vals = pd.Series(df_batch[country_col].astype(str)).dropna().unique()[:20]

                        # apply robust mapping (uses utils.robust_map_country_column)
                        df_batch = robust_map_country_column(df_batch, country_col=country_col, country_map=COUNTRY_MAP, new_col="Country")
                        # now show how many mapped / unmapped
                        mapped_count = df_batch["Country"].notna().sum()
                        total_count = len(df_batch)

                # ---------- UPLOAD to BQ ----------
                inserted = upload_to_bq(df_batch)
                total_inserted += inserted
                all_new_rows.extend(batch_new)

            batch_index += 1
            if len(rows) < ROW_LIMIT:
                break
            start_row += len(rows)

        print(f"[INFO] Batch {i} summary: fetched_total={fetched_total_for_batch}, new_candidates={new_candidates_for_batch}, inserted={0 if fetched_total_for_batch==0 else 'see per-page logs'}", flush=True)
        total_fetched_overall += fetched_total_for_batch
        total_new_candidates_overall += new_candidates_for_batch

    df_all_new = pd.DataFrame(all_new_rows)
    print(f"[INFO] Fetch_GSC_Data summary: fetched_overall={total_fetched_overall}, new_candidates_overall={total_new_candidates_overall}, inserted_overall={total_inserted}", flush=True)
    return df_all_new, total_inserted

# ---------- Batch 5: Isolated No-Index fetch (ISOLATED) ----------
def fetch_noindex_batch(start_date, end_date, existing_keys):
    """
    Fetch rows where 'page' is NULL/empty in dimensions ['date','page'].
    These represent the No-Index / unknown-page records we want to label as __NO_INDEX__.
    """
    service = get_gsc_service()
    start_row = 0
    noindex_rows = []
    fetched_total = 0
    new_candidates = 0
    while True:
        request = {
            "startDate": start_date,
            "endDate": end_date,
            "dimensions": ["date", "page"],
            "rowLimit": ROW_LIMIT,
            "startRow": start_row,
        }
        try:
            resp = service.searchanalytics().query(siteUrl=SITE_URL, body=request).execute()
        except Exception as e:
            print(f"[ERROR] No-Index batch error: {e}, retrying in {RETRY_DELAY} sec...", flush=True)
            time.sleep(RETRY_DELAY)
            continue

        rows = resp.get("rows", [])
        if not rows:
            break

        fetched_total += len(rows)
        for r in rows:
            keys = r.get("keys", [])
            # Expect keys = [date, page] for this dims
            if len(keys) == 2:
                page_val = keys[1]
                if (page_val is None) or (str(page_val).strip() == ""):
                    # this is a no-index-like record (page NULL/empty)
                    row = {
                        "Date": keys[0],
                        "Query": "__NO_INDEX__",
                        "Page": "__NO_INDEX__",
                        "Country": None,
                        "Device": None,
                        "SearchAppearance": None,  #Null
                        "Clicks": r.get("clicks", 0),
                        "Impressions": r.get("impressions", 0),
                        "CTR": r.get("ctr", 0.0),
                        "Position": r.get("position", 0.0),
                        "SearchType": "web",
                    }
                    row["unique_key"] = generate_unique_key(row)
                    if row["unique_key"] not in existing_keys:
                        existing_keys.add(row["unique_key"])
                        noindex_rows.append(row)
                        new_candidates += 1

        if len(rows) < ROW_LIMIT:
            break
        start_row += len(rows)

    inserted = 0
    if noindex_rows:
        df_noindex = pd.DataFrame(noindex_rows)
        inserted = upload_to_bq(df_noindex)

    print(f"[INFO] Batch 5, No-Index summary: fetched_total={fetched_total}, new_candidates={new_candidates}, inserted={inserted}", flush=True)
    return pd.DataFrame(noindex_rows), inserted

# =================================================
#  BLOCK 7: fetch_sitewide_batch  (No-DML Version)
# =================================================
def fetch_sitewide_batch(start_date, end_date, existing_keys):
    """
    Fetch sitewide GSC data (Query='__SITE_TOTAL__', Page='__SITE_TOTAL__')
    Inserts new rows and replaces incomplete ones, without using DML (no UPDATE/MERGE).
    Works even when billing is disabled.
    """
    from google.cloud import bigquery
    import pandas as pd

    client = bigquery.Client()
    full_table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

    print(f"[INFO] Fetching sitewide data for {start_date} → {end_date}", flush=True)

    # Step 0: load existing keys and incomplete keys
    existing_bq_keys = get_existing_sitewide_keys(start_date, end_date, client, full_table_id)
    incomplete_keys = get_incomplete_keys(start_date, end_date, client, full_table_id)

    batch_new = []
    rows_to_update = []
    new_candidates = 0
    updated_count = 0
    skipped_count = 0

    # --------------------------------------------
    # Step 1: Fetch GSC data (sitewide)
    # --------------------------------------------
    df_site = fetch_gsc_data(start_date=start_date, end_date=end_date)
    if df_site.empty:
        print(f"[WARN] No sitewide data found for {start_date} → {end_date}", flush=True)
        return pd.DataFrame([]), 0

    for _, row in df_site.iterrows():
        unique_key = row["unique_key"]

        if unique_key in existing_bq_keys:
            if unique_key in incomplete_keys:
                rows_to_update.append(row.copy())
                updated_count += 1
            else:
                skipped_count += 1
                continue
        elif unique_key not in existing_keys:
            existing_keys.add(unique_key)
            batch_new.append(row)
            new_candidates += 1

    print(f"[INFO] Sitewide: new={new_candidates}, updates={updated_count}, skipped={skipped_count}", flush=True)

    # Convert new rows
    df_new = pd.DataFrame(batch_new) if batch_new else pd.DataFrame([])

    # --------------------------------------------
    # Step 2: Rebuild partial table (no DML)
    # --------------------------------------------
    if updated_count > 0:
        df_updates = pd.DataFrame(rows_to_update)

        # prepare list of keys for exclusion
        keys_list = df_updates["unique_key"].astype(str).tolist()
        keys_quoted = ",".join([f"'{k}'" for k in keys_list])

        print(f"[INFO] Reading existing rows excluding {len(keys_list)} keys for rewrite...", flush=True)
        read_query = f"""
            SELECT *
            FROM `{full_table_id}`
            WHERE Date BETWEEN '{start_date}' AND '{end_date}'
              AND NOT unique_key IN ({keys_quoted})
        """

        try:
            df_existing = client.query(read_query).to_dataframe()
        except Exception as e:
            print(f"[ERROR] Could not read existing rows: {e}", flush=True)
            df_existing = pd.DataFrame([])

        # merge and rewrite
        if df_existing.empty:
            df_merged = df_updates
        else:
            df_merged = pd.concat([df_existing, df_updates], ignore_index=True, sort=False)

        print(f"[INFO] Rewriting {len(df_merged)} rows to {full_table_id} (no DML)...", flush=True)

        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        try:
            load_job = client.load_table_from_dataframe(df_merged, full_table_id, job_config=job_config)
            load_job.result()
            print(f"[SUCCESS] Rewrote {len(df_merged)} rows to BigQuery successfully.", flush=True)
        except Exception as e:
            print(f"[ERROR] Failed to load merged data: {e}", flush=True)

    # --------------------------------------------
    # Step 3: Insert new rows
    # --------------------------------------------
    if not df_new.empty:
        print(f"[INFO] Inserting {len(df_new)} new sitewide rows...", flush=True)
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        try:
            load_job = client.load_table_from_dataframe(df_new, full_table_id, job_config=job_config)
            load_job.result()
            print(f"[SUCCESS] Inserted {len(df_new)} new sitewide rows.", flush=True)
        except Exception as e:
            print(f"[ERROR] Failed to insert new rows: {e}", flush=True)

    return df_site, len(df_new)

# ---------- MAIN ----------
def main():
    ensure_table()
    print(f"[INFO] Fetching data from {START_DATE} to {END_DATE}", flush=True)

    # ---------- Check existing keys (once) ----------
    existing_keys = get_existing_keys()
    print(f"[INFO] Retrieved {len(existing_keys)} existing keys from BigQuery. (used across all blocks)", flush=True)

    # --- Normal FullFetch Batch (main pipeline) ---
    df_new, inserted_main = fetch_gsc_data(START_DATE, END_DATE, existing_keys)

    # --- Isolated No-Index pass (replaces Batch7) ---
    df_noindex, inserted_noindex = fetch_noindex_batch(START_DATE, END_DATE, existing_keys)

    # ----------------------------
    # B. Fetch Batch 4: Date + Page (Page IS NOT NULL)
    # ----------------------------
    print("[INFO] Fetching Batch 6 (Date + Page, excluding NULL pages)...", flush=True)
    try:
        service = get_gsc_service()
        start_row = 0
        all_rows = []

        fetched_b4 = 0
        new_b4 = 0
        while True:
            request = {
                "startDate": START_DATE,
                "endDate": END_DATE,
                "dimensions": ["date", "page"],
                "rowLimit": ROW_LIMIT,
                "startRow": start_row,
            }
            resp = service.searchanalytics().query(siteUrl=SITE_URL, body=request).execute()
            rows = resp.get("rows", [])
            if not rows:
                break

            fetched_b4 += len(rows)
            for r in rows:
                keys = r.get("keys", [])
                if len(keys) == 2 and keys[1]:  # فقط صفحات non-null
                    row = {
                        "Date": keys[0],
                        "Query": "__PAGE_TOTAL__",
                        "Page": keys[1],
                        "Country": None,
                        "Device": None,
                        "SearchAppearance": None,  #Null
                        "Clicks": r.get("clicks", 0),
                        "Impressions": r.get("impressions", 0),
                        "CTR": r.get("ctr", 0.0),
                        "Position": r.get("position", 0.0),
                        "SearchType": "web",
                    }
                    unique_key = generate_unique_key(row)
                    if unique_key not in existing_keys:
                        existing_keys.add(unique_key)
                        row["unique_key"] = unique_key
                        all_rows.append(row)
                        new_b4 += 1

            if len(rows) < ROW_LIMIT:
                break
            start_row += len(rows)

        inserted_b4 = 0
        if all_rows:
            df_batch4 = pd.DataFrame(all_rows)
            print(f"[INFO] Batch 6 fetched rows: {len(df_batch4)}", flush=True)
            if not df_batch4.empty:
                inserted_b4 = upload_to_bq(df_batch4)
                print(f"[INFO] Batch 6: Inserted {inserted_b4} new rows to BigQuery.", flush=True)
        else:
            print("[INFO] Batch 6: No non-null page rows found.", flush=True)

        print(f"[INFO] Batch 6 summary: fetched_total={fetched_b4}, new_candidates={new_b4}, inserted={inserted_b4}", flush=True)

    except Exception as e:
        print(f"[ERROR] Failed to fetch Batch 6 (Date + Page): {e}", flush=True)
        inserted_b4 = 0
        df_batch4 = pd.DataFrame([])

    # --- run isolated sitewide batch ---
    df_site, inserted_site = fetch_sitewide_batch(START_DATE, END_DATE, existing_keys)

    total_all_inserted = inserted_main + inserted_noindex + inserted_b4 + inserted_site

    # Compose CSV output if requested
    if CSV_TEST_FILE:
        try:
            parts = []
            if not df_new.empty:
                parts.append(df_new)
            if not df_noindex.empty:
                parts.append(df_noindex)
            if 'df_batch4' in locals() and not df_batch4.empty:
                parts.append(df_batch4)
            if not df_site.empty:
                parts.append(df_site)
            if parts:
                df_combined = pd.concat(parts, ignore_index=True)
                df_combined.to_csv(CSV_TEST_FILE, index=False)
                print(f"[INFO] CSV test output written: {CSV_TEST_FILE}", flush=True)
            else:
                # write empty csv with headers
                cols = ["Date","Query","Page","Country","Device","SearchAppearance","Clicks","Impressions","CTR","Position","unique_key","SearchType"]
                pd.DataFrame(columns=cols).to_csv(CSV_TEST_FILE, index=False)
                print(f"[INFO] CSV test output written (empty): {CSV_TEST_FILE}", flush=True)
        except Exception as e:
            print(f"[WARN] Failed to write CSV test file: {e}", flush=True)

    # Final summary
    print("[INFO] Final summary:", flush=True)
    print(f"  - fetch_gsc_data inserted: {inserted_main}", flush=True)
    print(f"  - noindex inserted:       {inserted_noindex}", flush=True)
    print(f"  - batch4 inserted:        {inserted_b4}", flush=True)
    print(f"  - sitewide inserted:      {inserted_site}", flush=True)
    print(f"[INFO] Total new rows fetched/inserted: {total_all_inserted}", flush=True)
    print("[INFO] Finished.", flush=True)


if __name__ == "__main__":
    main()
