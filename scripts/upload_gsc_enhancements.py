#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ============================================================
# File: upload_gsc_enhancements.py
# ============================================================
# Version: 34 (new)
# ============================================================

import os
import pandas as pd
import hashlib
from datetime import datetime
from google.cloud import bigquery

# ============================================================
# Block 1: Configuration and constants
# ============================================================

BQ_PROJECT = "seo-reports"
BQ_DATASET = "bamtabridsazan"
BQ_TABLE = "bamtabridsazan__gsc__raw_enhancements"
GSC_DIR = "gsc_enhancements"
SITE_NAME = "bamtabridsazan"

# ============================================================
# Block 2: Helper functions
# ============================================================

def hash_unique_key(*args):
    """Generate SHA1 hash for unique key."""
    key_str = "".join([str(a) for a in args if a is not None])
    return hashlib.sha1(key_str.encode("utf-8")).hexdigest()

def read_excel_safe(file_path, sheet_name):
    """Safe read Excel sheet, returns empty DataFrame if fails."""
    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name)
        return df
    except Exception as e:
        print(f"[WARN] Failed to read {sheet_name} sheet in {file_path}: {e}")
        return pd.DataFrame()

# ============================================================
# Block 3: Extract enhancements from files
# ============================================================

def process_file(file_name):
    """Process a single Excel file and return DataFrame for BigQuery upload."""
    file_path = os.path.join(GSC_DIR, file_name)
    
    # Parse metadata from filename
    parts = file_name.split("-")
    enhancement_name = parts[1].strip() if len(parts) > 1 else None
    date_part = parts[-1].split(".")[0]
    file_date = pd.to_datetime(date_part, errors='coerce').date()
    status_hint = "Valid" if "Valid" in file_name else None

    # ============================================================
    # Block 3.1: Read Table sheet for item_name (new)
    # ============================================================
    df_table = read_excel_safe(file_path, sheet_name="Table")
    # Extract page and item_name only if DataFrame not empty
    if not df_table.empty:
        df_table = df_table.rename(columns={
            "URL": "page",
            "Item name": "item_name"
        })[["page", "item_name"]]
    else:
        df_table = pd.DataFrame(columns=["page", "item_name"])

    # ============================================================
    # Block 3.2: Read Chart sheet for Impressions (new)
    # ============================================================
    df_chart = read_excel_safe(file_path, sheet_name="Chart")
    if not df_chart.empty and "Impressions" in df_chart.columns:
        df_chart = df_chart.rename(columns={"Impressions": "impressions"})
        df_chart = df_chart[["page", "impressions"]]
    else:
        df_chart = pd.DataFrame(columns=["page", "impressions"])

    # ============================================================
    # Block 3.3: Merge Table and Chart data
    # ============================================================
    df = pd.merge(df_table, df_chart, on="page", how="outer")

    if df.empty:
        print(f"[INFO] No data found in {file_name} — skipping.")
        return pd.DataFrame()

    # ============================================================
    # Block 3.4: Add metadata columns
    # ============================================================
    df["site"] = SITE_NAME
    df["date"] = file_date
    df["enhancement_name"] = enhancement_name
    df["appearance_type"] = enhancement_name.upper().replace(" ", "_")
    df["issue_name"] = None
    df["last_crawled"] = None
    df["status"] = status_hint
    df["fetch_id"] = datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + hash_unique_key(file_name)
    df["fetch_date"] = datetime.now().date()
    df["source_file"] = file_name
    df["unique_key"] = df.apply(lambda row: hash_unique_key(
        row["site"], row["date"], row["enhancement_name"], row["page"]
    ), axis=1)

    # ============================================================
    # Block 3.5: Add clicks, ctr, position (placeholders)
    # ============================================================
    df["clicks"] = None
    df["ctr"] = None
    df["position"] = None

    return df

# ============================================================
# Block 4: Upload to BigQuery
# ============================================================

def upload_to_bq(df):
    client = bigquery.Client()
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    try:
        client.get_table(table_id)
        print(f"[INFO] Table {table_id} exists, skipping creation.")
    except:
        schema = [
            bigquery.SchemaField("site", "STRING"),
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("enhancement_name", "STRING"),
            bigquery.SchemaField("appearance_type", "STRING"),
            bigquery.SchemaField("page", "STRING"),
            bigquery.SchemaField("item_name", "STRING"),
            bigquery.SchemaField("issue_name", "STRING"),
            bigquery.SchemaField("last_crawled", "DATE"),
            bigquery.SchemaField("status", "STRING"),
            bigquery.SchemaField("fetch_id", "STRING"),
            bigquery.SchemaField("fetch_date", "DATE"),
            bigquery.SchemaField("source_file", "STRING"),
            bigquery.SchemaField("unique_key", "STRING"),
            bigquery.SchemaField("impressions", "INTEGER"),
            bigquery.SchemaField("clicks", "INTEGER"),
            bigquery.SchemaField("ctr", "FLOAT"),
            bigquery.SchemaField("position", "FLOAT"),
        ]
        client.create_table(bigquery.Table(table_id, schema=schema))
        print(f"[INFO] Table {table_id} created.")

    # Remove duplicates based on unique_key
    existing_keys = [row["unique_key"] for row in client.query(f"SELECT unique_key FROM `{table_id}`").result()]
    df_new = df[~df["unique_key"].isin(existing_keys)]
    if not df_new.empty:
        client.load_table_from_dataframe(df_new, table_id).result()
        print(f"[INFO] Inserted {len(df_new)} rows to {table_id}.")
    else:
        print("[INFO] No new rows to upload.")

# ============================================================
# Block 5: Main execution
# ============================================================

def main():
    all_files = [f for f in os.listdir(GSC_DIR) if f.endswith(".xlsx")]
    all_dfs = []
    for f in all_files:
        df = process_file(f)
        if not df.empty:
            all_dfs.append(df)
    if all_dfs:
        final_df = pd.concat(all_dfs, ignore_index=True)
        upload_to_bq(final_df)
    print("[INFO] Finished processing GSC enhancements.")

# ============================================================
# Block 6: Run main
# ============================================================

if __name__ == "__main__":
    main()
