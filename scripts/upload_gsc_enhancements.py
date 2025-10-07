#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ============================================================
# File: upload_gsc_enhancements.py
# Version: 35.4 (Debug + Valid/Table URL Fix)
# ============================================================

import os
import re
import pandas as pd
import numpy as np
from google.cloud import bigquery
from datetime import datetime
from pathlib import Path

# ============================================================
# CONFIG
# ============================================================
PROJECT_ID = "bamtabridsazan"
DATASET_ID = "seo_reports"
TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.bamtabridsazan__gsc__raw_enhancements"

# ============================================================
# HELPERS
# ============================================================
def clean_str(val):
    """Clean string values safely"""
    if pd.isna(val):
        return None
    if isinstance(val, (int, float)):
        return str(val)
    return str(val).strip()

def normalize_colnames(df):
    """Normalize column names"""
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    return df

def safe_float(val):
    """Convert safely to float"""
    try:
        return float(str(val).replace(",", "").strip())
    except:
        return np.nan

# ============================================================
# PARSER
# ============================================================
def parse_excel_file(file_path):
    """Parse a single Excel file and return details_df, metrics_df"""
    xls = pd.ExcelFile(file_path)
    sheet_names = [s.strip() for s in xls.sheet_names]

    # --- details_df ---
    details_sheet = "Details" if "Details" in sheet_names else sheet_names[0]
    details_df = pd.read_excel(file_path, sheet_name=details_sheet)
    details_df = normalize_colnames(details_df)

    # --- url normalization ---
    url_cols = [c for c in details_df.columns if 'url' in c.lower() or 'page' in c.lower()]
    if url_cols:
        details_df['url'] = details_df[url_cols[0]]
    else:
        details_df['url'] = None

    # --- metrics_df (Chart or Table) ---
    metrics_df = None
    metrics_fallback = None

    # ✅ NEW LOGIC: handle Valid files properly
    if 'Valid' in file_path:
        try:
            # Try loading 'Table' sheet (this one contains URL)
            metrics_fallback = pd.read_excel(file_path, sheet_name='Table')
            metrics_fallback.columns = [c.strip() for c in metrics_fallback.columns]

            if 'URL' in metrics_fallback.columns:
                metrics_fallback.rename(columns={'URL': 'url'}, inplace=True)
            else:
                print(f"[WARN] 'URL' column not found in Table sheet: {file_path}")

            # keep only key columns
            keep_cols = ['url']
            for c in ['Impressions', 'Clicks', 'CTR', 'Position']:
                if c in metrics_fallback.columns:
                    keep_cols.append(c)
            metrics_fallback = metrics_fallback[keep_cols]

            print(f"[INFO] Loaded metrics_fallback from Table sheet ({file_path}), columns={keep_cols}")

        except Exception as e:
            print(f"[ERROR] Failed to load Table sheet for metrics_fallback: {file_path}, error={e}")
    else:
        # fallback for non-Valid files
        try:
            metrics_fallback = pd.read_excel(file_path, sheet_name='Chart')
            metrics_fallback.columns = [c.strip() for c in metrics_fallback.columns]
            print(f"[INFO] Loaded metrics_fallback from Chart sheet ({file_path})")
        except Exception as e:
            print(f"[WARN] No Chart sheet found in {file_path}: {e}")

    metrics_df = metrics_fallback if metrics_fallback is not None else pd.DataFrame()

    # --- ensure URL field is normalized ---
    if not metrics_df.empty:
        url_cols_m = [c for c in metrics_df.columns if 'url' in c.lower() or 'page' in c.lower()]
        if url_cols_m:
            metrics_df['url'] = metrics_df[url_cols_m[0]]
        else:
            print(f"[WARN] No 'url' column found in metrics_fallback for {file_path}")

    # --- unify item_name if exists ---
    if 'item_name' not in details_df.columns:
        details_df['item_name'] = None
    if 'item_name' not in metrics_df.columns:
        metrics_df['item_name'] = None

    # --- merge section ---
    if not details_df.empty and not metrics_df.empty:
        details_df['merge_key'] = details_df.apply(lambda r: f"{r.get('url','')}|{r.get('item_name','')}", axis=1)
        metrics_df['merge_key'] = metrics_df.apply(lambda r: f"{r.get('url','')}|{r.get('item_name','')}", axis=1)
        details_df = details_df.merge(metrics_df.drop_duplicates(subset=['merge_key']).drop(columns=['url']),
                                      on='merge_key', how='left', suffixes=('','_metric'))

        # fallback merge by url if impressions all NaN
        if 'impressions' in details_df.columns and details_df['impressions'].isna().all():
            try:
                details_df = details_df.merge(metrics_df.drop_duplicates(subset=['url']).drop(columns=['item_name']),
                                              on='url', how='left', suffixes=('','_metric'))
            except Exception as e:
                print(f"[WARN] metrics fallback by url failed: {e}")

        details_df.drop(columns=['merge_key'], inplace=True)

    return details_df, metrics_df

# ============================================================
# MAIN
# ============================================================
def main():
    base_dir = "./data"
    all_files = sorted(Path(base_dir).glob("*.xlsx"))
    if not all_files:
        print("[ERROR] No Excel files found.")
        return

    all_dfs = []
    for file_path in all_files:
        file_path = str(file_path)
        m = re.match(r".*-(?P<enhancement>[^-]+)(-(?P<status>Valid|Invalid))?-(?P<date>\d{4}-\d{2}-\d{2})\.xlsx$", os.path.basename(file_path))
        if not m:
            print(f"[WARN] Filename not matching pattern: {file_path}")
            continue

        enhancement = m.group("enhancement")
        date_str = m.group("date")
        status_hint = m.group("status")
        print(f"[INFO] Processing file: {os.path.basename(file_path)} -> enhancement='{enhancement}', date={date_str}, status_hint={status_hint}")

        try:
            details_df, metrics_df = parse_excel_file(file_path)
            if details_df.empty:
                print(f"[WARN] details_df empty for {file_path}")
                continue

            details_df['enhancement_name'] = enhancement
            details_df['date'] = date_str
            details_df['status_hint'] = status_hint

            # drop duplicates
            before = len(details_df)
            details_df.drop_duplicates(inplace=True)
            after = len(details_df)
            if before != after:
                print(f"[INFO] Removed {before - after} duplicate rows in file {os.path.basename(file_path)}")

            all_dfs.append(details_df)

        except Exception as e:
            print(f"[ERROR] Failed to process {file_path}: {e}")

    if not all_dfs:
        print("[WARN] No data parsed.")
        return

    final_df = pd.concat(all_dfs, ignore_index=True)
    print(final_df.head(20))

    print(f"[INFO] Total new rows to upload: {len(final_df)}")
    upload_to_bigquery(final_df)

# ============================================================
# UPLOAD
# ============================================================
def upload_to_bigquery(df):
    client = bigquery.Client(project=PROJECT_ID)
    schema = []

    # Create table if not exists
    try:
        table_ref = bigquery.Table(TABLE_ID)
        table_ref = client.create_table(table_ref, exists_ok=True)
        print(f"[INFO] Table created.")
    except Exception as e:
        print(f"[WARN] Could not create table: {e}")

    job = client.load_table_from_dataframe(df, TABLE_ID)
    job.result()
    print(f"[INFO] Inserted {len(df)} rows to {TABLE_ID}.")

# ============================================================
# ENTRY
# ============================================================
if __name__ == "__main__":
    main()
