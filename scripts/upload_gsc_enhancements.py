#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ============================================================
# File: upload_gsc_enhancements.py
# Revision: Rev.34 — Fixed duplicate rows and merged item_name + metrics properly
# Purpose: Parse GSC Enhancement XLSX exports (placed in gsc_enhancements/),
#          build per-URL raw enhancements table and load to BigQuery with dedupe.
# ============================================================

import os
import re
import argparse
import hashlib
from datetime import datetime, date
from uuid import uuid4
import warnings
# نادیده گرفتن هشدارهای مربوط به Workbook بدون style پیش‌فرض
warnings.filterwarnings(
    "ignore",
    category=UserWarning,
    module="openpyxl.styles.stylesheet"
)

import pandas as pd
import glob
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField

# =================================================
# BLOCK 1: CONFIG / CONSTANTS
# =================================================
PROJECT_ID = "bamtabridsazan"
DATASET_ID = "seo_reports"
TABLE_ID = "bamtabridsazan__gsc__raw_enhancements"
MAPPING_TABLE = f"{PROJECT_ID}.{DATASET_ID}.bamtabridsazan__gsc__searchappearance_enhancement_mapping"
GITHUB_LOCAL_PATH_DEFAULT = "gsc_enhancements"

# Final schema columns (must match table schema if exists)
FINAL_COLUMNS = [
    "site",
    "date",
    "enhancement_name",
    "appearance_type",
    "page",
    "item_name",
    "issue_name",
    "last_crawled",
    "status",
    "fetch_id",
    "fetch_date",
    "source_file",
    "unique_key",
    # Metrics dynamic
    "impressions",
    "clicks",
    "ctr",
    "position"
]

DEBUG_MODE = False  # حالت پیش‌فرض

# =================================================
# BLOCK 2: ARGPARSE
# =================================================
parser = argparse.ArgumentParser(description="Upload GSC Enhancements Data to BigQuery")
parser.add_argument("--start-date", type=str, help="Start date for fetching data (optional)")
parser.add_argument("--end-date", type=str, help="End date for fetching data (optional)")
parser.add_argument("--local-path", type=str, help="Path to local folder containing Excel files", default="gsc_enhancements")
parser.add_argument("--csv-test", type=str, help="Optional CSV test file path", default=None)
parser.add_argument("--debug", action="store_true", help="Enable debug mode")

args = parser.parse_args()
start_date = args.start_date
end_date = args.end_date
enhancement_folder = args.local_path
csv_test_path = args.csv_test
DEBUG_MODE = args.debug

# =================================================
# BLOCK 3: CLIENTS & UTIL
# =================================================
bq_client = bigquery.Client(project=PROJECT_ID)

def now_fetch_id():
    return datetime.utcnow().strftime("%Y%m%d_%H%M%S") + "_" + uuid4().hex[:8]

FETCH_ID = now_fetch_id()
FETCH_DATE = datetime.utcnow().date()

# =================================================
# BLOCK 4: PARSING HELPERS
# =================================================
def parse_filename_metadata(filename):
    base = os.path.basename(filename)
    name = re.sub(r'\.xlsx$', '', base, flags=re.IGNORECASE)
    m = re.search(r'^(?P<prefix>.+)-(?P<date>\d{4}-\d{2}-\d{2})$', name)
    if not m:
        return {"site_raw": None, "site": None, "enhancement_name": None, "date": None, "status_hint": None, "source_file": base}
    prefix = m.group("prefix")
    date_str = m.group("date")
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d").date()
    except Exception:
        dt = None
    parts = prefix.split("-")
    site_raw = parts[0] if parts else ""
    status_hint = None
    last_token = parts[-1].strip().lower() if len(parts) >= 2 else ""
    if last_token in ("valid", "invalid", "valid_items", "invalid_items"):
        status_hint = "Valid" if "valid" in last_token else "Invalid"
        enh_tokens = parts[1:-1]
    else:
        enh_tokens = parts[1:]
    enhancement_name = "-".join(enh_tokens).strip()
    site = site_raw
    if site and site.lower().endswith(".com"):
        site = site[:-4]
    else:
        site = re.sub(r'\.[a-z]{2,}$', '', site)
    return {"site_raw": site_raw, "site": site, "enhancement_name": enhancement_name, "date": dt, "status_hint": status_hint, "source_file": base}

# =================================================
# BLOCK 5: EXCEL PARSE (Details + Chart)
# =================================================
def _normalize_columns(cols):
    normalized = []
    for c in cols:
        if not isinstance(c, str):
            c = str(c)
        s = c.strip().lower().replace("\n", " ").replace("\r", " ")
        s = re.sub(r'\s+', '_', s)
        normalized.append(s)
    return normalized

def parse_excel_file(file_path):
    try:
        xls = pd.ExcelFile(file_path)
    except Exception as e:
        print(f"[WARN] Cannot open {file_path}: {e}")
        return pd.DataFrame(), pd.DataFrame()

    details_frames = []

    # =================================================
    # BLOCK 5.1: Extract "Table" sheet for Item name (fix for Rev.34)
    # =================================================
    if "Table" in xls.sheet_names:
        try:
            df_table = pd.read_excel(xls, sheet_name="Table")
            if "Item name" in df_table.columns:
                df_table['item_name'] = df_table['Item name']

            # Safe url selection from multiple possible columns
            for col in ['URL', 'Page', 'page']:
                if col in df_table.columns:
                    df_table['url'] = df_table[col]
                    break
            else:
                df_table['url'] = None
            details_frames.append(df_table)
        except Exception as e:
            print(f"[WARN] Failed to read Table sheet in {file_path}: {e}")

    # =================================================
    # BLOCK 5.2: Extract "Chart" sheet for Metrics
    # =================================================
    def extract_chart_metrics(file_path):
        metrics_frames = []
        try:
            xls = pd.ExcelFile(file_path)
        except Exception as e:
            print(f"[WARN] Cannot open {file_path}: {e}")
            return pd.DataFrame()

        if "Chart" in xls.sheet_names:
            try:
                df_chart = pd.read_excel(xls, sheet_name="Chart")
                df_chart.columns = df_chart.columns.str.strip().str.lower()
                required_cols = [
                    "page", "appearance_type", "impressions", "clicks", "ctr",
                    "position", "item_name", "issue_name", "last_crawled", "status"
                ]
                for col in required_cols:
                    if col not in df_chart.columns:
                        df_chart[col] = None
                df_chart = df_chart.dropna(how="all")
                metric_cols = ["impressions", "clicks", "ctr", "position"]
                for col in metric_cols:
                    if col in df_chart.columns:
                        df_chart[col] = pd.to_numeric(df_chart[col], errors="coerce")
                metrics_frames.append(df_chart)
            except Exception as e:
                print(f"[WARN] Failed to read Chart sheet in {file_path}: {e}")
        return pd.concat(metrics_frames, ignore_index=True) if metrics_frames else pd.DataFrame()

    metrics_df = extract_chart_metrics(file_path)

    # ======================
    # Merge Table + Metrics safely
    # ======================
    if details_frames:
        details_df = pd.concat(details_frames, ignore_index=True)
    else:
        details_df = pd.DataFrame()

    if not details_df.empty and not metrics_df.empty:
        # merge on index (safe) to avoid duplicates
        details_df = pd.merge(details_df, metrics_df, left_index=True, right_index=True, how='left', suffixes=('', '_metric'))

    # ensure essential columns exist
    for col in ['url','item_name']:
        if col not in details_df.columns:
            details_df[col] = None

    return details_df, metrics_df

# =================================================
# BLOCK 6: Unique key
# =================================================
def build_unique_key_series(df, site, enhancement_name, date_val, status_col='status'):
    def row_key(r):
        page = r.get('url') or ""
        item_name = r.get('item_name') or ""
        status = r.get(status_col) or ""
        lastc = r.get('last_crawled')
        if isinstance(lastc, (datetime,)):
            lastc = lastc.date()
        lastc_str = str(lastc) if lastc else ""
        date_str = str(date_val) if date_val else ""
        raw = f"{site}|{enhancement_name}|{page}|{item_name}|{status}|{lastc_str}|{date_str}"
        return hashlib.sha256(raw.encode('utf-8')).hexdigest()
    return df.apply(row_key, axis=1)

# =================================================
# BLOCK 7: BQ helpers
# =================================================
def ensure_table_exists():
    table_ref = bq_client.dataset(DATASET_ID).table(TABLE_ID)
    try:
        table = bq_client.get_table(table_ref)
        print(f"[INFO] Table {DATASET_ID}.{TABLE_ID} exists, skipping creation.")
    except Exception:
        print(f"[INFO] Table {DATASET_ID}.{TABLE_ID} not found, creating...")
        schema = [bigquery.SchemaField(c, "STRING") for c in FINAL_COLUMNS]
        date_columns = ["date", "last_crawled", "fetch_date"]
        for col in date_columns:
            if col not in [f.name for f in schema]:
                schema.append(bigquery.SchemaField(col, "DATE"))
        table = bigquery.Table(table_ref, schema=schema)
        bq_client.create_table(table)
        print(f"[INFO] Table {DATASET_ID}.{TABLE_ID} created.")

def get_existing_unique_keys():
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    try:
        query = f"SELECT unique_key FROM `{table_ref}`"
        df = bq_client.query(query).to_dataframe()
        keys = set(df['unique_key'].astype(str).tolist())
        print(f"[INFO] Retrieved {len(keys)} existing keys from BigQuery.")
        return keys
    except Exception as e:
        print(f"[WARN] Could not fetch existing keys: {e}")
        return set()

def get_mapping_dict():
    try:
        df = bq_client.query(f"SELECT SearchAppearance, Enhancement_Name FROM `{MAPPING_TABLE}`").to_dataframe()
        df['enh_norm'] = df['Enhancement_Name'].astype(str).str.strip().str.lower()
        return dict(zip(df['enh_norm'], df['SearchAppearance']))
    except Exception:
        return {}

# =================================================
# BLOCK 8: Upload (fixed + pyarrow-safe dates)
# =================================================
def upload_to_bq(df):
    if df is None or df.empty:
        print("[INFO] No new rows to upload.")
        return
    table_ref = bq_client.dataset(DATASET_ID).table(TABLE_ID)
    try:
        table = bq_client.get_table(table_ref)
        allowed_cols = [f.name for f in table.schema]
    except Exception:
        allowed_cols = FINAL_COLUMNS

    df = df.copy()
    for c in allowed_cols:
        if c not in df.columns:
            df[c] = None
    df = df[allowed_cols]

    for dcol in ['date', 'last_crawled', 'fetch_date']:
        if dcol not in df.columns:
            df[dcol] = pd.NaT
    for dcol in ['date', 'last_crawled', 'fetch_date']:
        df[dcol] = pd.to_datetime(df[dcol], errors='coerce')
    for dcol in ['date', 'last_crawled', 'fetch_date']:
        df[dcol] = df[dcol].dt.strftime("%Y-%m-%d")
    df = df.where(pd.notnull(df), None)

    if DEBUG_MODE:
        out_fn = f"gsc_enhancements_upload_preview_{FETCH_ID}.csv"
        df.to_csv(out_fn, index=False)
        print(f"[DEBUG] Wrote preview CSV: {out_fn} (rows: {len(df)})")
        return

    try:
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        job = bq_client.load_table_from_dataframe(df, f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}", job_config=job_config)
        job.result()
        print(f"[INFO] Inserted {len(df)} rows to {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}.")
    except Exception as e:
        print(f"[ERROR] Failed to insert rows: {e}")

# =================================================
# BLOCK 9: MAIN
# =================================================
def main():
    ensure_table_exists()
    existing_keys = get_existing_unique_keys()
    mapping_dict = get_mapping_dict()
    all_new = []

    if not os.path.isdir(enhancement_folder):
        print(f"[ERROR] Local path '{enhancement_folder}' not found. Exiting.")
        return

    for fname in sorted(os.listdir(enhancement_folder)):
        if not fname.lower().endswith(".xlsx"):
            continue
        file_path = os.path.join(enhancement_folder, fname)
        meta = parse_filename_metadata(fname)
        site = meta.get("site") or ""
        enhancement_name = meta.get("enhancement_name") or ""
        date_val = meta.get("date")
        status_hint = meta.get("status_hint")
        source_file = meta.get("source_file")

        print(f"[INFO] Processing file: {fname} -> enhancement='{enhancement_name}', date={date_val}, status_hint={status_hint}")

        details_df, metrics_df = parse_excel_file(file_path)
        if details_df.empty and metrics_df.empty:
            print(f"[INFO] No data found in {fname} — skipping.")
            continue

        if not details_df.empty:
            if 'url' not in details_df.columns or details_df['url'].isna().all():
                for col in ['URL', 'Page', 'page']:
                    if col in details_df.columns:
                        details_df['url'] = details_df[col]
                        break
                else:
                    details_df['url'] = None
        else:
            details_df['url'] = None
        details_df = details_df[details_df['url'].notna()] if not details_df.empty else pd.DataFrame()

        details_df['site'] = site
        details_df['date'] = date_val
        details_df['enhancement_name'] = enhancement_name
        details_df['status'] = details_df.get('status').fillna(status_hint) if 'status' in details_df.columns else status_hint
        details_df['fetch_id'] = FETCH_ID
        details_df['fetch_date'] = FETCH_DATE
        details_df['source_file'] = source_file
        details_df['last_crawled'] = details_df.get('last_crawled')

        # =================================================
        # BLOCK 9.1: Merge metrics safely (Rev.34)
        # =================================================
        if not metrics_df.empty and not details_df.empty:
            metrics_df.columns = metrics_df.columns.str.strip().str.lower()
            for mcol in ['impressions','clicks','ctr','position']:
                if mcol in metrics_df.columns:
                    details_df[mcol] = metrics_df[mcol]
                else:
                    details_df[mcol] = None

        # unique key
        details_df['unique_key'] = build_unique_key_series(details_df, site, enhancement_name, date_val)
        # remove duplicates
        details_df = details_df.drop_duplicates(subset=['unique_key']).reset_index(drop=True)
        all_new.append(details_df)

    final_df = pd.concat(all_new, ignore_index=True) if all_new else pd.DataFrame()
    if not final_df.empty:
        final_df = final_df.drop_duplicates(subset=['unique_key']).reset_index(drop=True)
    upload_to_bq(final_df)

if __name__ == "__main__":
    main()
