#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ============================================================
# File: upload_gsc_enhancements.py
# Revision: Rev.20 — Normalize filenames, sheets, columns; debug fix; retain all v19 logic.
# Purpose: Parse GSC Enhancement XLSX exports (placed in gsc_enhancements/),
#          build per-URL raw enhancements table and load to BigQuery with dedupe.
# ============================================================

import os
import re
import argparse
import hashlib
from datetime import datetime, date
from uuid import uuid4

import pandas as pd
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
    "unique_key"
]
DEBUG_MODE = False  # حالت پیش‌فرض

# =================================================
# BLOCK 2: ARGPARSE (optional debug / path override)
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
# BLOCK 4: PARSING HELPERS (filename -> metadata)
# =================================================
def parse_filename_metadata(filename):
    """
    Parse site, enhancement_name, date, status_hint from filename.
    """
    base = os.path.basename(filename).strip()
    name = re.sub(r'\.xlsx$', '', base, flags=re.IGNORECASE)
    # normalize name
    name = re.sub(r'\s+', ' ', name).strip()
    m = re.search(r'^(?P<prefix>.+)-(?P<date>\d{4}-\d{2}-\d{2})$', name)
    if not m:
        return {
            "site_raw": None,
            "site": None,
            "enhancement_name": None,
            "date": None,
            "status_hint": None,
            "source_file": base
        }
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

    return {
        "site_raw": site_raw,
        "site": site,
        "enhancement_name": enhancement_name,
        "date": dt,
        "status_hint": status_hint,
        "source_file": base
    }

# =================================================
# BLOCK 5: EXCEL PARSE (Details extraction)
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

def parse_excel_file(file_path, enhancement_name_hint=None):
    """
    Read an .xlsx and return a DataFrame (details_df) that contains all per-URL rows.
    """
    try:
        xls = pd.ExcelFile(file_path)
    except Exception as e:
        print(f"[WARN] Cannot open {file_path}: {e}")
        return pd.DataFrame()

    details_frames = []

    # normalize sheet names
    normalized_sheets = [s.strip().lower() for s in xls.sheet_names]

    for sheet in xls.sheet_names:
        try:
            df = pd.read_excel(xls, sheet_name=sheet)
        except Exception as e:
            print(f"[WARN] failed read sheet {sheet} in {file_path}: {e}")
            continue
        if df is None or df.empty:
            continue

        df_cols_norm = _normalize_columns(df.columns.tolist())
        rename_map = {orig: norm for orig, norm in zip(df.columns.tolist(), df_cols_norm)}
        df = df.rename(columns=rename_map)

        # detect Details-like sheet by URL/page column
        if any(c in df.columns for c in ("url","page","link","page_url")):
            for c in ("url","page","item_name","item","issue","issue_name","last_crawled","status"):
                if c not in df.columns:
                    df[c] = None
            if "url" not in df.columns and "page" in df.columns:
                df["url"] = df["page"]
            if "item" in df.columns and "item_name" not in df.columns:
                df["item_name"] = df["item"]
            if "issue" not in df.columns and "issue_name" in df.columns:
                df["issue"] = df["issue_name"]
            last_candidates = [c for c in df.columns if "last" in c and "crawl" in c]
            if last_candidates and "last_crawled" not in df.columns:
                df["last_crawled"] = df[last_candidates[0]]
            details_frames.append(df)

    if details_frames:
        details_df = pd.concat(details_frames, ignore_index=True)
        details_df['url'] = details_df['url'].astype(str).str.strip().replace({'nan': None})
        for c in ['item_name','issue']:
            if c in details_df.columns:
                details_df[c] = details_df[c].astype(object).where(~details_df[c].isna(), None)
        if 'last_crawled' in details_df.columns:
            details_df['last_crawled'] = pd.to_datetime(details_df['last_crawled'], errors='coerce').dt.date
        return details_df
    else:
        return pd.DataFrame()

# =================================================
# BLOCK 6: Unique key (SHA-256) builder
# =================================================
def build_unique_key_series(df, site, enhancement_name, date_val, status_col='status'):
    def row_key(r):
        page = r.get('url') or ""
        status = r.get(status_col) or ""
        lastc = r.get('last_crawled')
        if isinstance(lastc, (datetime,)):
            lastc = lastc.date()
        lastc_str = str(lastc) if lastc else ""
        date_str = str(date_val) if date_val else ""
        raw = f"{site}|{enhancement_name}|{page}|{status}|{lastc_str}|{date_str}"
        return hashlib.sha256(raw.encode('utf-8')).hexdigest()
    return df.apply(row_key, axis=1)

# =================================================
# BLOCK 7: BigQuery helpers
# =================================================
def ensure_table_exists():
    table_ref = bq_client.dataset(DATASET_ID).table(TABLE_ID)
    try:
        bq_client.get_table(table_ref)
        print(f"[INFO] Table {DATASET_ID}.{TABLE_ID} exists.")
    except Exception:
        print(f"[INFO] Table {DATASET_ID}.{TABLE_ID} not found. Creating...")
        schema = [SchemaField(c, "STRING") if c not in ('date','last_crawled','fetch_date') else SchemaField(c,'DATE') for c in FINAL_COLUMNS]
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
        mapd = dict(zip(df['enh_norm'], df['SearchAppearance']))
        return mapd
    except Exception:
        return {}

# =================================================
# BLOCK 8: Upload to BQ
# =================================================
def upload_to_bq(df):
    if df.empty:
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

    for date_col in ['date','last_crawled','fetch_date']:
        if date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce').dt.date

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

        print(f"[INFO] Processing file: {fname}  -> enhancement='{enhancement_name}', date={date_val}, status_hint={status_hint}")

        details_df = parse_excel_file(file_path, enhancement_name_hint=enhancement_name)
        if details_df.empty:
            print(f"[INFO] No detail rows found in {fname} — skipping per-URL import.")
            continue

        # normalize columns already handled in parse_excel_file
        details_df['url'] = details_df['url'].astype(object).where(~details_df['url'].isna(), None)
        details_df = details_df[details_df['url'].notna() & details_df['url'].astype(str).str.strip().ne('')].copy()
        if details_df.empty:
            print(f"[INFO] After filtering empty URLs, nothing to upload from {fname}.")
            continue

        details_df['site'] = site
        details_df['date'] = date_val
        details_df['enhancement_name'] = enhancement_name
        details_df['status'] = details_df.get('status').fillna(status_hint) if 'status' in details_df.columns else status_hint
        details_df['status'] = details_df['status'].apply(lambda v: (str(v).strip() if v and str(v).strip().lower()!='nan' else None))
        details_df['item_name'] = details_df.get('item_name')
        details_df['issue_name'] = details_df.get('issue')
        details_df['fetch_id'] = FETCH_ID
        details_df['fetch_date'] = FETCH_DATE
        details_df['source_file'] = source_file

        enh_norm = enhancement_name.strip().lower() if enhancement_name else ""
        details_df['appearance_type'] = mapping_dict.get(enh_norm)

        details_df['unique_key'] = build_unique_key_series(details_df, site, enhancement_name, date_val, status_col='status')

        new_df = details_df[~details_df['unique_key'].isin(existing_keys)].copy()
        if new_df.empty:
            print(f"[INFO] All rows from {fname} are duplicates (skipped).")
            continue
        existing_keys.update(new_df['unique_key'].tolist())

        for c in FINAL_COLUMNS:
            if c not in new_df.columns:
                new_df[c] = None
        new_df = new_df[FINAL_COLUMNS]

        all_new.append(new_df)

    if not all_new:
        print("[INFO] No new rows across all files.")
        return

    final_df = pd.concat(all_new, ignore_index=True)
    upload_to_bq(final_df)
    print("[INFO] Finished processing GSC enhancements.")

# =================================================
# BLOCK 10: ENTRYPOINT
# =================================================
if __name__ == "__main__":
    main()
