#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ============================================================
# File: upload_gsc_enhancements.py
# Revision: Rev.8 — Fixed all column & schema issues
# ============================================================

import os
import pandas as pd
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField

# ===============================
# تنظیمات اصلی
# ===============================
PROJECT_ID = "bamtabridsazan"
DATASET_ID = "seo_reports"
TABLE_ID = "bamtabridsazan__gsc__raw_enhancements"
GITHUB_LOCAL_PATH = "gsc_enhancements"
UNIQUE_KEY_COLUMNS = ["url", "item_name", "last_crawled", "enhancement_type"]

client = bigquery.Client(project=PROJECT_ID)

# ===============================
# تابع استخراج داده از فایل اکسل
# ===============================
def parse_excel_file(file_path, enhancement_type):
    try:
        xls = pd.ExcelFile(file_path)
        sheets = xls.sheet_names

        chart_df = pd.read_excel(xls, "Chart") if "Chart" in sheets else None
        table_df = pd.read_excel(xls, "Table sheet") if "Table sheet" in sheets else None
        metadata_df = pd.read_excel(xls, "Metadata") if "Metadata" in sheets else None

        for df in [chart_df, table_df, metadata_df]:
            if df is not None:
                df["enhancement_type"] = enhancement_type

        return chart_df, table_df, metadata_df
    except Exception as e:
        print(f"❌ Error parsing {file_path}: {e}")
        return None, None, None

# ===============================
# تابع ایجاد Unique Key
# ===============================
def create_unique_key(df, columns):
    df = df.copy()
    df["unique_key"] = df[columns].astype(str).agg("__".join, axis=1)
    return df

# ===============================
# بررسی وجود جدول در / ایجاد آن BQ
# ===============================
def ensure_table_exists():
    table_ref = client.dataset(DATASET_ID).table(TABLE_ID)
    try:
        client.get_table(table_ref)
        print(f"✅ Table {DATASET_ID}.{TABLE_ID} already exists.")
    except Exception:
        print(f"⚙️ Table {DATASET_ID}.{TABLE_ID} not found. Creating...")
        schema = [
            SchemaField("date", "DATE"),
            SchemaField("url", "STRING"),
            SchemaField("item_name", "STRING"),
            SchemaField("last_crawled", "DATE"),
            SchemaField("site", "STRING"),
            SchemaField("appearance_type", "STRING"),
            SchemaField("status", "STRING"),
            SchemaField("unique_key", "STRING"),
        ]
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)
        print(f"✅ Table {DATASET_ID}.{TABLE_ID} created successfully.")

# ===============================
# دریافت کلیدهای موجود از BQ
# ===============================
def get_existing_unique_keys():
    query = f"SELECT DISTINCT unique_key FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`"
    df = client.query(query).to_dataframe()
    return set(df["unique_key"].tolist())

# ===============================
# آپلود داده جدید به BQ
# ===============================
def append_to_bigquery(df):
    # تبدیل ستون‌های تاریخ به DATE
    for date_col in ["date", "last_crawled"]:
        if date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col], errors="coerce").dt.date

    # مرتب‌سازی و اضافه کردن ستون‌های گمشده مطابق اسکیما
    schema_cols = ["date", "url", "item_name", "last_crawled", "site", "appearance_type", "status", "unique_key"]
    for col in schema_cols:
        if col not in df.columns:
            df[col] = None
    df = df[schema_cols]

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    job = client.load_table_from_dataframe(df, f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}", job_config=job_config)
    job.result()
    print(f"✅ Uploaded {len(df)} new rows")

# ===============================
# تابع اصلی
# ===============================
ensure_table_exists()

VALID_COLUMNS = ["date", "url", "item_name", "last_crawled", "site", "appearance_type", "status", "unique_key"]

def main():
    all_new_records = []
    existing_keys = get_existing_unique_keys()
    print(f"🔹 Loaded {len(existing_keys)} existing unique keys")

    for enhancement_folder in os.listdir(GITHUB_LOCAL_PATH):
        folder_path = os.path.join(GITHUB_LOCAL_PATH, enhancement_folder)
        if not os.path.isdir(folder_path):
            continue

        for file_name in os.listdir(folder_path):
            if not file_name.endswith(".xlsx"):
                continue

            file_path = os.path.join(folder_path, file_name)
            enhancement_type = enhancement_folder
            print(f"📄 Processing {file_path}")

            chart_df, table_df, metadata_df = parse_excel_file(file_path, enhancement_type)

            for df in [chart_df, table_df, metadata_df]:
                if df is None or df.empty:
                    continue

                df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

                for col in ["url", "item_name", "last_crawled", "date", "site", "appearance_type", "status"]:
                    if col not in df.columns:
                        df[col] = None

                df = create_unique_key(df, UNIQUE_KEY_COLUMNS)
                new_df = df[~df["unique_key"].isin(existing_keys)]
                existing_keys.update(new_df["unique_key"].tolist())

                if not new_df.empty:
                    new_df = new_df[[col for col in VALID_COLUMNS if col in new_df.columns]]
                    for date_col in ["date", "last_crawled"]:
                        if date_col in new_df.columns:
                            new_df[date_col] = pd.to_datetime(new_df[date_col], errors="coerce").dt.date
                    all_new_records.append(new_df)

    if all_new_records:
        final_df = pd.concat(all_new_records, ignore_index=True)
        append_to_bigquery(final_df)
    else:
        print("⚠️ No new records found.")

if __name__ == "__main__":
    main()
