
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ============================================================
# File: upload_gsc_enhancements.py
# Revision: Rev.4 — read existing_keys once; Duplicate-check enforced for Blocks B & C; logs improved
# Purpose: Full fetch from GSC -> BigQuery with duplicate prevention and sitewide total batch
# ============================================================

import os
import re
import pandas as pd
from google.cloud import bigquery

# ===============================
# تنظیمات اصلی
# ===============================
PROJECT_ID = "bamtabridsazan"
DATASET_ID = "seo_reports"
TABLE_ID = "bamtabridsazan__gsc__raw_enhancements"
GITHUB_LOCAL_PATH = "gsc_enhancements"  # مسیر ریشه فولدر
UNIQUE_KEY_COLUMNS = ["url", "item_name", "last_crawled", "enhancement_type"]

client = bigquery.Client(project=PROJECT_ID)

# ===============================
# تابع استخراج داده از فایل اکسل
# ===============================
def parse_excel_file(file_path, enhancement_type):
    try:
        xls = pd.ExcelFile(file_path)
        sheets = xls.sheet_names

        chart_df = None
        table_df = None
        metadata_df = None

        if "Chart" in sheets:
            chart_df = pd.read_excel(xls, "Chart")
        if "Table sheet" in sheets:
            table_df = pd.read_excel(xls, "Table sheet")
        if "Metadata" in sheets:
            metadata_df = pd.read_excel(xls, "Metadata")

        # اضافه کردن ستون Enhancement Type
        if chart_df is not None:
            chart_df["enhancement_type"] = enhancement_type
        if table_df is not None:
            table_df["enhancement_type"] = enhancement_type
        if metadata_df is not None:
            metadata_df["enhancement_type"] = enhancement_type

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
    """Create the target BigQuery table if it doesn't exist."""
    from google.cloud.bigquery import SchemaField

    dataset_id = "seo_reports"
    table_id = "bamtabridsazan__gsc__raw_enhancements"
    table_ref = client.dataset(dataset_id).table(table_id)

    try:
        client.get_table(table_ref)
        print(f"✅ Table {dataset_id}.{table_id} already exists.")
    except Exception:
        print(f"⚙️ Table {dataset_id}.{table_id} not found. Creating...")
        schema = [
            SchemaField("site", "STRING"),
            SchemaField("appearance_type", "STRING"),
            SchemaField("status", "STRING"),
            SchemaField("checked_date", "DATE"),
            SchemaField("unique_key", "STRING"),
        ]
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)
        print(f"✅ Table {dataset_id}.{table_id} created successfully.")

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
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    job = client.load_table_from_dataframe(df, f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}", job_config=job_config)
    job.result()
    print(f"✅ Uploaded {len(df)} new rows")

# ===============================
# تابع اصلی
# ===============================
ensure_table_exists()
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
            enhancement_type = enhancement_folder  # نام فولدر

            print(f"📄 Processing {file_path}")

            chart_df, table_df, metadata_df = parse_excel_file(file_path, enhancement_type)

            # هر فایل ممکن است چند شیت داشته باشد
            for df in [chart_df, table_df, metadata_df]:
                if df is None or df.empty:
                    continue

                # نرمال‌سازی نام ستون‌ها
                df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

                # اطمینان از وجود ستون‌های لازم
                for col in ["url", "item_name", "last_crawled"]:
                    if col not in df.columns:
                        df[col] = None

                df = create_unique_key(df, ["url", "item_name", "last_crawled", "enhancement_type"])

                # فیلتر رکوردهای جدید
                new_df = df[~df["unique_key"].isin(existing_keys)]
                existing_keys.update(new_df["unique_key"].tolist())

                if not new_df.empty:
                    all_new_records.append(new_df)

    # آپلود نهایی
    if all_new_records:
        final_df = pd.concat(all_new_records, ignore_index=True)
        append_to_bigquery(final_df)
    else:
        print("⚠️ No new records found.")

if __name__ == "__main__":
    main()
