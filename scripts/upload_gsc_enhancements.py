#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ============================================================
# File: upload_gsc_enhancements.py
# Revision: Rev.15 — Chart sheet ignored; parse_excel_file updated; all structure preserved
# Purpose: Full fetch from GSC -> BigQuery with duplicate prevention
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
GITHUB_LOCAL_PATH = "gsc_enhancements"  # مسیر ریشه فولدر
UNIQUE_KEY_COLUMNS = ["url", "item_name", "last_crawled", "enhancement_type"]

client = bigquery.Client(project=PROJECT_ID)

# ===============================
# تابع استخراج داده از فایل اکسل (فقط Table و Metadata)
# ===============================
def parse_excel_file(file_path, enhancement_type, appearance_type):
    try:
        xls = pd.ExcelFile(file_path)

        table_sheet = None
        metadata_sheet = None

        # جستجوی case-insensitive برای شیت‌ها
        for sheet in xls.sheet_names:
            lower = sheet.strip().lower()
            if lower == "table sheet" or lower == "table":
                table_sheet = sheet
            elif lower == "metadata":
                metadata_sheet = sheet

        table_df = pd.read_excel(xls, table_sheet) if table_sheet else pd.DataFrame()
        metadata_df = pd.read_excel(xls, metadata_sheet) if metadata_sheet else pd.DataFrame()

        # اضافه کردن ستون enhancement_type
        if not table_df.empty:
            table_df["enhancement_type"] = enhancement_type
        if not metadata_df.empty:
            metadata_df["enhancement_type"] = enhancement_type

        # پاک‌سازی نام ستون‌ها
        if not table_df.empty:
            table_df.columns = table_df.columns.str.strip()
        if not metadata_df.empty:
            metadata_df.columns = metadata_df.columns.str.strip()

        return table_df, metadata_df

    except Exception as e:
        print(f"❌ Failed to parse {file_path}: {e}")
        return pd.DataFrame(), pd.DataFrame()

# ===============================
# تابع ایجاد Unique Key
# ===============================
def create_unique_key(df, columns):
    df = df.copy()
    df["unique_key"] = df[columns].astype(str).agg("__".join, axis=1)
    return df

# ===============================
# بررسی وجود جدول در BQ / ایجاد آن
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
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.date
    if 'last_crawled' in df.columns:
        df['last_crawled'] = pd.to_datetime(df['last_crawled'], errors='coerce').dt.date

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    job = client.load_table_from_dataframe(df, f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}", job_config=job_config)
    job.result()
    print(f"✅ Uploaded {len(df)} new rows")

# ===============================
# ستون‌های معتبر مطابق BQ
# ===============================
VALID_COLUMNS = ["date", "url", "item_name", "last_crawled", "site", "appearance_type", "status", "unique_key"]

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
            enhancement_type = enhancement_folder
            appearance_type = enhancement_folder  # در صورت نیاز

            print(f"📄 Processing {file_path}")

            table_df, metadata_df = parse_excel_file(file_path, enhancement_type, appearance_type)

            for df in [table_df, metadata_df]:
                if df.empty:
                    continue

                # نرمال‌سازی ستون‌ها
                df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

                # اطمینان از ستون‌های لازم
                for col in ["url", "item_name", "last_crawled"]:
                    if col not in df.columns:
                        df[col] = None

                # ساخت unique_key
                df = create_unique_key(df, ["url", "item_name", "last_crawled", "enhancement_type"])

                # فیلتر رکوردهای جدید
                new_df = df[~df["unique_key"].isin(existing_keys)]
                existing_keys.update(new_df["unique_key"].tolist())

                if not new_df.empty:
                    # فقط ستون‌های معتبر را نگه دار
                    new_df = new_df[[col for col in VALID_COLUMNS if col in new_df.columns]]

                    all_new_records.append(new_df)

    # آپلود نهایی
    if all_new_records:
        final_df = pd.concat(all_new_records, ignore_index=True)
        append_to_bigquery(final_df)
    else:
        print("⚠️ No new records found.")

if __name__ == "__main__":
    main()
