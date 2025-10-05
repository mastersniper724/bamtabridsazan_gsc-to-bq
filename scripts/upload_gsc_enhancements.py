#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ============================================================
# File: upload_gsc_enhancements.py
# Revision: Rev.13 — Fixed all column & schema issues
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
# تابع استخراج داده از فایل اکسل (ویرایش ۱۰)
# ===============================
def parse_excel_file(file_path, site, appearance_type):
    """
    Parse Excel file and extract normalized data for BigQuery.
    - Handles case-insensitive sheet names (Chart / Table)
    - Normalizes column names (lowercase, underscores)
    - Ensures all required columns exist
    """

    try:
        # 🔹 باز کردن فایل اکسل و لیست شیت‌ها
        wb = load_workbook(file_path, data_only=True)
        sheet_names = [s.lower() for s in wb.sheetnames]

        # 🔹 پیدا کردن نام دقیق شیت‌ها (case-insensitive)
        chart_sheet_name = None
        table_sheet_name = None
        for s in wb.sheetnames:
            sl = s.lower()
            if "chart" in sl:
                chart_sheet_name = s
            elif "table" in sl:
                table_sheet_name = s

        if not chart_sheet_name and not table_sheet_name:
            raise ValueError("No Chart or Table sheet found in file.")

        records = []

        # 🔹 پردازش شیت Chart (در صورت وجود)
        if chart_sheet_name:
            df_chart = pd.read_excel(file_path, sheet_name=chart_sheet_name)

            # نرمال‌سازی نام ستون‌ها
            df_chart.columns = [str(c).strip().lower().replace(" ", "_") for c in df_chart.columns]

            # اطمینان از وجود ستون date
            if "date" not in df_chart.columns:
                raise ValueError(f"'date' column not found in Chart sheet: {chart_sheet_name}")

            # فقط ستون تاریخ رو نگه می‌داریم
            df_chart = df_chart[["date"]].dropna(subset=["date"])
            df_chart["date"] = pd.to_datetime(df_chart["date"], errors="coerce").dt.date

            chart_dates = df_chart["date"].dropna().unique().tolist()
        else:
            chart_dates = []

        # 🔹 پردازش شیت Table (در صورت وجود)
        if table_sheet_name:
            df_table = pd.read_excel(file_path, sheet_name=table_sheet_name)
            df_table.columns = [str(c).strip().lower().replace(" ", "_") for c in df_table.columns]

            # تعریف ستون‌های موردنیاز
            required_cols = ["url", "item_name", "last_crawled", "status"]
            for col in required_cols:
                if col not in df_table.columns:
                    df_table[col] = None

            df_table["last_crawled"] = pd.to_datetime(df_table["last_crawled"], errors="coerce").dt.date

            # اگر شیت Chart تاریخ دارد، همه سطرها را برای آن تاریخ‌ها تکثیر می‌کنیم
            if chart_dates:
                for d in chart_dates:
                    for _, row in df_table.iterrows():
                        records.append({
                            "date": d,
                            "url": row.get("url"),
                            "item_name": row.get("item_name"),
                            "last_crawled": row.get("last_crawled"),
                            "site": site,
                            "appearance_type": appearance_type,
                            "status": row.get("status"),
                        })
            else:
                # در صورت نبودن chart_dates، فقط داده‌های table را بدون تاریخ می‌فرستیم
                for _, row in df_table.iterrows():
                    records.append({
                        "date": None,
                        "url": row.get("url"),
                        "item_name": row.get("item_name"),
                        "last_crawled": row.get("last_crawled"),
                        "site": site,
                        "appearance_type": appearance_type,
                        "status": row.get("status"),
                    })

        # 🔹 تبدیل به DataFrame نهایی
        if not records:
            raise ValueError("No records extracted from file.")

        df = pd.DataFrame(records)

        # نرمال‌سازی نهایی نام ستون‌ها
        df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]

        # اطمینان از وجود همه ستون‌ها برای اسکیما BQ
        all_cols = ["date", "url", "item_name", "last_crawled", "site", "appearance_type", "status", "unique_key"]
        for col in all_cols:
            if col not in df.columns:
                df[col] = None

        # ساخت unique_key
        df["unique_key"] = df.apply(
            lambda r: f"{r['site']}_{r['appearance_type']}_{r['url']}_{r['item_name']}_{r['date']}", axis=1
        )

        return df

    except Exception as e:
        print(f"❌ Error parsing {file_path}: {e}")
        return pd.DataFrame(columns=["date", "url", "item_name", "last_crawled", "site", "appearance_type", "status", "unique_key"])


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
    enhancement_folder = "enhancements"
    enhancement_files = get_excel_files(enhancement_folder)

    all_new_records = []
    existing_keys = load_existing_unique_keys()

    for file_path in enhancement_files:
        site = "bamtabridsazan.com"   # دامنه سایت
        appearance_type = enhancement_folder  # نوع Enhancement

        df = parse_excel_file(file_path, site, appearance_type)
        if df is None or df.empty:
            continue

        # نرمال‌سازی نام ستون‌ها
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

        # اطمینان از وجود ستون‌های لازم
        for col in ["url", "item_name", "last_crawled"]:
            if col not in df.columns:
                df[col] = None

        # ساخت unique key
        df = create_unique_key(df, ["url", "item_name", "last_crawled", "appearance_type"])

        # فیلتر رکوردهای جدید
        new_df = df[~df["unique_key"].isin(existing_keys)]
        existing_keys.update(new_df["unique_key"].tolist())

        if not new_df.empty:
            all_new_records.append(new_df)

    # ✅ این بخش رو دقیقاً همین‌طوری نگه دار
    if all_new_records:
        final_df = pd.concat(all_new_records, ignore_index=True)
        append_to_bigquery(final_df)
    else:
        print("⚠️ No new records found.")


if __name__ == "__main__":
    main()
