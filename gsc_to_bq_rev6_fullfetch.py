#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import pandas as pd
from google.cloud import bigquery
from gsc_fetcher import fetch_gsc_data_batch  # فرض: ماژول داخلی برای fetch

# ====================================================
# پارامترهای ورودی
# ====================================================
parser = argparse.ArgumentParser(description="Fetch GSC data and load to BigQuery (Rev6.5 without SearchAppearance)")
parser.add_argument("--start-date", required=True, help="Start date YYYY-MM-DD")
parser.add_argument("--end-date", required=True, help="End date YYYY-MM-DD")
parser.add_argument("--csv-test", help="Optional CSV test output")
args = parser.parse_args()

START_DATE = args.start_date
END_DATE = args.end_date
CSV_TEST_FILE = args.csv_test

# ====================================================
# اتصال به BigQuery
# ====================================================
client = bigquery.Client()
TABLE_ID = "bamtabridsazan.seo_reports.bamtabridsazan__gsc__raw_data_fullfetch"

# ====================================================
# بارگذاری دیتا به BigQuery (Batch Load)
# ====================================================
def upload_to_bq(df: pd.DataFrame):
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True,
        clustering_fields=["query", "page"],  # کلاسترینگ حفظ شد
    )

    with pd.io.common.StringIO() as csv_buffer:
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        job = client.load_table_from_file(csv_buffer, TABLE_ID, job_config=job_config)
        job.result()  # منتظر تکمیل Job
        print(f"[INFO] Inserted {len(df)} rows to BigQuery (Batch Load).")

# ====================================================
# اجرای batchهای مختلف بدون searchAppearance
# ====================================================
def main():
    print(f"[INFO] Fetching data from {START_DATE} to {END_DATE}")

    # Batch 1: date + query + page
    df1 = fetch_gsc_data_batch(START_DATE, END_DATE, dimensions=["date", "query", "page"])
    print(f"[INFO] Batch 1, dims ['date', 'query', 'page']: fetched {len(df1)} rows")
    upload_to_bq(df1)

    # Batch 2: date + query + country
    df2 = fetch_gsc_data_batch(START_DATE, END_DATE, dimensions=["date", "query", "country"])
    print(f"[INFO] Batch 2, dims ['date', 'query', 'country']: fetched {len(df2)} rows")
    upload_to_bq(df2)

    # Batch 3: date + query + device
    df3 = fetch_gsc_data_batch(START_DATE, END_DATE, dimensions=["date", "query", "device"])
    print(f"[INFO] Batch 3, dims ['date', 'query', 'device']: fetched {len(df3)} rows")
    upload_to_bq(df3)

    # CSV test output (در صورت نیاز)
    if CSV_TEST_FILE:
        df_all = pd.concat([df1, df2, df3], ignore_index=True)
        df_all.to_csv(CSV_TEST_FILE, index=False)
        print(f"[INFO] CSV test output written: {CSV_TEST_FILE}")

    print(f"[INFO] Finished fetching all data. Total rows: {len(df1)+len(df2)+len(df3)}")

# ====================================================
if __name__ == "__main__":
    main()
