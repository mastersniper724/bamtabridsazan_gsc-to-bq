# gsc-to-bq-debug-batches.py
import sys
import os
from gsc_to_bq import fetch_gsc_data  # تابع اصلی از فایل gsc_to_bq.py
import datetime
import pandas as pd
import ast

def main():
    # ---------- تنظیمات تست ----------
    batch_size = 25000
    total_batches = 3

    start_date = "2025-09-01"
    end_date = "2025-09-30"

    print(f"Running debug with {total_batches} batches of size {batch_size}")
    print(f"Date range: {start_date} to {end_date}")

    all_batches = []

    debug_file = "output_debug.txt"
    with open(debug_file, "w", encoding="utf-8") as f:

        for i in range(total_batches):
            print(f"Fetching batch {i+1}...")
            batch = fetch_gsc_data(start_date=start_date, end_date=end_date)
            print(f"Fetched {len(batch)} rows in batch {i+1}")

            # اگر batch شامل رشته است (مثل لیست repr شده دیکشنری‌ها)
            if isinstance(batch, list) and all(isinstance(r, str) for r in batch):
                batch = [ast.literal_eval(r) for r in batch]

            # تبدیل به DataFrame و اضافه کردن batch_id
            df_batch = pd.DataFrame(batch)
            df_batch['batch_id'] = i + 1

            all_batches.append(df_batch)

            # نوشتن log
            for row in batch:
                f.write(str(row) + "\n")

        # جمع کل ردیف‌ها
        df_all = pd.concat(all_batches, ignore_index=True)
        print(f"Total rows fetched: {len(df_all)}")
        f.write(f"\nTotal rows fetched: {len(df_all)}\n")

    # ---------- بررسی duplicate و ساخت unique_key جدید ----------
    if not df_all.empty:
        # بررسی duplicateهای فعلی بر اساس Query, Page, Date
        duplicates = df_all[df_all.duplicated(subset=["Query", "Page", "Date"], keep=False)]
        print(f"Found {len(duplicates)} duplicates")
        duplicates.to_csv("duplicates_report.csv", index=False)

        # ساخت unique_key جدید با اضافه کردن batch_id
        df_all["unique_key_new"] = df_all["Query"] + "|" + df_all["Page"] + "|" + df_all["Date"].astype(str) + "|" + df_all["batch_id"].astype(str)

        # بررسی duplicate بعد از اصلاح
        duplicates_after = df_all[df_all.duplicated(subset=["unique_key_new"], keep=False)]
        print(f"Duplicates after unique_key fix: {len(duplicates_after)}")

        # ذخیره CSV نهایی
        df_all.to_csv("output_debug.csv", index=False)
        print("Debug CSV with unique_key_new saved as output_debug.csv")

    print(f"Debug output saved to {debug_file}")

if __name__ == "__main__":
    main()
