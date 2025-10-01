# gsc-to-bq-debug-batches.py
import sys
import os
from gsc_to_bq import fetch_gsc_data  # تابع اصلی از فایل gsc_to_bq.py
import datetime
import pandas as pd

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
            
            # اضافه کردن batch_id برای شناسایی هر batch
            for row in batch:
                row['batch_id'] = i + 1

            all_batches.extend(batch)

            for row in batch:
                f.write(str(row) + "\n")

        print(f"Total rows fetched: {len(all_batches)}")
        f.write(f"\nTotal rows fetched: {len(all_batches)}\n")

    # ---------- بررسی duplicate و unique_key جدید ----------
    if all_batches:
        df = pd.DataFrame(all_batches)
        # بررسی duplicateهای فعلی
        duplicates = df[df.duplicated(subset=["Query", "Page", "Date"], keep=False)]
        print(f"Found {len(duplicates)} duplicates")
        duplicates.to_csv("duplicates_report.csv", index=False)
        
        # ساخت unique_key جدید
        df["unique_key_new"] = df["Query"] + "|" + df["Page"] + "|" + df["Date"].astype(str) + "|" + df["batch_id"].astype(str)
        
        # بررسی duplicate بعد از اصلاح
        duplicates_after = df[df.duplicated(subset=["unique_key_new"], keep=False)]
        print(f"Duplicates after unique_key fix: {len(duplicates_after)}")

        # ذخیره DataFrame کامل با unique_key جدید
        df.to_csv("output_debug.csv", index=False)
        print("Debug CSV with unique_key_new saved as output_debug.csv")

    print(f"Debug output saved to {debug_file}")

if __name__ == "__main__":
    main()
