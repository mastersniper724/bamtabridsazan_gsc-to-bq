# gsc-to-bq-debug-batches.py
import sys
import os
from gsc_to_bq import fetch_gsc_data  # تابع اصلی از فایل gsc_to_bq.py
import datetime

def main():
    # ---------- تنظیمات تست ----------
    batch_size = 25000
    total_batches = 3

    # تاریخ‌های ثابت برای تست
    start_date = "2025-09-01"
    end_date = "2025-09-30"

    print(f"Running debug with {total_batches} batches of size {batch_size}")
    print(f"Date range: {start_date} to {end_date}")

    all_batches = []

    debug_file = "output_debug.txt"
    # باز کردن فایل لاگ برای نوشتن داده‌های واقعی
    with open(debug_file, "w", encoding="utf-8") as f:

        for i in range(total_batches):
            print(f"Fetching batch {i+1}...")
            batch = fetch_gsc_data(start_date=start_date, end_date=end_date)  # خط اصلاح‌شده
            print(f"Fetched {len(batch)} rows in batch {i+1}")
            
            all_batches.extend(batch)

            # ذخیره ردیف‌ها در فایل لاگ
            for row in batch:
                f.write(str(row) + "\n")

        # جمع کل ردیف‌ها
        print(f"Total rows fetched: {len(all_batches)}")
        f.write(f"\nTotal rows fetched: {len(all_batches)}\n")

    print(f"Debug output saved to {debug_file}")

if __name__ == "__main__":
    main()
