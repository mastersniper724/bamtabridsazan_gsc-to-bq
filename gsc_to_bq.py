# =================================================
# FILE: gsc-to-bq_run_debug_compare.py
# REV: 1
# PURPOSE: Compare keys across multiple runs to detect real duplicates
# =================================================

import hashlib
from datetime import datetime
import pandas as pd

# فرض نمونه داده‌ها (اینجا باید داده‌های GSC شما read شوند)
# برای تست می‌توانید df1 و df2 را از دو run مختلف شبیه‌سازی کنید
# df1 = pd.read_csv('run1_sample.csv')
# df2 = pd.read_csv('run2_sample.csv')

def stable_key(row):
    """
    Generate deterministic hash key for a row
    """
    # ۱. Normalize fields
    query = row.get('Query', '').strip()
    page = row.get('Page', '').strip().rstrip('/')  # remove trailing slash
    date_raw = row.get('Date')
    
    # ۲. Normalize Date
    if isinstance(date_raw, str):
        date = date_raw[:10]
    elif isinstance(date_raw, datetime):
        date = date_raw.strftime("%Y-%m-%d")
    else:
        date = str(date_raw)[:10]

    # ۳. Deterministic tuple & hash
    det_tuple = (query, page, date)
    s = "|".join(det_tuple)
    return hashlib.sha256(s.encode('utf-8')).hexdigest()

# =================================================
# DEBUG COMPARISON FUNCTION
# =================================================
def compare_runs(run1_df, run2_df):
    """
    Compare two runs and print duplicates between them
    """
    print(f"[INFO] Run1 rows: {len(run1_df)}, Run2 rows: {len(run2_df)}")

    # Generate stable keys if not already present
    if 'unique_key' not in run1_df.columns:
        run1_df['unique_key'] = run1_df.apply(stable_key, axis=1)
    if 'unique_key' not in run2_df.columns:
        run2_df['unique_key'] = run2_df.apply(stable_key, axis=1)

    # Compare keys
    keys_run1 = set(run1_df['unique_key'])
    keys_run2 = set(run2_df['unique_key'])

    duplicates = keys_run1 & keys_run2
    new_in_run2 = keys_run2 - keys_run1

    print(f"[INFO] Duplicates between runs: {len(duplicates)}")
    for k in list(duplicates)[:5]:
        print(f"  DUPLICATE KEY: {k}")

    print(f"[INFO] New keys in Run2: {len(new_in_run2)}")
    for k in list(new_in_run2)[:5]:
        print(f"  NEW KEY: {k}")

    return duplicates, new_in_run2

# =================================================
# BATCH DEBUG (مثلاً ۳ batch)
# =================================================
def process_batches(df, batch_size=5):
    all_keys = set()
    num_rows = len(df)
    for i in range(0, num_rows, batch_size):
        batch = df.iloc[i:i+batch_size]
        print(f"\n[DEBUG] Processing batch {i//batch_size + 1}")
        for _, row in batch.iterrows():
            key = stable_key(row)
            print(f"[DEBUG] RAW: {row}, GENERATED KEY: {key}")
            all_keys.add(key)
    return all_keys

# =================================================
# مثال تست با داده‌های شبیه‌سازی شده
# =================================================
if __name__ == "__main__":
    # اینجا باید df1 و df2 واقعی شما جایگزین شود
    df1 = pd.DataFrame([
        {'Query': 'سردخانه', 'Page': 'https://bamtabridsazan.com/product/cold-storage/', 'Date': '2025-08-27'},
        {'Query': 'طراحی تهویه مطبوع', 'Page': 'https://bamtabridsazan.com/comprehensive-hvac-systems-guide/', 'Date': '2025-06-15'},
    ])
    df2 = pd.DataFrame([
        {'Query': 'سردخانه', 'Page': 'https://bamtabridsazan.com/product/cold-storage/', 'Date': '2025-08-27'},
        {'Query': 'فروش هواساز صنعتی', 'Page': 'https://bamtabridsazan.com/product/air-conditioning-and-air-handling-unit/', 'Date': '2025-06-15'},
    ])

    # ۱. پردازش batch-wise
    print("\n=== Processing Run1 Batches ===")
    keys_run1 = process_batches(df1, batch_size=5)
    print("\n=== Processing Run2 Batches ===")
    keys_run2 = process_batches(df2, batch_size=5)

    # ۲. مقایسه
    compare_runs(df1, df2)
