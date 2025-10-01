import csv
from gsc_to_bq import fetch_gsc_data  # فرض می‌کنیم فانکشن اصلی همین باشه

# مسیر ذخیره فایل‌ها
LOG_FILE_1 = "run_debug_batches_1.log"
LOG_FILE_2 = "run_debug_batches_2.log"

def save_debug_log(filename, batches):
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter="\t")
        # Header
        writer.writerow(["batch_id", "query", "page", "date", "unique_key"])
        for batch_id, rows in enumerate(batches, start=1):
            for row in rows:
                writer.writerow([
                    batch_id,
                    row["query"],
                    row["page"],
                    row["date"],
                    row["unique_key"]
                ])
    print(f"✅ Debug log saved to {filename}")

def main():
    # Run اول
    batches_1 = fetch_batches(batch_size=25000, total_batches=3)
    save_debug_log(LOG_FILE_1, batches_1)

    # Run دوم
    batches_2 = fetch_batches(batch_size=25000, total_batches=3)
    save_debug_log(LOG_FILE_2, batches_2)

if __name__ == "__main__":
    main()
