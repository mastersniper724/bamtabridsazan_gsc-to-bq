import pandas as pd
from google.cloud import bigquery
from datetime import datetime, timedelta
import logging
import argparse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# -----------------------------
# پارامترهای ورودی
# -----------------------------
parser = argparse.ArgumentParser()
parser.add_argument("--start-date", required=True, help="Start date YYYY-MM-DD")
parser.add_argument("--end-date", required=True, help="End date YYYY-MM-DD")
args = parser.parse_args()

START_DATE = args.start_date
END_DATE = args.end_date

# -----------------------------
# کلاینت BigQuery
# -----------------------------
client = bigquery.Client()
table_id = "bamtabridsazan.seo_reports.bamtabridsazan__gsc__raw_data_fullfetch"

# -----------------------------
# تعریف اسکیمای جدول
# -----------------------------
schema = [
    bigquery.SchemaField("Date", "DATE"),
    bigquery.SchemaField("Query", "STRING"),
    bigquery.SchemaField("Page", "STRING"),
    bigquery.SchemaField("Country", "STRING"),
    bigquery.SchemaField("Device", "STRING"),
    bigquery.SchemaField("Clicks", "INTEGER"),
    bigquery.SchemaField("Impressions", "INTEGER"),
    bigquery.SchemaField("CTR", "FLOAT"),
    bigquery.SchemaField("Position", "FLOAT"),
    bigquery.SchemaField("unique_key", "STRING")
]

# -----------------------------
# تابع نمونه برای دریافت داده از GSC
# -----------------------------
def fetch_gsc_data(start_date, end_date):
    """
    تابع نمونه؛ داده‌های واقعی GSC را برمی‌گرداند.
    در این نسخه، searchAppearance حذف شده است.
    """
    # نمونه داده شبیه‌سازی شده
    data = [
        {
            "Date": start_date,
            "Query": "hvac",
            "Page": "/services/hvac",
            "Country": "US",
            "Device": "Desktop",
            "Clicks": 10,
            "Impressions": 100,
            "CTR": 0.1,
            "Position": 5.0,
            "unique_key": f"{start_date}_hvac_/services/hvac"
        },
        # می‌توانید داده‌های بیشتری اضافه کنید
    ]
    return pd.DataFrame(data)

# -----------------------------
# آپلود به BigQuery با DataFrame و Clustering
# -----------------------------
def upload_to_bq(df):
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition="WRITE_APPEND",
        clustering_fields=["Query", "Page"]
    )

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # منتظر تمام شدن job
    logger.info(f"Inserted {len(df)} rows to BigQuery (Clustered on Query + Page).")

# -----------------------------
# Main
# -----------------------------
def main():
    logger.info(f"Fetching data from {START_DATE} to {END_DATE}")
    df = fetch_gsc_data(START_DATE, END_DATE)
    
    if df.empty:
        logger.info("No data fetched.")
        return

    # تبدیل ستون Date به datetime
    df["Date"] = pd.to_datetime(df["Date"])
    
    upload_to_bq(df)
    logger.info("Finished fetching all data.")

if __name__ == "__main__":
    main()
