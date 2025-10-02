import argparse
import pandas as pd
from google.cloud import bigquery
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import logging

# تنظیمات لاگ
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

# -----------------------
# پارامترهای ورودی
# -----------------------
parser = argparse.ArgumentParser()
parser.add_argument('--start-date', required=True)
parser.add_argument('--end-date', required=True)
parser.add_argument('--csv-test', default=None)
args = parser.parse_args()

START_DATE = args.start_date
END_DATE = args.end_date
CSV_TEST_FILE = args.csv_test

# -----------------------
# اتصال به BigQuery و GSC
# -----------------------
bq_client = bigquery.Client()
gsc_service = build('searchconsole', 'v1')

PROJECT = 'bamtabridsazan'
DATASET = 'seo_reports'
TABLE = 'bamtabridsazan__gsc__raw_data_fullfetch'

# -----------------------
# تعریف batch ها
# -----------------------
BATCHES = [
    ['date', 'query', 'page'],
    ['date', 'query', 'country'],
    ['date', 'query', 'device'],
    ['date', 'searchAppearance'],  # فقط date + searchAppearance
]

# -----------------------
# توابع کمکی
# -----------------------
def fetch_batch(dims):
    """Fetch data from GSC for a given dimensions batch"""
    logging.info(f"Batch {BATCHES.index(dims)+1}, dims {dims}: fetching data...")
    body = {
        'startDate': START_DATE,
        'endDate': END_DATE,
        'dimensions': dims,
        'rowLimit': 25000
    }
    try:
        response = gsc_service.searchanalytics().query(
            siteUrl='https://bamtabridsazan.com/',
            body=body
        ).execute()
    except HttpError as e:
        logging.error(f"Batch {BATCHES.index(dims)+1} failed: {e}")
        return []

    rows = []
    for r in response.get('rows', []):
        # ساخت mapping ابعاد
        dim_map = {dims[i]: r['keys'][i] for i in range(len(r['keys']))}
        rows.append({
            'Date': r['keys'][0] if 'date' in dims else None,
            'Query': dim_map.get('query'),
            'Page': dim_map.get('page'),
            'Country': dim_map.get('country'),
            'Device': dim_map.get('device'),
            'SearchAppearance': dim_map.get('searchAppearance'),
            'Clicks': r.get('clicks'),
            'Impressions': r.get('impressions'),
            'CTR': r.get('ctr'),
            'Position': r.get('position'),
            'Unique_Key': r.get('keys')  # می‌توانید اصلاح کنید
        })
    logging.info(f"Batch {BATCHES.index(dims)+1}, dims {dims}: fetched {len(rows)} rows")
    return rows

def upload_to_bq(rows):
    """Upload rows to BigQuery"""
    if not rows:
        logging.info("No new rows to insert.")
        return
    df = pd.DataFrame(rows)
    table_ref = bq_client.dataset(DATASET).table(TABLE)
    errors = bq_client.insert_rows_json(table_ref, df.to_dict(orient='records'))
    if errors:
        logging.error(f"BigQuery insert errors: {errors}")
    else:
        logging.info(f"Inserted {len(rows)} rows to BigQuery.")

# -----------------------
# اصلی
# -----------------------
def main():
    all_rows = []
    for dims in BATCHES:
        batch_rows = fetch_batch(dims)
        all_rows.extend(batch_rows)
        upload_to_bq(batch_rows)

    if CSV_TEST_FILE:
        pd.DataFrame(all_rows).to_csv(CSV_TEST_FILE, index=False)
        logging.info(f"CSV test output written: {CSV_TEST_FILE}")

    logging.info(f"Finished fetching all data. Total rows: {len(all_rows)}")

if __name__ == "__main__":
    main()
