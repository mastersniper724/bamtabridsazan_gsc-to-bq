from google.oauth2 import service_account
from googleapiclient.discovery import build
from google.cloud import bigquery
import pandas as pd
from datetime import datetime, timedelta
import hashlib
import time
import os
import json

# ---------- CONFIG ----------
SITE_URL = 'https://bamtabridsazan.com/'
BQ_PROJECT = 'bamtabridsazan'
BQ_DATASET = 'seo_reports'
BQ_TABLE = 'raw_gsc_data'
ROW_LIMIT = 25000
START_DATE = (datetime.utcnow() - timedelta(days=480)).strftime('%Y-%m-%d') # 16 months ago
END_DATE = (datetime.utcnow() - timedelta(days=1)).strftime('%Y-%m-%d')      # yesterday
RETRY_DELAY = 60  # seconds in case of timeout

# ---------- CREDENTIALS ----------
sa_info = json.loads(os.environ["BAMTABRIDSAZAN_GCP_SA_KEY"])
credentials = service_account.Credentials.from_service_account_info(sa_info)

# ---------- GSC SERVICE ----------
service = build('searchconsole', 'v1', credentials=credentials)

# ---------- BIGQUERY CLIENT ----------
bq_client = bigquery.Client(credentials=credentials, project=credentials.project_id)
table_ref = bq_client.dataset(BQ_DATASET).table(BQ_TABLE)

# ---------- ENSURE TABLE EXISTS ----------
def ensure_table():
    try:
        bq_client.get_table(table_ref)
        print(f"[INFO] Table {BQ_TABLE} exists.")
    except:
        print(f"[INFO] Table {BQ_TABLE} not found. Creating...")
        schema = [
            bigquery.SchemaField("Date", "DATE"),
            bigquery.SchemaField("Query", "STRING"),
            bigquery.SchemaField("Page", "STRING"),
            bigquery.SchemaField("Clicks", "INTEGER"),
            bigquery.SchemaField("Impressions", "INTEGER"),
            bigquery.SchemaField("CTR", "FLOAT"),
            bigquery.SchemaField("Position", "FLOAT"),
            bigquery.SchemaField("unique_key", "STRING"),
        ]
        table = bigquery.Table(table_ref, schema=schema)
        bq_client.create_table(table)
        print(f"[INFO] Table {BQ_TABLE} created.")

# ---------- HELPER: create unique key ----------
def generate_key(date, query, page):
    return hashlib.md5(f"{date}||{query}||{page}".encode()).hexdigest()

# ---------- FETCH EXISTING KEYS FROM BIGQUERY ----------
def get_existing_keys():
    try:
        query = f"SELECT unique_key FROM `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}`"
        df = bq_client.query(query).to_dataframe()
        print(f"[INFO] Retrieved {len(df)} existing keys from BigQuery.")
        return set(df['unique_key'].astype(str).tolist())
    except Exception as e:
        print(f"[WARN] Failed to fetch existing keys: {e}")
        return set()

# ---------- FETCH GSC DATA ----------
def fetch_gsc_data(start_date, end_date):
    all_rows = []
    start_row = 0
    existing_keys = get_existing_keys()

    while True:
        request = {
            'startDate': start_date,
            'endDate': end_date,
            'dimensions': ['date','query','page'],
            'rowLimit': ROW_LIMIT,
            'startRow': start_row
        }

        try:
            resp = service.searchanalytics().query(siteUrl=SITE_URL, body=request).execute()
        except Exception as e:
            print(f"[ERROR] Timeout or error: {e}, retrying in {RETRY_DELAY} sec...")
            time.sleep(RETRY_DELAY)
            continue

        rows = resp.get('rows', [])
        if not rows:
            print("[INFO] No more rows returned from GSC.")
            break

        batch_count = 0
        for r in rows:
            date = r['keys'][0]
            query_text = r['keys'][1]
            page = r['keys'][2]
            clicks = r.get('clicks',0)
            impressions = r.get('impressions',0)
            ctr = r.get('ctr',0)
            position = r.get('position',0)
            key = generate_key(date, query_text, page)
            if key not in existing_keys:
                existing_keys.add(key)
                all_rows.append([date, query_text, page, clicks, impressions, ctr, position, key])
                batch_count += 1

        print(f"[INFO] Fetched batch: {len(rows)} rows, {batch_count} new rows to insert.")
        if len(rows) < ROW_LIMIT:
            break
        start_row += len(rows)

    return pd.DataFrame(all_rows, columns=['Date','Query','Page','Clicks','Impressions','CTR','Position','unique_key'])

# ---------- UPLOAD TO BIGQUERY ----------
def upload_to_bq(df):
    if df.empty:
        print("[INFO] No new rows to insert.")
        return
    try:
        job = bq_client.load_table_from_dataframe(df, table_ref)
        job.result()
        print(f"[INFO] {len(df)} rows inserted to BigQuery.")
    except Exception as e:
        print(f"[ERROR] Failed to insert rows: {e}")

# ---------- MAIN ----------
if __name__ == "__main__":
    ensure_table()
    df = fetch_gsc_data(START_DATE, END_DATE)
    upload_to_bq(df)
