#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import logging
from datetime import datetime
import pandas as pd
from google.cloud import bigquery
from googleapiclient.discovery import build
from google.oauth2 import service_account
import time

# ===========================
# Config
# ===========================
SCOPES = ['https://www.googleapis.com/auth/webmasters.readonly']
GSC_SITE_URL = 'https://bamtabridsazan.com/'

# ===========================
# Logging setup
# ===========================
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

# ===========================
# GSC client
# ===========================
def get_gsc_service():
    credentials = service_account.Credentials.from_service_account_file(
        'gcp-key.json', scopes=SCOPES)
    service = build('webmasters', 'v3', credentials=credentials, cache_discovery=False)
    return service

# ===========================
# Fetch GSC data
# ===========================
def fetch_gsc_data(start_date, end_date):
    service = get_gsc_service()
    all_rows = []

    batches = [
        ['date', 'query', 'page'],
        ['date', 'query', 'country'],
        ['date', 'query', 'device'],
        # searchAppearance removed
    ]

    for dims in batches:
        logging.info(f"Batch {batches.index(dims)+1}, dims {dims}: fetching data...")
        # Simple mock loop for pagination
        rows = fetch_batch(service, dims, start_date, end_date)
        logging.info(f"Batch {batches.index(dims)+1}, dims {dims}: fetched {len(rows)} rows")
        all_rows.extend(rows)
    df = pd.DataFrame(all_rows)
    return df

def fetch_batch(service, dimensions, start_date, end_date):
    request = {
        'startDate': start_date,
        'endDate': end_date,
        'dimensions': dimensions,
        'rowLimit': 25000
    }
    max_retries = 5
    for attempt in range(max_retries):
        try:
            response = service.searchanalytics().query(siteUrl=GSC_SITE_URL, body=request).execute()
            rows = []
            if 'rows' in response:
                for r in response['rows']:
                    row_data = {
                        'Date': r['keys'][0] if len(r['keys']) > 0 else None,
                        'Query': r['keys'][1] if 'query' in dimensions and len(r['keys']) > 1 else None,
                        'Page': r['keys'][2] if 'page' in dimensions and len(r['keys']) > 2 else None,
                        'Country': r['keys'][2] if 'country' in dimensions and len(r['keys']) > 2 else None,
                        'Device': r['keys'][2] if 'device' in dimensions and len(r['keys']) > 2 else None,
                        'Clicks': r.get('clicks', 0),
                        'Impressions': r.get('impressions', 0),
                        'CTR': r.get('ctr', 0),
                        'Position': r.get('position', 0),
                        'unique_key': '_'.join([str(k) for k in r.get('keys', [])])
                    }
                    rows.append(row_data)
            return rows
        except Exception as e:
            logging.error(f"Error fetching batch {dimensions}: {e}, retrying in 60 sec...")
            time.sleep(60)
    return []

# ===========================
# Upload to BigQuery (Batch Load)
# ===========================
def upload_to_bq(df):
    client = bigquery.Client()
    table_id = "bamtabridsazan.seo_reports.bamtabridsazan__gsc__raw_data_fullfetch"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND"
    )
    # Save to CSV and load
    csv_file = "gsc_temp.csv"
    df.to_csv(csv_file, index=False)
    with open(csv_file, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)
    job.result()
    logging.info(f"Inserted {len(df)} rows to BigQuery (Batch Load)")

# ===========================
# Main
# ===========================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--csv-test", default=None)
    args = parser.parse_args()

    start_date = args.start_date
    end_date = args.end_date

    logging.info(f"Fetching data from {start_date} to {end_date}")
    df = fetch_gsc_data(start_date, end_date)
    if args.csv_test:
        df.to_csv(args.csv_test, index=False)
        logging.info(f"CSV test output written: {args.csv_test}")
    upload_to_bq(df)
    logging.info(f"Finished fetching all data. Total rows: {len(df)}")

if __name__ == "__main__":
    main()
