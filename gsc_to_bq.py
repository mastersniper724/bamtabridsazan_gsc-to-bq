# =================================================
# FILE: gsc_to_bq.py
# REV: 1
# PURPOSE: Safe idempotent GSC -> BigQuery upload
# NOTES: - deterministic stable_key
#        - robust get_existing_keys (no pyarrow dependency required)
#        - upload only truly new rows
# =================================================

from google.oauth2 import service_account
from googleapiclient.discovery import build
from google.cloud import bigquery
import pandas as pd
from datetime import datetime, timedelta
import hashlib
import time
import os
import json
import sys
import urllib.parse

# ---------- CONFIG ----------
SITE_URL = 'https://bamtabridsazan.com/'
BQ_PROJECT = 'bamtabridsazan'
BQ_DATASET = 'seo_reports'
BQ_TABLE = 'bamtabridsazan__gsc__raw_data'
ROW_LIMIT = 25000
START_DATE = (datetime.utcnow() - timedelta(days=480)).strftime('%Y-%m-%d')  # 16 months ago
END_DATE = (datetime.utcnow() - timedelta(days=1)).strftime('%Y-%m-%d')      # yesterday
RETRY_DELAY = 60  # seconds in case of timeout
MAX_RETRIES = 3

# ---------- CREDENTIALS ----------
service_account_file = os.environ.get("SERVICE_ACCOUNT_FILE", "gcp-key.json")
if not os.path.exists(service_account_file):
    print(f"[ERROR] Service account file not found: {service_account_file}", flush=True)
    sys.exit(1)

with open(service_account_file, "r", encoding="utf-8") as f:
    sa_info = json.load(f)

credentials = service_account.Credentials.from_service_account_info(sa_info)

# Build Search Console service
service = build('searchconsole', 'v1', credentials=credentials)

# ---------- BIGQUERY CLIENT ----------
bq_client = bigquery.Client(credentials=credentials, project=credentials.project_id)
table_ref = bq_client.dataset(BQ_DATASET).table(BQ_TABLE)

# ---------- ENSURE TABLE EXISTS ----------
def ensure_table():
    try:
        bq_client.get_table(table_ref)
        print(f"[INFO] Table {BQ_TABLE} exists.", flush=True)
    except Exception:
        print(f"[INFO] Table {BQ_TABLE} not found. Creating...", flush=True)
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
        # clustering not mandatory but can help
        table.clustering_fields = ["Date", "Query"]
        bq_client.create_table(table)
        print(f"[INFO] Table {BQ_TABLE} created.", flush=True)

# ---------- HELPER: normalize URL (basic) ----------
def normalize_url(url):
    """
    Basic URL normalization:
    - strip, lower
    - remove trailing slash
    - remove fragments
    - keep path+host; drop common UTM query params (but keep meaningful params)
    """
    if not url:
        return ""
    url = url.strip()
    try:
        parsed = urllib.parse.urlparse(url)
        # drop fragment
        scheme = parsed.scheme.lower()
        netloc = parsed.netloc.lower()
        path = parsed.path.rstrip('/')
        # keep query but remove utm_* params
        qs = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
        cleaned_qs = [(k, v) for k, v in qs if not k.startswith('utm_')]
        query = urllib.parse.urlencode(cleaned_qs)
        normalized = urllib.parse.urlunparse((scheme, netloc, path, '', query, ''))
        return normalized
    except Exception:
        return url.lower().rstrip('/')

# ---------- HELPER: create stable deterministic key ----------
def stable_key(row):
    """
    Deterministic unique_key based on normalized Query, Page, Date (YYYY-MM-DD).
    Returns hex sha256 digest string.
    """
    # Normalize Query
    query = (row.get('Query') or '').strip().lower()
    # Normalize Page / URL
    page = normalize_url(row.get('Page') or '')
    # Normalize Date to YYYY-MM-DD
    date_raw = row.get('Date')
    if isinstance(date_raw, str):
        date = date_raw[:10]
    elif isinstance(date_raw, datetime):
        date = date_raw.strftime("%Y-%m-%d")
    else:
        date = str(date_raw)[:10]
    det_tuple = (query, page, date)
    s = "|".join(det_tuple)
    return hashlib.sha256(s.encode('utf-8')).hexdigest()

# ---------- FETCH EXISTING KEYS FROM BIGQUERY (robust, memory-savvy) ----------
def get_existing_keys():
    """
    Fetch existing unique_key values from BigQuery table.
    Uses query().result() iterator to avoid requiring to_dataframe / pyarrow.
    Returns a Python set of strings.
    """
    existing = set()
    try:
        sql = f"SELECT unique_key FROM `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}`"
        query_job = bq_client.query(sql)
        for row in query_job.result():  # RowIterator
            uk = row.get("unique_key") if hasattr(row, "get") else row[0]
            if uk is not None:
                existing.add(str(uk))
        print(f"[INFO] Retrieved {len(existing)} existing keys from BigQuery.", flush=True)
    except Exception as e:
        print(f"[WARN] Failed to fetch existing keys: {e}", flush=True)
        # return empty set (safe); caller must handle potential duplicates at load time
    return existing

# ---------- UPLOAD TO BIGQUERY ----------
def upload_to_bq(df):
    if df is None or df.empty:
        print("[INFO] No new rows to insert.", flush=True)
        return 0
    # ensure Date is datetime.date or datetime
    try:
        df['Date'] = pd.to_datetime(df['Date']).dt.date
    except Exception:
        # if conversion fails, leave as-is; BigQuery will attempt cast
        pass

    # load using load_table_from_dataframe (works with or without pyarrow; will warn)
    try:
        job = bq_client.load_table_from_dataframe(df, table_ref)
        job.result()
        inserted = int(job.output_rows) if hasattr(job, "output_rows") else len(df)
        print(f"[INFO] Inserted {inserted} rows to BigQuery.", flush=True)
        return inserted
    except Exception as e:
        print(f"[ERROR] Failed to insert rows: {e}", flush=True)
        return 0

# ---------- FETCH GSC DATA (idempotent) ----------
def fetch_gsc_data(start_date, end_date):
    all_new_rows = []  # list of lists for dataframe rows
    existing_keys = get_existing_keys()
    start_row = 0
    batch_index = 1
    retry_count = 0

    while True:
        request = {
            'startDate': start_date,
            'endDate': end_date,
            'dimensions': ['date', 'query', 'page'],
            'rowLimit': ROW_LIMIT,
            'startRow': start_row
        }

        try:
            resp = service.searchanalytics().query(siteUrl=SITE_URL, body=request).execute()
        except Exception as e:
            retry_count += 1
            if retry_count > MAX_RETRIES:
                print(f"[ERROR] Too many retries fetching GSC: {e}", flush=True)
                break
            print(f"[WARN] Timeout/error from GSC: {e} — retrying in {RETRY_DELAY}s...", flush=True)
            time.sleep(RETRY_DELAY)
            continue

        rows = resp.get('rows', [])
        if not rows:
            print("[INFO] No more rows returned from GSC.", flush=True)
            break

        batch_new_count = 0
        for r in rows:
            date = r['keys'][0]
            query_text = r['keys'][1]
            page = r['keys'][2]
            clicks = r.get('clicks', 0)
            impressions = r.get('impressions', 0)
            ctr = r.get('ctr', 0)
            position = r.get('position', 0)

            row_dict = {'Date': date, 'Query': query_text, 'Page': page}
            key = stable_key(row_dict)

            # check against existing_keys set (keeps updated to avoid duplicates within run)
            if key not in existing_keys:
                existing_keys.add(key)
                all_new_rows.append([date, query_text, page, clicks, impressions, ctr, position, key])
                batch_new_count += 1

        print(f"[INFO] Batch {batch_index}: Fetched {len(rows)} rows, {batch_new_count} new rows.", flush=True)

        # upload only current batch's new rows (safer for memory; can also accumulate and upload once)
        if batch_new_count > 0:
            df_batch = pd.DataFrame(
                all_new_rows[-batch_new_count:],
                columns=['Date','Query','Page','Clicks','Impressions','CTR','Position','unique_key']
            )
            inserted = upload_to_bq(df_batch)
            # inserted count used for logging; existing_keys already updated

        else:
            print(f"[INFO] Batch {batch_index} has no new rows.", flush=True)

        # paging logic
        if len(rows) < ROW_LIMIT:
            break
        start_row += len(rows)
        batch_index += 1

    # final combined DataFrame for return (all new rows across run)
    df_all = pd.DataFrame(all_new_rows, columns=['Date','Query','Page','Clicks','Impressions','CTR','Position','unique_key'])
    return df_all

# ---------- MAIN ----------
if __name__ == "__main__":
    ensure_table()
    df_new = fetch_gsc_data(START_DATE, END_DATE)
    print(f"[INFO] Finished fetching all data. Total new rows: {len(df_new)}", flush=True)
