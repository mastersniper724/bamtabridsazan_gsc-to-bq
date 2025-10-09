# =================================================
# BLOCK 1: CONFIGURATION & ARGUMENT PARSING
# =================================================
import pandas as pd
import pycountry
from google.cloud import bigquery

# BigQuery settings
BQ_PROJECT = "bamtabridsazan"
BQ_DATASET = "seo_reports"
BQ_TABLE_DIM_COUNTRY = "gsc_dim_country"

# =================================================
# BLOCK 2: DATA GENERATION
# =================================================
def generate_country_df():
    """Generate DataFrame of countries from pycountry."""
    data = [(c.alpha_2, c.name) for c in pycountry.countries]
    df = pd.DataFrame(data, columns=['country_code', 'country_name'])
    
    # Sort alphabetically by country_name
    df = df.sort_values(by='country_name', ascending=True)
    
    # Drop duplicate country codes just in case
    df = df.drop_duplicates(subset=['country_code'])
    
    return df

df_country = generate_country_df()

# =================================================
# BLOCK 3: LOAD TO BIGQUERY
# =================================================
def load_to_bq(df, project, dataset, table):
    """
    Load DataFrame to BigQuery table. 
    Overwrites existing table.
    """
    client = bigquery.Client()
    table_id = f"{project}.{dataset}.{table}"
    
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # Wait for completion
    
    print(f"[INFO] BigQuery table {table_id} updated successfully with {len(df)} rows.")

load_to_bq(df_country, BQ_PROJECT, BQ_DATASET, BQ_TABLE_DIM_COUNTRY)

# =================================================
# BLOCK 4: OPTIONAL - VALIDATION
# =================================================
# Check for missing country codes or names
missing_codes = df_country[df_country['country_code'].isnull()]
missing_names = df_country[df_country['country_name'].isnull()]

if not missing_codes.empty or not missing_names.empty:
    print("[WARNING] Missing country codes or names found!")
    if not missing_codes.empty:
        print(missing_codes)
    if not missing_names.empty:
        print(missing_names)
else:
    print("[INFO] All country codes and names are valid.")
