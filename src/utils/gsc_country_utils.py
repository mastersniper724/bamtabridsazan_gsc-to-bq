from google.cloud import bigquery
import pandas as pd

# =================================================
# Function: load_country_map
# =================================================
def load_country_map(project: str, dataset: str, table: str) -> dict:
    """
    Load GSC country dimension from BigQuery and return a dictionary for mapping
    country_code (ISO Alpha-2) -> country_name (full name)
    """
    client = bigquery.Client()
    query = f"""
        SELECT country_code, country_name
        FROM `{project}.{dataset}.{table}`
    """
    df = client.query(query).to_dataframe()
    
    # Ensure uppercase ISO Alpha-2
    df['country_code'] = df['country_code'].str.upper()
    
    return dict(zip(df['country_code'], df['country_name']))

# =================================================
# Function: map_country_column
# =================================================
def map_country_column(df: pd.DataFrame, country_col: str, country_map: dict, new_col: str = "country") -> pd.DataFrame:
    """
    Map a DataFrame column containing country codes to full country names
    """
    df[country_col] = df[country_col].str.upper()
    df[new_col] = df[country_col].map(country_map)
    return df
