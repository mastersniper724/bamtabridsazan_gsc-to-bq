#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ============================================================
# File: gsc_country_utils.py
# Revision: Rev.2.1 - Adding Kosovo country
# Purpose: Converting ISO 3166 Alpha-2 / Alpha-3 Codes or names
#          to full Country Name using BigQuery reference table.
# ============================================================

from google.cloud import bigquery
import pandas as pd

# =================================================
# Function: load_country_map
# =================================================
def load_country_map(project: str, dataset: str, table: str) -> dict:
    """
    Load GSC country dimension from BigQuery and return a dictionary mapping:
      - ISO Alpha-2 → Country Name
      - ISO Alpha-3 → Country Name
    This ensures compatibility with GSC data that may include 2 or 3 letter codes.
    """
    client = bigquery.Client()
    query = f"""
        SELECT country_code_alpha2, country_code_alpha3, country_name
        FROM `{project}.{dataset}.{table}`
    """
    df = client.query(query).to_dataframe()
    
    df['country_code_alpha2'] = df['country_code_alpha2'].str.upper()
    df['country_code_alpha3'] = df['country_code_alpha3'].str.upper()
    
    # Create unified dictionary for both code types
    map_alpha2 = dict(zip(df['country_code_alpha2'], df['country_name']))
    map_alpha3 = dict(zip(df['country_code_alpha3'], df['country_name']))
    
    # Merge them (Alpha-3 overrides Alpha-2 if duplicate — harmless)
    country_map = {**map_alpha2, **map_alpha3}
    
    # Add explicit Unknown Region mapping
    country_map["ZZ"] = "Unknown Region"
    country_map["ZZZ"] = "Unknown Region"
    country_map["UNKNOWN"] = "Unknown Region"
    country_map["XKK"] = "Kosovo"
    
    return country_map


# =================================================
# Function: robust_map_country_column
# =================================================
def robust_map_country_column(
    df: pd.DataFrame, 
    country_col: str, 
    country_map: dict, 
    new_col: str = "Country"
) -> pd.DataFrame:
    """
    Robust mapping for GSC country values:
      - Handles both Alpha-2 and Alpha-3 codes directly from the map
      - Handles 'zzz', 'zz', or 'unknown' as 'Unknown Region'
      - Non-mapped values return None
    """
    if df is None or df.empty or country_map is None:
        return df

    def map_one(val):
        if pd.isna(val):
            return None
        s = str(val).strip().upper()
        if s in ("", "ZZ", "ZZZ", "UNKNOWN"):
            return "Unknown Region"
        return country_map.get(s, None)

    df[new_col] = df[country_col].apply(map_one)
    return df
