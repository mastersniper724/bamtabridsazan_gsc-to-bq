#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ============================================================
# File: gsc_country_utils.py.py
# Revision: Rev.1 - adding "Unknown Region" for the countries with "zzz" country_code
# Purpose: Converting ISO 3166 Alpha-2 Codes country values to full Country Name.
# ============================================================

from google.cloud import bigquery
import pycountry
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
    Robust mapping for country codes/names:
      - handles 2-letter codes (alpha-2)
      - handles 3-letter codes (alpha-3)
      - handles full country names (with fuzzy fallback)
      - maps 'zzz' or unknown regions to 'Unknown'
    Writes result into `new_col` (replaces or creates column).
    """
    if df is None or df.empty or country_map is None:
        return df

    def map_one(val):
        if pd.isna(val):
            return None
        s = str(val).strip()
        if s == "":
            return None

        # handle unknown region
        if s.lower() == "zzz":
            return "Unknown Region"

        s_up = s.upper()

        # direct alpha-2
        if s_up in country_map:
            return country_map[s_up]

        # alpha-3 -> alpha-2
        if len(s_up) == 3:
            try:
                c = pycountry.countries.get(alpha_3=s_up)
                if c:
                    code2 = c.alpha_2.upper()
                    if code2 in country_map:
                        return country_map[code2]
            except Exception:
                pass

        # lookup by full name
        try:
            c = pycountry.countries.get(name=s)
            if c:
                code2 = c.alpha_2.upper()
                if code2 in country_map:
                    return country_map[code2]
        except Exception:
            pass

        # fuzzy search
        try:
            res = pycountry.countries.search_fuzzy(s)
            if res:
                code2 = res[0].alpha_2.upper()
                if code2 in country_map:
                    return country_map[code2]
        except Exception:
            pass

        # fallback: not found
        return None

    df[new_col] = df[country_col].apply(map_one)
    return df

