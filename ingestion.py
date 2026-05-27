import requests
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
import os
from datetime import datetime, timezone

# Load credentials from .env file
load_dotenv()

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────

# Countries to pull (G20 major economies — good for global corporate finance)
COUNTRIES = ['US', 'GB', 'CA', 'DE', 'FR', 'JP', 'CN', 'IN', 'BR', 'AU']

# Indicators to pull
INDICATORS = {
    'GDP':       'NY.GDP.MKTP.CD',
    'INFLATION': 'FP.CPI.TOTL.ZG',
    'FX_RATES':  'PA.NUS.FCRF',
    'GOVT_DEBT': 'GC.DOD.TOTL.GD.ZS'
}

# Target tables for each indicator
TARGET_TABLES = {
    'GDP':       'RAW.WORLD_BANK_GDP',
    'INFLATION': 'RAW.WORLD_BANK_INFLATION',
    'FX_RATES':  'RAW.WORLD_BANK_FX_RATES',
    'GOVT_DEBT': 'RAW.WORLD_BANK_GOVT_DEBT'
}

# ─────────────────────────────────────────────
# STEP 1: FETCH DATA FROM WORLD BANK API
# ─────────────────────────────────────────────

def fetch_world_bank_data(indicator_code, countries, start_year=2000, end_year=2023):
    """
    Calls the World Bank API for a given indicator across multiple countries.
    Returns a clean pandas DataFrame.
    """
    country_string = ';'.join(countries)  # API accepts multiple countries separated by semicolon
    url = (
        f"https://api.worldbank.org/v2/country/{country_string}"
        f"/indicator/{indicator_code}"
        f"?format=json&per_page=1000&date={start_year}:{end_year}"
    )

    print(f"  Calling API: {url}")
    response = requests.get(url, timeout=60)

    if response.status_code != 200:
        print(f"  ERROR: API returned status {response.status_code}")
        return pd.DataFrame()

    data = response.json()

    # World Bank API returns a list with 2 elements:
    # [0] = metadata (page info), [1] = actual records
    if len(data) < 2 or data[1] is None:
        print(f"  WARNING: No data returned for indicator {indicator_code}")
        return pd.DataFrame()

    records = []
    for item in data[1]:
        # Skip records where value is None (data not available for that year/country)
        if item['value'] is None:
            continue
        records.append({
            'COUNTRY_CODE':    item['countryiso3code'] or item['country']['id'],
            'COUNTRY_NAME':    item['country']['value'],
            'INDICATOR_CODE':  item['indicator']['id'],
            'INDICATOR_NAME':  item['indicator']['value'],
            'YEAR':            int(item['date']),
            'VALUE':           float(item['value']),
            'LOADED_AT':       datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        })

    df = pd.DataFrame(records)
    print(f"  Fetched {len(df)} rows for indicator {indicator_code}")
    return df


# ─────────────────────────────────────────────
# STEP 2: CONNECT TO SNOWFLAKE
# ─────────────────────────────────────────────

def get_snowflake_connection():
    """Creates and returns a Snowflake connection using credentials from .env"""
    conn = snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE')
    )
    print("  Connected to Snowflake successfully.")
    return conn


# ─────────────────────────────────────────────
# STEP 3: LOAD DATA INTO SNOWFLAKE
# ─────────────────────────────────────────────

def load_to_snowflake(conn, df, table_name):
    """
    Inserts rows from a DataFrame into a Snowflake table.
    Uses executemany for efficiency.
    """
    if df.empty:
        print(f"  Skipping {table_name} — no data to load.")
        return

    cursor = conn.cursor()

    # Truncate first so we don't get duplicates on re-runs
    cursor.execute(f"TRUNCATE TABLE {table_name}")

    insert_sql = f"""
        INSERT INTO {table_name}
            (COUNTRY_CODE, COUNTRY_NAME, INDICATOR_CODE, INDICATOR_NAME, YEAR, VALUE, LOADED_AT)
        VALUES
            (%s, %s, %s, %s, %s, %s, %s)
    """

    rows = list(df.itertuples(index=False, name=None))
    cursor.executemany(insert_sql, rows)
    log_sql = """
    INSERT INTO RAW.PIPELINE_LOG (INDICATOR, ROWS_LOADED, STATUS, MESSAGE)
    VALUES (%s, %s, %s, %s)
    """
    cursor.execute(log_sql, (table_name, len(df), 'SUCCESS', 'Loaded successfully'))
    conn.commit()
    

    print(f"  Loaded {len(df)} rows into {table_name}")
    cursor.close()


# ─────────────────────────────────────────────
# MAIN PIPELINE
# ─────────────────────────────────────────────

def run_pipeline():
    print("=" * 60)
    print(f"Pipeline started at {datetime.now(timezone.utc)}")
    print("=" * 60)

    # Connect to Snowflake once, reuse connection
    conn = get_snowflake_connection()

    for indicator_name, indicator_code in INDICATORS.items():
        print(f"\nProcessing: {indicator_name} ({indicator_code})")
        df = fetch_world_bank_data(indicator_code, COUNTRIES)
        table = TARGET_TABLES[indicator_name]
        load_to_snowflake(conn, df, table)

    conn.close()
    print("\n" + "=" * 60)
    print(f"Pipeline completed at {datetime.utcnow()}")
    print("=" * 60)


if __name__ == "__main__":
    run_pipeline()