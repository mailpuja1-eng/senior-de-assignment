import pandas as pd
import numpy as np
import os
import re
import requests
import time
from datetime import datetime
from pytz import UTC

# --- Constants ---
INPUT_CSV = 'transactions.csv'
OUTPUT_DIR = 'outputs'
RAW_OUTPUT = os.path.join(OUTPUT_DIR, 'raw_transactions.csv')
QUARANTINE_OUTPUT = os.path.join(OUTPUT_DIR, 'quarantine_sample.csv')

# Allowed values
ALLOWED_CURRENCIES = {'USD', 'EUR', 'GBP', 'CHF', 'JPY', 'AUD', 'CAD'}
ALLOWED_TYPES = {'debit', 'credit'}
ALLOWED_CATEGORIES = {
    "e-commerce", "travel", "food_and_beverage", "groceries", "electronics",
    "retail", "entertainment", "health", "transportation", "home_and_garden",
    "payroll", "transfer"
}
ALLOWED_STATUS = {'completed', 'pending', 'failed', 'reversed'}

# ISO 3166-1 alpha-2 country codes (official, assigned, subset for brevity)
ISO_COUNTRIES = {
    'US','GB','DE','JP','AU','FR','NL','ES','CH','CA','IT','BE','IE','AT','DK','SE','NO','FI','NZ','PT','GR','LU','LI','MC','SM','VA','IS','CZ','PL','HU','SK','SI','EE','LV','LT','BG','RO','HR','RU','UA','BY','MD','GE','AM','AZ','KZ','UZ','TM','KG','TJ','MN','CN','IN','KR','SG','MY','TH','VN','PH','ID','HK','TW','MO','IL','SA','AE','QA','KW','OM','BH','JO','LB','SY','IQ','IR','YE','AF','PK','BD','LK','NP','MV','MM','KH','LA','BN','TL','PG','FJ','SB','VU','NC','PF','WS','TO','TV','NR','KI','MH','FM','PW','GU','MP','AS','CK','NU','TK','WF','EH','MA','DZ','TN','LY','EG','SD','SS','ET','ER','DJ','SO','KE','UG','TZ','RW','BI','MZ','MG','ZM','ZW','MW','AO','NA','BW','SZ','LS','ZA','CM','NG','GH','CI','BF','NE','TG','BJ','SN','GM','GW','CV','SL','LR','ML','MR','ST','GQ','GA','CG','CD','AO','CM','TD','CF','GAB','GN','TD','SD','SS','EG','LY','TN','DZ','MA'
}

# ---  Functions ---

def validate_row(row):
    errors = []

    # transaction_id
    if not isinstance(row['transaction_id'], str) or not re.fullmatch(r'^TXN-\d{4}$', row['transaction_id']):
        errors.append('Invalid transaction_id format')

    # account_id
    if not isinstance(row['account_id'], str) or not re.fullmatch(r'^ACC-\d{4}$', row['account_id']):
        errors.append('Invalid account_id format')

    # transaction_date
    date_str = str(row['transaction_date'])
    if not re.fullmatch(r'^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])T([01]\d|2[0-3]):[0-5]\d:[0-5]\dZ$', date_str):
        errors.append('Invalid transaction_date format')
    else:
        try:
            dt = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ")
            # Already caught by strptime, but double check
            if dt.year != int(date_str[:4]):
                errors.append('transaction_date year mismatch')
        except Exception:
            errors.append('Invalid calendar date in transaction_date')

    # amount
    try:
        amt = float(row['amount'])
        if amt <= 0:
            errors.append('amount must be > 0')
        elif round(amt, 2) != amt:
            errors.append('amount must be rounded to 2 decimals')
    except Exception:
        errors.append('amount is not a number')

    # currency
    if row['currency'] not in ALLOWED_CURRENCIES:
        errors.append('Invalid currency')

    # transaction_type
    if row['transaction_type'] not in ALLOWED_TYPES:
        errors.append('Invalid transaction_type')

    # merchant_name
    if not isinstance(row['merchant_name'], str) or not re.search(r'\S', row['merchant_name']):
        errors.append('merchant_name must contain non-whitespace')

    # merchant_category
    if row['merchant_category'] not in ALLOWED_CATEGORIES:
        errors.append('Invalid merchant_category')

    # status
    if row['status'] not in ALLOWED_STATUS:
        errors.append('Invalid status')

    # country_code
    cc = str(row['country_code'])
    if not re.fullmatch(r'^[A-Z]{2}$', cc):
        errors.append('country_code must be two uppercase letters')
    elif cc not in ISO_COUNTRIES:
        errors.append('country_code is not an assigned ISO 3166-1 alpha-2 code')

    return errors

def get_natural_key(row):
    # All fields except transaction_id
    return (
        row['account_id'], row['transaction_date'], row['amount'], row['currency'],
        row['transaction_type'], row['merchant_name'], row['merchant_category'],
        row['status'], row['country_code']
    )

def fetch_transactions_from_api():
    BASE_URL = "https://fgbjekjqnbmtkmeewexb.supabase.co/rest/v1/transactions"
    API_KEY = "sb_publishable_W2MbiakvFFthMHtlrzSkQw_URTiUI6G"
    HEADERS = {
        "apikey": API_KEY,
        "Authorization": f"Bearer {API_KEY}"
    }
    transactions = []
    offset = 0
    limit = 1000
    max_retries = 3

    while True:
        params = {"limit": limit, "offset": offset}
        retries = 0
        while retries <= max_retries:
            try:
                resp = requests.get(BASE_URL, headers=HEADERS, params=params, timeout=10)
                if resp.status_code == 200:
                    batch = resp.json()
                    if not batch:
                        return transactions
                    transactions.extend(batch)
                    if len(batch) < limit:
                        return transactions
                    offset += limit
                    break  # break retry loop
                elif resp.status_code in (429, 500, 502, 503, 504):
                    wait = 2 ** retries
                    print(f"Transient error {resp.status_code}, retrying in {wait}s...")
                    time.sleep(wait)
                    retries += 1
                elif resp.status_code == 400:
                    print(f"Bad request: {resp.text}")
                    return None
                elif resp.status_code == 401:
                    print("Unauthorized: check API key.")
                    return None
                else:
                    print(f"Unhandled error: {resp.status_code} {resp.text}")
                    return None
            except requests.exceptions.Timeout:
                wait = 2 ** retries
                print(f"Timeout, retrying in {wait}s...")
                time.sleep(wait)
                retries += 1
            except Exception as e:
                print(f"Request failed: {e}")
                return None
        else:
            print("Max retries reached, falling back to CSV.")
            return None

def load_transactions():
    print("Attempting to fetch from API...")
    api_data = fetch_transactions_from_api()
    if api_data is not None:
        print("Fetched from API")
        df = pd.DataFrame(api_data)
    else:
        print("Falling back to CSV")
        df = pd.read_csv(INPUT_CSV, dtype=str).replace({np.nan: None})
    return df

# --- Main Pipeline ---

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    df = load_transactions()

    # Convert amount to float for validation
    df['amount'] = df['amount'].astype(float)

    # Validation
    quarantine_rows = []
    valid_rows = []

    # For duplicate detection
    seen_natural_keys = {}

    for idx, row in df.iterrows():
        errors = validate_row(row)
        natural_key = get_natural_key(row)

        # Duplicate detection (by natural key, not transaction_id)
        if natural_key in seen_natural_keys:
            is_duplicate = True
        else:
            is_duplicate = False
            seen_natural_keys[natural_key] = row['transaction_id']

        if errors:
            row_dict = row.to_dict()
            row_dict['error_reason'] = '; '.join(errors)
            quarantine_rows.append(row_dict)
        else:
            row_dict = row.to_dict()
            row_dict['is_duplicate'] = is_duplicate
            valid_rows.append(row_dict)

    # Add ingestion_timestamp to valid rows
    ingestion_time = datetime.utcnow().replace(tzinfo=UTC).isoformat()
    for row in valid_rows:
        row['ingestion_timestamp'] = ingestion_time

    # Write outputs
    if valid_rows:
        valid_df = pd.DataFrame(valid_rows)
        valid_df.to_csv(RAW_OUTPUT, index=False)
    else:
        pd.DataFrame(columns=list(df.columns) + ['is_duplicate', 'ingestion_timestamp']).to_csv(RAW_OUTPUT, index=False)

    if quarantine_rows:
        quarantine_df = pd.DataFrame(quarantine_rows)
        quarantine_df.to_csv(QUARANTINE_OUTPUT, index=False)
    else:
        pd.DataFrame(columns=list(df.columns) + ['error_reason']).to_csv(QUARANTINE_OUTPUT, index=False)

    print(f"Raw output written to: {RAW_OUTPUT}")
    print(f"Quarantine output written to: {QUARANTINE_OUTPUT}")

if __name__ == "__main__":
    main()