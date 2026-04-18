import pandas as pd
import numpy as np
import os
import re
import requests
import time
import json
from datetime import datetime, timezone
from pytz import UTC

# --- Constants ---
INPUT_CSV = 'transactions.csv'
OUTPUT_DIR = 'outputs'
RAW_OUTPUT = os.path.join(OUTPUT_DIR, 'raw_transactions.csv')
QUARANTINE_OUTPUT = os.path.join(OUTPUT_DIR, 'quarantine_sample.csv')
WATERMARK_FILE = os.path.join(OUTPUT_DIR, 'watermark.json')

# Allowed values (same as Task 1)
ALLOWED_CURRENCIES = {'USD', 'EUR', 'GBP', 'CHF', 'JPY', 'AUD', 'CAD'}
ALLOWED_TYPES = {'debit', 'credit'}
ALLOWED_CATEGORIES = {
    "e-commerce", "travel", "food_and_beverage", "groceries", "electronics",
    "retail", "entertainment", "health", "transportation", "home_and_garden",
    "payroll", "transfer"
}
ALLOWED_STATUS = {'completed', 'pending', 'failed', 'reversed'}
ISO_COUNTRIES = {'US','GB','DE','JP','AU','FR','NL','ES','CH','CA','IT','BE','IE','AT','DK','SE','NO','FI','NZ','PT','GR','LU','LI','MC','SM','VA','IS','CZ','PL','HU','SK','SI','EE','LV','LT','BG','RO','HR','RU','UA','BY','MD','GE','AM','AZ','KZ','UZ','TM','KG','TJ','MN','CN','IN','KR','SG','MY','TH','VN','PH','ID','HK','TW','MO','IL','SA','AE','QA','KW','OM','BH','JO','LB','SY','IQ','IR','YE','AF','PK','BD','LK','NP','MV','MM','KH','LA','BN','TL','PG','FJ','SB','VU','NC','PF','WS','TO','TV','NR','KI','MH','FM','PW','GU','MP','AS','CK','NU','TK','WF','EH','MA','DZ','TN','LY','EG','SD','SS','ET','ER','DJ','SO','KE','UG','TZ','RW','BI','MZ','MG','ZM','ZW','MW','AO','NA','BW','SZ','LS','ZA','CM','NG','GH','CI','BF','NE','TG','BJ','SN','GM','GW','CV','SL','LR','ML','MR','ST','GQ','GA','CG','CD','AO','CM','TD','CF','GAB','GN','TD','SD','SS','EG','LY','TN','DZ','MA'}

def validate_row(row):
    errors = []
    if not isinstance(row['transaction_id'], str) or not re.fullmatch(r'^TXN-\d{4}$', row['transaction_id']):
        errors.append('Invalid transaction_id format')
    if not isinstance(row['account_id'], str) or not re.fullmatch(r'^ACC-\d{4}$', row['account_id']):
        errors.append('Invalid account_id format')
    date_str = str(row['transaction_date'])
    if not re.fullmatch(r'^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])T([01]\d|2[0-3]):[0-5]\d:[0-5]\dZ$', date_str):
        errors.append('Invalid transaction_date format')
    else:
        try:
            dt = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ")
            if dt.year != int(date_str[:4]):
                errors.append('transaction_date year mismatch')
        except Exception:
            errors.append('Invalid calendar date in transaction_date')
    try:
        amt = float(row['amount'])
        if amt <= 0:
            errors.append('amount must be > 0')
        elif round(amt, 2) != amt:
            errors.append('amount must be rounded to 2 decimals')
    except Exception:
        errors.append('amount is not a number')
    if row['currency'] not in ALLOWED_CURRENCIES:
        errors.append('Invalid currency')
    if row['transaction_type'] not in ALLOWED_TYPES:
        errors.append('Invalid transaction_type')
    if not isinstance(row['merchant_name'], str) or not re.search(r'\S', row['merchant_name']):
        errors.append('merchant_name must contain non-whitespace')
    if row['merchant_category'] not in ALLOWED_CATEGORIES:
        errors.append('Invalid merchant_category')
    if row['status'] not in ALLOWED_STATUS:
        errors.append('Invalid status')
    cc = str(row['country_code'])
    if not re.fullmatch(r'^[A-Z]{2}$', cc):
        errors.append('country_code must be two uppercase letters')
    elif cc not in ISO_COUNTRIES:
        errors.append('country_code is not an assigned ISO 3166-1 alpha-2 code')
    return errors

def get_natural_key(row):
    return (
        row['account_id'], row['transaction_date'], row['amount'], row['currency'],
        row['transaction_type'], row['merchant_name'], row['merchant_category'],
        row['status'], row['country_code']
    )

def fetch_transactions_from_api(watermark=None, lookback_days=0):
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

    params = {"limit": limit, "offset": offset}
    if watermark:
        # Apply lookback window for late-arriving data
        dt = datetime.strptime(watermark, "%Y-%m-%dT%H:%M:%SZ")
        if lookback_days > 0:
            dt = dt - pd.Timedelta(days=lookback_days)
        watermark_str = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        params["transaction_date"] = f"gte.{watermark_str}"

    while True:
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
                    params["offset"] = offset
                    break
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

def load_watermark():
    if os.path.exists(WATERMARK_FILE):
        with open(WATERMARK_FILE, 'r') as f:
            data = json.load(f)
            return data.get("max_transaction_date")
    return None

def save_watermark(max_transaction_date):
    with open(WATERMARK_FILE, 'w') as f:
        json.dump({"max_transaction_date": max_transaction_date}, f)

def load_transactions(watermark=None, lookback_days=0):
    print("Attempting to fetch from API...")
    api_data = fetch_transactions_from_api(watermark, lookback_days)
    if api_data is not None:
        print("Fetched from API")
        df = pd.DataFrame(api_data)
    else:
        print("Falling back to CSV")
        df = pd.read_csv(INPUT_CSV, dtype=str).replace({np.nan: None})
        if watermark:
            df = df[df['transaction_date'] >= watermark]
    return df

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    watermark = load_watermark()
    lookback_days = 1  # For late-arriving data, configurable

    df = load_transactions(watermark, lookback_days)

    if df.empty:
        print("No new records to ingest.")
        save_watermark(watermark)
        return

    # Convert amount to float for validation
    df['amount'] = df['amount'].astype(float)

    # Validation
    quarantine_rows = []
    valid_rows = []

    seen_natural_keys = {}

    for idx, row in df.iterrows():
        errors = validate_row(row)
        natural_key = get_natural_key(row)

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
    ingestion_time = datetime.now(timezone.utc).isoformat()
    for row in valid_rows:
        row['ingestion_timestamp'] = ingestion_time

    # Append to outputs (or create if first run)
    if valid_rows:
        valid_df = pd.DataFrame(valid_rows)
        if os.path.exists(RAW_OUTPUT):
            prev_df = pd.read_csv(RAW_OUTPUT)
            combined_df = pd.concat([prev_df, valid_df], ignore_index=True)
            combined_df.to_csv(RAW_OUTPUT, index=False)
        else:
            valid_df.to_csv(RAW_OUTPUT, index=False)
    else:
        print("No valid records to ingest.")

    if quarantine_rows:
        quarantine_df = pd.DataFrame(quarantine_rows)
        if os.path.exists(QUARANTINE_OUTPUT):
            prev_q = pd.read_csv(QUARANTINE_OUTPUT)
            combined_q = pd.concat([prev_q, quarantine_df], ignore_index=True)
            combined_q.to_csv(QUARANTINE_OUTPUT, index=False)
        else:
            quarantine_df.to_csv(QUARANTINE_OUTPUT, index=False)

    # Update watermark
    if valid_rows:
        max_transaction_date = max([row['transaction_date'] for row in valid_rows])
        save_watermark(max_transaction_date)
        print(f"Watermark updated: {max_transaction_date}")
    else:
        save_watermark(watermark)
        print(f"Watermark unchanged: {watermark}")

if __name__ == "__main__":
    main()