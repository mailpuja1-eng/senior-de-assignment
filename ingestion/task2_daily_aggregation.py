import pandas as pd
import os
from datetime import datetime

# --- Constants ---
RAW_INPUT = os.path.join('outputs', 'raw_transactions.csv')
OUTPUT_DIR = 'outputs'
AGG_OUTPUT = os.path.join(OUTPUT_DIR, 'daily_summary_output.csv')

def main():
    # Load raw data
    df = pd.read_csv(RAW_INPUT)

    # Only completed records, not duplicates
    df = df[(df['status'] == 'completed') & (df['is_duplicate'] == False)]

    # Convert transaction_date to datetime and extract date
    df['transaction_date'] = pd.to_datetime(df['transaction_date'], format="%Y-%m-%dT%H:%M:%SZ", errors='coerce')
    df['date'] = df['transaction_date'].dt.date

    # Group by account and date
    group = df.groupby(['account_id', 'date'])

    # Aggregations
    summary = group.agg(
        total_debit_amount=pd.NamedAgg(column='amount', aggfunc=lambda x: x[df.loc[x.index, 'transaction_type'] == 'debit'].sum()),
        total_credit_amount=pd.NamedAgg(column='amount', aggfunc=lambda x: x[df.loc[x.index, 'transaction_type'] == 'credit'].sum()),
        transaction_count=pd.NamedAgg(column='transaction_id', aggfunc='count'),
        distinct_merchants=pd.NamedAgg(column='merchant_name', aggfunc=lambda x: x.nunique()),
        currencies=pd.NamedAgg(column='currency', aggfunc=lambda x: ','.join(sorted(set(x))))
    ).reset_index()

    # Net amount
    summary['net_amount'] = summary['total_credit_amount'] - summary['total_debit_amount']

    # Top category (by spend)
    def top_category_func(subdf):
        spend = subdf[subdf['transaction_type'] == 'debit'].groupby('merchant_category')['amount'].sum()
        if spend.empty:
            return None
        return spend.idxmax()

    summary['top_category'] = group.apply(top_category_func).values

    # Add updated_at
    summary['updated_at'] = datetime.utcnow().isoformat() + "Z"

    # Rename date column
    summary.rename(columns={'date': 'transaction_date'}, inplace=True)

    # --- Data Quality Assertions ---
    assert summary['account_id'].notnull().all(), "Null account_id found"
    assert summary['transaction_date'].notnull().all(), "Null transaction_date found"
    assert summary['transaction_count'].min() >= 0, "Negative transaction_count found"
    assert summary['total_debit_amount'].min() >= 0, "Negative total_debit_amount found"
    assert summary['total_credit_amount'].min() >= 0, "Negative total_credit_amount found"
    assert summary['currencies'].apply(lambda x: isinstance(x, str)).all(), "Non-string currencies found"
    # Unique per account/date
    assert summary.groupby(['account_id', 'transaction_date']).size().max() == 1, "Duplicate account_id/date found"

    print("Data quality assertions passed.")

    # Save output
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    summary.to_csv(AGG_OUTPUT, index=False)
    print(f"Daily summary written to: {AGG_OUTPUT}")

if __name__ == "__main__":
    main()