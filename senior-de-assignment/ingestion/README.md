# Senior Data Engineer Assignment

## Overview

This repository contains a data ingestion and transformation pipeline implemented in Python using Pandas.
It ingests transaction data from an API (with fallback to CSV), applies validation and deduplication, supports incremental ingestion with watermarking, and produces daily summary outputs.

---

## Inputs

- **transactions.csv**  
  The source file containing transaction records. Used as a fallback if the API is unreachable.  
  Must match the schema specified below.

- **API Endpoint**  
  `https://fgbjekjqnbmtkmeewexb.supabase.co/rest/v1/transactions`  
  Used for primary ingestion, with authentication and pagination.

---

## Task Approaches

### Task 1: Ingestion & Validation

- **Source:** API with pagination, authentication, and retry logic. If the API is unreachable, falls back to `transactions.csv`.
- **Validation:** Each record is validated against the schema (field types, allowed values, formats, etc.).
- **Quarantine:** Invalid records are written to `outputs/quarantine_sample.csv` with an `error_reason`.
- **Deduplication:** Natural key deduplication is applied (across all ingested data).
- **Output:** Valid, non-duplicate records are written to `outputs/raw_transactions.csv` with an `ingestion_timestamp`.

### Task 2: Daily Aggregation

- **Source:** Reads only completed, non-duplicate records from the raw layer.
- **Aggregation:** Produces per-account, per-day summaries, including:
  - Total debit amount
  - Total credit amount
  - Net amount
  - Transaction count
  - Distinct merchants
  - Currencies used
  - Top merchant category by spend
  - `updated_at` timestamp
- **Data Quality:** Data quality assertions are included in the script to ensure output integrity.
- **Output:** Written to `outputs/daily_summary_output.csv`.

### Task 3: Incremental Ingestion & Watermarking

- **Watermarking:** The pipeline tracks the maximum `transaction_date` ingested in `outputs/watermark.json`.
- **Incremental Loads:** On each run, only records with `transaction_date` greater than or equal to the watermark (minus a lookback window) are processed.
- **Late-arriving Data:** A configurable lookback window (default: 1 day) ensures late-arriving records are ingested. Deduplication prevents double-counting.
- **Persistence:** The watermark is updated after each successful run. Sequential watermark files (e.g., `watermark_run1.json`, `watermark_run2.json`) may be saved to demonstrate pipeline state over time.

---

## Data Quality Assertions

The following assertions are applied (in code, not dbt):

- No nulls in primary key fields (`transaction_id`, `account_id`, `transaction_date`)
- All amounts are positive and rounded to two decimals
- Only allowed values for currency, transaction type, status, and merchant category
- Proper ISO 3166-1 alpha-2 country codes
- No duplicate records by natural key
- Output summary checks: no nulls in key fields, counts and amounts are non-negative, per-account-per-day uniqueness

---

## Technology Stack Choice and Rationale

- **Python**: Widely used for data engineering, flexible, and easy to integrate with APIs and CSVs.
- **Pandas**: Powerful for data manipulation and validation, suitable for prototyping and handling tabular data.
- **Requests**: Simple and robust library for HTTP/API access.
- **CSV/JSON**: Standard formats for data interchange and storage, making outputs easy to inspect and share.
- **No dbt**: Data quality assertions are implemented directly in Python for transparency and flexibility.

---

## Watermark Strategy and Late-Arriving Data Approach

- **Watermarking:** After each run, the pipeline records the maximum `transaction_date` ingested in `outputs/watermark.json`.
- **Incremental Loads:** On subsequent runs, only records newer than the watermark (with a configurable lookback window) are processed.
- **Late-Arriving Data:** The lookback window (default: 1 day) ensures that records arriving late are still ingested. Deduplication prevents double-counting.
- **Persistence:** Sequential watermark files (e.g., `watermark_run1.json`, `watermark_run2.json`) can be saved to demonstrate state across runs.

---

## Improvements Given More Time

- **Scalability:** Transition to a distributed processing framework (e.g., Apache Spark) for larger datasets.
- **Robustness:** Use a transactional database or Delta Lake for watermark and raw layer persistence.
- **Automation:** Implement orchestration with Airflow or Prefect for scheduled runs and monitoring.
- **Testing:** Add unit/integration tests and CI/CD pipelines.
- **Data Quality Framework:** Integrate with tools like Great Expectations or dbt for richer data quality checks.
- **Enhanced Error Handling:** More granular logging and alerting for failures.
- **API Reliability:** Retry with exponential backoff, and alert if API is unreachable for extended periods.

---

## Output Schemas

### outputs/raw_transactions.csv

| Column                | Type    | Description                                 |
|-----------------------|---------|---------------------------------------------|
| transaction_id        | string  | Unique transaction ID                       |
| account_id            | string  | Account ID                                  |
| transaction_date      | string  | ISO 8601 UTC timestamp                      |
| amount                | float   | Transaction amount                          |
| currency              | string  | 3-letter currency code                      |
| transaction_type      | string  | debit/credit                                |
| merchant_name         | string  | Merchant name                               |
| merchant_category     | string  | Merchant category                           |
| status                | string  | Transaction status                          |
| country_code          | string  | ISO country code                            |
| is_duplicate          | bool    | True if duplicate by natural key            |
| ingestion_timestamp   | string  | UTC timestamp of ingestion                  |

### outputs/quarantine_sample.csv

| ...all input columns... | error_reason (string) |

### outputs/daily_summary_output.csv

| Column                | Type    | Description                                  |
|-----------------------|---------|----------------------------------------------|
| account_id            | string  | Account ID                                   |
| transaction_date      | date    | Date (YYYY-MM-DD)                            |
| total_debit_amount    | float   | Sum of debits                                |
| total_credit_amount   | float   | Sum of credits                               |
| net_amount            | float   | total_credit_amount - total_debit_amount     |
| transaction_count     | int     | Number of transactions                       |
| distinct_merchants    | int     | Number of unique merchants                   |
| currencies            | string  | Comma-separated list of currencies           |
| top_category          | string  | Merchant category with highest debit spend   |
| updated_at            | string  | UTC timestamp of aggregation                 |

---

## Limitations

- If the API is unreachable (e.g., network issues), the pipeline falls back to the local CSV file.
- If no new records are available, the watermark does not advance.
- The pipeline expects the input CSV to match the schema exactly.

---

## How to Run

1. **Install dependencies:**
   ```bash
   pip install pandas requests