"""
Currency Exchange ETL Pipeline
==============================
Extract  -> daily exchange rates from the Frankfurter API
Transform -> one clean record per target currency
Load     -> insert records into MongoDB

Data is passed between tasks ONLY through *named* XCom keys
(ti.xcom_push / ti.xcom_pull with an explicit `key=`), never through
implicit return values (do_xcom_push=False on every task).
"""

from datetime import datetime, timedelta, timezone

import pendulum
import requests
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from pymongo import ASCENDING, UpdateOne

# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------
API_URL = "https://api.frankfurter.dev/v2/rates"
BASE_CURRENCY = "EUR"
TARGET_CURRENCIES = ["USD", "GBP", "JPY", "CHF", "AUD"]
REQUEST_TIMEOUT_SECONDS = 30

SOURCE_NAME = "Frankfurter API"

# MongoDB settings (credentials come from the Airflow connection, not the code)
MONGO_CONN_ID = "mongo_default"          # built from AIRFLOW_CONN_MONGO_DEFAULT in .env
MONGO_DATABASE = "currency_db"
MONGO_COLLECTION = "exchange_rates"

# Named XCom keys (the "labels" on the boxes passed between tasks)
XCOM_KEY_RAW_RATES = "raw_exchange_rates"
XCOM_KEY_TRANSFORMED_RATES = "transformed_exchange_rates"
XCOM_KEY_LOAD_SUMMARY = "mongodb_load_summary"


# --------------------------------------------------------------------------
# Task 1: Extract
# --------------------------------------------------------------------------
def extract_exchange_rates(**context):
    """Fetch the latest rates from Frankfurter and push them to XCom."""
    ti = context["ti"]
    params = {"base": BASE_CURRENCY, "quotes": ",".join(TARGET_CURRENCIES)}
    print(f"Requesting {API_URL} with params {params}")

    # 1) Call the API and handle network / HTTP errors
    try:
        response = requests.get(API_URL, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()  # raises for 4xx / 5xx status codes
    except requests.exceptions.Timeout as err:
        raise RuntimeError(f"Frankfurter API timed out after {REQUEST_TIMEOUT_SECONDS}s") from err
    except requests.exceptions.HTTPError as err:
        raise RuntimeError(
            f"Frankfurter API returned HTTP {response.status_code}: {response.text[:200]}"
        ) from err
    except requests.exceptions.RequestException as err:
        raise RuntimeError(f"Could not reach Frankfurter API: {err}") from err

    # 2) Make sure the body is valid JSON
    try:
        data = response.json()
    except ValueError as err:
        raise ValueError(f"Frankfurter API did not return valid JSON: {response.text[:200]}") from err

    # 3) Handle empty / unexpected responses
    if not data:
        raise ValueError("Frankfurter API returned an empty response - nothing to process.")
    if not isinstance(data, list):
        raise ValueError(f"Unexpected response format (expected a list): {str(data)[:200]}")

    required_fields = {"date", "base", "quote", "rate"}
    valid_rates = [item for item in data if isinstance(item, dict) and required_fields <= item.keys()]
    if not valid_rates:
        raise ValueError(f"No usable rate entries in API response: {str(data)[:200]}")

    missing = set(TARGET_CURRENCIES) - {item["quote"] for item in valid_rates}
    if missing:
        print(f"WARNING: API returned no rate for: {sorted(missing)}")

    payload = {
        "base_currency": BASE_CURRENCY,
        "requested_currencies": TARGET_CURRENCIES,
        "retrieved_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
        "rates": valid_rates,
    }

    # 4) Push the raw data with a NAMED XCom key
    ti.xcom_push(key=XCOM_KEY_RAW_RATES, value=payload)
    print(f"Pushed {len(valid_rates)} rates to XCom key '{XCOM_KEY_RAW_RATES}': {valid_rates}")


# --------------------------------------------------------------------------
# Task 2: Transform
# --------------------------------------------------------------------------
def transform_exchange_rates(**context):
    """Pull raw rates, build one clean record per target currency, push to XCom."""
    ti = context["ti"]

    # 1) Pull the raw data using the NAMED key from the extract task
    raw = ti.xcom_pull(task_ids="extract_exchange_rates", key=XCOM_KEY_RAW_RATES)
    if not raw or not raw.get("rates"):
        raise ValueError(f"No data found in XCom key '{XCOM_KEY_RAW_RATES}' - did extract run?")

    retrieved_at = raw["retrieved_at"]
    expected_base = raw.get("base_currency", BASE_CURRENCY).upper()

    # 2) Build one record per target currency (handles any number of currencies)
    records = {}
    for item in raw["rates"]:
        try:
            base = str(item["base"]).upper()
            target = str(item["quote"]).upper()
            rate = float(item["rate"])
            rate_date = datetime.strptime(item["date"], "%Y-%m-%d").strftime("%Y-%m-%d")
        except (KeyError, TypeError, ValueError) as err:
            print(f"WARNING: skipping malformed entry {item}: {err}")
            continue

        if base != expected_base:
            print(f"WARNING: skipping {item} - base {base} != expected {expected_base}")
            continue
        if rate <= 0:
            print(f"WARNING: skipping {item} - rate must be positive")
            continue

        # key by (target, date) so duplicates in the response are not stored twice
        records[(target, rate_date)] = {
            "base_currency": base,
            "target_currency": target,
            "exchange_rate": round(rate, 6),
            "rate_date": rate_date,
            "source": SOURCE_NAME,
            "retrieved_at": retrieved_at,
        }

    transformed = sorted(records.values(), key=lambda r: r["target_currency"])
    if not transformed:
        raise ValueError("Transformation produced no valid records.")

    # 3) Push the clean records with another NAMED XCom key
    ti.xcom_push(key=XCOM_KEY_TRANSFORMED_RATES, value=transformed)
    print(f"Pushed {len(transformed)} records to XCom key '{XCOM_KEY_TRANSFORMED_RATES}':")
    for record in transformed:
        print(f"  {record}")


# --------------------------------------------------------------------------
# Task 3: Load
# --------------------------------------------------------------------------
def load_to_mongodb(**context):
    """Pull transformed records, write them to MongoDB, verify they are stored."""
    ti = context["ti"]
    records = ti.xcom_pull(task_ids="transform_exchange_rates", key=XCOM_KEY_TRANSFORMED_RATES)
    if not records:
        raise ValueError(f"No data found in XCom key '{XCOM_KEY_TRANSFORMED_RATES}' - did transform run?")
    hook = MongoHook(mongo_conn_id=MONGO_CONN_ID)
    client = hook.get_conn()
    try:
        collection = client[MONGO_DATABASE][MONGO_COLLECTION]
        collection.create_index(
            [("base_currency", ASCENDING), ("target_currency", ASCENDING), ("rate_date", ASCENDING)],
            unique=True,
            name="uniq_base_target_date",
        )

        operations = [
            UpdateOne(
                {
                    "base_currency": r["base_currency"],
                    "target_currency": r["target_currency"],
                    "rate_date": r["rate_date"],
                },
                {"$set": r},
                upsert=True,
            )
            for r in records
        ]
        result = collection.bulk_write(operations, ordered=False)
        print(
            f"MongoDB write: inserted={result.upserted_count}, "
            f"updated={result.modified_count}, unchanged={result.matched_count - result.modified_count}"
        )
        stored = []
        for r in records:
            doc = collection.find_one(
                {
                    "base_currency": r["base_currency"],
                    "target_currency": r["target_currency"],
                    "rate_date": r["rate_date"],
                },
                {"_id": 0},
            )
            if doc is None:
                raise RuntimeError(f"Verification failed - record not found in MongoDB: {r}")
            stored.append(doc)

        total_docs = collection.count_documents({})
        print(f"Verified {len(stored)}/{len(records)} records in {MONGO_DATABASE}.{MONGO_COLLECTION}:")
        for doc in stored:
            print(f"  {doc}")
        print(f"Collection now holds {total_docs} documents in total.")
    finally:
        client.close()
    summary = {
        "database": MONGO_DATABASE,
        "collection": MONGO_COLLECTION,
        "records_received": len(records),
        "inserted": result.upserted_count,
        "updated": result.modified_count,
        "verified": len(stored),
        "collection_total": total_docs,
    }
    ti.xcom_push(key=XCOM_KEY_LOAD_SUMMARY, value=summary)
    print(f"Pushed load summary to XCom key '{XCOM_KEY_LOAD_SUMMARY}': {summary}")


# --------------------------------------------------------------------------
# DAG definition
# --------------------------------------------------------------------------
default_args = {
    "owner": "l230880",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="currency_exchange_etl",
    description="Daily ETL: Frankfurter API -> transform -> MongoDB (named XComs)",
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["etl", "currency", "mongodb", "xcom"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_exchange_rates",
        python_callable=extract_exchange_rates,
        do_xcom_push=False,  # no implicit return_value XCom; we push with a named key
    )

    transform_task = PythonOperator(
        task_id="transform_exchange_rates",
        python_callable=transform_exchange_rates,
        do_xcom_push=False,
    )

    load_task = PythonOperator(
        task_id="load_to_mongodb",
        python_callable=load_to_mongodb,
        do_xcom_push=False,
    )

    # Order: extract -> transform -> load
    extract_task >> transform_task >> load_task
