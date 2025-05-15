import random
import time
import json
from datetime import datetime, timedelta
import pandas as pd
from faker import Faker
from google.cloud import pubsub_v1, bigquery

# ─── GCP CONFIGURATION ─────────────────────────────────────────────────────────
PROJECT_ID            = "financial-transactions-data"
DATASET               = "transactions_data"
DIM_ACCOUNT_TABLE     = "dim_account"
# TRANSACTIONS_TOPIC_ID = "fact_transactions"

# ─── REFRESH SETTINGS ───────────────────────────────────────────────────────────
# Interval (in seconds) to refresh dim_account data
REFRESH_INTERVAL_SECONDS = 5  # e.g., every 5 minutes

# ─── LOOKUP CSV FILES ───────────────────────────────────────────────────────────

MERCHANTS_CSV = "data/merchants.csv"

# ─── GLOBAL CLIENTS & STATE ─────────────────────────────────────────────────────
bq_client    = bigquery.Client(project=PROJECT_ID)
publisher    = pubsub_v1.PublisherClient()
# topic_path   = publisher.topic_path(PROJECT_ID, TRANSACTIONS_TOPIC_ID)
fake         = Faker('en_IN')
fake.seed_instance(12345)

# State containers
dim_accounts = []
last_refresh = datetime.min

# ─── DATA RANGES ─────────────────────────────────────────────────────────────────
TX_START = datetime(2024, 1, 1)
TX_END   = datetime.now()
P_RECURRING = 0.1

# ─── LOAD LOOKUP CSVs ────────────────────────────────────────────────────────────
def load_lookups():
    global recurring_merchants, non_recurring_merchants
    merchants_df = pd.read_csv(MERCHANTS_CSV)
    # Records with is_recurring == True
    recurring_df = merchants_df[merchants_df['is_recurring'] == True]

    # Records with is_recurring == False
    non_recurring_df = merchants_df[merchants_df['is_recurring'] == False]

    # Convert to JSON strings
    recurring_json = recurring_df.to_json(orient='records')
    non_recurring_json = non_recurring_df.to_json(orient='records')

    
    recurring_merchants = json.loads(recurring_json)
    non_recurring_merchants = json.loads(non_recurring_json)

    print(f"Loaded {len(recurring_merchants)} recurring merchants")
    print(f"Loaded {len(non_recurring_merchants)} non recurring merchants")



# ─── FETCH DIM_ACCOUNT DATA ──────────────────────────────────────────────────────
def load_dim_accounts():
    """
    Fetch customer_id and account_id from BigQuery and store in global list.
    """
    query = f"SELECT customer_id, account_id FROM `{DATASET}.{DIM_ACCOUNT_TABLE}`"
    rows = bq_client.query(query).result()
    global dim_accounts
    dim_accounts = [(r['customer_id'], r['account_id']) for r in rows]
    print(f"Refreshed dim_accounts: {len(dim_accounts)} records loaded.")

# ─── TRANSACTION TIMESTAMP ───────────────────────────────────────────────────────
def random_transaction_timestamp():
    span = (TX_END - TX_START).total_seconds()
    offset = random.random() * span
    return TX_START + timedelta(seconds=offset)

# ─── MAIN GENERATOR ─────────────────────────────────────────────────────────────
def generate_transactions(starting_txn_id: int, num_records: int = None):
    txn_id = starting_txn_id
    count = 0
    global last_refresh

    while num_records is None or count < num_records:
        # Refresh dim_accounts if interval elapsed
        now = datetime.now()
        if (now - last_refresh).total_seconds() >= REFRESH_INTERVAL_SECONDS:
            load_dim_accounts()
            last_refresh = now

        if not dim_accounts:
            print("No dim_account data available yet. Waiting to retry...")
            time.sleep(5)
            continue

        customer_id, account_id = random.choice(dim_accounts)
        
        P_RECURRING = 0.2
        # pick merchant so that only P_RECURRING of txns come from merchants flagged recurring
        if random.random() < P_RECURRING:
            merchant_details = random.choice(recurring_merchants)
            merchant_id = merchant_details['merchant_id']
            merchant_category_id = merchant_details['merchant_category_id']
            is_recurring = True
            
        else:
            merchant_details = random.choice(non_recurring_merchants)
            merchant_id = merchant_details['merchant_id']
            merchant_category_id = merchant_details['merchant_category_id']
            is_recurring = False


        amount   = round(random.uniform(50, 5000), 2)
        tax_amt  = round(amount * random.uniform(0, 0.18), 2)
        disc_amt = round(amount * random.uniform(0, 0.10), 2)
        total    = round(amount + tax_amt - disc_amt, 2)

        record = {
            "transaction_id":            txn_id,
            "customer_id":               customer_id,
            "account_id":                account_id,
            "merchant_id":               merchant_id,
            "merchant_category_id":      merchant_category_id,
            "is_recurring":              is_recurring,
            "transaction_datetime":      random_transaction_timestamp().isoformat(),
            "amount":                    amount,
            "tax_amount":                tax_amt,
            "discount_amount":           disc_amt,
            "total_bill_amount":         total
        }
        print(record)
        # data_bytes = json.dumps(record).encode('utf-8')
        # future = publisher.publish(topic_path, data=data_bytes)
        # print(f"Published TXN {txn_id} (cust {customer_id}, acct {account_id}) -> {future.result()}")

        txn_id += 1
        count  += 1
        time.sleep(0.1)

# ─── ENTRY POINT ─────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    load_lookups()
    load_dim_accounts()
    last_refresh = datetime.now()
    generate_transactions(starting_txn_id=1_000_000_000)
