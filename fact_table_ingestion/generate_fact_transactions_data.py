import random
import time
import uuid
import json
from datetime import datetime, timedelta
import pandas as pd
from faker import Faker
from google.cloud import pubsub_v1, bigquery

# ─── GCP CONFIGURATION───────────────────────────────────────────────────────────
PROJECT_ID            = "financial-transactions-data"
DATASET               = "transactions_data"
DIM_ACCOUNT_TABLE     = "dim_account"
TRANSACTIONS_TOPIC_ID = "fact_transactions"

# ─── REFRESH SETTINGS────────────────────────────────────────────────────────────
REFRESH_INTERVAL_SECONDS = 300  # e.g., every 5 minutes

# ─── LOOKUP CSV FILE─────────────────────────────────────────────────────────────
MERCHANTS_CSV = "data/merchants.csv"
# DEVICE_TYPES_CSV        = "data/device_type.csv"
# PAYMENT_GATEWAYS_CSV    = "data/payment_gateway.csv"

# ─── GLOBAL CLIENTS & STATE──────────────────────────────────────────────────────
bq_client    = bigquery.Client(project=PROJECT_ID)
publisher    = pubsub_v1.PublisherClient()
topic_path   = publisher.topic_path(PROJECT_ID, TRANSACTIONS_TOPIC_ID)
fake         = Faker('en_IN')
fake.seed_instance(12345)

dim_accounts = []
last_refresh = datetime.min

# ─── DATE BASE───────────────────────────────────────────────────────────────────
BASE_DATE = datetime(2023, 1, 1)
TX_END    = datetime.now()

# ─── LOAD LOOKUP CSVs────────────────────────────────────────────────────────────
def load_lookups():
    global recurring_merchants, non_recurring_merchants
    merchants_df = pd.read_csv(MERCHANTS_CSV)
    recurring_df = merchants_df[merchants_df['is_recurring'] == True]
    non_recurring_df = merchants_df[merchants_df['is_recurring'] == False]
    recurring_merchants = recurring_df.to_dict(orient='records')
    non_recurring_merchants = non_recurring_df.to_dict(orient='records')
    # print(f"Loaded {len(recurring_merchants)} recurring and {len(non_recurring_merchants)} non-recurring merchants")

    # device_types_df     = pd.read_csv(DEVICE_TYPES_CSV)
    # payment_gateways_df = pd.read_csv(PAYMENT_GATEWAYS_CSV)

# ─── FETCH DIM_ACCOUNT DATA──────────────────────────────────────────────────────
def load_dim_accounts():
    """
    Fetch customer_id, account_id, account_type, open_date_id, close_date_id.
    compute actual open_date and close_date.
    close_date_id may be NULL if still open.
    """
    query = (
        f"SELECT customer_id, account_id, account_type, open_date_id, close_date_id"
        f" FROM `{PROJECT_ID}.{DATASET}.{DIM_ACCOUNT_TABLE}`"
    )
    rows = bq_client.query(query).result()
    global dim_accounts
    dim_accounts = []
    for r in rows:
        open_date = BASE_DATE + timedelta(days=r['open_date_id'])
        close_date = (BASE_DATE + timedelta(days=r['close_date_id'])) if r['close_date_id'] is not None else TX_END
        dim_accounts.append({
            'customer_id':  r['customer_id'],
            'account_id':   r['account_id'],
            'account_type': r['account_type'],
            'open_date':    open_date,
            'close_date':   close_date
        })
    print(f"Refreshed dim_accounts: {len(dim_accounts)} records loaded.")

# ─── TRANSACTION TIMESTAMP──────────────────────────────────────────────────────
def random_transaction_timestamp(open_d: datetime, close_d: datetime):
    span = (close_d - open_d).total_seconds()
    offset = random.random() * span
    return open_d + timedelta(seconds=offset)

# ─── MAIN GENERATOR──────────────────────────────────────────────────────────────
def generate_transactions( num_records: int = None):
    global last_refresh
    count = 0
    P_RECURRING = 0.2

    while num_records is None or count < num_records:
        now = datetime.now()
        if (now - last_refresh).total_seconds() >= REFRESH_INTERVAL_SECONDS:
            load_dim_accounts()
            last_refresh = now

        if not dim_accounts:
            print("No dim_account data yet; retrying...")
            time.sleep(5)
            continue

        acct = random.choice(dim_accounts)
        cust_id = acct['customer_id']
        acct_id = acct['account_id']
        acct_type = acct['account_type']
        open_d = acct['open_date']
        close_d = acct['close_date']
        acct_type_lower = acct_type.lower()

        # Decide recurring only if account_type contains credit or debit card
        is_rec = False
        merchant = None
        if ('credit card' in acct_type_lower or 'debit card' in acct_type_lower) and random.random() < P_RECURRING:
            is_rec = True
            merchant = random.choice(recurring_merchants)
            # Recurring channel: 40% POS, 60% Online Payment Gateways
            channel = 'Online Payment Gateway'
        else:
            is_rec = False
            merchant = random.choice(non_recurring_merchants)
            # Non-recurring channel: 60% Card, 10% Net Banking, 20% UPI
            r = random.random()
            if r < 0.3:
                channel = 'POS'
            elif r < 0.6:
                channel = 'Online Payment Gateway'
            elif r < 0.7:
                channel = 'Net Banking'
            else:
                channel = 'UPI'

        merch_id = merchant['merchant_id']
        mcc_id = merchant['merchant_category_id']

        # Amount logic
        if not is_rec and acct_type in ("Private Banking Account", "Business Credit Card"):
            amount = round(random.uniform(10000, 50000), 2)
        else:
            amount = round(random.uniform(50, 5000), 2)

        # Tax always
        tax_amt = round(amount * random.uniform(0, 0.18), 2)
        # Discount 10% chance
        disc_amt = round(amount * random.uniform(0, 0.10), 2) if random.random() < 0.1 else 0.0
        total = round(amount + tax_amt - disc_amt, 2)

        txn_dt = random_transaction_timestamp(open_d, close_d)

         # If channel is POS or Online Payment Gateway, add card details
        if channel in ("POS", "Online Payment Gateway"):
            card_number = fake.credit_card_number()
            card_bin = card_number[:6]
            card_provider= fake.credit_card_provider()
        else :
            card_number = None
            card_bin = None
            card_provider= None


        # transaction_channel-based payment_gateway_id
        other_pg_ids  = [3, 4, 6, 7, 8, 9, 10]
        if channel in ("POS", "Online Payment Gateway"):
            r_pg = random.random()
            if r_pg < 0.30:
                payment_gateway_id = 1   # Razorpay
            elif r_pg < 0.20:
                payment_gateway_id = 2   # Stripe
            elif r_pg < 0.10:
                payment_gateway_id = 5   # BillDesk
            else:
                payment_gateway_id = random.choice(other_pg_ids)
        else:
            payment_gateway_id = None

        # device_type_id for all channels
        r_dev = random.random()
        if channel == "POS":
            device_type_id = 7       # POS terminal
        elif r_dev < 0.50:
            device_type_id = 1       # Android Mobile
        elif r_dev < 0.60:
            device_type_id = 2       # Desktop Web
        elif r_dev < 0.80:
            device_type_id = 3       # iOS Mobile
        elif r_dev < 0.85:
            device_type_id = 4       # Android Tablet
        elif r_dev < 0.94:
            device_type_id = 5       # Mobile Web
        else:
            device_type_id = 6       # iPad


        # fake IP 
        ip_address = fake.ipv4()
        # risk_score
        if random.random() < 0.20:
            risk_score = round(random.uniform(0.60, 1.00), 2)
        else:
            risk_score = round(random.uniform(0.00, 0.60), 2)

        txn_id = uuid.uuid4().hex[:20]
        record = {
            "transaction_id":            txn_id,
            "customer_id":               cust_id,
            "account_id":                acct_id,
            "merchant_id":               merch_id,
            "merchant_category_code_id": mcc_id,
            "is_recurring":              is_rec,
            "transaction_datetime":      txn_dt.isoformat(),
            "amount":                    amount,
            "tax_amount":                tax_amt,
            "discount_amount":           disc_amt,
            "total_amount":         total,
            "transaction_channel":       channel,
            'card_number' :{"string": card_number} if card_number else None ,
            "card_bin" :{"string": card_bin} if card_bin else None,
            "card_provider":{"string":  card_provider} if card_provider else None,
            "payment_gateway_id" :{"int": payment_gateway_id} if payment_gateway_id else None,
            "device_type_id" : device_type_id,
            "ip_address": ip_address,
            "risk_score" : risk_score
        }

        print(record)
        data_bytes = json.dumps(record).encode('utf-8')
        future = publisher.publish(topic_path, data=data_bytes)
        print(f"Published TXN {txn_id} (cust {cust_id}, acct {acct_id}) -> {future.result()}")
        count  += 1
        time.sleep(0.1)

# ─── ENTRY POINT────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    load_lookups()
    load_dim_accounts()
    last_refresh = datetime.now()
    generate_transactions()
