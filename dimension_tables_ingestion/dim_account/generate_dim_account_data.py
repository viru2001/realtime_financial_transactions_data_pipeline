import random
import time
from faker import Faker
from datetime import date, timedelta
import json
from google.cloud import pubsub_v1, bigquery

# GCP configuration
PROJECT_ID = "financial-transactions-data"
DATASET="transactions_data"
CUSTOMER_TABLE = "dim_customer"
ACCOUNTS_TOPIC_ID = "dim_account"

# Initialize Pub/Sub publisher
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, ACCOUNTS_TOPIC_ID)

# Initialize BigQuery client
bq_client = bigquery.Client(project=PROJECT_ID)

# Initialize Faker
fake = Faker('en_IN')
fake.seed_instance(42)

# Date boundaries for accounts
OPEN_START = date(2023, 1, 1)
OPEN_END = date(2024, 12, 31)
BASE_DATE = OPEN_START  # for date_id computation

# Distribution for account_status
status_distribution = {'Open': 85, 'Closed': 10, 'Suspended': 5}
statuses = list(status_distribution.keys())
status_weights = list(status_distribution.values())

# Account types per customer segment
SEGMENT_ACCOUNT_TYPES = {
    'Retail Banking': ['Basic Savings Account', 'Basic Credit Card','Basic Debit Card'],
    'Mass Affluent': ['High-Yield Savings Account', 'Premium Credit Card','Premium Debit Card'],
    'High Net Worth Individuals (HNWIs)': ['Private Banking Account', 'Business Credit Card'],
    'SMEs': ['Business Current Account', 'Merchant Services Account'],
    'Corporate Clients': ['Corporate Current Account', 'Treasury Services Account'],
    'NRIs': ['NRE Account', 'NRO Account', 'FCNR Account'],
    'Agriculture and Rural Banking': ['Basic Savings Account Rural', 'Kisan Credit Card','Kisan Debit Card'],
    'Government and Institutional Banking': ['Government Current Account','Corporate Current Account'],
    'Young Professionals': ['Salary Account', 'Entry-Level Debit Card'],
    'Retirees': ['Senior Citizen Savings Scheme Account', 'Pension Account']
}

# Target distribution of accounts by segment
segment_distribution = {
    'Retail Banking': 45,
    'Mass Affluent': 15,
    'High Net Worth Individuals (HNWIs)': 5,
    'SMEs': 5,
    'Corporate Clients': 8,
    'NRIs': 5,
    'Agriculture and Rural Banking': 5,
    'Government and Institutional Banking': 4,
    'Young Professionals': 5,
    'Retirees': 3
}
segments = list(segment_distribution.keys())
segment_weights = list(segment_distribution.values())

# Interval for refreshing customer list (in seconds)
REFRESH_INTERVAL = 15


seen_customer_ids = set()
customers = []


# def fetch_customers():
#     """
#     Load existing customers with their segments from BigQuery.
#     """
#     query = f"SELECT customer_id, customer_segment,signup_date_id FROM `{DATASET}.{CUSTOMER_TABLE}`"
#     result = bq_client.query(query).result()
#     return [{'customer_id': row['customer_id'], 'segment': row['customer_segment'],'signup_date_id': row['signup_date_id']} for row in result]

def fetch_new_customers():
    """
    Pull all customers, but only add ones whose customer_id
    hasn't been seen before.
    """
    global seen_customer_ids, customers

    # fetch every customer record
    query = (
        f"SELECT customer_id, customer_segment, signup_date_id "
        f"FROM `{DATASET}.{CUSTOMER_TABLE}`"
    )
    result = bq_client.query(query).result()

    new_customers = []
    for row in result:
        cid = row['customer_id']
        if cid not in seen_customer_ids:
            seen_customer_ids.add(cid)
            cust = {
                'customer_id': cid,
                'segment': row['customer_segment'],
                'signup_date_id': row['signup_date_id']
            }
            new_customers.append(cust)

    if new_customers:
        customers.extend(new_customers)
        print(f"Discovered {len(new_customers)} new customers")
    return new_customers


def generate_account_records(last_id, num_records=None):
    """
    Continuously generate account records with targeted segment distribution and ensuring open_date >= signup_date.
    """
    account_id = last_id
    customers = fetch_new_customers()
    last_refresh = time.time()

    # Build buckets by segment
    def build_buckets():
        buckets = {seg: [] for seg in segments}
        for c in customers:
            seg = c['segment']
            if seg in buckets:
                buckets[seg].append(c)
        return buckets

    buckets = build_buckets()
    count = 0

    while num_records is None or count < num_records:
        # Refresh customer list periodically
        if time.time() - last_refresh >= REFRESH_INTERVAL:
            customers[:] = fetch_new_customers()
            buckets = build_buckets()
            last_refresh = time.time()
            print(f"Refreshed customer list: {len(customers)} total")

        # Choose segment based on target distribution
        seg = random.choices(segments, weights=segment_weights, k=1)[0]
        bucket = buckets.get(seg, [])
        if not bucket:
            # fallback to random customer if none in segment
            cust = random.choice(customers)
            seg = cust['segment']
        else:
            cust = random.choice(bucket)

        customer_id = cust['customer_id']
        signup_date_id = cust.get('signup_date_id', 0)
        # Compute the earliest valid open date
        signup_date = BASE_DATE + timedelta(days=signup_date_id)
        min_open = max(OPEN_START, signup_date)
        

        # Choose account type based on segment
        types = SEGMENT_ACCOUNT_TYPES.get(seg, [])
        account_type = random.choice(types) if types else 'Checking'

        # Generate open date
        open_date = fake.date_between_dates(date_start=min_open, date_end=OPEN_END)
        open_date_id = (open_date - BASE_DATE).days

        # Determine status and close date
        status = random.choices(statuses, weights=status_weights, k=1)[0]
        if status == 'Closed':
            min_close = open_date + timedelta(days=1)
            close_date = fake.date_between_dates(date_start=min_close, date_end=OPEN_END)
            close_date_id = (close_date - BASE_DATE).days
        else:
            close_date_id = None

        record = {
            'account_id': account_id,
            'customer_id': customer_id,
            'account_type': account_type,
            'open_date_id': open_date_id,
            'close_date_id': {"int":close_date_id} if close_date_id else None ,
            'account_status': status
        }
        print(record)
        # Publish
        data_bytes = json.dumps(record).encode('utf-8')
        future = publisher.publish(topic_path, data=data_bytes)
        print(f"Published message ID: {future.result()}")     
        print(f"Published acct {account_id} for cust {customer_id} (seg={seg})")

        account_id += 1
        count += 1
        time.sleep(0.5)


def main():
    # Fetch starting account_id from metadata or BigQuery
    last_account_id = 500000000
    generate_account_records(last_account_id)


if __name__ == "__main__":
    main()
