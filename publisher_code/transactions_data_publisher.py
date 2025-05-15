from faker import Faker
import pandas as pd
import uuid
import random
from datetime import datetime, timedelta

def generate_fake_transactions(n_records=1000, start_date=None, end_date=None):
    fake = Faker()
    # Default to generating transactions over the past year
    if end_date is None:
        end_date = datetime.now()
    if start_date is None:
        start_date = end_date - timedelta(days=365)
    
    records = []
    for _ in range(n_records):
        transaction_dt = fake.date_time_between(start_date=start_date, end_date=end_date)
        time_id = int(transaction_dt.strftime("%Y%m%d%H%M%S"))  # example surrogate time_id
        
        record = {
            "transaction_id": str(uuid.uuid4()),
            "order_id": uuid.uuid4().hex[:8] if random.random() < 0.8 else None,  # 80% have an order
            "customer_id": fake.random_int(min=1, max=5000),
            "merchant_id": fake.bothify(text="MRC####"),
            "time_id": time_id,
            "amount": round(random.uniform(5.0, 500.0), 2),
            "transaction_fee": round(random.uniform(0.5, 5.0), 2),
            "tax_amount": round(random.uniform(0.0, 50.0), 2),
            "discount_amount": round(random.uniform(0.0, 30.0), 2),
            "risk_score": round(random.uniform(0, 100), 1),
            "status": random.choice(["completed", "failed", "pending"]),
            "payment_method": random.choice(["credit_card", "paypal", "bank_transfer", "apple_pay"]),
            "gateway": random.choice(["Stripe", "PayPal", "Square"]),
            "ip_address": fake.ipv4_public(),
            "device_fingerprint": fake.sha1(),
            "session_id": fake.uuid4(),
            "campaign_id": fake.bothify(text="CMP####"),
            "is_recurring": fake.boolean(chance_of_getting_true=10),
            "transaction_date": transaction_dt,
            "created_at": transaction_dt + timedelta(seconds=random.randint(0, 300)),
            "updated_at": transaction_dt + timedelta(seconds=random.randint(301, 600))
        }
        records.append(record)
    
    df = pd.DataFrame(records)
    return df

if __name__ == "__main__":
    # Generate 5,000 fake transactions
    df_transactions = generate_fake_transactions(n_records=5000)
    
    # Show first few rows
    print(df_transactions.head())
    
    # Export to CSV (or any other format)
    df_transactions.to_csv("fact_online_transactions_fake.csv", index=False)
    print("Exported to fact_online_transactions_fake.csv")
