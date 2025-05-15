from faker import Faker
import random
import uuid
import json
from datetime import datetime, timedelta
import pandas as pd
fake = Faker()
Faker.seed(42)
random.seed(42)

# Pre-defined recurring merchants
recurring_merchants = ["Netflix", "Amazon Prime", "Spotify", "Hulu", "Dropbox"]
non_recurring_merchants = ["Walmart", "Target", "Starbucks", "Uber", "Airbnb"]
merchant_categories = {
    **{m: "Subscription" for m in recurring_merchants},
    **{m: "Retail" for m in non_recurring_merchants}
}

def generate_transaction(idx):
    txn_date = fake.date_time_between(start_date="-90d", end_date="now")
    is_rec = random.random() < 0.2  # 20% recurring
    merchant = random.choice(recurring_merchants if is_rec else non_recurring_merchants)
    amount = round(random.uniform(5, 500), 2)
    tax_rate = 0.1 if merchant_categories[merchant] == "Subscription" else 0.08
    tax_amount = round(amount * tax_rate, 2)
    discount = round(amount * random.choice([0, 0.05, 0.1]), 2)
    customer_id = fake.uuid4()
    
    # Rolling metrics placeholders
    days_since_last = random.randint(1, 60)
    txn_count_30d = random.randint(1, 20)
    avg_txn_val_30d = round(random.uniform(20, 200), 2)
    
    data = {
        "transaction_id": str(uuid.uuid4()),
        "customer_id": customer_id,
        "merchant": merchant,
        "merchant_category_code": merchant_categories[merchant],
        "is_recurring": is_rec,
        "transaction_date": txn_date.isoformat(),
        "amount": amount,
        "tax_amount": tax_amount,
        "discount_amount": discount,
        "payment_method": random.choice(["credit_card", "debit_card", "paypal", "apple_pay"]),
        "gateway": random.choice(["Stripe", "PayPal", "Adyen", "Braintree"]),
        "status": random.choice(["success", "failed", "pending"]),
        "currency": "USD",
        "exchange_rate": 1.0,
        "device_type": random.choice(["desktop", "mobile", "tablet"]),
        "transaction_channel": random.choice(["web", "mobile_app", "in_store"]),
        "ip_address": fake.ipv4(),
        "user_agent": fake.user_agent(),
        "utm_source": random.choice(["google", "facebook", "newsletter", "affiliate"]),
        "utm_medium": random.choice(["cpc", "email", "organic", "referral"]),
        "utm_campaign": f"campaign_{random.randint(1,10)}",
        "utm_term": fake.word(),
        "utm_content": fake.credit_card_provider(),
        "risk_score": random.uniform(0, 1),
        "ip_risk_score": random.uniform(0, 1),
        "email_risk_score": random.uniform(0, 1),
        "days_since_last_txn": days_since_last,
        "txn_count_last_30d": txn_count_30d,
        "avg_txn_value_last_30d": avg_txn_val_30d,
        "day_of_week": txn_date.weekday(),
        "hour_of_day": txn_date.hour
    }

    return data

# Generate and save 1000 transactions
transactions = [generate_transaction(i) for i in range(1000)]
df_transactions = pd.DataFrame(transactions)
# with open("transactions.json", "w") as f:
#     json.dump(transactions, f, indent=2)
 # Export to CSV (or any other format)
print(df_transactions.columns)
# df_transactions.to_csv("fact_online_transactions.csv", index=False)

print("Generated 1000 synthetic transactions to transactions.json")
