from faker import Faker
import random

def generate_transaction():
    """
    Generates a fake online financial transaction data dictionary.
    """
    fake = Faker()
    
    # Generate datetime first
    dt = fake.date_time_between(start_date="-30d", end_date="now")
    year = dt.year
    
    # transaction_id
    transaction_id = str(fake.uuid4())
    
    # customer_id
    customer_id = f"cust_{fake.random_int(min=100, max=999)}"
    
    # merchant_id
    merchant_id = f"merch_{fake.random_int(min=100, max=999)}"
    
    # time_id
    time_id = dt.strftime("%Y%m%d%H%M")
    
    # order_id
    order_id = f"order_{fake.random_int(min=100, max=999)}"
    
    # amount
    amount = round(fake.random_int(min=1000, max=100000) / 100.0, 2)
    
    # transaction_fee
    transaction_fee = round(fake.random_int(min=0, max=100) / 10.0, 2)
    
    # tax_amount
    tax_amount = round(fake.random_int(min=0, max=100) / 10.0, 2)
    
    # discount_amount
    discount_amount = round(fake.random_int(min=0, max=100) / 10.0, 2)
    
    # risk_score
    risk_score = fake.random_int(min=0, max=100)
    
    # transaction_velocity
    transaction_velocity = fake.random_int(min=1, max=10)
    
    # cart_abandonment_flag
    cart_abandonment_flag = fake.boolean()
    
    # status
    status = random.choice(["completed", "pending", "failed"])
    
    # payment_method
    payment_method = random.choice(["credit_card", "paypal", "bank_transfer"])
    
    # gateway
    gateway = random.choice(["stripe", "paypal", "bank"])
    
    # ip_address
    ip_address = fake.ipv4()
    
    # device_fingerprint
    device_fingerprint = f"device_{fake.random_int(min=100, max=999)}"
    
    # session_id
    session_id = f"session_{fake.random_int(min=100, max=999)}"
    
    # campaign_id
    campaign_id = f"camp_{year}_{fake.word()}"
    
    # utm_source
    utm_source = random.choice(["google", "facebook", "bing"])
    
    # utm_medium
    utm_medium = random.choice(["cpc", "organic", "referral"])
    
    # utm_campaign
    utm_campaign = "_".join(fake.words(nb=2))
    
    # is_loyalty_member
    is_loyalty_member = fake.boolean()
    
    # processing_time_ms
    processing_time_ms = fake.random_int(min=100, max=1000)
    
    # geolocation_country
    geolocation_country = fake.country()
    
    # geolocation_city
    geolocation_city = fake.city()
    
    # transaction_date
    transaction_date = dt.strftime("%Y-%m-%dT%H:%M:%S") + ".000Z"
    
    # created_at and updated_at
    created_at = transaction_date
    updated_at = transaction_date
    
    # Create the transaction dictionary
    transaction = {
        "transaction_id": transaction_id,
        "customer_id": customer_id,
        "merchant_id": merchant_id,
        "time_id": time_id,
        "order_id": order_id,
        "amount": amount,
        "transaction_fee": transaction_fee,
        "tax_amount": tax_amount,
        "discount_amount": discount_amount,
        "risk_score": risk_score,
        "transaction_velocity": transaction_velocity,
        "cart_abandonment_flag": cart_abandonment_flag,
        "status": status,
        "payment_method": payment_method,
        "gateway": gateway,
        "ip_address": ip_address,
        "device_fingerprint": device_fingerprint,
        "session_id": session_id,
        "campaign_id": campaign_id,
        "utm_source": utm_source,
        "utm_medium": utm_medium,
        "utm_campaign": utm_campaign,
        "is_loyalty_member": is_loyalty_member,
        "processing_time_ms": processing_time_ms,
        "geolocation_country": geolocation_country,
        "geolocation_city": geolocation_city,
        "transaction_date": transaction_date,
        "created_at": created_at,
        "updated_at": updated_at
    }
    
    return transaction

# Example usage
fake_transaction = generate_transaction()
print(fake_transaction)