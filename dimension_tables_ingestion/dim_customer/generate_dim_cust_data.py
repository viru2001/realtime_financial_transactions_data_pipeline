import random
from faker import Faker
from datetime import date
import json
from google.cloud import pubsub_v1
import time


# Pub/Sub configuration
PROJECT_ID = "financial-transactions-data"
TOPIC_ID = "dim_customer"
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

# Initialize Faker with Indian locale and enable uniqueness
fake = Faker('en_IN')
fake.seed_instance(42)
fake.unique.clear()  # ensure clean slate for .unique generation

# Define desired distribution of customer_status in percentages
# Sum of values must equal 100
status_distribution = {
    'Active': 75,
    'Closed': 10,
    'Inactive': 10,
    'Suspended': 5
}

# Convert percentages to weights for random.choices
statuses = list(status_distribution.keys())
statuses_weights = list(status_distribution.values())

# Define desired distribution of customer_segments in percentages
segment_distribution = {
    'Retail Banking': 45,
    'Mass Affluent': 15,
    'High Net Worth Individuals (HNWIs)': 2,
    'SMEs': 5,
    'Corporate Clients': 8,
    'NRIs': 5,
    'Agriculture and Rural Banking': 5,
    'Government and Institutional Banking': 4,
    'Young Professionals': 8,
    'Retirees': 3
}

# Convert percentages to weights for random.choices
segments = list(segment_distribution.keys())
segments_weights = list(segment_distribution.values())


# Load location data from JSON file
with open('../data/city_state_pincode_data.json', 'r', encoding='utf-8-sig') as f:
    location_data = json.load(f)


# Base date for signup_date_id calculation
base_date = date(2023, 1, 1)

def generate_customer_data(last_id,number_of_records):
    customer_id = last_id 
    for _ in range(number_of_records):
        # Names
        first_name = fake.first_name()
        last_name = fake.last_name()

        # Email matching the name
        email = f"{first_name.lower()}.{last_name.lower()}@example.com"

        # Phone
        phone = fake.phone_number()

        # Date of birth (18–70 yrs)
        date_of_birth = fake.date_of_birth(minimum_age=18, maximum_age=70)

        # Signup date between 2023-01-02 and 2024-12-31
        signup_date = fake.date_between_dates(
            date_start=date(2023, 1, 2),
            date_end=date(2024, 12, 31)
        )
        # days since 2023-01-01
        signup_date_id = (signup_date - base_date).days

        # Pick status by your distribution
        customer_status = random.choices(statuses, weights=statuses_weights, k=1)[0]
        
        
        customer_segment = random.choices(segments, weights=segments_weights, k=1)[0]


        # Address fields
        
        # Manually construct address_line_2
        address_line_1 = f"{random.choice(['Room No.','Flat No.'])} {fake.random_int(min=1, max=999)}, {random.choice(['Apt.', 'Floor'])} {fake.random_int(min=1, max=40)}"
        address_line_2 = fake.street_name()
        # Select a random location entry
        location = random.choice(location_data)
        city = location['City']
        state = location['State']
        pincode = location['Pincode']

        record = {
            'customer_id': customer_id,
            'first_name': first_name,
            'last_name': last_name,
            'email': email,
            'phone': phone,
            'date_of_birth': date_of_birth.strftime('%Y-%m-%d'),
            'signup_date_id': signup_date_id,
            'customer_status': customer_status,
            'customer_segment':customer_segment,
            'address_line_1': address_line_1,
            'address_line_2': address_line_2,
            'city': city,
            'state': state,
            'pincode': pincode
        }
        print(record)
         # Serialize to JSON and publish
        data_bytes = json.dumps(record).encode('utf-8')
        future = publisher.publish(topic_path, data=data_bytes)
        print(f"Published message ID: {future.result()}")

         # Increament customer_id
        customer_id = customer_id + 1
        time.sleep(0.2)

if __name__ == "__main__":
    last_id=1968022735
    number_of_records = 30000
    data = generate_customer_data(last_id,number_of_records)

