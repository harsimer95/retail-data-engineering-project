import os
import random
import pandas as pd
from datetime import datetime, timedelta

# Configuration

OUTPUT_FOLDER = "data/incoming"
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

today = datetime.today().date()
file_name = f"sales_{today}.csv"
file_path = os.path.join(OUTPUT_FOLDER, file_name)

TOTAL_RECORDS = 100
BAD_RECORDS = 5

# Sample values

categories = ["Technology", "Furniture", "Office Supplies"]
regions = ["East", "West", "Central", "South"]
segments = ["Consumer", "Corporate", "Home Office"]
ship_modes = ["Standard Class", "Second Class", "First Class", "Same Day"]

cities_states = [
    ("New York", "New York"),
    ("Los Angeles", "California"),
    ("Chicago", "Illinois"),
    ("Houston", "Texas"),
    ("Seattle", "Washington"),
]

# Helper functions

def format_date(date_value):
    return f"{date_value.month}/{date_value.day}/{date_value.year}"


def create_good_record(row_id):
    order_date = today
    ship_date = today + timedelta(days=random.randint(1, 7))

    sales = round(random.uniform(10, 2000), 2)
    quantity = random.randint(1, 10)
    discount = round(random.choice([0, 0.1, 0.2, 0.3, 0.4]), 2)
    profit = round(sales * random.uniform(-0.2, 0.3), 2)

    city, state = random.choice(cities_states)

    return {
        "row_id": row_id,
        "order_id": f"ORD-{today}-{row_id}",
        "order_date": order_date,
        "ship_date": ship_date,
        "ship_mode": random.choice(ship_modes),
        "customer_id": f"CUST-{random.randint(1000, 9999)}",
        "customer_name": f"Customer {row_id}",
        "segment": random.choice(segments),
        "country": "United States",
        "city": city,
        "state": state,
        "postal_code": random.randint(10000, 99999),
        "region": random.choice(regions),
        "product_id": f"PROD-{random.randint(1000, 9999)}",
        "category": random.choice(categories),
        "sub_category": random.choice(["Phones", "Chairs", "Tables", "Storage", "Binders"]),
        "product_name": f"Product {random.randint(1, 100)}",
        "sales": sales,
        "quantity": quantity,
        "discount": discount,
        "profit": profit
    }


def make_bad_record(record):
    error_type = random.choice([
        "bad_quantity",
        "bad_discount",
        "bad_sales",
        "bad_ship_date",
        "missing_quantity"
    ])

    if error_type == "bad_quantity":
        record["quantity"] = -random.randint(1, 5)

    elif error_type == "bad_discount":
        record["discount"] = round(random.uniform(1.1, 2.0), 2)

    elif error_type == "bad_sales":
        record["sales"] = -round(random.uniform(10, 500), 2)

    elif error_type == "bad_ship_date":
        record["ship_date"] = record["order_date"] - timedelta(days=random.randint(1, 3))

    elif error_type == "missing_quantity":
        record["quantity"] = None

    return record

# Generate daily sales data

records = []

for i in range(1, TOTAL_RECORDS + 1):
    record = create_good_record(i)

    if i <= BAD_RECORDS:
        record = make_bad_record(record)

    # Convert dates to M/d/yyyy format before writing CSV
    record["order_date"] = format_date(record["order_date"])
    record["ship_date"] = format_date(record["ship_date"])

    records.append(record)

df = pd.DataFrame(records)
df.to_csv(file_path, index=False)

print(f"Daily sales file generated successfully: {file_path}")
print(f"Total records generated: {len(df)}")
print(f"Bad records intentionally generated: {BAD_RECORDS}")