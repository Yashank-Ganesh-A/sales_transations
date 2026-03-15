"""
Generate sample CSV data for the data pipeline project.
- Object storage: sales_transactions.csv (simulates data from S3/external storage)
- On-premise: products.csv (simulates local enterprise data)
"""
import csv
import random
from datetime import datetime, timedelta

random.seed(42)

# Products (on-premise master data)
PRODUCTS = [
    (101, "Widget A", "Electronics", 15.00),
    (102, "Widget B", "Electronics", 25.00),
    (103, "Desk Lamp", "Home", 30.00),
    (104, "Office Chair", "Furniture", 120.00),
    (105, "Notebook Set", "Stationery", 8.00),
    (106, "Monitor Stand", "Electronics", 45.00),
    (107, "Bookshelf", "Furniture", 85.00),
    (108, "Pen Pack", "Stationery", 5.00),
    (109, "Desk Organizer", "Home", 22.00),
    (110, "USB Hub", "Electronics", 35.00),
]

# Regions for sales
REGIONS = ["North", "South", "East", "West"]

def generate_transactions(n=200):
    """Generate sales transactions across 2023 and 2024."""
    rows = []
    start = datetime(2023, 1, 1)
    for i in range(1, n + 1):
        days = random.randint(0, 800)
        dt = start + timedelta(days=days)
        product_id, _, _, unit_cost = random.choice(PRODUCTS)
        quantity = random.randint(1, 5)
        unit_price = round(unit_cost * random.uniform(1.2, 2.0), 2)
        amount = round(quantity * unit_price, 2)
        customer_id = random.randint(1000, 1999)
        region = random.choice(REGIONS)
        rows.append({
            "transaction_id": i,
            "date": dt.strftime("%Y-%m-%d"),
            "product_id": product_id,
            "customer_id": customer_id,
            "quantity": quantity,
            "unit_price": unit_price,
            "amount": amount,
            "region": region,
        })
    return rows

def main():
    base = "data_sources"
    # On-premise: products
    with open(f"{base}/on_premise/products.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["product_id", "product_name", "category", "unit_cost"])
        w.writerows(PRODUCTS)
    print(f"Wrote {base}/on_premise/products.csv")

    # Object storage: sales transactions
    rows = generate_transactions(200)
    with open(f"{base}/object_storage/sales_transactions.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=rows[0].keys())
        w.writeheader()
        w.writerows(rows)
    print(f"Wrote {base}/object_storage/sales_transactions.csv")

if __name__ == "__main__":
    main()
