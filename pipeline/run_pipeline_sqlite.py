"""
Same pipeline logic using SQLite (no PostgreSQL required).
Use this if PostgreSQL is not installed. Produces the same outputs in outputs/.
"""
import csv
import sqlite3
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_OBJECT_STORAGE = PROJECT_ROOT / "data_sources" / "object_storage" / "sales_transactions.csv"
DATA_ONPREMISE = PROJECT_ROOT / "data_sources" / "on_premise" / "products.csv"
OUTPUTS_DIR = PROJECT_ROOT / "outputs"
DB_PATH = PROJECT_ROOT / "warehouse.db"


def run():
    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)

    # Load from object storage
    sales = pd.read_csv(DATA_OBJECT_STORAGE)
    sales["date"] = pd.to_datetime(sales["date"])
    sales.to_sql("sales_transactions", conn, if_exists="replace", index=False)

    # Load from on-premise
    products = pd.read_csv(DATA_ONPREMISE)
    products.to_sql("products", conn, if_exists="replace", index=False)

    # Merge in warehouse (fact_sales)
    merge_sql = """
    CREATE TABLE IF NOT EXISTS fact_sales AS
    SELECT
        s.transaction_id, s.date AS sale_date, s.product_id, p.product_name, p.category,
        s.customer_id, s.quantity, s.unit_price, s.amount, s.region,
        CAST(strftime('%Y', s.date) AS INTEGER) AS year
    FROM sales_transactions s
    JOIN products p ON s.product_id = p.product_id
    """
    conn.execute("DROP TABLE IF EXISTS fact_sales")
    conn.execute(merge_sql)

    # Business objective 1: Average sales per year
    obj1 = pd.read_sql("""
        SELECT year, COUNT(*) AS num_transactions, SUM(amount) AS total_sales, AVG(amount) AS avg_sale_value
        FROM fact_sales GROUP BY year ORDER BY year
    """, conn)
    path1 = OUTPUTS_DIR / "business_objective_1_avg_sales_per_year.csv"
    path1_txt = OUTPUTS_DIR / "business_objective_1_avg_sales_per_year.txt"
    obj1.to_csv(path1, index=False)
    with open(path1_txt, "w") as f:
        f.write("Business Objective 1: Average value of sales per year\n")
        f.write("=" * 60 + "\n")
        for _, row in obj1.iterrows():
            f.write(f"Year {int(row['year'])}: Transactions={int(row['num_transactions'])}, Total Sales={row['total_sales']:,.2f}, Avg Sale Value={row['avg_sale_value']:,.2f}\n")

    # Business objective 2: Total sales by category
    obj2 = pd.read_sql("""
        SELECT category, COUNT(*) AS num_transactions, SUM(amount) AS total_sales
        FROM fact_sales GROUP BY category ORDER BY total_sales DESC
    """, conn)
    path2 = OUTPUTS_DIR / "business_objective_2_sales_by_category.csv"
    path2_txt = OUTPUTS_DIR / "business_objective_2_sales_by_category.txt"
    obj2.to_csv(path2, index=False)
    with open(path2_txt, "w") as f:
        f.write("Business Objective 2: Total sales by product category\n")
        f.write("=" * 60 + "\n")
        for _, row in obj2.iterrows():
            f.write(f"{row['category']}: Transactions={int(row['num_transactions'])}, Total Sales={row['total_sales']:,.2f}\n")

    conn.close()
    print("Pipeline (SQLite) finished. Outputs saved to:", OUTPUTS_DIR)
    print("  -", path1)
    print("  -", path1_txt)
    print("  -", path2)
    print("  -", path2_txt)


if __name__ == "__main__":
    run()
