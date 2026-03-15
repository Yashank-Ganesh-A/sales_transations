-- Data Warehouse Schema for Final Project
-- Two sources: object storage (sales_transactions) + on-premise (products)
-- Pipeline merges them and supports business objectives.

-- Staging: raw load from object storage (sales_transactions.csv)
CREATE TABLE IF NOT EXISTS staging.sales_transactions (
    transaction_id INTEGER,
    date DATE,
    product_id INTEGER,
    customer_id INTEGER,
    quantity INTEGER,
    unit_price NUMERIC(10, 2),
    amount NUMERIC(10, 2),
    region VARCHAR(50),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Staging: raw load from on-premise (products.csv)
CREATE TABLE IF NOT EXISTS staging.products (
    product_id INTEGER PRIMARY KEY,
    product_name VARCHAR(100),
    category VARCHAR(50),
    unit_cost NUMERIC(10, 2),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Warehouse: merged sales fact (sales + product dimension attributes)
CREATE TABLE IF NOT EXISTS warehouse.fact_sales (
    transaction_id INTEGER,
    sale_date DATE,
    product_id INTEGER,
    product_name VARCHAR(100),
    category VARCHAR(50),
    customer_id INTEGER,
    quantity INTEGER,
    unit_price NUMERIC(10, 2),
    amount NUMERIC(10, 2),
    region VARCHAR(50),
    year INTEGER,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create schema if not exist (run once)
-- CREATE SCHEMA IF NOT EXISTS staging;
-- CREATE SCHEMA IF NOT EXISTS warehouse;
