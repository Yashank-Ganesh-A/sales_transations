# Data Warehouse Pipeline – Final Project

**Data Engineering (Winter 2025)** – SRH Campus Hamburg

This project implements a **data pipeline** that merges **two data sources** into a **data warehouse** and fulfills **two business objectives**.

---

## Data Sources

1. **Object storage** (simulated): `data_sources/object_storage/sales_transactions.csv`  
   - Sales transactions (transaction_id, date, product_id, customer_id, quantity, unit_price, amount, region).

2. **On-premise** (local files): `data_sources/on_premise/products.csv`  
   - Product master (product_id, product_name, category, unit_cost).

The pipeline loads both into the warehouse, merges them (sales + product attributes), and then computes the business metrics below.
## github repo link: https://github.com/Yashank-Ganesh-A/sales_transations.git

---

## Business Objectives

1. **Average value of sales per year** – Average sale amount and total sales by year.  
2. **Total sales by product category** – Total sales and transaction count per category.

Results are written to the `outputs/` folder (CSV and TXT).

---

## How to Run the Project

### Option A: Run without PostgreSQL (SQLite – recommended for quick run)

No database setup needed. Uses SQLite and produces the same outputs.

```bash
# From project root
pip install pandas
python pipeline/run_pipeline_sqlite.py
```

Outputs appear in `outputs/`:
- `business_objective_1_avg_sales_per_year.csv` and `.txt`
- `business_objective_2_sales_by_category.csv` and `.txt`

---

### Option B: Run with PostgreSQL (full data warehouse)

1. **Install PostgreSQL** and create a database:

   ```bash
   createdb data_warehouse
   ```

2. **Set environment variables** (optional; defaults work for local PostgreSQL):

   ```bash
   export PGHOST=localhost
   export PGPORT=5432
   export PGDATABASE=data_warehouse
   export PGUSER=postgres
   export PGPASSWORD=postgres
   ```

3. **Install dependencies and run the Prefect pipeline:**

   ```bash
   pip install -r requirements.txt
   python pipeline/run_pipeline.py
   ```

   This will:
   - Create `staging` and `warehouse` schemas and tables
   - Load object-storage and on-premise CSVs into staging
   - Merge into `warehouse.fact_sales`
   - Compute the two business objectives and write results to `outputs/`

---

## Project Structure

```
Yashank/
├── data_sources/
│   ├── object_storage/
│   │   └── sales_transactions.csv    # Source 1
│   └── on_premise/
│       └── products.csv              # Source 2
├── sql/
│   ├── init_schemas.sql              # Create staging + warehouse schemas
│   └── schema.sql                    # Table definitions
├── pipeline/
│   ├── run_pipeline.py               # Prefect + PostgreSQL pipeline
│   └── run_pipeline_sqlite.py        # SQLite pipeline (no PostgreSQL)
├── scripts/
│   └── generate_sample_data.py       # Generates the two CSV sources
├── outputs/                           # Business objective results (and screenshots)
│   ├── business_objective_1_avg_sales_per_year.csv
│   ├── business_objective_1_avg_sales_per_year.txt
│   ├── business_objective_2_sales_by_category.csv
│   ├── business_objective_2_sales_by_category.txt
│   └── screenshots/                   # Add screenshots of outputs here
├── requirements.txt
├── prompts.txt                        # Prompts used if AI-assisted development
└── README.md
```

---

## Testing with New Data

To verify that the pipeline reflects new data:

1. Add or change rows in:
   - `data_sources/object_storage/sales_transactions.csv`
   - `data_sources/on_premise/products.csv`
2. Run the pipeline again (Option A or B).
3. Check that the files in `outputs/` show updated numbers for the two business objectives.

---

## Submission Checklist

- [x] Pipeline merges two data sources (object storage + on-premise) in a data warehouse.
- [x] At least two business objectives (average sales per year; sales by category).
- [x] README with instructions to run the project.
- [x] Outputs saved in `outputs/` (CSV and TXT).
- [ ] **You:** Add screenshots of the outputs in `outputs/screenshots/`.
- [ ] **You:** Record a short video explaining the workflow and that the pipeline runs.
- [ ] **You:** Submit via e-campus and then download the zip to confirm all files are present.

---

## Source Files for Running

All source files are in this repository:

- CSV data: `data_sources/object_storage/` and `data_sources/on_premise/`
- Pipeline code: `pipeline/run_pipeline.py` and `pipeline/run_pipeline_sqlite.py`
- SQL: `sql/init_schemas.sql` and `sql/schema.sql`
- Sample data generator: `scripts/generate_sample_data.py`

No S3 or cloud storage is required for the on-premise option; the “object storage” source is the local CSV in `data_sources/object_storage/`.
