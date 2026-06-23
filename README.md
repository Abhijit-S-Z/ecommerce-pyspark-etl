# E-Commerce Sales Intelligence Pipeline using PySpark

## Project Overview
This project is an end-to-end batch ETL analytics pipeline built using PySpark on the Olist Brazilian E-Commerce Dataset.

The goal is to process raw e-commerce transactional data from multiple source files, transform it into analytics-ready datasets, and generate business insights.

This project simulates a real-world Data Engineering workflow involving:
- Data ingestion
- Data cleaning
- Data transformation
- Multi-table joins
- Business KPI generation
- Logging and monitoring

---

## Business Problem

An e-commerce company wants to generate business insights from raw transactional data spread across multiple source systems.

Key business questions:
- Identify top customers by revenue
- Generate monthly revenue trends
- Analyze revenue by city and state
- Find most used payment methods
- Identify delayed orders
- Generate top-selling product categories
- Track customer order frequency

---

## Dataset

Dataset used:
Olist Brazilian E-Commerce Dataset

Files used:
- olist_customers_dataset.csv
- olist_orders_dataset.csv
- olist_order_items_dataset.csv
- olist_products_dataset.csv
- olist_order_payments_dataset.csv

---

## Tech Stack

- Python
- PySpark
- Spark SQL
- VS Code
- Git & GitHub

---

## Project Structure

```bash
ecommerce-pyspark-project/
│
├── config/
│   └── config.ini
│
├── logs/
│
├── output/
│
├── source_files/
│
├── main.py
├── utils.py
├── schema.py
├── requirements.txt
└── README.md
```

---

## ETL Pipeline Flow

```text
Raw CSV Files
    ↓
Data Ingestion
    ↓
Data Cleaning
    ↓
Data Transformation
    ↓
Data Joining
    ↓
Business KPI Generation
    ↓
Output Layer
```

---

## Key Features

### Data Ingestion
- Read multiple CSV files using PySpark
- Schema-driven ingestion
- Config-driven paths

### Data Cleaning
- Null handling
- Data validation
- Data type validation

### Data Transformation
- Derived columns
- Revenue calculation
- Date-based transformations

### Data Joins
- Inner joins
- Multi-table joins

### Analytics
Generated business KPIs:
- Customer revenue analysis
- Monthly revenue trends
- Geographic revenue analysis
- Payment analysis
- Delivery performance analysis

### Logging
- Pipeline execution logs
- Error tracking
- Monitoring support

---

## Business KPIs Generated

### 1. Top Customers by Revenue
Identify customers contributing maximum revenue.

### 2. Monthly Revenue Trends
Analyze revenue growth month-over-month.

### 3. Revenue by City and State
Identify top-performing regions.

### 4. Most Used Payment Methods
Analyze customer payment preferences.

### 5. Delayed Orders
Track delayed deliveries and delay metrics.

### 6. Top-Selling Product Categories
Identify highest revenue generating product categories.

### 7. Customer Order Frequency
Identify repeat and frequent customers.

---

## Key Learnings

- PySpark DataFrame operations
- Schema management
- Spark joins and aggregations
- Batch ETL pipeline design
- Logging and debugging
- Data Engineering best practices

---

## Future Improvements

- Refactor KPI-specific joins for better optimization
- Add parquet output support
- Improve Spark optimization using cache/persist
- Move logic into modular layers (ingestion, cleaning, analytics)
- Deploy pipeline on Databricks or cloud environment

---

## How to Run

Install dependencies:
```bash
pip install -r requirements.txt
```

Run project:
```bash
python main.py
```

---

## Author

Abhijit Zagade  
Data Engineer | PySpark | SQL | ETL | Python
