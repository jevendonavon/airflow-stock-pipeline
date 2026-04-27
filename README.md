# Airflow Stock Pipeline

This is my third project in my data engineering portfolio. The goal was to learn how to schedule and automate an ETL pipeline using Apache Airflow instead of running scripts manually.

## What it does

A DAG runs every day at 9AM UTC and pulls 6 months of historical stock data for 4 companies — AAPL, MSFT, GOOGL, and AMZN — using the yfinance library. It then cleans the data, calculates daily return and a 7-day moving average, and loads everything into a local PostgreSQL database.

The three tasks in the DAG map pretty directly to a standard ETL flow:

```
extract_stock_data → transform_data → load_to_postgres
```

## Stack

- Apache Airflow (task scheduling and orchestration)
- Python — yfinance, pandas, sqlalchemy
- PostgreSQL (running on Ubuntu/WSL2)

## How to run it locally

Make sure Airflow is installed and your PostgreSQL database (`stock_etl`) is up and running.

```bash
# start the scheduler and webserver
airflow scheduler &
airflow webserver

# or trigger the DAG manually from CLI
airflow dags trigger stock_pipeline
```

The DAG is set to `0 9 * * *` (every day at 9AM UTC). You can also just click "Trigger DAG" from the Airflow UI at `localhost:8080`.

## What I learned

Before this project I had no idea what Airflow was. A few things that clicked for me:

- DAGs are just Python files — you define tasks and the order they run, and Airflow handles the rest
- The difference between a scheduled run and a manual trigger
- How to pass data between tasks using XCom
- Debugging failed tasks through the Airflow UI logs (this took a while to get used to)

## Notes

This runs on WSL2 (Ubuntu) on a Windows machine, not on a cloud server, so you'd need to adjust the database connection string if you're running it somewhere else. The connection is set up in `airflow_settings` or directly in the DAG file via SQLAlchemy.

---

*Part of my data engineering learning portfolio. Previous projects: [Stock ETL Pipeline](https://github.com/jevendonavon/stock-etl-pipeline) | [Multi-Stock Dashboard](https://github.com/jevendonavon/multi-stock-dashboard)*# airflow-stock-pipeline
Automated stock ETL pipeline scheduled with Apache Airflow
