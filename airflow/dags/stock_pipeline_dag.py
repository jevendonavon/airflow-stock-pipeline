from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine

# --- Default settings for the DAG ---
default_args = {
    "owner": "jeven",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# --- Define the DAG ---
with DAG(
    dag_id="stock_etl_pipeline",
    default_args=default_args,
    description="Daily stock ETL pipeline",
    schedule="0 9 * * *",  # runs every day at 9AM
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["stocks", "etl"],
) as dag:

    # --- Task 1: Extract ---
    def extract():
        tickers = ["AAPL", "MSFT", "GOOGL", "AMZN"]
        all_data = []
        for ticker in tickers:
            print(f"Extracting {ticker}...")
            stock = yf.Ticker(ticker)
            df = stock.history(period="6mo")
            df = df.reset_index()
            df["ticker"] = ticker
            all_data.append(df)
        combined = pd.concat(all_data, ignore_index=True)
        combined.to_csv("/tmp/raw_stocks.csv", index=False)
        print(f"✅ Extracted {len(combined)} rows")

    # --- Task 2: Transform ---
    def transform():
        df = pd.read_csv("/tmp/raw_stocks.csv")
        df = df[["Date", "Open", "High", "Low",
                 "Close", "Volume", "ticker"]].copy()
        df["Date"] = pd.to_datetime(df["Date"], utc=True).dt.date
        for col in ["Open", "High", "Low", "Close"]:
            df[col] = df[col].round(2)
        df["daily_return"] = (
            df.groupby("ticker")["Close"]
            .pct_change()
            .round(4)
        )
        df["ma_7"] = (
            df.groupby("ticker")["Close"]
            .transform(lambda x: x.rolling(window=7).mean())
            .round(2)
        )
        df = df.dropna()
        df.to_csv("/tmp/transformed_stocks.csv", index=False)
        print(f"✅ Transformed {len(df)} rows")

    # --- Task 3: Load ---
    def load():
        df = pd.read_csv("/tmp/transformed_stocks.csv")
        engine = create_engine(
            "postgresql://postgres:postgres123@localhost:5432/stock_etl"       
        )
        df.to_sql(
            name="stock_prices",
            con=engine,
            if_exists="replace",
            index=False
        )
        print(f"✅ Loaded {len(df)} rows into PostgreSQL")

    # --- Define tasks ---
    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform,
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=load,
    )

    # --- Set task order ---
    extract_task >> transform_task >> load_task