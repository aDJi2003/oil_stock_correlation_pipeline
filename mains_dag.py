from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests
from bs4 import BeautifulSoup
import pandas as pd
from sqlalchemy import create_engine

def scrape_oil_prices():
    url = "https://oilprice.com/"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, "html.parser")
            oil_prices = []
            current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            rows = soup.find_all("tr", class_="link_oilprice_row")
            for row in rows:
                oil_type = row.get("data-spread")
                price_tag = row.find("td", class_="value")
                price = price_tag.text.strip() if price_tag else None

                if oil_type and price:
                    oil_prices.append({
                        "type": oil_type,
                        "price": float(price.replace(",", "")),
                        "datetime": current_datetime
                    })

            # Save raw data to a file for sharing between tasks
            pd.DataFrame(oil_prices).to_csv("/tmp/raw_oil_data.csv", index=False)
            print("Scraping completed and data saved.")
        else:
            raise Exception(f"Gagal mengambil data, status code: {response.status_code}")

    except Exception as e:
        print(f"Error: {e}")

def transform_oil_data():
    try:
        raw_data = pd.read_csv("/tmp/raw_oil_data.csv")
        if raw_data.empty:
            print("Oil DataFrame is empty. Returning an empty DataFrame.")
            return

        raw_data['price'] = pd.to_numeric(raw_data['price'], errors='coerce')
        raw_data['price'].fillna(raw_data['price'].mean(), inplace=True)
        raw_data['price_change'] = raw_data['price'].pct_change().fillna(0) * 100
        raw_data['normalized_price'] = (raw_data['price'] - raw_data['price'].min()) / (raw_data['price'].max() - raw_data['price'].min())
        raw_data.sort_values(by='datetime', inplace=True)
        raw_data.reset_index(drop=True, inplace=True)

        raw_data.to_csv("/tmp/transformed_oil_data.csv", index=False)
        print("Transformation completed and data saved.")

    except Exception as e:
        print(f"Error during transformation: {e}")

def save_oil_to_database():
    db_config = {
        "username": "your_username",
        "password": "your_password",
        "host": "your_host",
        "port": "your_port",
        "database": "your_database"
    }
    db_url = f"postgresql+psycopg2://{db_config['username']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    engine = create_engine(db_url, pool_pre_ping=True)

    try:
        transformed_data = pd.read_csv("/tmp/transformed_oil_data.csv")
        if not transformed_data.empty:
            transformed_data.to_sql("oil_prices", engine, index=False, if_exists="append", method="multi", chunksize=1000)
            print("Data berhasil dimasukkan ke database setelah transformasi.")
        else:
            print("Data kosong, tidak ada yang disimpan ke database.")

    except Exception as e:
        print(f"Error saat menyimpan data ke database: {e}")
    finally:
        engine.dispose()

def get_stock_data(tickers):
    stock_data = []
    current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for ticker in tickers:
        url = f"https://finance.yahoo.com/quote/{ticker}/"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
        }
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, "html.parser")
            span_tag = soup.find('fin-streamer', class_="livePrice yf-1tejb6").find('span')
            detailed_tag = soup.find('fin-streamer', class_="yf-11uk5vd")

            if span_tag:
                price_tag = span_tag.text.strip()
                close_tag = detailed_tag.text.strip()
                stock_data.append({
                    "ticker": ticker,
                    "open": close_tag,
                    "price": price_tag,
                    "datetime": current_datetime
                })
        else:
            print(f"Gagal mengambil data untuk {ticker}")

    pd.DataFrame(stock_data).to_csv("/tmp/raw_stock_data.csv", index=False)
    print("Stock scraping completed.")

def transform_stock_data():
    try:
        raw_data = pd.read_csv("/tmp/raw_stock_data.csv")
        if raw_data.empty:
            print("Stock DataFrame is empty.")
            return

        numeric_columns = ['open', 'price']
        for column in numeric_columns:
            raw_data[column] = pd.to_numeric(raw_data[column], errors='coerce')
            raw_data[column].fillna(raw_data[column].mean(), inplace=True)

        raw_data['average_stock_price'] = raw_data[['open', 'price']].mean(axis=1)
        raw_data['stock_change'] = raw_data['price'] - raw_data['open']
        raw_data.sort_values(by='datetime', inplace=True)
        raw_data.reset_index(drop=True, inplace=True)

        raw_data.to_csv("/tmp/transformed_stock_data.csv", index=False)
        print("Stock transformation completed.")

    except Exception as e:
        print(f"Error during stock transformation: {e}")

def save_stock_to_database():
    db_config = {
        "username": "your_username",
        "password": "your_password",
        "host": "your_host",
        "port": "your_port",
        "database": "your_database"
    }
    db_url = f"postgresql+psycopg2://{db_config['username']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    engine = create_engine(db_url, pool_pre_ping=True)

    try:
        transformed_data = pd.read_csv("/tmp/transformed_stock_data.csv")
        if not transformed_data.empty:
            transformed_data.to_sql("company_stocks", engine, index=False, if_exists="append", method="multi", chunksize=1000)
            print("Stock data berhasil dimasukkan ke database.")
        else:
            print("Data kosong, tidak ada yang disimpan ke database.")

    except Exception as e:
        print(f"Error saat menyimpan data saham ke database: {e}")
    finally:
        engine.dispose()

default_args = {
    'owner': 'user',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='main_dag',
    default_args=default_args,
    description='ETL Pipeline untuk oil dan stock data',
    schedule_interval='*/30 * * * *',
    start_date=datetime(2024, 11, 20),
    catchup=False,
) as dag:

    scrape_oil = PythonOperator(
        task_id='scrape_oil_prices_task',
        python_callable=scrape_oil_prices
    )

    transform_oil = PythonOperator(
        task_id='transform_oil_data_task',
        python_callable=transform_oil_data
    )

    save_oil = PythonOperator(
        task_id='save_oil_to_database_task',
        python_callable=save_oil_to_database
    )

    scrape_stock = PythonOperator(
        task_id='scrape_stock_data_task',
        python_callable=get_stock_data,
        op_kwargs={'tickers': ["2222.SR", "XOM", "CVX", "SHEL", "601857.SS", "TTE"]}
    )

    transform_stock = PythonOperator(
        task_id='transform_stock_data_task',
        python_callable=transform_stock_data
    )

    save_stock = PythonOperator(
        task_id='save_stock_to_database_task',
        python_callable=save_stock_to_database
    )

    scrape_oil >> transform_oil >> save_oil >> scrape_stock >> transform_stock >> save_stock
