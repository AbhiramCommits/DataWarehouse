from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import requests
import json

@dag(
    schedule='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["stock_etl"],
    description="ETL DAG to fetch stock prices and load into Snowflake"
)
def stock_etl_dag():

    @task()
    def extract_stock_data():
        api_key = Variable.get("ALPHA_VANTAGE_KEY")
        symbol = "AAPL"
        url = (
            f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&"
            f"symbol={symbol}&outputsize=compact&apikey={api_key}"
        )
        response = requests.get(url)
        data = response.json()
        if "Time Series (Daily)" not in data:
            raise ValueError("Failed to fetch data from API")
        return json.dumps(data["Time Series (Daily)"])

    @task()
    def transform_data(raw_json):
        symbol = "AAPL"
        raw_data = json.loads(raw_json)
        rows = []
        for date, values in raw_data.items():
            rows.append((
                symbol,
                date,
                float(values["1. open"]),
                float(values["2. high"]),
                float(values["3. low"]),
                float(values["4. close"]),
                int(float(values["5. volume"])),
            ))
        return rows

    @task()
    def load_to_snowflake(data_rows):
        hook = SnowflakeHook(snowflake_conn_id="snowflake")
        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("DELETE FROM RAW.STOCK_PRICES")

        insert_sql = """
            INSERT INTO RAW.STOCK_PRICES (SYMBOL, DATE, OPEN, HIGH, LOW, CLOSE, VOLUME)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        cursor.executemany(insert_sql, data_rows)
        conn.commit()
        cursor.close()
        conn.close()

    raw_data = extract_stock_data()
    transformed_data = transform_data(raw_data)
    load_to_snowflake(transformed_data)


dag = stock_etl_dag()