from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


def session_to_snowflake():
    """
    Creates RAW schema, tables, a stage pointing to the public S3 bucket,
    and copies two CSVs into Snowflake using the SnowflakeHook.
    """
    hook = SnowflakeHook(snowflake_conn_id="snowflake")  # use the actual conn_id from Airflow UI
    conn = hook.get_conn()
    cur = conn.cursor()

    try:
        # 1️⃣ Create schema
        cur.execute("CREATE SCHEMA IF NOT EXISTS RAW;")
        print("✅ Schema RAW created or already exists.")

        # 2️⃣ Create tables
        cur.execute("""
            CREATE OR REPLACE TABLE RAW.USER_SESSION_CHANNEL (
                USERID INT NOT NULL,
                SESSIONID VARCHAR(64),
                CHANNEL VARCHAR(64) DEFAULT 'direct'
            );
        """)
        cur.execute("""
            CREATE OR REPLACE TABLE RAW.SESSION_TIMESTAMP (
                SESSIONID VARCHAR(64),
                TS TIMESTAMP_NTZ
            );
        """)
        print("✅ Tables created successfully.")

        # 3️⃣ Create stage for public S3 bucket
        cur.execute("""
            CREATE OR REPLACE STAGE RAW.BLOB_STAGE
            URL='s3://s3-geospatial/readonly/'
            FILE_FORMAT = (
                TYPE = CSV
                SKIP_HEADER = 1
                FIELD_OPTIONALLY_ENCLOSED_BY='"'
            );
        """)
        print("✅ Stage created successfully.")

        # 4️⃣ Copy data from S3 into tables
        copy_sqls = [
            """
            COPY INTO RAW.USER_SESSION_CHANNEL
            FROM @RAW.BLOB_STAGE/user_session_channel.csv
            ON_ERROR='CONTINUE';
            """,
            """
            COPY INTO RAW.SESSION_TIMESTAMP
            FROM @RAW.BLOB_STAGE/session_timestamp.csv
            ON_ERROR='CONTINUE';
            """
        ]
        for sql in copy_sqls:
            cur.execute(sql)

        conn.commit()
        print("✅ Data copied into both RAW tables successfully.")

    finally:
        cur.close()
        conn.close()
        print("🔒 Snowflake connection closed.")


# ---------- DAG DEFINITION ----------
default_args = {"owner": "data226", "retries": 0}

with DAG(
    dag_id="SessionToSnowflake_Hook",
    description="ETL via SnowflakeHook: create tables, stage, and copy data from S3",
    default_args=default_args,
    schedule=None,  # Airflow 3.x uses 'schedule', not 'schedule_interval'
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["etl", "snowflake", "raw"],
) as dag:

    load_task = PythonOperator(
        task_id="session_to_snowflake_hook",
        python_callable=session_to_snowflake,
    )

    load_task