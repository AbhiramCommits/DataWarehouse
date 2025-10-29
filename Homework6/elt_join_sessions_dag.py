from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


# ---------- STEP FUNCTIONS ----------

def create_analytics_schema():
    hook = SnowflakeHook(snowflake_conn_id="snowflake")
    conn = hook.get_conn()
    cur = conn.cursor()
    try:
        cur.execute("CREATE SCHEMA IF NOT EXISTS ANALYTICS;")
        conn.commit()
        print("✅ ANALYTICS schema created or already exists.")
    finally:
        cur.close()
        conn.close()


def create_session_summary():
    hook = SnowflakeHook(snowflake_conn_id="snowflake")
    conn = hook.get_conn()
    cur = conn.cursor()
    try:
        join_sql = """
        CREATE OR REPLACE TABLE ANALYTICS.SESSION_SUMMARY AS
        SELECT
            s.SESSIONID,
            s.TS AS SESSION_TIMESTAMP,
            c.USERID,
            c.CHANNEL
        FROM RAW.SESSION_TIMESTAMP s
        JOIN RAW.USER_SESSION_CHANNEL c
          ON s.SESSIONID = c.SESSIONID
        WHERE s.SESSIONID IS NOT NULL
          AND c.USERID IS NOT NULL
        QUALIFY ROW_NUMBER()
                OVER (PARTITION BY s.SESSIONID ORDER BY s.TS DESC) = 1;
        """
        cur.execute(join_sql)
        conn.commit()
        print("✅ ANALYTICS.SESSION_SUMMARY created successfully.")
    finally:
        cur.close()
        conn.close()


def check_duplicates():
    hook = SnowflakeHook(snowflake_conn_id="snowflake")
    conn = hook.get_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT SESSIONID, COUNT(*) AS DUP_COUNT
            FROM ANALYTICS.SESSION_SUMMARY
            GROUP BY SESSIONID
            HAVING DUP_COUNT > 1;
        """)
        rows = cur.fetchall()
        if rows:
            print(f"⚠️ Found duplicates: {rows}")
        else:
            print("✅ No duplicate sessions found.")
    finally:
        cur.close()
        conn.close()


# ---------- DAG DEFINITION ----------

default_args = {
    "owner": "data226",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="elt_join_sessions_dag",
    description="ELT via SnowflakeHook: Join RAW tables and create ANALYTICS.SESSION_SUMMARY",
    default_args=default_args,
    schedule=None,  # Airflow 3.x
    start_date=datetime(2025, 10, 25),
    catchup=False,
    tags=["ELT", "Snowflake", "Analytics"],
) as dag:

    # Define hook-based tasks
    create_schema_task = PythonOperator(
        task_id="create_analytics_schema",
        python_callable=create_analytics_schema,
    )

    create_summary_task = PythonOperator(
        task_id="create_session_summary",
        python_callable=create_session_summary,
    )

    check_duplicates_task = PythonOperator(
        task_id="check_duplicates",
        python_callable=check_duplicates,
    )

    # DAG flow
    create_schema_task >> create_summary_task >> check_duplicates_task
