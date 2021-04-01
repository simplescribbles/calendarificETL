from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from calenderific_etl import run_calenderific_etl

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2021, 3, 28),
    "email": ['youremailaddress'],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
    "calenderific_dag",
    default_args=default_args,
    description="My first DAG with ETL process!",
    schedule_interval=timedelta(days=1),
)


run_etl = PythonOperator(
    task_id='whole_calenderific_etl',
    python_callable=run_calenderific_etl,
    dag=dag,
)

run_etl