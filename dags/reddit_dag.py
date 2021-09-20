from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from reddit_etl import run_reddit_etl

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.today(),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=15)
}

dag = DAG(
    'reddit_dag',
    default_args=default_args,
    description='unixporn subreddit scraper',
    schedule_interval=timedelta(days=1),
)

run_etl = PythonOperator(
    task_id='unixporn_subreddit_etl',
    python_callable=run_reddit_etl,
    dag=dag,
)

run_etl
