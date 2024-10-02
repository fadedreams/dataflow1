import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1, 10, 00),
}

with DAG(
    'user_automation',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:
    go_streaming_task = BashOperator(
        task_id='go_stream_data_from_api',
        bash_command=os.path.join('./dags', 'rust_streamer')
    )

