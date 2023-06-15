import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.sensors.file_sensor import FileSensor

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(hours=1),
    'email_on_failure': False,
    'email_on_retry': False
}

# Create the DAG
dag = DAG(
    'sensor',
    default_args=default_args,
    description='A CSV processing pipeline',
    schedule_interval=timedelta(hours=1),
    catchup=False
)

data_share = {}


def do_something():
    print("something done...")


sensor_task = FileSensor(
    task_id="sensor_task",
    filepath="result.txt",
    fs_conn_id="mfs",
    poke_interval=10,
    dag=dag
)

do_something_task = PythonOperator(
    task_id="do_something_task",
    python_callable=do_something,
    dag=dag
)

# Set up task dependencies
sensor_task >> do_something_task
