from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from datetime import datetime, timedelta


args = {
    "owner": "Anna Bobkova",
    "retries": 5,
    "retry_delay": timedelta(minutes=5),

}

def hello_world():
    print('Hello, Airflow!')
    

dag = DAG(
    dag_id='file_sensor_example',
    default_args=args,
    description='This is a test file sensor dag',
    start_date=datetime(2023,4,28),
    schedule_interval='@daily',
    tags=['test']
)

sensing_task = FileSensor(
    task_id='sensing_task',
    filepath='/tmp/temporary_file_for_testing',
    fs_conn_id='my_filepath',
    poke_interval=10
)

task1 = PythonOperator(
    task_id='python',
    python_callable=hello_world,
    dag=dag
)


sensing_task>>task1
