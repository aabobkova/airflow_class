import json
import hashlib
import requests
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

def get_ip(**kwargs):
    response = requests.get("https://api.ipify.org?format=json")
    ip = response.json()['ip']
    kwargs['ti'].xcom_push(key='ip', value=ip)
    return ip

def get_location(ip, **kwargs):
    response = requests.get(f"https://ipapi.co/{ip}/json/")
    location_data = response.json()
    kwargs['ti'].xcom_push(key='location_data', value=location_data)
    return location_data

def get_md5_hash(ip, **kwargs):
    md5_hash = hashlib.md5(ip.encode('utf-8')).hexdigest()
    return md5_hash

def print_location_data(**kwargs):
    location_data = kwargs['ti'].xcom_pull(key='location_data')
    print(json.dumps(location_data, indent=2))

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 4, 28),
}

dag = DAG(
    'ip_dag',
    default_args=default_args,
    description='A DAG with logic and data passed between tasks',
    schedule_interval=timedelta(days=1),
    catchup=False
)

task1 = PythonOperator(
    task_id='get_ip',
    python_callable=get_ip,
    provide_context=True,
    dag=dag
)

task2 = PythonOperator(
    task_id='get_location',
    python_callable=get_location,
    provide_context=True,
    op_args=[task1.output],
    dag=dag
)

task3 = PythonOperator(
    task_id='get_md5_hash',
    python_callable=get_md5_hash,
    provide_context=True,
    op_args=[task1.output],
    dag=dag
)

task4 = PythonOperator(
    task_id='print_location_data',
    python_callable=print_location_data,
    provide_context=True,
    dag=dag
)

task1 >> task2 >> task4
task1 >> task3