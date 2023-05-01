from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


args = {
    "owner": "Anna Bobkova",
    "retries": 5,
    "retry_delay": timedelta(minutes=5),

}

def hello_world(ti):
    name = ti.xcom_pull(task_ids='get_name', key='name')
    date = ti.xcom_pull(task_ids='get_date', key='date')
    print(f"Hello, {name}!"
          f"Today is {date}")
    
def get_name(ti):
    name = ti.xcom_push(key='name', value='Airflow')

def get_date(ti):
    date = ti.xcom_push(key='date', value='2023-04-28')
    

dag = DAG(
    'xcom_example',
    default_args=args,
    description='This is a test xcom dag',
    start_date=datetime(2023,4,28),
    schedule_interval='@daily',
    tags=['test']
)

task1 = PythonOperator(
    task_id='python',
    python_callable=hello_world,
    dag=dag,
    op_kwargs={'name':'Airflow','date':'2023-04-28'}
)

task2 = PythonOperator(
    task_id='get_name',
    python_callable=get_name,
    dag=dag
)
task3 = PythonOperator(
    task_id='get_date',
    python_callable=get_date,
    dag=dag
)


[task2, task3]  >> task1
