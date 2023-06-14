from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 6, 9),
    'email': ['your-email@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'my_dag', default_args=default_args, schedule_interval=timedelta(days=1))

def my_python_func():
    print('Running Python function')

t1 = BashOperator(
    task_id='bash_task',
    bash_command='echo "Hello, World!"',
    dag=dag)

t2 = PythonOperator(
    task_id='python_task',
    python_callable=my_python_func,
    dag=dag)

t3 = EmailOperator(
    task_id='send_email',
    to='your-email@example.com',
    subject='Daily report generated',
    html_content='Body of the email',
    dag=dag)

t1 >> t2 >> t3
