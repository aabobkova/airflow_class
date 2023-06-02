from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

default_args ={
    "owner":"Alan",
    'start_date': datetime(2023, 6, 1),
    'email': 'alan_bishaev@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('kaggle_table_dag', default_args=default_args, schedule_interval='* /1 * * * *')

def download_table_func():
    bash_command = 'curl -o "res.csv" "https://docs.google.com/spreadsheets/d/15nMcN0YCBDNx5S5FBvPd1RgAG1FRNe_f10dMXMCEayc/edit?usp=sharing"'

download_table = BashOperator(
    task_id='download_table',
    bash_command=download_table_func,
    dag=dag
)

def process_table_func():
    table_path = "res.csv"
    with open(table_path, 'r') as file:
        data = file.read()
    return data

process_table = PythonOperator(
    task_id='process_table',
    python_callable=process_table_func,
    dag=dag
)

send_email = EmailOperator(
    task_id='send_email',
    to='alanbishaev@gmail.com',
    subject='Table Processing Results',
    html_content='Attached is the processed table',
    files=['output_processed_table.csv'],
    dag=dag
)

download_table >> process_table >> send_email
