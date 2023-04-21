from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


args = {
    "owner": "Anna Bobkova",
    "catchup": False,
    "depends_on_past": False,
    "start_date": datetime(2023, 4, 18),
    #"email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

def hello_world():
    print('Hello, Airflow!')

with DAG(
    'test_dag',
    default_args=args,
    schedule_interval=None,
    description='This is a test dag',
    tags=['test']
) as dag:
    
    bash = BashOperator(
        task_id='bash_task',
        bash_command='echo Hello, Airflow!'
    )

    python = PythonOperator(
        task_id='python',
        python_callable=hello_world

    )

python.set_upstream(bash)
#bash >> python
