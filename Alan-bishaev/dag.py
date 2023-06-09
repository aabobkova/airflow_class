import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta
import telebot

default_args = {
    'owner': 'Alan',
    'start_date': datetime(2023, 6, 1),
    'retry_delay': timedelta(seconds=30),
}

dag = DAG(
    'my_dag', default_args=default_args, schedule_interval=timedelta(days=1)
)

bash_task = BashOperator(
    task_id='bash_task',
    bash_command='echo "Hello Airflow!"',
    dag=dag,
)


def process_csv(**kwargs):
    df = pd.read_csv("~/Desktop/atp_tennis.csv")
    df = df.drop_duplicates()
    sorted_df = df.sort_values(by=df.columns[0])
    sorted_df.to_csv("~/Desktop/result.csv", index=False)


python_task = PythonOperator(
    task_id='python_task',
    python_callable=process_csv,
    dag=dag,
)


def send_bot():
    bot = telebot.TeleBot('6171117824:AAGoy-sVv2V12wlJ-IorupxqUGMGei4xAJs')
    bot.send_message(chat_id=957743253, text='CSV was created successful')


telegram_task = PythonOperator(
    task_id='telegram_task',
    python_callable=send_bot,
    dag=dag,
)

bash_task >> python_task >> telegram_task