import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from datetime import datetime, timedelta
import telebot
import json

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


telegram_task = SimpleHttpOperator(
        task_id="telegram_task",
        endpoint="/bot6171117824:AAGoy-sVv2V12wlJ-IorupxqUGMGei4xAJs/sendMessage",
        http_conn_id="telegram_conn_id",
        method="POST",
        data=json.dumps(
            {
                "text": "Connection is successful!",
                "chat_id": "957743253",
            }
        ),
        headers={"Content-Type": "application/json"},
        response_check=lambda response: response.json()["ok"] == True,
        dag=dag,
)

bash_task >> python_task >> telegram_task