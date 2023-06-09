"""
README
This is a Directed Acyclic Graph (DAG) implemented using Apache Airflow that fetches pollen data and provides a daily report via email.

The pipeline is straightforward and performs the following steps:

1.Check that the pollen data API is accessible.
2.Retrieve the latest pollen data from the API.
3.Store the retrieved data in an SQLite database.
4.Compare the current day's pollen data with the data from the previous day and the average data from the last week.
5.Generate an email report that includes the current day's data, a comparison with the previous day's data, and a comparison with the last week's average data.
6.Send the report to the specified email address.
This pipeline uses an API that provides real-time pollen data for Moscow, Russia. The data includes the count and risk of different types of pollen, such as grass pollen, tree pollen, and weed pollen. The pipeline also uses SQLite for simplicity.

Please note that this pipeline requires certain configurations and dependencies, including the correct setup of Apache Airflow, SMTP configurations for sending out emails. Please make sure to check and satisfy these requirements before running the pipeline.
"""

import requests
from datetime import timedelta, timedelta
from airflow import DAG
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import json
import sqlite3

def create_database():
    conn = sqlite3.connect('pollen_data.db')
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS pollen_data (
            date TEXT PRIMARY KEY,
            grass_pollen INTEGER,
            tree_pollen INTEGER,
            weed_pollen INTEGER,
            grass_pollen_risk TEXT,
            tree_pollen_risk TEXT,
            weed_pollen_risk TEXT
        )
    ''')
    conn.commit()
    conn.close()

def store_pollen_data(**context):
    data = context['task_instance'].xcom_pull(task_ids='get_pollen_data_task')
    if "error" not in data:
        conn = sqlite3.connect('pollen_data.db')
        c = conn.cursor()
        for item in data["data"]:
            date = item["updatedAt"].split('T')[0]
            grass_pollen = item["Count"]["grass_pollen"]
            tree_pollen = item["Count"]["tree_pollen"]
            weed_pollen = item["Count"]["weed_pollen"]
            grass_pollen_risk = item["Risk"]["grass_pollen"]
            tree_pollen_risk = item["Risk"]["tree_pollen"]
            weed_pollen_risk = item["Risk"]["weed_pollen"]
            c.execute('''
                INSERT OR REPLACE INTO pollen_data
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (date, grass_pollen, tree_pollen, weed_pollen, grass_pollen_risk, tree_pollen_risk, weed_pollen_risk))
        conn.commit()
        conn.close()

def get_pollen_data():
    url = "https://api.ambeedata.com/latest/pollen/by-place?place=Moscow,Russia"
    headers = {"x-api-key": "<your api key here>"}
    response = requests.get(url, headers=headers)
    data = response.json()
    if response.status_code == 200:
        return data
    else:
        return {"error": "API request failed"}


def prepare_email_content(**context):
    data = context['task_instance'].xcom_pull(task_ids='get_pollen_data_task')
    conn = sqlite3.connect('pollen_data.db')
    c = conn.cursor()
    if "error" not in data:
        email_content = f"<h2>Pollen Data for Today</h2>"
        email_content += f"<p><strong>Latitude:</strong> {data['lat']}</p>"
        email_content += f"<p><strong>Longitude:</strong> {data['lng']}</p>"
        for item in data["data"]:
            email_content += "<hr>"
            date = item["updatedAt"].split('T')[0]
            # Get the data of the previous day
            c.execute("SELECT * FROM pollen_data WHERE date = date(?, '-1 day')", (date,))
            previous_day_data = c.fetchone()
            # Get the average of the last 7 days
            c.execute("SELECT avg(grass_pollen), avg(tree_pollen), avg(weed_pollen) FROM pollen_data WHERE date BETWEEN date(?, '-7 day') AND date(?, '-1 day')", (date, date))
            last_7_days_average = c.fetchone()
            email_content += "<p><strong>Counts:</strong></p>"
            for i, type in enumerate(["grass_pollen", "tree_pollen", "weed_pollen"]):
                count = item["Count"][type]
                email_content += f"<p>{type.title().replace('_', ' ')}: {count}"
                if previous_day_data:
                    email_content += f" (Yesterday: {previous_day_data[i+1]}"
                    if last_7_days_average:
                        email_content += f", Last 7 days average: {last_7_days_average[i]}"
                    email_content += ")"
                email_content += "</p>"
            email_content += f"<p><strong>Risks:</strong></p>"
            for type, risk in item["Risk"].items():
                email_content += f"<p>{type.title().replace('_', ' ')}: {risk}</p>"
            email_content += f"<p><strong>Species:</strong></p>"
            for type, species in item["Species"].items():
                if isinstance(species, dict):
                    email_content += f"<p>{type.title()}:<br>"
                    for subtype, count in species.items():
                        email_content += f"{subtype.title()}: {count}<br>"
                    email_content += "</p>"
                else:
                    email_content += f"<p>{type.title()}: {species}</p>"
            email_content += f"<p><strong>Updated At:</strong> {item['updatedAt']}</p>"
    else:
        email_content = f"<h2>Error</h2><p>{data['error']}</p>"
    conn.close()
    return email_content

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['dmitriiKocheshkov@yandex.ru'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'nofify_pollen_data_everyday_dag',
    default_args=default_args,
    description='Pollen monitoring DAG',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    catchup=False,
    tags=['pollen'],
) as dag:

    create_database_task = PythonOperator(
        task_id='create_database_task',
        python_callable=create_database,
        dag=dag
    )

    store_pollen_data_task = PythonOperator(
    task_id='store_pollen_data_task',
    python_callable=store_pollen_data,
    dag=dag
    )

    get_pollen_data_task = PythonOperator(
        task_id='get_pollen_data_task',
        python_callable=get_pollen_data,
        dag=dag
    )

    send_email_task = EmailOperator(
        task_id='send_email',
        to='dmitriiKocheshkov@yandex.ru',
        subject='Daily Pollen Data',
        html_content="{{ task_instance.xcom_pull(task_ids='prepare_email_content_task') }}",
        dag=dag,
    )

    prepare_email_content_task = PythonOperator(
        task_id='prepare_email_content_task',
        python_callable=prepare_email_content,
        dag=dag
    )

create_database_task >> get_pollen_data_task >> prepare_email_content_task >> send_email_task >> store_pollen_data_task
