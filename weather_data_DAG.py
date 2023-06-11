'''
# README: Weather Data Pipeline

This is a Directed Acyclic Graph (DAG) implemented using Apache Airflow that fetches weather data from OpenWeatherMap API, stores it in a SQLite database, and provides a weekly report as a CSV file.

## Pipeline Steps

The pipeline performs the following steps:

1. **API Check**: The pipeline first checks that the OpenWeatherMap API is accessible.

2. **Data Retrieval**: It then retrieves the latest weather data for the specified location from the API.

3. **Data Processing**: The raw data is then processed to extract the required fields.

4. **Data Storage**: The processed data is then stored in a SQLite database.

5. **Conditional Check**: Every time data is stored, a check is made to determine if it's Monday.

6. **Weekly Report**: If the current day is Monday, the pipeline exports the entire SQLite database to a CSV file.

The pipeline is scheduled to run every six hours. The weekly report, however, is only generated on Mondays.

## Configuration

This pipeline uses the OpenWeatherMap API that provides real-time weather data. It fetches data for the location of Moscow, Russia.

It uses SQLite for simplicity and to minimize the setup requirements. The SQLite database is named `weather_data.db` and the exported CSV file is named `weather_data.csv`.

Please note that this pipeline requires certain configurations and dependencies, including:

- Correct setup of Apache Airflow.
- An active connection to the OpenWeatherMap API named `open_weather_api`.
- SQLite should be installed and accessible where Airflow is running.

Please ensure to satisfy these requirements before running the pipeline.
'''

from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
import json
import csv
import sqlite3

def is_it_monday(**kwargs):
    if datetime.today().weekday() == 0:
        return 'export_to_csv'
    else:
        return 'dummy_task'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 6, 11),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'weather_data_pipeline',
    default_args=default_args,
    description='An Airflow DAG to fetch weather data',
    schedule_interval=timedelta(hours=6),
)


dummy_task = DummyOperator(
    task_id='dummy_task',
    dag=dag,
)

get_weather_data = SimpleHttpOperator(
    task_id='get_weather_data',
    method='GET',
    http_conn_id='open_weather_api',
    endpoint='data/2.5/weather?lat=55.7558&lon=37.6173&appid=<appid>',
    dag=dag,
)

def process_data(**kwargs):
    ti = kwargs['ti']
    raw_data = ti.xcom_pull(task_ids='get_weather_data')
    data = json.loads(raw_data)
    return data['coord']['lon'], data['coord']['lat'], data['weather'][0]['description'], data['main']['temp'], data['wind']['speed']

process_data = PythonOperator(
    task_id='process_data',
    provide_context=True,
    python_callable=process_data,
    dag=dag,
)

def insert_into_db(data, **kwargs):
    conn = sqlite3.connect('weather_data.db')
    cur = conn.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS weather_data
        (lon REAL, lat REAL, weather_description TEXT, temperature REAL, wind_speed REAL)
    ''')
    cur.execute('INSERT INTO weather_data VALUES (?, ?, ?, ?, ?)', data)
    conn.commit()
    conn.close()

insert_into_db = PythonOperator(
    task_id='insert_into_db',
    provide_context=True,
    python_callable=insert_into_db,
    op_kwargs={'data': process_data.output},
    dag=dag,
)

branching = BranchPythonOperator(
    task_id='branching',
    python_callable=is_it_monday,
    provide_context=True,
    dag=dag
)

def export_db_to_csv():
    conn = sqlite3.connect('weather_data.db')
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM weather_data")
    with open('weather_data.csv', 'w', newline='') as f_output:
        csv_output = csv.writer(f_output)
        csv_output.writerow([description[0] for description in cursor.description])  # write headers
        csv_output.writerows(cursor.fetchall())  # write data

    conn.close()

export_to_csv = PythonOperator(
    task_id='export_to_csv',
    python_callable=export_db_to_csv,
    dag=dag,
)

get_weather_data >> process_data >> insert_into_db >> branching
branching >> export_to_csv
branching >> dummy_task
