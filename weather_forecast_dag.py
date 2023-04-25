from datetime import datetime, timedelta
import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 21),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'print_current_weather',
    default_args=default_args,
    description='Prints the current weather from an external API',
    schedule_interval=timedelta(hours=1),
)

def print_current_weather():
    response = requests.get('https://api.openweathermap.org/data/2.5/weather?units=metric&lat=55.751244&lon=37.618423&appid=2f2624c17022202b12620ce8221786d7')
    if response.status_code == 200:
        weather_data = response.json()
        temperature = weather_data['main']['temp']
        weather_description = weather_data['weather'][0]['description']
        print(f"The current temperature in Moscow is {temperature} C, with {weather_description}")
    else:
        print("Unable to retrieve weather data")

print_weather = PythonOperator(
    task_id='print_weather',
    python_callable=print_current_weather,
    dag=dag,
)

print_weather