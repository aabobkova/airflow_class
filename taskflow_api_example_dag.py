from datetime import datetime, timedelta
from airflow.decorators import dag, task


args = {
    "owner": "Anna Bobkova",
    "retries": 5,
    "retry_delay": timedelta(minutes=5),

}

@dag(
    'taskflow_api_example',
    default_args=args,
    description='This is a test taskflow api dag',
    start_date=datetime(2023,4,28),
    schedule_interval='@daily',
    tags=['test']
)

def hello_world_dec():

    @task(multiple_outputs=True)
    def get_name():
        return {
            'name': 'Ivan',
            'surname': 'Ivanov'
        }

    @task()
    def get_date():
        return "2023-04-28"
    
    @task()
    def hello(name, surname, date):
        print(f"Hello, {name} {surname}!"
          f"Today is {date}")
        
    name_dict = get_name()
    date = get_date()
    hello(name=name_dict['name'],
          surname=name_dict['surname'],
           date=date)

hello_dag = hello_world_dec()
