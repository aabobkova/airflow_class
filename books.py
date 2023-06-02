from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from io import StringIO

default_args = {
    'owner': 'andrushchenko',
}

def process_data(ds_nodash):
    hook = S3Hook(aws_conn_id='minio_conn')
    data = hook.read_key('books.csv', 'airflow')
    df = pd.read_csv(StringIO(data), sep=',')

    most_talked_book = str(df[df['text_reviews_count'] == df['text_reviews_count'].max()]['title'].item())
    avg_rating = float(df["average_rating"].mean())
    biggest_book = str(df[df['  num_pages'] == df['  num_pages'].max()]['title'].item())

    postgres_hook = PostgresHook(postgres_conn_id='postgres_conn')
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("insert into statistics(dt, most_talked_book, arg_raiting, biggest_book) values (%s, %s, %s, %s)", (ds_nodash, most_talked_book, avg_rating, biggest_book))
    conn.commit()

    cursor.close()
    conn.close()

with DAG(
    dag_id='books',
    default_args=default_args,
    start_date=datetime(2023, 6, 2, 2),
    schedule_interval='@daily'
) as dag:
    dummy_task = BashOperator(
        task_id='dummy_task',
        bash_command='echo Start dag'
    )

    check_books_file_exist = S3KeySensor(
        task_id='sensor_minio_s3',
        bucket_name='airflow',
        bucket_key='books.csv',
        aws_conn_id='minio_conn',
        mode='poke',
        poke_interval=5,
        timeout=30
    )

    create_table = PostgresOperator(
        task_id='create_statistic_table',
        postgres_conn_id='postgres_conn',
        sql="""
        create table if not exists statistics(
            dt date,
            most_talked_book text,
            arg_raiting float,
            biggest_book text
        )
        """
    )
    
    process_books = PythonOperator(
        task_id='process_data_from_bucket',
        python_callable=process_data,
    )

    dummy_task >> [check_books_file_exist, create_table] >> process_books