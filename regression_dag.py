"""
        README
This is DAG which implements basic Machine learning infrastructure.
Pipeline is pretty simple:
1. Check that web resourse is available
2. Download dataset from web resource
3. Preprocess dataset and save it
4. Whait for data to de saved.
5. Whait for parameters to be set.
6. Train blob of models and save to db their results
7. Report success result to email

It's obvious that this flow is far from production but
I tried more to understand basics of how Apache airflow works
than to build something production-ready.

For example as web resource i used google drive, since it's
pretty simple and reliable. As dataset I chose classic 
vine quality dataset from Kaggle. And obviously here
could be used more sophisticated deep learning model
but it will delay debug and development of a flow.

"""
from airflow import DAG
from airflow.sensors.bash import BashSensor
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta
from sklearn.neural_network import MLPRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error
import pandas as pd
import pickle
import uuid
# Start with some imports :)

# Our bash script to check availability of web service
BASH_CHECHER = """
response=$(curl -s -o /dev/null -w "%{http_code}" "https://drive.google.com/file/d/1s5PYVTzkSB8FN8dtyP8E01TSVj4jsJMB/view?usp=sharing")

if [ "$response" -eq 200 ]; then
  exit 0
else
  exit -1
fi
"""

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 6, 9),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'regression_dag',
    default_args=default_args,
    description='Train basic regression on dataset and save metrics.',
    schedule_interval=timedelta(days=1),
)

t1 = BashSensor(
    task_id='is_data_available',
    bash_command=BASH_CHECHER,
    dag=dag
)

t2 = BashOperator(
    task_id='download_dataset',
    bash_command='curl -L -o "result.csv" "https://drive.google.com/uc?export=download&id=1s5PYVTzkSB8FN8dtyP8E01TSVj4jsJMB"',
    dag=dag,
)


def preprocess_data(**kwargs):
    # Load the data
    df = pd.read_csv('result.csv')

    # Handle missing values - we'll just drop them here
    df = df.dropna()

    # Convert data types if necessary
    # Here, we'll ensure that all quality ratings are integers and replace wine type with numbers
    df['quality'] = df['quality'].astype(int)
    df['type'] = df['type'].apply(lambda x: 1 if x == 'white' else 0)

    # Save the preprocessed data (please set your own path here)
    df.to_csv(
        '/Users/lohmat/Desktop/hseDistributed/temp/result_preprocessed.csv', index=False)


t3 = PythonOperator(
    task_id='preprocess_data',
    python_callable=preprocess_data,
    dag=dag,
)

t4 = FileSensor(task_id='data_sensor', filepath='/Users/lohmat/Desktop/hseDistributed/temp/result_preprocessed.csv', dag=dag)
t5 = FileSensor(task_id='params_sensor',
    filepath='/Users/lohmat/Desktop/hseDistributed/temp/params.txt',dag=dag)


def train_blob(**kwargs):
    # Establish a connection to the database
    hook = PostgresHook(postgres_conn_id='my_postgres_connection')

    # Create table if it is a first flow run
    hook.run(
        """
        create table if not exists model_results
        (
            id text primary key,
            mae_result float not null,
            stamp text
        );
        """
    )

    # Load preprocessed dataset
    df = pd.read_csv(
        '/Users/lohmat/Desktop/hseDistributed/temp/result_preprocessed.csv')

    train_labels = df['quality']
    train_data = df.loc[:, df.columns != 'quality']

    X_train, X_test, y_train, y_test = train_test_split(
        train_data, train_labels, test_size=0.2, random_state=1)

    with open('/Users/lohmat/Desktop/hseDistributed/temp/params.txt') as f:
        count = 0
        while True:
            line = f.readline()
            if not line:
                break
            args = line.split(';')

            clf = MLPRegressor(hidden_layer_sizes=int(args[0]), alpha=float(
                args[1]), activation=args[2].strip(), solver=args[3].strip(), random_state=1)

            # Train model
            clf = clf.fit(X_train, y_train)

            mae = mean_absolute_error(y_test, clf.predict(X_test))

            # Insert data
            hook.run(
                'insert into model_results (id, mae_result, stamp) values (%(id)s, %(mae_result)s, %(stamp)s)',
                parameters={'id': str(count), 'mae_result': mae,
                            'stamp': datetime.now().isoformat()},
            )
            count += 1


t6 = PythonOperator(
    task_id='train_blob',
    python_callable=train_blob,
    dag=dag,
)


# Send an email upon successful completion
t7 = EmailOperator(
    task_id='send_email_report',
    to='lokhmatikov.htc@gmail.com',
    subject='Airflow Alert',
    html_content='''<h3>Email alert</h3><br>
        DAG: {dag}<br>
        Task: {task}<br>
        Succeeded on: {ds}<br>'''.format(dag=dag.dag_id, task=t5.task_id, ds='{{ ds }}'),
    dag=dag,
    trigger_rule='all_success',
)

t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7
