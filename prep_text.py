import json
import string
from datetime import datetime, timedelta

import nltk
import numpy as np
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from nltk.stem.snowball import SnowballStemmer

# get russian stop words
nltk.download('stopwords')
stop_words = nltk.corpus.stopwords.words('russian')

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False
}

# Create the DAG
dag = DAG(
    'pipeline',
    default_args=default_args,
    description='A CSV processing pipeline',
    schedule_interval=timedelta(minutes=1),
    catchup=False
)

data_share = {}


def extract(**kwargs):
    ti = kwargs['ti']
    # with open("/files/train.json", encoding="utf-8") as json_file:
    with open("/usr/local/airflow/dags/files/train.json", encoding="utf-8") as json_file:
        data = json.load(json_file)
    ti.xcom_push("data", data)


# Function to transform the extracted data
def clear(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids="extract-task", key="data")
    dates = [str(x) for x in np.arange(1900, 2023)],

    texts = []

    word_tokenizer = nltk.WordPunctTokenizer()
    for item in data:
        # collect nlabels of news
        text_lower = item['text'].lower()  # convert words in a text to lower case
        tokens = word_tokenizer.tokenize(text_lower)  # splits the text into tokens (words)

        # remove punct and stop words from tokens
        tokens = [word for word in tokens if
                  (word not in string.punctuation and word not in stop_words and word not in dates)]

        texts.append(tokens)  # collect the text tokens

    ti.xcom_push("texts", texts)


def normalization(**kwargs):
    ti = kwargs['ti']
    texts = ti.xcom_pull(task_ids="clear_task", key="texts")
    stemmer = SnowballStemmer("russian")
    stemmed_texts = []

    for i, item in enumerate(texts):
        stemmed_texts.append([])
        for text in item:
            stemmed_texts[i].append(' '.join([stemmer.stem(x) for x in text.split(' ')]))

    ti.xcom_push("stemmed_texts", stemmed_texts)


def load(**kwargs):
    ti = kwargs['ti']
    result = ti.xcom_pull(task_ids="normalization_task", key="stemmed_texts")
    with open("/usr/local/airflow/dags/files/result.txt", "w") as result_file:
        for item in result:
            line = " ".join(item)
            result_file.write(line + "\n")

    print(result)


extract_task = PythonOperator(
    task_id='extract-task',
    python_callable=extract,
    provide_context=True,
    dag=dag
)

clear_task = PythonOperator(
    task_id='clear_task',
    python_callable=clear,
    provide_context=True,
    dag=dag
)

normalization_task = PythonOperator(
    task_id='normalization_task',
    python_callable=normalization,
    provide_context=True,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_task',
    python_callable=load,
    provide_context=True,
    dag=dag
)

# send_email_task = EmailOperator(
#     task_id='email_task',
#     to="n.d.2002z@gmail.com",
#     subject="Airflow",
#     html_content="Some html"
# )

# Set up task dependencies
extract_task >> clear_task >> normalization_task >> load_task
