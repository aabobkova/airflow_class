from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.contrib.sensors.file_sensor import FileSensor
import kaggle

def download(dataset_name, dir):
    kaggle.api.authenticate()
    kaggle.api.dataset_download_files(dataset_name, path = dir, unzip = True)

def retrieve_logs(**context):
    log_messages = context['ti'].xcom_pull(task_ids='spark_analytic').replace("||", "\n")
    print("Retrieved logs: ", log_messages)
    return log_messages

def send_email(message, **context):    
    send_email = EmailOperator(
        task_id='send_email_system',
        to='ssuperdeveloper@mail.ru',
        subject='Spark results',
        html_content='<p>{}</p>'.format(message),
        files=['/Users/germandilio/airflow/results/correlation_matrix_all_data.png', '/Users/germandilio/airflow/results/correlation_matrix_only_mathscore.png', '/Users/germandilio/airflow/results/correlation_matrix_only_readingscore.png', '/Users/germandilio/airflow/results/correlation_matrix_only_writingscore.png', '/Users/germandilio/airflow/results/Correlation_with_MathScore_without_other_scores.png', '/Users/germandilio/airflow/results/Correlation_with_MathScore.png', '/Users/germandilio/airflow/results/Correlation_with_ReadingScore_without_other_scores.png', '/Users/germandilio/airflow/results/Correlation_with_ReadingScore.png', '/Users/germandilio/airflow/results/Correlation_with_WritingScore_without_other_scores.png', '/Users/germandilio/airflow/results/Correlation_with_WritingScore.png'],
        dag=dag
    )
    send_email.execute(context=context)


default_args = {
    'owner': 'germandilio & elviz',
    'start_date': datetime(2023, 6, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False
}

# Create the DAG
dag = DAG(
    'group_project_analytics',
    default_args=default_args,
    description='Pipiline used for download specified dataset, run analysis in spark, save results drive and send email',
    schedule='0 * * * *',
    catchup=False
)


download_dataset = PythonOperator(
    task_id='download_dataset',
    python_callable=download,
    op_args=['desalegngeb/students-exam-scores', '/Users/germandilio/airflow/dags/dags/grades/'],
    dag=dag
)

dataset_existanse_sensor = FileSensor(
    task_id="is_dataset_available",
    filepath="/Users/germandilio/airflow/dags/grades/Expanded_data_with_more_features.csv",
    poke_interval=5,
    timeout=20,
    dag=dag
)

spark_analytics_bash = BashOperator(
    task_id='spark_analytic',
    bash_command='python /Users/germandilio/airflow/scripts/analysis.py | tr \'\n\' \'||\'',
    dag=dag
)

retrieve_results_from_logs = PythonOperator(
    task_id='extract_results',
    python_callable=retrieve_logs,
    provide_context=True,
    dag=dag
)

send_email_task = PythonOperator(
    task_id='send_email',
    python_callable=send_email,
    op_args=[retrieve_results_from_logs.output],
    provide_context=True,
    dag=dag
)

# Set up task dependencies
download_dataset >> dataset_existanse_sensor >> spark_analytics_bash >> retrieve_results_from_logs >> send_email_task