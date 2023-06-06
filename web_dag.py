import os
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import logging
import requests
import re
from lxml import html


class ExtendedPythonOperator(PythonOperator):
    '''
    extending the python operator so macros
    get processed for the op_kwargs field.
    '''
    template_fields = ('templates_dict', 'op_kwargs')


def remove_file(file_name):
    '''
    Removes a local file.
    '''
    if os.path.isfile(file_name):
        os.remove(file_name)
        logging.info('removed {}'.format(file_name))


def weekday_branch():
    '''
    Returns task_id based on day of week.
    '''
    if datetime.today().weekday() in range(0, 5):
        return 'check_conn'
    else:
        return 'end'


def parse_web(**kwargs):
    '''
    Parses web page and gets text values by xpath from dom
    '''
    url = kwargs['url']
    xpath = "//div[contains(@class,'main__feed')]"
    response = requests.get(url)
    tree = html.fromstring(response.content)
    elems = tree.xpath(xpath)
    hrefs = tree.xpath(xpath + "/a")
    div_text = [elem.text_content() for elem in elems]
    href_text = [elem.attrib['href'] for elem in hrefs]
    news_str = ""
    with open(kwargs['file_name'], "w+", encoding="utf-8") as file:
        for elem, href in zip(div_text, href_text):
            replaced = re.sub("//s+", "", elem)
            print(replaced)
            news_str += "{replaced} link: {link}\n".format(replaced=replaced, link=href)
        file.write(news_str)
    write_db(news_str)


def write_db(data):
    postgres_hook = PostgresHook(postgres_conn_id='postgres_conn')
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("insert into news(dt, content) values (%s, %s)",
                   (datetime.today().strftime('%Y-%m-%d'),
                    data))
    conn.commit()
    cursor.close()
    conn.close()


def load_content(file_name):
    '''
    Reads a local file to string.
    '''
    print('DEBUG: loading content')
    str_cnt = ""
    with open(file_name, "w+", encoding="utf-8") as file:
        for line in file:
            print(line)
            str_cnt += line
    return str_cnt


default_args = {
    'owner': 'whysobluebunny',
    'start_date': datetime(2023, 6, 1),
    'end_date': datetime(2023, 6, 10),
    'retries': 1,
    'email': ['wsb.bart@gmail.com'],
    'retries_delay': timedelta(seconds=30)
}

dag = DAG('rbc_news_collector_dag',
          schedule_interval='@daily',
          default_args=default_args)

start = DummyOperator(
    task_id='start',
    dag=dag)

check_conn = BashOperator(
    task_id='check_conn',
    bash_command='nc -vz rbc.ru 443',
    dag=dag
)

weekday_branch = BranchPythonOperator(
    python_callable=weekday_branch,
    task_id='weekday_branch',
    dag=dag)

parse_web = PythonOperator(
    task_id='parse_web',
    python_callable=parse_web,
    op_kwargs={'url': 'https://www.rbc.ru/',
               'file_name': 'tmpnews.txt'},
    dag=dag
)

send_email = EmailOperator(
    task_id='send_email',
    to='wsb.bart@gmail.com',
    subject='Airflow Send News',
    html_content='''<h3>Email alert</h3><br>
        DAG: {dag}<br>
        Complete on: {ds}<br>'''.format(dag=dag.dag_id, ds='{{ ds }}'),
    files=['tmpnews.txt'],
    dag=dag
)

remove_file = ExtendedPythonOperator(
    python_callable=remove_file,
    op_kwargs={'file_name': 'tmpnews.txt'},
    task_id='remove_file',
    dag=dag)

end = DummyOperator(
    task_id='end',
    dag=dag)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_conn',
    sql="""
        create table if not exists news(
            dt date,
            content text
        )
        """,
    dag=dag
)

start >> weekday_branch
weekday_branch >> check_conn
check_conn >> create_table
create_table >> parse_web
parse_web >> send_email
send_email >> remove_file
remove_file >> end
weekday_branch >> end
