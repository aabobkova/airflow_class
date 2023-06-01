import os
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.bash_operator import BashOperator
import airflow.hooks.S3_hook
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


def remove_file(file_name, local_path):
    '''
    Removes a local file.
    '''
    file_path = os.path.join(local_path, file_name)
    if os.path.isfile(file_path):
        os.remove(file_path)
        logging.info('removed {}'.format(file_path))


def weekday_branch():
    '''
    Returns task_id based on day of week.
    '''
    if datetime.today().weekday() in range(0, 5):
        return 'ping_rbc'
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
    with open('tmpnews.txt', "w+", encoding="utf-8") as file:
        for elem, href in zip(div_text, href_text):
            replaced = re.sub("//s+", "", elem)
            print(replaced)
            file.write("{replaced} link: {link}".format(replaced=replaced, link=href))
            file.write('\n')


def load_content(file_name, local_path):
    '''
    Reads a local file to string.
    '''
    print('DEBUG: loading content')
    strCont = ""
    with open('tmpnews.txt', "w+", encoding="utf-8") as file:
        for line in file:
            print(line)
            strCnt = strCnt + line
    return strCont


date = '{{ ds_nodash }}'
file_name = 'tmpnews.txt'
cwd = os.getcwd()
local_downloads = os.path.join(cwd, 'downloads')

default_args = {
    'owner': 'whysobluebunny',
    'start_date': datetime(2023, 6, 1),
    'end_date': datetime(2023, 6, 3),
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

ping_rbc = BashOperator(
    task_id='ping_rbc',
    bash_command='ls',
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
               'file_name': file_name,
               'local_path': local_downloads},
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
    op_kwargs={'file_name': file_name,
               'local_path': local_downloads},
    task_id='remove_file',
    dag=dag)

end = DummyOperator(
    task_id='end',
    dag=dag)

start >> weekday_branch
weekday_branch >> ping_rbc
ping_rbc >> parse_web
parse_web >> send_email
send_email >> remove_file
remove_file >> end
weekday_branch >> end