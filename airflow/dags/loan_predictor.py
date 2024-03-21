import logging
from datetime import datetime, timedelta

from datetime import datetime, timedelta
from airflow import DAG
import os
from airflow.operators.bash_operator import BashOperator
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils import dates
from dotenv import find_dotenv, load_dotenv


logger = logging.getLogger('airflowLogger')

load_dotenv(find_dotenv())

default_args = {
    
    'owner': 'alicja', #os.environ.get('airflow_owner'),
    'description': 'Loan prediction setup',
    'depend_on_past': False,
    'start_date': datetime(2024, 3, 11),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id='loans_setup',
    start_date=dates.days_ago(2),
    default_args=default_args,
)

t1 = DockerOperator(
    task_id='database_creator',
    image='database_creator',
    auto_remove=True,
    #env_file='.env', set this path
    dag=dag
)

t2 = DockerOperator(
    task_id='dataset_puller',
    image='dataset_puller',
    auto_remove=True,
    #env_file='.env', set this path
    dag=dag
)

t3 = DockerOperator(
    task_id='data_modifier',
    image='data_modifier',
    auto_remove=True,
    #env_file='.env', set this path
    dag=dag
)

t1 >> t2 >> t3
