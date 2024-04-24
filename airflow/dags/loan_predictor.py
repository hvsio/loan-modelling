import logging
import os
import sys
from datetime import datetime, timedelta

from airflow.operators.docker_operator import DockerOperator
from dotenv import find_dotenv, load_dotenv

from airflow import DAG

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

load_dotenv(find_dotenv())

default_args = {
    'owner': os.environ.get('airflow_owner'),
    'description': 'Loan prediction setup',
    'depend_on_past': False,
    'start_date': datetime(2024, 3, 11),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='loans_setup',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
) as dag:

    t1 = DockerOperator(
        task_id='database_creator',
        image='h4sio/database_creator3.1:1.0.2',
        mount_tmp_dir=False,
        env_file='./.env',
        command='python3 db_create.py',
    )

    t2 = DockerOperator(
        task_id='dataset_puller',
        image='h4sio/dataset_puller3.1:1.0.2',
        mount_tmp_dir=False,
        xcom_all=True,
        env_file='./.env',
        command='python3 data_pulling.py',
    )

    t3 = DockerOperator(
        task_id='data_modifier',
        image='h4sio/data_modifier3.1:1.0.2',
        mount_tmp_dir=False,
        env_file='./.env',
        command='python3 data_modifier.py -t "{{ ti.xcom_pull(task_ids="dataset_puller")[0] }}"',
    )

t1 >> t2 >> t3
