import os
from datetime import datetime, timedelta

from airflow.operators.docker_operator import DockerOperator
from dotenv import find_dotenv, load_dotenv

from airflow import DAG

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

    database_creator = DockerOperator(
        task_id='database_creator',
        image='h4sio/database_creator3.1:1.0.3',
        mount_tmp_dir=False,
        env_file='./.env',
        command='python3 db_create.py',
    )

    dataset_puller = DockerOperator(
        task_id='dataset_puller',
        image='h4sio/dataset_puller3.1:1.0.4',
        mount_tmp_dir=False,
        xcom_all=True,
        env_file='./.env',
        command='python3 data_pulling.py',
    )

    data_modifier = DockerOperator(
        task_id='data_modifier',
        image='h4sio/data_modifier3.1:1.0.4',
        mount_tmp_dir=False,
        env_file='./.env',
        command='python3 data_modifier.py -t "{{ ti.xcom_pull(task_ids="dataset_puller")[0] }}"',
    )

database_creator >> dataset_puller >> data_modifier
