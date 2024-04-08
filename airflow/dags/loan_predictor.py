import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from dotenv import find_dotenv, load_dotenv
import os

logger = logging.getLogger('airflowLogger')

load_dotenv(find_dotenv())

default_args = {
    'owner': os.environ.get('airflow_owner'),
    'description': 'Loan prediction setup',
    'depend_on_past': False,
    'start_date': datetime(2024, 3, 11),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='loans_setup',
    default_args=default_args,
) as dag:

    t1 = DockerOperator(
        task_id='database_creator',
        image='h4sio/database_creator3.1:1.0.0',
        mount_tmp_dir=False,
        auto_remove="force",
        env_file='/.env',
        command="echo 'Execution started {{ ds_nodash }}'",
        dag=dag
    )

    t2 = DockerOperator(
        task_id='dataset_puller',
        image='h4sio/dataset_puller3.1:1.0.0',
        mount_tmp_dir=False,
        auto_remove="force",
        env_file='/.env',
        command="echo 'Execution started {{ ds_nodash }}'",
        dag=dag
    )

    t3 = DockerOperator(
        task_id='data_modifier',
        image='h4sio/data_modifier3.1:1.0.0',
        mount_tmp_dir=False,
        auto_remove="force",
        env_file='/.env',
        command="echo 'Execution started {{ ds_nodash }}'",
        dag=dag
    )

t1 >> t2 >> t3
