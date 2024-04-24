import logging
import os
from datetime import datetime, timedelta

from airflow.providers.cncf.kubernetes.operators.pod import (
    KubernetesPodOperator,
)
from kubernetes.client import models as k8s
from kubernetes import client, config

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
    dag_id='loans_setup_kubernetesV2',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
) as dag:
    t1 = KubernetesPodOperator(
        task_id='database_creator',
        namespace='airflow',
        image='h4sio/database_creator3.1:1.0.0',
        is_delete_operator_pod=True,
        configmaps=['db-configs'],
        cluster_context='minikube',
        cmds=["echo 'Execution started {{ ds_nodash }}'"],
        dag=dag,
    )

    t2 = KubernetesPodOperator(
        task_id='dataset_puller',
        namespace='airflow',
        image='h4sio/dataset_puller3.1:1.0.0',
        is_delete_operator_pod=True,
        configmaps=['db-configs'],
        cluster_context='minikube',
        cmds="echo 'Execution started {{ ds_nodash }}'",
        dag=dag,
    )

    # t3 = KubernetesPodOperator(
    #     task_id='data_modifier',
    #     image='h4sio/data_modifier3.1:1.0.0',
    #     mount_tmp_dir=False,
    #     auto_remove='force',
    #     env_file='/.env',
    #     command="echo 'Execution started {{ ds_nodash }}'",
    #     dag=dag,
    # )

t1 >> t2
