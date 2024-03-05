from airflow import DAG
import datetime as dt
from airflow.models import Variable

default_args = {
    'owner': Variable.get('airflow_owner'),
    'retry': 3,
    'retry_delay': dt.timedelta(minutes=5),
}

with DAG(schedule_interval=dt.timedelta(hours=6),
         description='Backup database and models related to metals ingestion',
         start_date=dt.datetime(2024, 1, 25),
         is_paused_upon_creation=False,
         default_args=default_args,) as dag:

    op = Operator()

    
