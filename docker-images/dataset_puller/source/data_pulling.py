import logging
import os

import pandas as pd
from dotenv import find_dotenv, load_dotenv
from kaggle.api.kaggle_api_extended import KaggleApi
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

load_dotenv(find_dotenv())

db_url = (
    f"postgresql://{os.environ.get('db_user')}"
    f":{os.environ.get('db_pass')}"
    f"@{os.environ.get('db_host')}"
    f":{str(os.environ.get('db_port'))}"
    f"/{os.environ.get('db_name')}"
)

try:
    logging.info('Downloading dataset...')

    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files(
        'vikasukani/loan-eligible-dataset', '/tmp/data', unzip=True
    )

    downloaded_files = os.listdir('/tmp/data')
    assert (
        len(downloaded_files) == 2
        and 'loan-test.csv' in downloaded_files
        and 'loan-train.csv' in downloaded_files
    )
except Exception as error:
    logging.error(f"Couldn't pull the dataset: {error}")


try:
    engine = create_engine(db_url, echo=False)
    df_test = pd.read_csv('/tmp/data/loan-test.csv')
    df_train = pd.read_csv('/tmp/data/loan-train.csv')

    logging.info('Ingesting dataset into lakehouses...')
    with engine.begin() as connection:
        df_test.to_sql(
            name=os.environ.get('lakehouse_test_name'),
            con=connection,
            if_exists='append',
        )
        df_train.to_sql(
            name=os.environ.get('lakehouse_train_name'),
            con=connection,
            if_exists='append',
        )
        logging.info('Success.')
except SQLAlchemyError as error:
    print(f'Error when ingesting raw data: {error}')
