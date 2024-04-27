import os
import shutil
from datetime import datetime

import pandas as pd
from kaggle.api.kaggle_api_extended import KaggleApi
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from constants import get_logger, get_db_url

temp_storage_path = '/tmp/data'
lakehouse_test_name = os.environ.get('lakehouse_test_name')
lakehouse_train_name = os.environ.get('lakehouse_train_name')
test_file = 'loan-test.csv'
train_file = 'loan-train.csv'

if __name__ == '__main__':
    logger = get_logger()
    db_url = get_db_url

    timestamp = datetime.now()
    timestamp = timestamp.strftime('%d%m%Y%H%M')
    logger.info(timestamp)

    try:
        api = KaggleApi()
        api.authenticate()
        api.dataset_download_files(
            'vikasukani/loan-eligible-dataset', temp_storage_path, unzip=True
        )

        downloaded_files = os.listdir(temp_storage_path)
        assert (
            len(downloaded_files) == 2
            and test_file in downloaded_files
            and train_file in downloaded_files
        )
    except Exception as error:
        logger.error(f"Couldn't pull the dataset: {error}")
        exit(1)

    try:
        engine = create_engine(db_url, echo=False)
        df_test = pd.read_csv(f'{temp_storage_path}/{test_file}')
        df_train = pd.read_csv(f'{temp_storage_path}/{train_file}')

        df_test['insertion_date'] = int(timestamp)
        df_train['insertion_date'] = int(timestamp)

        with engine.begin() as connection:
            df_test.to_sql(
                name=lakehouse_test_name,
                con=connection,
                if_exists='append',
            )
            df_train.to_sql(
                name=lakehouse_train_name,
                con=connection,
                if_exists='append',
            )
            logger.info('Success.')
    except SQLAlchemyError as error:
        logger.error(f'Error when ingesting raw data: {error}')
        exit(1)

    finally:
        if os.path.exists(temp_storage_path):
            shutil.rmtree(temp_storage_path)
