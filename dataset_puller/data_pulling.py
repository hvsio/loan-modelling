import pandas as pd
from sqlalchemy import create_engine
from dotenv import find_dotenv, load_dotenv
import os
from kaggle.api.kaggle_api_extended import KaggleApi
from sqlalchemy.exc import SQLAlchemyError

load_dotenv(find_dotenv())
api = KaggleApi()

try:
    api.authenticate()
    api.dataset_download_files('vikasukani/loan-eligible-dataset', '/tmp/data', unzip=True)
except Exception as error:
    print(f"Couldn't pull the dataset: {error}")

db_url = (
        f"postgresql://{os.environ.get('db_user')}"
        f":{os.environ.get('db_pass')}"
        f"@{os.environ.get('db_host')}"
        f":{str(os.environ.get('db_port'))}"
        f"/{os.environ.get('tablename')}"
    )

try:
    engine = create_engine(db_url, echo=False)
    df = pd.read_csv('../dataset_puller/data/loan-test.csv')
    with engine.begin() as connection:
        df.to_sql(name=some_name_lakehouse, con=connection, if_exists="append")
except SQLAlchemyError as error:
    print(f"Error when ingesting raw data: {error}")