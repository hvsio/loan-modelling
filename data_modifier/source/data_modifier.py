import pandas as pd
from sqlalchemy import create_engine
from dotenv import find_dotenv, load_dotenv
import os
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text
import logging
from utils import conver_to_bool_cols, prepare_id_columns, rename_columns, set_fk

load_dotenv(find_dotenv())

db_url = (
    f"postgresql://{os.environ.get('db_user')}"
    f":{os.environ.get('db_pass')}"
    f"@{os.environ.get('db_host')}"
    f":{str(os.environ.get('db_port'))}"
    f"/{os.environ.get('db_name')}"
)

customer_table = os.environ.get('customers_table')
loan_status_table = os.environ.get('status_table')
property_table = os.environ.get('property_table')
loans_table = os.environ.get('loans_table')

dimension_tables = {
    customer_table: {
        "columns": ["gender", "married", "dependents", 
                    "graduated", "self_employed", 
                    "applicant_income", "coapplicant_income",
                    "credit_history", "customer_id"],
        "fk": "customer_id"
    },
    loan_status_table: {
        "columns": ["loan_status", "prediction_id"],
        "fk": "prediction_id"
    },
    property_table: {
        "columns": ["property_area", "property_area_id"],
        "fk": "property_area_id"
    }
}

try:
    engine = create_engine(db_url, echo=False)
    with engine.begin() as connection:
        logging.info('Connected to the db...')
        for type in ["train", "test"]:
            logging.info(f'Attempting creating star schema in {type} dataset.')
            fks = [entry['fk'] for entry in dimension_tables.values()]
            schema_name = os.environ.get(f'schema_name_{type}')
            lakehouse_name = os.environ.get(f'lakehouse_{type}_name')

            df = pd.read_sql(
                text(f"SELECT * FROM public.{lakehouse_name}"), connection)
            logging.info(f'Size of data to be normalized: {len(df)} rows')

            df[fks] = 0
            df = prepare_id_columns(df, type)

            df = rename_columns(df)

            for table_name, data in dimension_tables.items():
                logging.info(f'Creating {table_name} dimension table...')
                dim_table = df.loc[:, data['columns']]
                dim_fk = data['fk']
                if table_name == property_table:
                    dim_table.drop_duplicates(inplace=True, ignore_index=True)
                    dim_table['id'] = range(0, len(dim_table))
                    set_fk(df, dim_table, dim_fk)

                else:
                    dim_table['id'] = range(0, len(dim_table))
                    dim_table.drop(columns=dim_fk)
                    if table_name == customer_table:
                        dim_table = conver_to_bool_cols(dim_table)

                dim_table.drop(columns=dim_fk, inplace=True)
                dim_table.to_sql(table_name, connection, index=False,
                               schema=schema_name, if_exists="append")
                logging.info('Successful creation!')

            logging.info('Creating loans fact table...')
            df_loans = df.loc[:, ["loan_id", "loan_amount", "loan_amount_term",
                                  "customer_id", "property_area_id", "prediction_id"]]
            df_loans.to_sql(loans_table, connection, index=False,
                            schema=schema_name, if_exists="append")
            logging.info('Successful creation!')
            

except SQLAlchemyError as error:
    logging.error(f"Error when ingesting raw data: {error}")
