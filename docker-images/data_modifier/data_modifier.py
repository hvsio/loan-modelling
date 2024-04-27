import argparse
import os

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from utils import conver_to_bool_cols, prepare_id_columns, rename_columns
from constants import get_logger, get_db_url

if __name__ == '__main__':
    logger = get_logger()
    db_url = get_db_url()

    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--timestamp')
    args = parser.parse_args()
    timestamp = args.timestamp

    assert timestamp and all(
        partial.isnumeric() for partial in timestamp.split('.')
    )

    customer_table = os.environ.get('customers_table')
    loan_status_table = os.environ.get('status_table')
    property_table = os.environ.get('property_table')
    loans_table = os.environ.get('loans_table')

    dimension_tables = {
        customer_table: {
            'columns': [
                'gender',
                'married',
                'dependents',
                'graduated',
                'self_employed',
                'applicant_income',
                'coapplicant_income',
                'credit_history',
            ],
            'fk': 'customer_id',
        },
        loan_status_table: {
            'columns': ['loan_status'],
            'fk': 'prediction_id',
        },
        property_table: {
            'columns': ['property_area'],
            'fk': 'property_area_id',
        },
    }

    try:
        engine = create_engine(db_url, echo=False)
        with engine.begin() as connection:

            logger.info('Connected to the db...')

            for type in ['train', 'test']:

                schema_name = os.environ.get(f'schema_name_{type}')
                lakehouse_name = os.environ.get(f'lakehouse_{type}_name')

                df = pd.read_sql(
                    text(
                        f'SELECT * FROM public.{lakehouse_name} WHERE insertion_date = {timestamp}'
                    ),
                    connection,
                )

                logger.info(f'Size of data to be normalized: {len(df)} rows')

                fks = [entry['fk'] for entry in dimension_tables.values()]
                df = prepare_id_columns(df, type)
                df = rename_columns(df)
                df = df.drop_duplicates(subset=['loan_id'], keep='first')
                df['loan_id'] = [
                    f'{index}_{timestamp}' for index in df['loan_id']
                ]

                for table_name, data in dimension_tables.items():
                    logger.info(f'Ingesting {table_name} dimension table...')

                    dim_table = df.loc[:, data['columns']]
                    dim_fk = data['fk']

                    if table_name == property_table:
                        dim_table = dim_table.drop_duplicates(
                            ignore_index=True
                        )
                        dim_table[dim_fk] = [
                            f'PROPAREA_{i}' for i in dim_table.index
                        ]
                        df = df.merge(
                            dim_table,
                            left_on='property_area',
                            right_on='property_area',
                            how='left',
                        )
                        dim_table = dim_table.set_index(dim_fk)
                        db_status = pd.read_sql(
                            text(f'SELECT * FROM {schema_name}.{table_name}'),
                            connection,
                        )
                        if len(db_status) != 0:
                            continue
                        exit()
                    elif table_name == loan_status_table:
                        dim_table[dim_fk] = [
                            f'LSTAT_{i}_{timestamp}' for i in dim_table.index
                        ]
                        dim_table = dim_table.set_index(dim_fk)
                        df[dim_fk] = dim_table.index
                    elif table_name == customer_table:
                        dim_table = conver_to_bool_cols(dim_table)
                        dim_table[dim_fk] = [
                            f'LCUST_{i}_{timestamp}' for i in dim_table.index
                        ]
                        dim_table = dim_table.set_index(dim_fk)
                        df[dim_fk] = dim_table.index

                    dim_table.to_sql(
                        table_name,
                        connection,
                        index=True,
                        index_label=dim_fk,
                        schema=schema_name,
                        if_exists='append',
                    )
                logger.info('Successful creation of dimensional tables.')

                logger.info('Ingesting loans fact table...')

                df_loans = df.loc[
                    :,
                    [
                        'loan_id',
                        'loan_amount',
                        'loan_amount_term',
                        'customer_id',
                        'property_area_id',
                        'prediction_id',
                    ],
                ]

                df_loans.to_sql(
                    loans_table,
                    connection,
                    index=False,
                    schema=schema_name,
                    if_exists='append',
                )

                logger.info('Successful ingestion')

    except SQLAlchemyError as error:
        logger.error(f'Error when ingesting raw data: {error}')
        exit(1)
