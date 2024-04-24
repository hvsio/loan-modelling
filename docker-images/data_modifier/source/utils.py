import numpy as np
import pandas as pd


def prepare_id_columns(df: pd.DataFrame, type: str):
    df.assign(customer_id=range(0, len(df)))
    if type == 'test':
        return df.assign(loan_status=np.nan, prediction=np.nan)
    elif type == 'train':
        return df.assign(prediction=range(0, len(df)))


def rename_columns(df: pd.DataFrame):
    df = df.rename(str.lower, axis='columns')
    return df.rename(
        {
            'applicantincome': 'applicant_income',
            'education': 'graduated',
            'coapplicantincome': 'coapplicant_income',
            'loanamount': 'loan_amount',
        },
        axis='columns',
    )


def conver_to_bool_cols(df: pd.DataFrame):
    df['married'] = df['married'].replace(['Yes', 'No'], [True, False])
    df['self_employed'] = df['self_employed'].replace(
        ['Yes', 'No'], [True, False]
    )
    df['credit_history'] = df['credit_history'].replace(
        [1.0, 0.0], [True, False]
    )
    df['graduated'] = df['graduated'].replace(
        ['Graduate', 'Not Graduate'], [True, False]
    )
    return df


def set_fk(loans_df, dim_df, fk):
    id_mapping = dim_df.set_index(fk)['id'].to_dict()
    loans_df[fk] = loans_df[fk].map(id_mapping)
    return loans_df
