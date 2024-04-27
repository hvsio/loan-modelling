import numpy as np
import pandas as pd


def prepare_id_columns(df: pd.DataFrame, type: str) -> pd.DataFrame:
    """Assigns default enumerative index to columns
    holding foreign keys.

    Args:
        df (pd.DataFrame): fact table
        type (str): "test" or "train" table

    Returns:
        pd.DataFrame: fact table with populated foreign keys columns
    """
    df.assign(customer_id=range(0, len(df)))
    if type == 'test':
        return df.assign(loan_status=np.nan, prediction=np.nan)
    elif type == 'train':
        return df.assign(prediction=range(0, len(df)))


def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Renames colums to a common convention

    Args:
        df (pd.DataFrame): fact table

    Returns:
        pd.DataFrame: fact table with renamed columns
    """
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


def conver_to_bool_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Converts columns with boolean-like values to actual boolean type.

    Args:
        df (pd.DataFrame): fact table

    Returns:
        pd.DataFrame: fact table with optimized boolean columns
    """
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


def set_fk(
    loans_df: pd.DataFrame, dim_df: pd.DataFrame, fk: str
) -> pd.DataFrame:
    """Translates id of dimension tables to correspond to its
    foreign key in fact table and sets it at the fact table

    Args:
        loans_df (pd.DataFrame): fact table
        dim_df (pd.DataFrame): dimension table
        fk (str): foreign key column used in fact table
            to refer to dimension table's entry

    Returns:
        pd.DataFrame: fact table populated with the foreign key
    """
    id_mapping = dim_df.set_index(fk)['id'].to_dict()
    loans_df[fk] = loans_df[fk].map(id_mapping)
    return loans_df
