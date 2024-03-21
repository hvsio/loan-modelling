import pandas as pd
import numpy as np

def prepare_id_columns(df: pd.DataFrame, type: str):
    df.assign(customer_id=range(0, len(df)))
    if type == "test":
        return df.assign(loan_status=np.nan, prediction=np.nan)
    elif type == "train":
        return df.assign(prediction=range(0, len(df)))


def rename_columns(df: pd.DataFrame):
    df = df.rename(str.lower, axis="columns")
    return df.rename({
        "applicantincome": "applicant_income", 
        "education": "graduated", 
        "coapplicantincome": "coapplicant_income", 
        "loanamount": "loan_amount"
        }, axis="columns")

def conver_to_bool_cols(df: pd.DataFrame):
    df['married'].replace("Yes", True, inplace=True)
    df['married'].replace("No", False, inplace=True)
    df['self_employed'].replace("Yes", True, inplace=True)
    df['self_employed'].replace("No", False, inplace=True)
    df['credit_history'].replace(1.0, True, inplace=True)
    df['credit_history'].replace(0.0, False, inplace=True)
    df['graduated'].replace("Graduate", True, inplace=True)
    df['graduated'].replace("Not Graduate", False, inplace=True)
    return df

def set_fk(loans_df, dim_df, fk):
    id_mapping = dim_df.set_index(fk)["id"].to_dict()
    loans_df[fk] = loans_df[fk].map(id_mapping)
    