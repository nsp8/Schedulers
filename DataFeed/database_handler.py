# -*- coding: utf-8 -*-
"""
Created on Fri Jun 21 11:36:59 2019

@author: NishantParmar
"""

from os import system
import constants as c
import pandas as pd

try:
    from sqlalchemy import create_engine
#    from sqlalchemy.dialects.mysql import insert
except ModuleNotFoundError:
    system('pip install sqlalchemy')
    from sqlalchemy import create_engine
#    from sqlalchemy.dialects.mysql import insert

try:
    engine_auth = 'mysql+pymysql://{user}:{pswd}@{host}:{port}'.format(
        user=c.USERNAME, pswd=c.PASSWORD, host=c.HOST, port=c.PORT)
except ModuleNotFoundError:
    system('pip install pymysql')
    engine_auth = 'mysql+pymysql://{user}:{pswd}@{host}:{port}'.format(
        user=c.USERNAME, pswd=c.PASSWORD, host=c.HOST, port=c.PORT)


def check_equality(df_1, df_2):
    """Checks if two dataframes are equal.
    Keyword arguments:
        df_1 - first dataframe;
        df_2 - second dataframe
    """
    if df_1.shape[0] != df_2.shape[0]:
        return False
    df_1_temp = df_1.fillna("NULL_VALUE")
    df_2_temp = df_2.fillna("NULL_VALUE")
    df_eq = df_1_temp.eq(df_2_temp)
    df_eq.drop_duplicates(inplace=True)
    if df_eq.shape[0] == 1:
        eq_values = df_eq.T.drop_duplicates()
        if len(eq_values) > 1:
            return False
        elif len(eq_values) == 1:
            return True
    else:
        return False


def get_columns(database=c.SCHEMA, table=c.TABLE):
    engine = create_engine(engine_auth)
    conn = engine.raw_connection()
    with conn.cursor() as cur:
        fetch_stmt = "show columns from {}.{};".format(database, table)
        cur.execute(fetch_stmt)
        results = cur.fetchall()
        print(list(results))
        col_df = pd.DataFrame(data=list(results),
                              columns=["Field", "Type", "Null", "Key",
                                       "Default", "Extra"])
        cols_ = list(col_df["Field"])
        return cols_


def table_to_dataframe(table_name):
    table_df = pd.DataFrame()
    try:
        table_df = pd.read_sql_table(table_name=table_name,
                                     con=engine_auth,
                                     schema=c.SCHEMA)
    except Exception as e:
        print("DataFrame from Table cannot be created because:\n{}".format(e))
    finally:
        return table_df


def dataframe_to_table(dataframe, table_name=c.TABLE):
    table_values = table_to_dataframe(table_name=table_name)
    print("Values in table: {}".format(table_values.shape))
    print("Values in dataframe: {}".format(dataframe.shape))
    combined_df = pd.DataFrame()
    equality = check_equality(table_values, dataframe)
    if not equality:
        combined_df = table_values.append(dataframe, ignore_index=True,
                                          sort=False)
        col_subset = ["text_data"]
        combined_df.drop_duplicates(subset=col_subset, inplace=True)
    try:
        if not combined_df.empty:
            dataframe.to_sql(
                name=table_name, con=engine_auth, index=False,
                schema=c.SCHEMA,
                if_exists="append")
        #                    if_exists="append")
        else:
            print("Data already present in table!")
    except Exception as e:
        print("Table from DataFrame cannot be created because:\n{}".format(e))


def get_records(database, table, columns="*"):
    engine = create_engine(engine_auth)
    conn = engine.raw_connection()
    if isinstance(columns, list):
        columns = ",".join(columns)
    with conn.cursor() as cur:
        data = dict()
        cur.execute("select {} FROM {}.{}".format(columns, database, table))
        rows = cur.fetchall()
        data["data"] = rows
        return data
