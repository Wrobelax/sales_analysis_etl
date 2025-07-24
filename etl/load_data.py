"""Script for loading data into pandas DataFrames and SQL including data cleaning"""

# Importing required modules
import pandas as pd
import sqlite3 as sql
import numpy as np



"""Loading data"""
# Importing data to DF and SQL.
try:
    df = pd.read_csv("../data/e-commerce_data.csv", encoding = "ISO-8859-1")
    conn = sql.connect("../data/e-commerce.db")
    df.to_sql("orders", conn, if_exists="replace", index=False)
except Exception as e:
    print(e)

result_df = pd.read_sql("SELECT * FROM orders LIMIT 10", conn)



"""Data cleaning"""
# Basic data exploration for cleaning.
# print(result_df)
# print(df.head())
# print(df.info()) # CustomerID is float.
# print(df.isnull().sum()) # Description and CustomerID are missing data.
# print(df.dtypes) # InvoiceDate change to date type.
# print(df[df.duplicated()]) # 5268 rows of duplicates.


# Cleaning missing data.
df["Description"] = df["Description"].replace("", np.nan)
df["CustomerID"] = df["CustomerID"].replace("", np.nan)


# Converting date.
df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"], format = "mixed", dayfirst = True)


# Changing CustomerID to int.
df["CustomerID"] = df["CustomerID"].astype("Int64")


# Dropping duplicates.
df = df.drop_duplicates()



"""Exporting data"""
# Saving results as a new table in SQL db and dropping connection.
try:
    df.to_sql("orders_cleaned", conn, if_exists="replace", index = False)
    conn.close()
except Exception as e:
    print(e)