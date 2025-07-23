"""Script for loading data into pandas DataFrames and SQL"""

# Importing required modules
import pandas as pd
import sqlite3 as sql

# Importing data to DF and SQL
df = pd.read_csv('../data/e-commerce_data.csv', encoding = "ISO-8859-1")

conn = sql.connect('../data/e-commerce.db')
df.to_sql('orders', conn , if_exists='replace', index = False)

result_df = pd.read_sql('SELECT * FROM orders LIMIT 10', conn)


# Basic data exploration for cleaning.
print(df.head())
print(df.info)
print(df.isnull().sum())
print(df.dtypes)