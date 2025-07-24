"""Script for data analysis and visualization"""

# Importing required modules.
import pandas as pd
import sqlite3 as sql



"""Loading data"""
# Setting connection and loading data to df.
try:
    conn = sql.connect("../data/e-commerce.db")
    sql_query = "SELECT * FROM orders_cleaned"
    df = pd.read_sql(sql_query, conn)
except Exception as e:
    print(e)