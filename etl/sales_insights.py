"""Script for data analysis and visualization"""

# Importing required modules.
import pandas as pd
import sqlite3 as sql
import matplotlib.pyplot as plt
import matplotlib.cm as cm



"""Functions"""
# Loading data.
query = {
    "orders_transformed" : "SELECT * FROM orders_transformed",
    "month_trends" : "SELECT * FROM month_trends",
    "orders_per_client" : "SELECT * FROM orders_per_client",
    "pivot_customer_month" : "SELECT * FROM customer_month"
}

def load_data(query: dict, conn) -> dict:
    dataframes = {}

    for keys, values in query.items():
        dataframes[keys] = pd.read_sql_query(values, conn)

    return dataframes



"""Loading data"""
# Setting connection and loading data to df.
try:
    conn = sql.connect("../data/e-commerce.db")
    df_data = load_data(query, conn)
    df_orders = df_data["orders_transformed"]
    df_trends = df_data["month_trends"]
    df_orders_clients = df_data["orders_per_client"]
    df_pivot = df_data["pivot_customer_month"]

except Exception as e:
    print(e)

print(df_trends.info())
print(df_orders.info())
print(df_pivot.info())
print(df_orders_clients.info())



"""Data Visualization"""
# Monthly orders.
data = df_pivot.sum(axis = 0)
normalize = plt.Normalize(data.min(), data.max())
colors = cm.viridis(normalize(data.values))

plt.figure(figsize = (10,6))
plt.bar(data.index, data.values, color = colors)
plt.title("Sum of monthly orders")
plt.xlabel("Month")
plt.ylabel("Orders")
plt.tight_layout()
plt.show()