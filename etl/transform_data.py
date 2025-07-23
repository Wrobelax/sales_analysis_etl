"""Script for data transformation"""

# Importing required modules.
import pandas as pd
import numpy as np
import sqlite3 as sql



"""Loading data"""
# Setting connection and loading data to df.
try:
    conn = sql.connect("../data/e-commerce.db")
    sql_query = "SELECT * FROM orders_cleaned"
    df_result = pd.read_sql(sql_query, conn)
except Exception as e:
    print(e)



"""SQL queries"""
# Number of unique orders per country.
query = ("""SELECT Country, COUNT(DISTINCT InvoiceNo) AS NumberOfOrders 
            FROM orders_cleaned 
            GROUP BY Country 
            ORDER BY NumberOfOrders DESC;""")
orders_per_country = pd.read_sql_query(query, conn)
# print(orders_per_country)


# Total revenue and average order per country.
query =("""SELECT Country, SUM(UnitPrice * Quantity) AS TotalRevenue, AVG(UnitPrice * Quantity) as AvgOrderValue 
           FROM orders_cleaned 
           GROUP BY Country 
           ORDER BY TotalRevenue DESC;""")
total_rev_avg_value_order = pd.read_sql_query(query, conn)
# print(total_rev_avg_value_order)


# Number of unique clients per country.
query = ("""SELECT Country, COUNT(DISTINCT CustomerID) as UniqueClients 
            FROM orders_cleaned 
            WHERE CustomerID IS NOT NULL 
            GROUP BY Country 
            ORDER BY UniqueClients DESC;""")
clients_per_country = pd.read_sql_query(query, conn)
# print(clients_per_country)


# Top 10 selling products.
query = ("""SELECT StockCode, Description, SUM(Quantity) AS TotalNumberSold 
            FROM orders_cleaned 
            GROUP BY StockCode 
            ORDER BY TotalNumberSold DESC LIMIT 10;""")
top_10_products = pd.read_sql_query(query, conn)
# print(top_10_products)


# Orders returned.
query =("""SELECT InvoiceNo, Description, Quantity 
           FROM orders_cleaned 
           WHERE Quantity < 0 
           ORDER BY InvoiceNo;""")
orders_returned = pd.read_sql_query(query, conn)
# print(orders_returned)


# Number of orders per day of week.
query = ("""SELECT 
                SUM(Quantity) AS TotalQuantity,
                   CASE strftime('%w', InvoiceDate)
                       WHEN '0' THEN 'Sunday'
                       WHEN '1' THEN 'Monday' 
                       WHEN '2' THEN 'Tuesday' 
                       WHEN '3' THEN 'Wednesday' 
                       WHEN '4' THEN 'Thursday' 
                       WHEN '5' THEN 'Friday' 
                       WHEN '6' THEN 'Saturday' 
                    END as DayName
            FROM orders_cleaned 
            GROUP BY DayName 
            ORDER BY 
                CASE strftime('%w', InvoiceDate) 
                    WHEN '0' THEN 7
                    ELSE CAST(strftime('%w', InvoiceDate) AS INTEGER)
                END;""")
orders_per_day_of_week = pd.read_sql_query(query, conn)
# print(orders_per_day_of_week)


# Top 10 clients with the greatest number of unique orders.
query = ("""SELECT CustomerID, COUNT(DISTINCT InvoiceNo) AS TotalOrders 
            FROM orders_cleaned 
            WHERE CustomerID IS NOT NULL 
            GROUP BY CustomerID 
            ORDER BY TotalOrders DESC LIMIT 10;""")
top_10_clients = pd.read_sql_query(query, conn)
# print(top_10_clients)


# Total products sold per day.
query = ("""SELECT 
                DATE(InvoiceDate) AS OrderDate, 
                SUM(Quantity) AS TotalQuantity,
            FROM orders_cleaned 
            GROUP BY OrderDate
            ORDER BY OrderDate;""")
total_sold_per_day = pd.read_sql_query(query, conn)
print(total_sold_per_day)