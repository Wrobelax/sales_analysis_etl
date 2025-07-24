"""Script for data transformation"""

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
                SUM(Quantity) AS TotalQuantity
            FROM orders_cleaned 
            GROUP BY OrderDate
            ORDER BY OrderDate;""")
total_sold_per_day = pd.read_sql_query(query, conn)
# print(total_sold_per_day)


# Orders with missing CustomerID.
query = ("""SELECT COUNT(Quantity) AS MissingCustomerId 
            FROM orders_cleaned 
            WHERE CustomerID IS NULL;""")
empty_customer_id = pd.read_sql_query(query, conn)
# print(empty_customer_id)


# Client's segmentation - recency, frequency, monetary.
query = ("""SELECT 
                CustomerID,
                CAST(
                    julianday((SELECT MAX(InvoiceDate) FROM orders_cleaned)) - 
                    julianday(MAX(InvoiceDate)) AS INTEGER) AS Recency,
                COUNT(DISTINCT InvoiceNo) AS Frequency, 
                SUM(UnitPrice * Quantity) AS Monetary
            FROM orders_cleaned
            WHERE CustomerID IS NOT NULL 
            GROUP BY CustomerID 
            ORDER BY Recency DESC;""")
segmentation = pd.read_sql_query(query, conn)
# print(segmentation)



"""Adding new columns to the table"""
df["OrderValue"] = df["UnitPrice"] * df["Quantity"]

df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"], format = "mixed", dayfirst = True)

df["Month"] = df["InvoiceDate"].dt.month

df["WeekDay"] = df["InvoiceDate"].dt.day_name()

df["Weekend"] = (df["InvoiceDate"].dt.day_name()).isin(["Saturday", "Sunday"]).astype(int)

df["Recency"] = pd.cut(
    segmentation["Recency"],
    bins = [0, 30, 90, float("inf")],
    labels = ["Active", "Regular", "Inactive"],
    include_lowest = True
)

df["Frequency"] = pd.cut(
    segmentation["Frequency"],
    bins = [0, 5, 9, float("inf")],
    labels = ["Rare", "Medium", "Frequent"]
)

df["Monetary"] = pd.cut(
    segmentation["Monetary"],
    bins = [0, 500, 999, float("inf")],
    labels = ["Low", "Medium", "High"]
)


# Segmentation.
segments = {
    "Top Clients" :   (df["Recency"] == "Active") & (df["Frequency"] == "Frequent") & (df["Monetary"] == "High"),
    "Loyal Clients" : (df["Recency"] == "Active") & (df["Frequency"] == "Frequent") & (df["Monetary"] == "Medium"),
    "Big Spenders" :  (df["Recency"] == "Regular") & (df["Monetary"] == "High"),
    "New Clients" :   (df["Recency"] == "Active") & (df["Frequency"] == "Rare"),
    "At Risk" :       (df["Recency"] == "Inactive") & (df["Frequency"] == "Frequent"),
    "Lost Clients" :  (df["Recency"] == "Inactive") & (df["Frequency"] == "Low"),
    "Low Value" :     (df["Frequency"] == "Rare") & (df["Monetary"] == "Low"),
}

df["ClientSegment"] = "Other"

for segment_name, conditions in segments.items():
    df.loc[conditions, "ClientSegment"] = segment_name



"""Data transformations"""
