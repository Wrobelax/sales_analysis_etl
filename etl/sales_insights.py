"""Script for data analysis and visualization"""

# Importing required modules.
import pandas as pd
import sqlite3 as sql
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import matplotlib.ticker as ticker
import seaborn as sns



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
    conn = sql.connect("C:\\Users\\adiw\\PyCharmMiscProject\\e-commerce.db")
    df_data = load_data(query, conn)
    df_orders = df_data["orders_transformed"]
    df_trends = df_data["month_trends"]
    df_orders_clients = df_data["orders_per_client"]
    df_pivot = df_data["pivot_customer_month"]

except Exception as e:
    print(e)

# print(df_trends.info())
# print(df_orders.info())
# print(df_pivot.info())
# print(df_orders_clients.info())



"""Data Visualization"""
# ===Monthly orders===
data = df_pivot.sum(axis = 0)
normalize = cm.colors.Normalize(vmin = data.min(), vmax = data.max())
colors = cm.plasma(normalize(data.values))

plt.figure(figsize = (10, 6))
plt.bar(data.index, data.values, color = colors)
plt.title("Sum of monthly orders")
plt.xlabel("Month")
plt.ylabel("Orders")

formatter = ticker.ScalarFormatter(useMathText = False)
formatter.set_scientific(False)
plt.gca().yaxis.set_major_formatter(formatter)

plt.tight_layout()
# plt.savefig("../outputs/sale_per_month.png")  # Saving results to file



# ===Orders per top 10 countries===
data = df_orders.groupby("Country")["OrderValue"].sum()
top_10_countries = data.sort_values(ascending = False).head(10)
other_countries = data.sort_values(ascending = False).iloc[:10].sum()
smallest = top_10_countries.sort_values().index[:5]
labels = top_10_countries.index

colors = cm.Pastel2(np.linspace(0, 1, len(top_10_countries)))
explode = [0.6 if country in smallest else 0 for country in labels]

plt.figure(figsize=(10,10))
plt.pie(top_10_countries.values,
        labels = top_10_countries.index,
        autopct = "%1.1f%%",
        colors = colors,
        explode = explode,
        startangle = 140,
        wedgeprops = {"edgecolor" : "white"})
plt.title("Sum of orders per top 10 countries")

plt.tight_layout()
plt.savefig("../outputs/orders_per_country.png")



# ===Orders UK vs. the rest of countries===
countries_all = df_orders.groupby("Country")["OrderValue"].sum().sort_values(ascending = False)
uk_value = countries_all["United Kingdom"]
rest_value = countries_all.drop("United Kingdom").sum()
data_uk_rest = {"United Kingdom": uk_value, "Rest": rest_value}

colors = ["#66c2a5", "#fc8d62"]

plt.figure(figsize=(10,10))

plt.figure(figsize=(10,10))
plt.pie(data_uk_rest.values(),
        labels = data_uk_rest.keys(),
        autopct = "%1.1f%%",
        colors = colors,
        startangle = 90)
plt.title("Sum of orders between UK and other countries")

plt.tight_layout()
# plt.savefig("../outputs/orders_per_country_uk_rest.png")



# ===Top 10 orders per sale===
orders = df_orders.groupby("Description")["Quantity"].sum().sort_values(ascending = False)
top_orders = orders.head(10)

normalize = cm.colors.Normalize(vmin = top_orders.min(), vmax = top_orders.max())
colors = cm.viridis(normalize(top_orders.values))

plt.figure(figsize=(10,6))
plt.barh(top_orders.index,
         top_orders.values,
         color = colors)

plt.title("Top 10 sold products")
plt.xlabel("Quantity")

plt.tight_layout()
# plt.savefig("../outputs/top_products.png")



# ===Weekly sale===
df_orders["InvoiceDate"] = pd.to_datetime(df_orders["InvoiceDate"])

df_orders = df_orders.set_index("InvoiceDate")

weekly_sales = df_orders["OrderValue"].resample("W").sum()
weekly_sales = weekly_sales[weekly_sales > 0]

plt.figure(figsize=(10,6))
plt.plot(weekly_sales.index,
         weekly_sales.values,
         marker = "o",
         linestyle = "-",
         color = "purple",
         label = "Sales over time"
         )

plt.title("Weekly sale per order value")
plt.xlabel("Week")
plt.ylabel("Total order value")
plt.xticks(rotation = 45)

plt.tight_layout()
plt.grid(True)
plt.savefig("../outputs/orders_daily.png")



# ===Hourly Activity===
df_orders = df_orders.reset_index()

plt.figure(figsize=(10,6))
plt.hist(df_orders["Hour"],
         bins = 24,
         edgecolor = "black",
         color = "skyblue",
         )

plt.title("Customer activity by hour")
plt.xlabel("Hour of day")
plt.ylabel("Number of orders")
plt.xticks(range(0, 24))
plt.grid(axis = "y", linestyle = "--", alpha = 0.7)

plt.tight_layout()
# plt.savefig("../outputs/orders_by_hour.png")



# ===Clients segments===
segments = df_orders.groupby("ClientSegment")["CustomerID"].nunique()
segments_drop = segments.drop("Other", errors = "ignore")

fig, (ax1, ax2) = plt.subplots(1,2, figsize = (16,8))

explode = [0, 0, 0, 0.2, 0, 0]


wedges, texts, autotexts = ax1.pie(segments_drop.values,
        labels = segments_drop.index,
        autopct = "%1.1f%%",
        colors = plt.cm.Pastel2.colors,
        explode = explode,
        startangle = 140,
        wedgeprops={'edgecolor': 'black'}
        )

ax1.set_title("Client segment (excluding 'other')")

bars = ax2.bar(segments.index,
               segments.values,
               color = "lightgreen",
               edgecolor = "black")

ax2.set_title("Number of clients in each segment")
ax2.set_xlabel("Segment")
ax2.set_ylabel("Number of clients")

plt.tight_layout()
plt.savefig("../outputs/segments.png")



# ===Distribution of orders value===
plt.figure(figsize = (10, 6))

plt.hist(df_orders["OrderValue"],
         bins = 50,
         color = "red",
         edgecolor = "black",
         log = True)

plt.title("Order value distribution")
plt.xlabel("Order value")
plt.ylabel("Number of orders")

formatter = ticker.ScalarFormatter(useMathText = False)
formatter.set_scientific(False)
plt.gca().yaxis.set_major_formatter(formatter)

plt.grid(True)
plt.tight_layout()
plt.savefig("../outputs/order_value_distribution.png")



# ===Top 10 clients per order value===
plt.figure(figsize = (10, 6))
df_orders = df_orders.reset_index()
top_10_clients = df_orders.groupby("CustomerID")["OrderValue"].sum()
top_10_clients = top_10_clients.sort_values(ascending = False).head(10)

top_10_clients.index = top_10_clients.index.astype(str)

plt.bar(top_10_clients.index,
        top_10_clients.values,
        color = "darkgreen",
        edgecolor = "black")

plt.title("Top 10 loyal customers")
plt.xlabel("CustomerID")
plt.ylabel("Value")

plt.tight_layout()
plt.savefig("../outputs/top_10_clients.png")
plt.show()