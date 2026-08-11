import pandas as pd
from dotenv import load_dotenv
import os
import mysql.connector

load_dotenv()

DB_HOST = os.getenv("DB_HOST")  # Default value if env var not set
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
SALESFORCE_URI = os.getenv("SALESFORCE_URI")
SALESFORCE_API_KEY = os.getenv("SALESFORCE_API_KEY")
# SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY")s
SALESFORCE_URL = os.getenv("SALESFORCE_URL")

# Load the Excel file
file_path = "excel.xlsx"  # Replace with the path to your Excel file
df = pd.read_excel(file_path, sheet_name="Sheet1")  # Specify sheet name if needed

orders = {}

for idx, row in df.iterrows():
    order_id = row["ID"]
    order_item_id = row["order_item_id"]
    if order_id in orders:
        orders[order_id].append(order_item_id)
    else:
        orders[order_id] = [order_item_id]

query = f"""
INSERT INTO trigger_table
(
    id,
    created_at,
    operation,
    table_name,
    is_processed
)
VALUES
(
    %s,
    NOW(),
    'INSERT',
    %s,
    0
)
"""
mydb = mysql.connector.connect(
    host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME
)
mycursor = mydb.cursor(dictionary=True)  # Fetch results as dictionaries

for order_id in list(orders.keys()):
    # Insert order
    mycursor.execute(query, [order_id, '7903_posts'])
    # Insert order items
    mycursor.executemany(query, [[item_id, '7903_woocommerce_order_items'] for item_id in orders[order_id]])
    pass

mydb.commit()
mydb.close()  # Close the connection as soon as we're done