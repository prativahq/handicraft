import mysql.connector  # MySQL Connector
import csv
import os
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST")  # Default value if env var not set
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

# Database connection settings (modify as per your database)
DB_CONFIG = {
    "host": DB_HOST,
    "user": DB_USER,
    "password": DB_PASSWORD,
    "database": DB_NAME
}

members = [
  129, 38, 267, 141, 105, 116, 198, 287, 68, 242, 236,
  74, 305, 360, 217, 21, 93, 277, 119, 260, 228, 256, 161, 97,
  289, 
]


query = """
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
            'UPDATE',               
            '7903_wc_customer_lookup',
            0
        );
"""

# Database connection
db = mysql.connector.connect(
    host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME
)
cursor = db.cursor()
cursor.executemany(query, [(id, ) for id in members])
db.commit()
cursor.close()
db.close()