import mysql.connector
import json
import time
import requests
import pandas as pd
import os, sys
import numpy as np
from datetime import datetime
from dotenv import load_dotenv
import logging
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Email, Content, Mail, To

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


load_dotenv()


tables = ["7903_wc_customer_lookup", "7903_wc_order_stats", "7903_wc_order_product_lookup", "7903_wc_product_meta_lookup", "7903_term_taxonomy"]


# Database configuration (store these in environment variables for security)
DB_HOST = os.getenv("DB_HOST")  # Default value if env var not set
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
SALESFORCE_URI = os.getenv("SALESFORCE_URI")
SALESFORCE_API_KEY = os.getenv("SALESFORCE_API_KEY")
SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY")


def send_email(content):
    try:
        emails = [To(email) for email in os.getenv("EMAILS").split(",")]
        sg = SendGridAPIClient(api_key=SENDGRID_API_KEY)
        from_email = Email("no-reply@prativa.in")
        to_email = emails
        content = Content("text/json", json.dumps(content))
        subject = "HC Notification"
        mail = Mail(from_email, to_email, subject, content, is_multiple=len(emails) > 1)
        
        response = sg.client.mail.send.post(request_body=mail.get())
        logging.info(response)
        return response
    except Exception as e:
        return e


def upload_data(df, table):
    result = {}
    result["from_db"] = df.to_dict(orient="records")
    csv = df.to_csv(index=False, header=True)
    # logging.info(csv)
    files = {
        "job": (
            None,
            json.dumps(
                {
                    "object": table,
                    "contentType": "CSV",
                    "operation": "insert",
                    "lineEnding": "CRLF" if sys.platform.startswith('win') else "LF",
                }
            ),
            "application/json",
        ),
        "content": ("content", csv, "text/csv"),
    }

    # Fix headers and authentication
    headers = {
        "Authorization": f"Bearer {SALESFORCE_API_KEY}",
        "Accept": "application/json",
    }

    # Make the request
    res = requests.post(SALESFORCE_URI, files=files, headers=headers)

    if res.status_code != 200:
        logging.info(res.text)
        return
    
    res = res.json()
    logging.info(res["id"])

    time.sleep(10)
    id = res["id"]
    while True:
        res = requests.get(
            f"{SALESFORCE_URI}/{id}",
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {SALESFORCE_API_KEY}",
            },
        )
        res = res.json()
        if res["state"] == "InProgress":
            time.sleep(10)
            continue

        if res["numberRecordsFailed"] > 0:
            res = requests.get(
                f"{SALESFORCE_URI}/{id}/failedResults",
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {SALESFORCE_API_KEY}",
                    "Accept": "text/csv",
                },
            )
            result["failed"] = res.text

        res = requests.get(
            f"{SALESFORCE_URI}/{id}/successfulResults",
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {SALESFORCE_API_KEY}",
                "Accept": "text/csv",
            },
        )
        result["success"] = res.text
        logging.info(result)
        send_email(result)
        return result
    

def fetch_changes(table_name):
    try:
        mydb = mysql.connector.connect(
            host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME
        )
        mycursor = mydb.cursor(dictionary=True)  # Fetch results as dictionaries

        query = f"SELECT * FROM trigger_table WHERE is_processed = 0 and table_name = '{table_name}'"
        mycursor.execute(query)
        changes = mycursor.fetchall()
        logging.info(f"Found {len(changes)} unprocessed changes")
        mydb.close()  # Close the connection as soon as we're done

        return changes
    except mysql.connector.Error as err:
        logging.info(f"Database error: {err}")
        return None


def convert(obj):
    if isinstance(obj, dict):
        return {k: convert(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert(i) for i in obj]
    elif isinstance(obj, datetime):
        return obj.strftime(r"%Y-%m-%d")
    else:
        return obj


def process_and_save_members(changes):
    if changes is None or len(changes) == 0:
        return
    ids = [change["id"] for change in changes]
    query = f"""
        SELECT 
            wcl.*,
            um.meta_value as phone,
            p.ID as membership_plan
        FROM 
            7903_wc_customer_lookup wcl
        LEFT JOIN 
            7903_usermeta um ON wcl.user_id = um.user_id AND um.meta_key = 'billing_phone'
        LEFT JOIN 
            7903_posts p ON wcl.user_id = p.post_author
        WHERE 
            wcl.customer_id IN ({', '.join(['%s'] * len(ids))})"""
    mydb = mysql.connector.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME
    )
    mycursor = mydb.cursor(dictionary=True)  # Fetch results as dictionaries
    mycursor.execute(query, ids)
    results = mycursor.fetchall()
    mydb.close()  # Close the connection as soon as we're done

    df = pd.DataFrame(results)
    logging.info(f"Processing {len(df)} records")
    

    df = df[["first_name", "last_name", "email", "city", "customer_id", "phone", "state", "postcode", "customer_id", "membership_plan"]]
    df.rename(columns={"customer_id": "Member_ID__c", 
                    "first_name": "First_Name__c", 
                    "last_name": "Last_Name__c", 
                    "email": "Email__c", 
                    "city": "City__c", 
                    "phone": "Primary_Phone__c", 
                    "state": "State__c", 
                    "postcode": "Zipcode__c", 
                    "membership_plan": "Membership_Plan__c"}, 
                    inplace=True, errors="ignore")
    df["Source__c"] = f"wpdatabridge - {datetime.now().strftime(r'%Y-%m-%d')}"
    df["Member_Status__c"] = "Active"
    df["Membership_Plans__c"] = df["membership_plan"].map({
        411: "Active Member",
        6510: "Active Mernber - Emeritus",
        6511: "Active Member - Out of Town",
        6514: "Active Mernber - Senior",
        7478: "Active Member - Staff",
        413: "Guest",
        412: "Guests with Provisional Status (GPS)",
        7472: "Leave of Absence (LOA)",
        7385: "Resigned - Bad",
        7384: "Resigned - Good",
        7386: "Deceased"
    })
    df = df.map(convert)

    upload_data(df, "HC_Member__c")


def process_and_save_orders(changes):
    if changes is None or len(changes) == 0:
        return
    ids = [change["id"] for change in changes]
    query = f"""SELECT * FROM 7903_wc_order_stats WHERE order_id IN ({', '.join(['%s'] * len(ids))})"""
    mydb = mysql.connector.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME
    )
    mycursor = mydb.cursor(dictionary=True)  # Fetch results as dictionaries
    mycursor.execute(query, ids)
    results = mycursor.fetchall()
    mydb.close()  # Close the connection as soon as we're done

    df = pd.DataFrame(results)
    logging.info(f"Processing {len(df)} records")

    #  Customer_Note__c, Order_Notes__c, Payment_Method__c, Reference__c, Refund_Items__c, Transaction_ID__c
    df = df[["date_completed", "customer_id", "status", "order_id", "date_created", "parent_id" ]]
    df.rename(columns={"date_completed": "Completed_Date__c", 
                        "customer_id": "Member_ID__c", 
                        "status": "Order_Status__c", 
                        "order_id": "Order_Number__c", 
                        "date_created": "Order_Date__c" }, inplace=True, errors="ignore")
    df["Source__c"] = f"wpdatabridge - {datetime.now().strftime(r'%Y-%m-%d')}"
    df["Status_c"].apply({"wc-refunded": "refunded",
                            "wc-processing": "processing",
                            "wc-completed": "completed",
                            "wc-on-hold": "on-hold",
                            "wc-cancelled": "cancelled"})
    df["Refund_Items__c"] = np.where((df["parent_id"] != 0) & (df["status"] == "wc-refunded"), "Refunded", "")
    
    
    df = df.map(convert)

    upload_data(df, "HC_Order__c")


def process_and_save_order_items(changes):
    if changes is None or len(changes) == 0:
        return
    ids = [change["id"] for change in changes]
    query = f"""SELECT * FROM 7903_wc_order_product_lookup WHERE order_id IN ({', '.join(['%s'] * len(ids))})"""
    mydb = mysql.connector.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME
    )
    mycursor = mydb.cursor(dictionary=True)  # Fetch results as dictionaries
    mycursor.execute(query, ids)
    results = mycursor.fetchall()
    mydb.close()  # Close the connection as soon as we're done

    df = pd.DataFrame(results)
    logging.info(f"Processing {len(df)} records")

    df = df[["product_quantity" ]]
    df.rename(columns={"product_quantity": "Item_Quantity__c" }, inplace=True, errors="ignore")
    df["Ken_s_Field__c"] = False
    df["Source__c"] = f"wpdatabridge - {datetime.now().strftime(r'%Y-%m-%d')}"

    df = df.map(convert)

    upload_data(df, "HC_Order_Item__c")


def process_and_save_product(changes):
    if changes is None or len(changes) == 0:
        return
    ids = [change["id"] for change in changes]
    query = f"""SELECT p.*
                opl.max_price as price
                FROM 7903_posts p 
                LEFT JOIN 7903_wc_order_product_lookup opl ON p.ID = opl.product_id
                WHERE p.post_type = 'product' 
                AND p.order_id IN ({', '.join(['%s'] * len(ids))})"""
    mydb = mysql.connector.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME
    )
    mycursor = mydb.cursor(dictionary=True)  # Fetch results as dictionaries
    mycursor.execute(query, ids)
    results = mycursor.fetchall()

    mycursor.execute("SELECT DISTINCT(object_id), term_taxonomy_id as cat FROM `7903_term_relationships` WHERE term_taxonomy_id IN (23, 192, 256, 27, 111, 42, 64);")
    categories = mycursor.fetchall()

    mycursor.execute("SELECT DISTINCT(object_id), term_taxonomy_id as cat FROM `7903_term_relationships` WHERE term_taxonomy_id IN (31, 32, 37, 34, 40, 48);")
    day_of_week = mycursor.fetchall()

    mydb.close()  # Close the connection as soon as we're done

    if results is None or len(results) == 0:
        return
    
    df = pd.DataFrame(results)
    categories = pd.DataFrame(categories)
    day_of_week = pd.DataFrame(day_of_week)
    logging.info(f"Processing {len(df)} records")

    df = df[["ID", "post_title", "post_date", "guid", "price" ]]
    df = df.map(convert)
    df.rename(columns={"ID": "Product_Identifier__c", "post_title": "Product_Name__c", "guid": "Product_Page_URL__c", "price": "Regular_Price__c" }, inplace=True, errors="ignore")
    df["Did_Not_Run__c"] = False
    df["Post_Date__c"] = pd.to_datetime(df["post_date"]).dt.strftime('%Y-%m-%d')
    df["Time__c"] = pd.to_datetime(df["post_date"]).dt.strftime('%H:%M:%S')
    df["Trimester__c"] = df["post_date"].dt.month.map(lambda x: "Spring" if x in [ 4, 5, 6, 7] else "Fall" if x in [8, 9, 10, 11] else "Winter")
    df["Year__c"] = df["post_date"].dt.year
    df["Source__c"] = f"wpdatabridge - {datetime.now().strftime(r'%Y-%m-%d')}"
    df["Category__c"] = df["ID"].apply(
        lambda x: categories.loc[categories.object_id == x, "cat"].iloc[0] 
        if x in categories.object_id.values else None).apply({
            23: "Classes",
            192: "Clubhouse Only",
            256: "Event",
            27: "Membership",
            111: "Open Studios",
            42: "Transportation",
            64: "Workshops"
        })
    df["Day_of_Week__c"] = df["ID"].apply(
        lambda x: day_of_week.loc[day_of_week.object_id == x, "cat"].iloc[0]
        if x in day_of_week.object_id.values else None).apply({
            31: "Monday",
            32: "Tuesday",
            37: "Wednesday",
            34: "Thursday",
            40: "Friday",
            48: "Saturday"
        })
    
    
    df = df.map(convert)
    upload_data(df, "HC_Product__c")

# SELECT 7903_terms.name
# FROM 7903_terms
# JOIN 7903_term_taxonomy ON 7903_terms.term_id = 7903_term_taxonomy.term_id
# WHERE 7903_term_taxonomy.taxonomy = 'product_tag';



def process_and_save_teachers(changes):
    if changes is None or len(changes) == 0:
        return
    ids = [change["id"] for change in changes]
    query = f"""SELECT * FROM 7903_terms WHERE term_id IN (SELECT term_id from 7903_term_taxonomy WHERE term_id  IN ({', '.join(['%s'] * len(ids))}) AND parent = 248)"""
    mydb = mysql.connector.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME
    )
    mycursor = mydb.cursor(dictionary=True)  # Fetch results as dictionaries
    mycursor.execute(query, ids)
    results = mycursor.fetchall()
    mydb.close()  # Close the connection as soon as we're done

    df = pd.DataFrame(results)
    logging.info(f"Processing {len(df)} records")

    df = df[["name" ]]
    df.rename(columns={"name": "Name" }, inplace=True, errors="ignore")
    df["Also_a_Member__c"] = False

    df = df.map(convert)

    upload_data(df, "HC_Teacher__c")

# def update_processed_flags(changes):
#     try:
#         mydb = mysql.connector.connect(
#             host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME
#         )
#         mycursor = mydb.cursor()

#         for change in changes:
#             update_query = "UPDATE table_changes SET processed = 1 WHERE id = %s AND change_time = %s"
#             val = (change["id"], change["change_time"])
#             mycursor.execute(update_query, val)

#         mydb.commit()
#         mydb.close()
#         logging.info(f"{mycursor.rowcount} record(s) updated")

#     except mysql.connector.Error as err:
#         logging.info(f"Database error while updating processed flags: {err}")




if __name__ == "__main__":
    for table in tables:
        changes_data = fetch_changes(table)
        if changes_data is None:
            continue
        if table == "7903_wc_customer_lookup": 
            process_and_save_members(changes_data)
        elif table == "7903_wc_order_stats":
            process_and_save_orders(changes_data)
        elif table == "7903_wc_order_product_lookup":
            process_and_save_order_items(changes_data)
        elif table == "7903_posts":
            process_and_save_product(changes_data)
        elif table == "7903_term_taxonomy":
            process_and_save_teachers(changes_data)

    
# order_item - 35  "Monday Knitting Circle with Kay Mehls"
# product - 762 

