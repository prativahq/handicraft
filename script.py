import mysql.connector
import json
from decimal import Decimal
import time
import requests
import pandas as pd
import os, sys
import numpy as np
from datetime import datetime
from dotenv import load_dotenv
from io import StringIO
import base64
import logging
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Email, Content, Mail, To, Attachment,FileContent, FileName, FileType, Disposition

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        return super().default(obj)

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


load_dotenv()


tables = [
    "7903_wc_customer_lookup",
    "7903_wc_order_stats",
    "7903_wc_order_product_lookup",
    "7903_posts",
    "7903_term_taxonomy",
]


# Database configuration (store these in environment variables for security)
DB_HOST = os.getenv("DB_HOST")  # Default value if env var not set
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
SALESFORCE_URI = os.getenv("SALESFORCE_URI")
SALESFORCE_API_KEY = os.getenv("SALESFORCE_API_KEY")
SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY")
SALESFORCE_URL = os.getenv("SALESFORCE_URL")

states = {
    "AL": "Alabama",
    "AZ": "Arizona",
    "CA": "California",
    "CO": "Colorado",
    "CT": "Connecticut",
    "DC": "District of Columbia",
    "DE": "Delaware",
    "FL": "Florida",
    "GA": "Georgia",
    "HI": "Hawaii",
    "IA": "Iowa",
    "ID": "Idaho",
    "IL": "Ilinois",
    "IN": "Indiana",
    "LA": "Louisiana",
    "MA": "Massachusetts",
    "MD": "Maryland",
    "ME": "Maine",
    "MI": "Michigan",
    "MN": "Minnesota",
    "MO": "Missouri",
    "MS": "Mississippi",
    "MT": "Montana",
    "NC": "North Carolina",
    "ND": "North Dakota",
    "NH": "New Hampshire",
    "NJ": "New Jersey",
    "NM": "New Mexico",
    "NY": "New York",
    "OH": "Ohio",
    "OR": "oregon",
    "PA": "Pennsytvania",
    "RI": "Rhode Island",
    "SC": "South carolina",
    "SD": "South Dakota",
    "TN": "Tennessee",
    "TX": "Texas",
    "VA": "vrgfmia",
    "VT": "Vermont",
    "WA": "Washington",
    "WI": "Wisconsin",
    "WV": "west Virginia",
    "WY": "Wyornng",
}


def send_email(content_data):
    try:
        email_list = os.getenv("EMAILS", "").split(",")
        to_emails = [To(email.strip()) for email in email_list]
        sg = SendGridAPIClient(api_key=SENDGRID_API_KEY)
        
        # Create email
        from_email = Email("noreply@prativa.in")
        html_content = f"""
            <html>
            <body>
                <h3>HC Notification</h3>
                <p>Dear Team,</p>
                <p>The data processing completed successfully. Summary:</p>
                <ul>
                    <li><b>Total Records Processed:</b> {len(content_data.get('success', '').splitlines())}</li>
                    <li><b>Failed Records:</b> {len(content_data.get('failed', '').splitlines())}</li>
                </ul>
                <p>Best Regards,</p>
                <p>Your Automation Script</p>
            </body>
            </html>
            """
        subject = "HC Notification"
        
        # Setup mail
        mail = Mail(
            from_email=from_email,
            to_emails=to_emails,
            subject=subject,
            html_content=html_content
        )
        
        # Convert data to JSON using custom encoder
        json_data = json.dumps(content_data, cls=DecimalEncoder)
        
        # Add JSON attachment
        attachment = Attachment()
        attachment.file_content = FileContent(base64.b64encode(json_data.encode()).decode())
        attachment.file_type = FileType("application/json")
        attachment.file_name = FileName("data.json")
        attachment.disposition = Disposition("attachment")
        mail.add_attachment(attachment)
        
        # Send email
        response = sg.send(mail)
        logging.info(f"Email sent successfully✅. Status code: {response.status_code}")
        return response
    except Exception as e:
        logging.error(f"Error sending email❌: {str(e)}")
        return None


def upload_data(df, table, changes):
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
                    "lineEnding": "CRLF" if sys.platform.startswith("win") else "LF",
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

    time.sleep(30)
    id = res["id"]
    # cnt = 0
    while True:
        res = requests.get(
            f"{SALESFORCE_URI}/{id}",
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {SALESFORCE_API_KEY}",
            },
        )

        res = res.json()
        logging.info(res)
        # if cnt == 3:
        #     logging.info("Failed to upload data")
        #     break
        # cnt += 1
        if res["state"] == "InProgress" or res["state"] == "UploadComplete":
            time.sleep(30)
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
        update_processed_flags(changes)
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
    logging.info(df)

    df = df[
        [
            "first_name",
            "last_name",
            "email",
            "city",
            "customer_id",
            "phone",
            "state",
            "postcode",
            "membership_plan",
        ]
    ]
    df.rename(
        columns={
            "customer_id": "Member_ID__c",
            "first_name": "First_Name__c",
            "last_name": "Last_Name__c",
            "email": "Email__c",
            "city": "City__c",
            "phone": "Primary_Phone__c",
            "state": "State__c",
            "postcode": "Zipcode__c",
            "membership_plan": "Membership_Plan__c",
        },
        inplace=True,
        errors="ignore",
    )
    df["Source__c"] = f"wpdatabridge - {datetime.now().strftime(r'%Y-%m-%d')}"
    df["Member_Status__c"] = "Active"
    df["State__c"] = df["State__c"].map(states)
    df["Membership_Plan__c"] = df["Membership_Plan__c"].map(
        {
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
            7386: "Deceased",
            None: "Active Member",
        }
    )
    df = df.fillna("")
    # df["Membership_Plan__c"] = "Active Member"

    df = df.map(convert)
    upload_data(df, "HC_Member__c",changes)

    # update_processed_flags(changes)


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
    df = df[
        [
            "date_completed",
            "customer_id",
            "status",
            "order_id",
            "date_created",
            "parent_id",
        ]
    ]
    df.rename(
        columns={
            "date_completed": "Completed_Date__c",
            "customer_id": "Member_ID__c",
            "status": "Order_Status__c",
            "order_id": "Order_Number__c",
            "date_created": "Order_Date__c",
        },
        inplace=True,
        errors="ignore",
    )
    df["Source__c"] = f"wpdatabridge - {datetime.now().strftime(r'%Y-%m-%d')}"
    df["Order_Status__c"] = df["Order_Status__c"].map(
        {
            "wc-refunded": "refunded",
            "wc-processing": "processing",
            "wc-completed": "completed",
            "wc-on-hold": "on-hold",
            "wc-cancelled": "cancelled",
        }
    )
    df["Refund_Items__c"] = np.where(
        (df["parent_id"] != 0) & (df["Order_Status__c"] == "refunded"), "Refunded", ""
    )

    df.drop(columns=["parent_id"], inplace=True)

    df = df.fillna("")
    df = df.map(convert)

    upload_data(df, "HC_Order__c",changes)

    # update_processed_flags(changes)


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

    df = df[["product_quantity"]]
    df.rename(
        columns={"product_quantity": "Item_Quantity__c"}, inplace=True, errors="ignore"
    )
    df["Ken_s_Field__c"] = False
    df["Source__c"] = f"wpdatabridge - {datetime.now().strftime(r'%Y-%m-%d')}"

    df = df.fillna("")
    df = df.map(convert)

    upload_data(df, "HC_Order_Item__c",changes)

    # update_processed_flags(changes)


def process_and_save_product(changes):
    if changes is None or len(changes) == 0:
        return
    ids = [change["id"] for change in changes]
    query = f"""SELECT p.*, opl.max_price
            FROM 7903_posts AS p
            LEFT JOIN 7903_wc_product_meta_lookup AS opl ON p.ID = opl.product_id
            WHERE p.post_type IN ('product', 'product_variation')
            AND p.ID IN ({', '.join(['%s'] * len(ids))})"""
            
    # Teacher mapping query with IDs
    # GROUP_CONCAT(t.name) as teacher,
    # GROUP_CONCAT(t.term_id) as teacher_ids
    teacher_query = """
            SELECT tr.object_id as products, 
            t.term_id as teacher_id
            FROM 7903_term_relationships tr 
            JOIN 7903_term_taxonomy tt ON tr.term_taxonomy_id = tt.term_taxonomy_id
            JOIN 7903_terms t ON tt.term_id = t.term_id
            WHERE tr.object_id IN ({})
            AND tt.parent = 248 GROUP BY tr.object_id
    """.format(', '.join(['%s'] * len(ids)))
    
    # Add tag query
    tag_query = """
        SELECT tr.object_id as product_id, GROUP_CONCAT(t.name SEPARATOR ';') as tags
        FROM 7903_term_relationships tr
        JOIN 7903_term_taxonomy tt ON tr.term_taxonomy_id = tt.term_taxonomy_id
        JOIN 7903_terms t ON tt.term_id = t.term_id
        WHERE tr.object_id IN ({})
        AND tt.taxonomy = 'product_tag'
        GROUP BY tr.object_id
    """.format(', '.join(['%s'] * len(ids)))
    
    mydb = mysql.connector.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME
    )
    mycursor = mydb.cursor(dictionary=True)  # Fetch results as dictionaries
    mycursor.execute(query, ids)
    results = mycursor.fetchall()

    mycursor.execute(
        "SELECT DISTINCT(object_id), term_taxonomy_id as cat FROM `7903_term_relationships` WHERE term_taxonomy_id IN (23, 192, 256, 27, 111, 42, 64);"
    )
    categories = mycursor.fetchall()

    mycursor.execute(
        "SELECT DISTINCT(object_id), term_taxonomy_id as cat FROM `7903_term_relationships` WHERE term_taxonomy_id IN (31, 32, 37, 34, 40, 48);"
    )
    day_of_week = mycursor.fetchall()
    
    # Execute teacher query
    mycursor.execute(teacher_query, ids)
    teachers = mycursor.fetchall()
    
    # Execute tag query
    mycursor.execute(tag_query, ids)
    tags = mycursor.fetchall()
    mydb.close()  # Close the connection as soon as we're done

    if results is None or len(results) == 0:
        return

    df = pd.DataFrame(results)
    categories = pd.DataFrame(categories)
    day_of_week = pd.DataFrame(day_of_week)
    teacher_df = pd.DataFrame(teachers)
    tags_df = pd.DataFrame(tags)

    
    logging.info(f"Processing {len(df)} records")

    df = df[["ID", "post_title", "post_date", "guid", "max_price","post_parent", "post_type"]]
    df = df.map(convert)
    df.rename(
        columns={
            "ID": "Product_Identifier__c",
            "post_title": "Name",
            "guid": "Product_Page_URL__c",
            "max_price": "Regular_Price__c",
            "post_parent":"Post_Parent__c",
            "post_type":"Product_Type__c",
        },
        inplace=True,
        errors="ignore",
    )
    
    # Create mapping of product ID to teacher term_id
    if not teacher_df.empty:
        teacher_mapping = dict(zip(teacher_df['products'], teacher_df['teacher_id']))
        
        # For each product, get teacher ID either directly or from parent
        def get_teacher_id(row):
            if row['Post_Parent__c'] != 0:  # If it's a variation
                return teacher_mapping.get(row['Post_Parent__c'])  # Get parent's teacher ID
            return teacher_mapping.get(row['Product_Identifier__c'])  # Get own teacher ID
        
        df['Id__c'] = df.apply(get_teacher_id, axis=1)
    
    # Map tags to products
    if not tags_df.empty:
        tag_mapping = dict(zip(tags_df['product_id'], tags_df['tags']))
        
        def get_tags(row):
            if row['Post_Parent__c'] != 0:  # If child product
                return tag_mapping.get(row['Post_Parent__c'], '')
            return tag_mapping.get(row['Product_Identifier__c'], '')
            
        df['Tags__c'] = df.apply(get_tags, axis=1)
    else:
        df['Tags__c'] = ''
        
    df["Did_Not_Run__c"] = False
    post_date = pd.to_datetime(df["post_date"])
    df["Post_Date__c"] = post_date.dt.strftime("%Y-%m-%d")
    df.drop(columns=["post_date"], inplace=True)
    df["Time__c"] = post_date.dt.strftime("%H:%M:%S")
    df["Trimester__c"] = post_date.dt.month.map(
        lambda x: "Spring"
        if x in [4, 5, 6, 7]
        else "Fall"
        if x in [8, 9, 10, 11]
        else "Winter"
    )
    df["Year__c"] = post_date.dt.year
    df["Source__c"] = f"wpdatabridge - {datetime.now().strftime(r'%Y-%m-%d')}"
    df["Category__c"] = (
        df["Product_Identifier__c"].map(
            categories.set_index("object_id")["cat"].to_dict()
        )
        .map(
            {
                23: "Classes",
                192: "Clubhouse Only",
                256: "Event",
                27: "Membership",
                111: "Open Studios",
                42: "Transportation",
                64: "Workshops",
            }
        )
    )
    df["Day_of_Week__c"] = df["Product_Identifier__c"].map(
            day_of_week.set_index("object_id")["cat"].to_dict()
        ).map(
            {
                31: "Monday",
                32: "Tuesday",
                37: "Wednesday",
                34: "Thursday",
                40: "Friday",
                48: "Saturday",
            }
        )

    df = df.fillna("")
    df = df.map(convert)
    
    logging.info("DataFrame with post_parent and teachers:")
    logging.info(df[["Product_Identifier__c", "Post_Parent__c", "Id__c", "Product_Type__c", "Category__c", "Time__c", "Tags__c"]].to_dict('records'))
    upload_data(df, "HC_Product__c",changes)
    # print("Product uploaded",df)
    # update_processed_flags(changes)

def process_and_save_teachers(changes):
    if changes is None or len(changes) == 0:
        return
    ids = [change["id"] for change in changes]
    query = f"""
        SELECT t.term_id, t.name 
        FROM 7903_terms t
        JOIN 7903_term_taxonomy tt ON t.term_id = tt.term_id
        WHERE tt.parent = 248 
        AND t.term_id IN ({', '.join(['%s'] * len(ids))})
    """
    mydb = mysql.connector.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME
    )
    mycursor = mydb.cursor(dictionary=True)  # Fetch results as dictionaries
    mycursor.execute(query, ids)
    results = mycursor.fetchall()
    mydb.close()  # Close the connection as soon as we're done

    df = pd.DataFrame(results)
    logging.info(f"Processing {len(df)} records")
    logging.info(f"Available columns: {df.columns.tolist()}")
    
    df = df[["name","term_id"]]
    df.rename(columns={"name": "Name","term_id":"Id__c"}, inplace=True, errors="ignore")
    df["Also_a_Member__c"] = False

    df = df.fillna("")
    df = df.map(convert)

    upload_data(df, "HC_Teacher__c",changes)

    # update_processed_flags(changes)

def update_processed_flags(changes):
    try:
        mydb = mysql.connector.connect(
            host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME
        )
        mycursor = mydb.cursor()

        for change in changes:
            update_query = "UPDATE trigger_table SET is_processed = 1 WHERE id = %s AND table_name = %s"
            val = (change["id"], change["table_name"])
            mycursor.execute(update_query, val)

        mydb.commit()
        mydb.close()
        logging.info(f"{mycursor.rowcount} record(s) updated")

    except mysql.connector.Error as err:
        logging.info(f"Database error while updating processed flags: {err}")


if __name__ == "__main__":
    for table in tables:
        changes_data = fetch_changes(table)
        if changes_data is None:
            continue
        logging.info(f"Processing {table}")
        if table == "7903_wc_customer_lookup":
            process_and_save_members(changes_data)
        if table == "7903_wc_order_stats":
            process_and_save_orders(changes_data)
        if table == "7903_wc_order_product_lookup":
            process_and_save_order_items(changes_data)
        if table == "7903_term_taxonomy":
            process_and_save_teachers(changes_data)
        if table == "7903_posts":
            process_and_save_product(changes_data)