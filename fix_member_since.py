"""
One-off backfill: correct Member_Since__c in Salesforce for specific members
to their FIRST-EVER membership start date, instead of whatever their
current/latest membership shows.

This is separate from script.py's regular sync on purpose. script.py's
process_and_save_members() derives Member_Since__c from the member's
CURRENT active membership post (joins on post_status = 'wcm-active') - that
behavior is untouched and keeps working the same way for ongoing syncs.
This script only fixes the members explicitly listed below, using the
earliest _start_date across ALL of that member's membership posts
(any status/plan), so members who e.g. resigned and were re-added under a
new membership don't lose their true original "member since" date.

Usage:
    python fix_member_since.py                 # dry run - prints what would change
    python fix_member_since.py --commit         # actually upserts to Salesforce

Edit MEMBERS_TO_FIX below to control who gets corrected.
"""

import argparse
import logging

import mysql.connector
import pandas as pd

from script import DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, upload_data_upsert, convert

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Members to fix, as (first_name, last_name) exactly as they appear in
# 7903_wc_customer_lookup. Matching is case-insensitive exact match.
MEMBERS_TO_FIX = [
    ("Leslie", "Fuller"),
    ("Lisa", "Ardente"),
]


def find_first_membership_start_dates(members):
    """For each (first_name, last_name), find their earliest membership
    _start_date across every wc_user_membership post they've ever had."""
    if not members:
        return pd.DataFrame()

    mydb = mysql.connector.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME
    )
    mycursor = mydb.cursor(dictionary=True)

    conditions = []
    params = []
    for first_name, last_name in members:
        conditions.append(
            "(LOWER(wcl.first_name) = LOWER(%s) AND LOWER(wcl.last_name) = LOWER(%s))"
        )
        params.extend([first_name, last_name])

    query = f"""
        SELECT
            wcl.customer_id,
            wcl.user_id,
            wcl.first_name,
            wcl.last_name,
            MIN(pm.meta_value) as first_membership_start
        FROM `7903_wc_customer_lookup` wcl
        JOIN `7903_posts` p
            ON p.post_author = wcl.user_id AND p.post_type = 'wc_user_membership'
        JOIN `7903_postmeta` pm
            ON pm.post_id = p.ID AND pm.meta_key = '_start_date'
        WHERE {' OR '.join(conditions)}
        GROUP BY wcl.customer_id, wcl.user_id, wcl.first_name, wcl.last_name
    """
    mycursor.execute(query, params)
    results = mycursor.fetchall()
    mydb.close()

    return pd.DataFrame(results)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--commit",
        action="store_true",
        help="Actually push the correction to Salesforce. Without this flag, "
             "only prints what would be changed.",
    )
    args = parser.parse_args()

    df = find_first_membership_start_dates(MEMBERS_TO_FIX)
    if df.empty:
        logger.info("No matching members found for %s", MEMBERS_TO_FIX)
        return

    logger.info("First-ever membership start dates found:\n%s", df)

    upload_df = df[["customer_id", "first_membership_start"]].rename(
        columns={
            "customer_id": "Member_ID__c",
            "first_membership_start": "Member_Since__c",
        }
    )
    upload_df = upload_df.fillna("")
    upload_df = upload_df.map(convert)

    if not args.commit:
        logger.info(
            "Dry run only - nothing was sent to Salesforce. "
            "Re-run with --commit to upsert this:\n%s",
            upload_df,
        )
        return

    logger.info("Uploading corrected Member_Since__c to Salesforce...")
    upload_data_upsert(upload_df, "HC_Member__c", None, "Member_ID__c")
    logger.info("Done.")


if __name__ == "__main__":
    main()
