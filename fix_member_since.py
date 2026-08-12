"""
Backfill: correct Member_Since__c in Salesforce for members to their
FIRST-EVER membership start date, instead of whatever their current/latest
membership shows.

script.py's regular sync (sync_members_to_salesforce) now computes
Member_Since__c the same way - MIN(_start_date) across all of a member's
membership posts - so going forward this stays correct on its own. This
script exists to backfill members who were already synced to Salesforce
under the OLD logic (which used whichever membership was currently active
at sync time), so their Member_Since__c may currently be wrong.

By default this processes EVERY member who has at least one membership post.
Pass --member to restrict to specific people (e.g. for a one-off test)
instead of the whole list.

Usage:
    python fix_member_since.py                                   # dry run, all members
    python fix_member_since.py --commit                          # fix all members
    python fix_member_since.py --member "Leslie Fuller" --commit  # fix just one
    python fix_member_since.py --member "Leslie Fuller" --member "Lisa Ardente" --commit
"""

import argparse
import logging

import mysql.connector
import pandas as pd

from script import DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, upload_data_upsert, convert

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def find_first_membership_start_dates(members=None):
    """Find each member's earliest _start_date across every
    wc_user_membership post they've ever had.

    `members` is an optional list of (first_name, last_name) tuples to
    restrict to (case-insensitive exact match). If None/empty, every member
    with at least one membership post is returned.
    """
    mydb = mysql.connector.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME
    )
    mycursor = mydb.cursor(dictionary=True)

    where_clause = ""
    params = []
    if members:
        conditions = []
        for first_name, last_name in members:
            conditions.append(
                "(LOWER(wcl.first_name) = LOWER(%s) AND LOWER(wcl.last_name) = LOWER(%s))"
            )
            params.extend([first_name, last_name])
        where_clause = f"WHERE {' OR '.join(conditions)}"

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
        {where_clause}
        GROUP BY wcl.customer_id, wcl.user_id, wcl.first_name, wcl.last_name
    """
    mycursor.execute(query, params)
    results = mycursor.fetchall()
    mydb.close()

    return pd.DataFrame(results)


def parse_member_arg(value):
    parts = value.strip().split(None, 1)
    if len(parts) != 2:
        raise argparse.ArgumentTypeError(
            f'--member expects "First Last", got: {value!r}'
        )
    return tuple(parts)


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--commit",
        action="store_true",
        help="Actually push the correction to Salesforce. Without this flag, "
             "only prints what would be changed.",
    )
    parser.add_argument(
        "--member",
        action="append",
        type=parse_member_arg,
        metavar='"First Last"',
        help="Restrict to this member (repeatable). Omit to process ALL members.",
    )
    args = parser.parse_args()

    df = find_first_membership_start_dates(args.member)
    if df.empty:
        logger.info("No matching members found for %s", args.member or "ALL members")
        return

    logger.info("First-ever membership start dates found (%d members):\n%s", len(df), df)

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
            "Re-run with --commit to upsert this (%d records):\n%s",
            len(upload_df),
            upload_df,
        )
        return

    logger.info("Uploading corrected Member_Since__c for %d members to Salesforce...", len(upload_df))
    upload_data_upsert(upload_df, "HC_Member__c", None, "Member_ID__c")
    logger.info("Done.")


if __name__ == "__main__":
    main()
