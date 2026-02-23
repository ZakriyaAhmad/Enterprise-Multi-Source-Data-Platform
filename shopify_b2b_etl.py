from __future__ import (
    annotations,
) 


import uuid
import os, json, csv, logging, time, datetime as dt, tempfile
import sys
import boto3, requests, pg8000  # pg8000 for Redshift test
import pandas as pd
import numpy as np
import pytz  # Glue‑safe timezone objects for pandas 1.x

_PST = pytz.timezone("America/Los_Angeles")  # pandas 1.x expects pytz objects
from botocore.exceptions import ClientError
from tenacity import retry, wait_exponential, stop_after_attempt
import textwrap

from typing import Any, Dict, Optional
from helpers import (
    read_state,
    _shipping_discount,
    _flatten,
    paginate,
    to_dataframe,
    shopify_gql,
    ensure_target_table,
    ORDERS_QUERY_GQL,
    upload_merge
)



##### resdhift:

REDSHIFT_HOST = ""
REDSHIFT_PORT = 5439
REDSHIFT_DATABASE = "dev"
REDSHIFT_USER = "admin"
REDSHIFT_PASSWORD = ""

conn = pg8000.connect(
        host=REDSHIFT_HOST,
        port=REDSHIFT_PORT,
        database=REDSHIFT_DATABASE,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD,
    )
cur = conn.cursor()



######## B2B CREDENTIALS
ACCESS_TOKEN = ""
SHOP = "l"
API_KEY = ""
API_secret_key = ""
API_VERSION = ""









def fetch_orders_dataset(
    first_orders: int = 250,
    first_lines: int = 50,
    since_iso: str | None = None,
    max_pages: int | None = None,
) -> pd.DataFrame:
    """Return a DataFrame of order line‑items shaped like the REST schema."""
    rows, cursor, pages = [], None, 0
    while True:
        data = shopify_gql(
            ORDERS_QUERY_GQL,
            {
                "firstOrders": first_orders,
                "firstLines": first_lines,
                "cursor": cursor,
                "search": f"updated_at:>={since_iso}"
                if since_iso
                else "updated_at:>=1970-01-01",
            },
        )["orders"]

        for edge in data["edges"]:
            order = edge["node"]
            ship_disc = _shipping_discount(order)
            for li_edge in order["lineItems"]["edges"]:
                rows.append(_flatten(order, li_edge["node"], ship_disc))

        if (max_pages and pages >= max_pages) or not data["pageInfo"]["hasNextPage"]:
            break
        cursor, pages = data["pageInfo"]["endCursor"], pages + 1

    df = pd.DataFrame(rows)
    return df







def main():

    
    STREAMS = {
        "b2b_orders": ("orders.json", ["id", "line_item_id"], "updated_at_min"),
        "b2b_customers": ("customers.json", ["id"], "updated_at_min"),
    }
    FULL_LOAD = True
    # RAW_SCHEMA = 'ryan_testing'
    RAW_SCHEMA = 'raw_data'
    # stream = 'b2b_orders'
    # pk_cols = ["id", "line_item_id"]


    for stream in STREAMS:
        print(stream)
        pk_cols = STREAMS[stream][1]
        target_table = f"{RAW_SCHEMA}.shopify_{stream}"
        print(pk_cols)
        print(target_table)



        if stream == 'b2b_orders':

            if FULL_LOAD:
                LOOKBACK_YEARS = 10
                since_for_stream = (
                            dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=365 * LOOKBACK_YEARS)
                        ).isoformat()
                
            else: #### need to update read_state for this etl!
                last_run = read_state()
                since_for_stream = (
                            dt.datetime.fromisoformat(last_run) - dt.timedelta(minutes=5)
                        ).isoformat()

            processed_tables: set[str] = set()
            df = fetch_orders_dataset(
                                first_orders=250, first_lines=50, since_iso=since_for_stream
                        )
            rows_loaded_total = 0
            grand_total = 0
            max_seen = since_for_stream
            if not df.empty:
                if target_table not in processed_tables:
                    ensure_target_table(
                        cur, target_table, list(df.columns), pk_cols
                    )
                    processed_tables.add(target_table)
                upload_merge(cur, df, stream, pk_cols, target_table)
                rows_loaded_total += len(df)
                # update watermark based on updated_at (fallback to processed_at)
                page_max = pd.to_datetime(
                    df["updated_at"].fillna(df["processed_at"]), errors="coerce"
                ).max()
                if pd.notna(page_max):
                    max_seen = max(max_seen, str(page_max))

            grand_total += rows_loaded_total
        else:  # customers

            if FULL_LOAD:
                LOOKBACK_YEARS = 10
                since_for_stream = (
                    dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=365 * LOOKBACK_YEARS)
                ).isoformat()
            else:  # incremental
                last_run = read_state()
                since_for_stream = (
                    dt.datetime.fromisoformat(last_run) - dt.timedelta(minutes=5)
                ).isoformat()

            endpoint = STREAMS[stream][0]

            processed_tables: set[str] = set()
            rows_loaded_total = 0
            grand_total = 0
            max_seen = since_for_stream

            for i, page in enumerate(paginate(endpoint, since_for_stream), start=1):
                df = to_dataframe("customers", page)
                if not df.empty:
                    if target_table not in processed_tables:
                        ensure_target_table(cur, target_table, list(df.columns), pk_cols)
                        processed_tables.add(target_table)

                    upload_merge(cur, df, stream, pk_cols, target_table)
                    rows_loaded_total += len(df)

                    page_max = pd.to_datetime(df["updated_at"], errors="coerce").max()
                    if pd.notna(page_max):
                        max_seen = max(max_seen, str(page_max))


            grand_total += rows_loaded_total

                            


if __name__ == "__main__":
    main()

