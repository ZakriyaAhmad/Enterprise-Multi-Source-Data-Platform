import pandas as pd 
from datetime import datetime, timedelta
from io import StringIO
import snowflake.connector
import boto3
import requests
from botocore.exceptions import NoCredentialsError, ClientError
import csv
import time
import json
from urllib.parse import quote_plus
from sqlalchemy import create_engine, MetaData, Table, Column, String, Float, Boolean, Date, TIMESTAMP
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text

# ─────────────────────────────────────────────────────────────
def sanitise_dataframe_for_redshift(df):
    text_cols = df.select_dtypes(include=["object"]).columns
    for col in text_cols:
        df[col] = (
            df[col].astype(str)
                   .str.replace(r'[\r\n]+', ' ', regex=True)
                   .str.replace('|', ' ')
                   .str.strip()
        )
    return df


def get_secret(secret_name):
    region_name = "us-west-1"
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        print(f"Error retrieving secret {secret_name}: {e}")
        raise e

    secret = get_secret_value_response['SecretString']
    secret_data = json.loads(secret)
    return secret_data['Host'], secret_data['Username'], secret_data['Password'], secret_data['Database']

snowflake_config = {
    'account': '',
    'user': '',
    'password': '',
    'database': '',
    'schema': 'PUBLIC',
    'warehouse': '',
    'role': '',
    'insecure_mode': True
}

queries = [
    {
        'query': "SELECT * FROM MARKET_INTELLIGENCE WHERE month_date BETWEEN CURRENT_DATE - INTERVAL '90 DAY' AND CURRENT_DATE;",
        's3_path': 's3://snowflake-data-extraction/incremental-load-snowflake//Three_MONTHS_DATA.csv'
    },
    {
        'query': """
        SELECT * REPLACE(
            TRY_TO_NUMBER(original_price::STRING)   AS original_price,
            TRY_TO_NUMBER(discounted_price::STRING) AS discounted_price
        )
        FROM BPUBLIC.HOODIE__COMMON
        WHERE DATE(seen_at) IN (CURRENT_DATE, CURRENT_DATE - INTERVAL '1 DAY');
        """,
        's3_path': 's3://snowflake-data-extraction/incremental-load-snowflake/two_days_data.csv'
    }
]

def print_recent_redshift_load_errors(conn, minutes_back: int = 15, limit: int = 50):
    query = f"""
    SELECT starttime, line_number, colname, err_reason, raw_field_value
    FROM stl_load_errors
    WHERE starttime >= current_timestamp - interval '{minutes_back} minute'
    ORDER BY starttime DESC
    LIMIT {limit};
    """
    print(f"DEBUG — stl_load_errors query:\n{query}")
    try:
        print(f"DEBUG — pulling last {limit} stl_load_errors rows ...")
        result = conn.execute(text(query))
        rows = result.fetchall()
        if not rows:
            print("DEBUG — No recent stl_load_errors rows.")
        else:
            for r in rows:
                print(r)
    except Exception as e:
        print(f"WARNING — could not read stl_load_errors: {e}")

def execute_query_and_upload_to_s3(query, s3_path):
    print(f"Starting Snowflake query; target S3: {s3_path}")
    conn = snowflake.connector.connect(**snowflake_config)
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        df = cursor.fetch_pandas_all()
        print(f"Fetched {len(df)} rows from Snowflake")
        print("DEBUG — Columns returned:", df.columns.tolist(), flush=True)
        df.columns = df.columns.str.strip().str.lower()
        sanitise_dataframe_for_redshift(df)

        if "hoodie_hoodie__market_intelligence" in s3_path:
            expected_columns = [
                "master_id", "dispensary_name", "banner", "state", "city", "brand", "category",
                "segment", "subsegment", "unit_of_measure", "infused", "concentrate_type", "flavor",
                "pack_size", "month_date", "dollar_sales", "unit_sales"
            ]


            if 'infused' in df.columns:
                df['infused'] = df['infused'].astype(str).str.strip().str.lower()
                df['infused'] = df['infused'].replace({
                    'true': 'true',
                    'false': 'false',
                    '1': 'true',
                    '0': 'false',
                    'yes': 'true',
                    'no': 'false',
                    '': '',
                    'nan': ''
                })
                df['infused'] = df['infused'].where(df['infused'].isin(['true', 'false', '']), '')

            for col in expected_columns:
                if col not in df.columns:
                    df[col] = pd.NA
            df = df[expected_columns]


        elif "hoodie_hoodie__common" in s3_path:
            redshift_columns = [
                "menu_item_id", "dispensary_id", "dispensary_name", "address", "city", "state",
                "postal_code", "country_code", "banner", "license_number", "rec_license_number",
                "tax_included", "dispensary_medical", "dispensary_recreational", "latitude", "longitude",
                "website", "email", "is_closed", "hemp_cbd", "variant_name", "strain", "brand",
                "parent_brand", "category", "segment", "subsegment", "measure", "uom", "product_id",
                "original_price", "discounted_price", "promotion_name", "is_active", "days_since_oos",
                "fill_rate", "seen_at", "first_seen_at", "last_seen_at", "is_medical", "is_recreational",
                "is_latest_row"
            ]

            for col in redshift_columns:
                if col not in df.columns:
                    df[col] = pd.NA
            df = df[redshift_columns]

            for col in ("original_price", "discounted_price"):
                if col in df.columns:
                    df[col] = df[col].astype(str).str.strip().replace({'None': '', 'none': '', 'NULL': '', 'null': ''})
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                    df[col] = df[col].apply(lambda x: "" if pd.isna(x) else f"{x}")

        # Write to CSV
        csv_buffer = StringIO()
        df.to_csv(
            csv_buffer,
            index=False,
            sep=',',
            quotechar='"',
            quoting=csv.QUOTE_MINIMAL,
            doublequote=True,
            na_rep='',
            line_terminator='\n'
        )

        # Upload to S3
        csv_buffer.seek(0)
        s3_client = boto3.client('s3')
        bucket, key = s3_path.replace("s3://", "").split("/", 1)
        s3_client.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())
        print(f"File successfully uploaded to {s3_path}")
    finally:
        cursor.close()
        conn.close()


for item in queries:
    execute_query_and_upload_to_s3(item['query'], item['s3_path'])



# Retrieve Redshift credentials from AWS Secrets Manager
db_host, db_username, db_password, db_name = get_secret("redshift-credentials")
db_port = 5439
conn_string = f"postgresql://{quote_plus(db_username)}:{quote_plus(db_password)}@{quote_plus(db_host)}:{db_port}/{quote_plus(db_name)}"
engine = create_engine(conn_string)



stored_procedures = [
    "raw_data_hoodie"
]

# ─────────────────────────────────────────────────────────────
# Execute stored proc(s) in AUTOCOMMIT so a failure does not
# poison the connection.  If the call fails we reconnect purely
# to read stl_load_errors.
# ─────────────────────────────────────────────────────────────
with engine.connect() as conn:
    try:
        for procedure in stored_procedures:
            print(f"Executing stored procedure: {procedure}")
            # run in autocommit mode so COPY commits (or fails)
            conn.execution_options(isolation_level="AUTOCOMMIT") \
                .execute(text(f"CALL {procedure}();"))

        print("Stored procedure executed successfully.")
    except Exception as e:
        # the connection is now in an ERROR state – close & reopen
        print(f"Error during execution: {e}")
        conn.close()

        # # new connection solely for stl_load_errors lookup
        # with engine.connect() as err_conn:
        #     print_recent_redshift_load_errors(err_conn)