import json
import html
import time
import traceback

from datetime import date, datetime, timedelta
from urllib.parse import quote_plus

import boto3
import requests
import pandas as pd

from botocore.exceptions import ClientError
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    String,
    Float,
    Boolean,
    Date,
    TIMESTAMP,
)
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text

import smtplib
from contextlib import redirect_stdout
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders


def upload_df_s3(df, path):
    """Uploads a DataFrame to S3."""

    df.to_csv(path, index=False, mode="w")  # Use 'w' to overwrite the file


def get_secret(secret_name):
    """Retrieves secrets from AWS Secrets Manager."""

    region_name = "us-west-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        print(f"Error retrieving secret {secret_name}: {e}")
        raise e

    secret = get_secret_value_response["SecretString"]
    secret_data = json.loads(secret)

    return (
        secret_data["Host"],
        secret_data["Username"],
        secret_data["Password"],
        secret_data["Database"],
    )


def convert_to_lowercase_and_underscore(text):
    """Converts to lowercase and replaces spaces with underscores."""

    return text.lower().replace(" ", "_").replace("-", "_")


def execute_query(session, query):
    """Executes a SQL query and returns the result."""
    try:
        result = session.execute(query)
        return result.fetchall()
    except Exception as e:
        print(f"Error executing query: {e}")
        try:
            session.rollback()
        except Exception:
            pass
        raise e


def truncate_table(session, table_name):
    """Truncates a table in the database."""
    try:
        session.execute(f"TRUNCATE TABLE {table_name};")
        print(f"Table {table_name} truncated successfully.")
    except Exception as e:
        print(f"Error truncating table {table_name}: {e}")
        try:
            session.rollback()
        except Exception:
            pass
        raise e


def upload_and_copy_to_redshift(
    session, data_frame, s3_path, schema_table_name, column_list
):
    try:
        upload_df_s3(data_frame, s3_path)

        copy_query = f"""
        COPY {schema_table_name} ({", ".join(column_list)})
        FROM '{s3_path}'
        IAM_ROLE ''
        REGION 'us-east-2' CSV
        IGNOREHEADER 1
        DATEFORMAT as 'YYYY-MM-DD'
        TIMEFORMAT 'YYYY-MM-DDTHH:MI:SS';
        COMMIT;
        """

        session.execute(copy_query)
        session.commit()
        return len(data_frame)
    except Exception as e:
        # Log the error message
        session.rollback()
        test_insert_sql = f"""
        INSERT INTO {schema_table_name}
        ({", ".join(column_list)})
        VALUES({", ".join(map(str, data_frame.iloc[0]))});
        """
        print(f"Debug Query: {test_insert_sql}")
        message = f"Redshift COPY failed for {schema_table_name}: {str(e)}"
        raise RuntimeError(message) from e


def merge_table(
    session,
    data_frame,
    s3_path,
    inquiry_id,
    column_list,
    primary_keys,
    schema_table_name,
):
    try:
        upload_df_s3(data_frame, s3_path)
        stage_table = f"{inquiry_id}_stage"

        match_conditions = [
            f"{schema_table_name}.{field} = {stage_table}.{field}"
            for field in primary_keys
        ]
        match_conditions = " AND ".join(match_conditions)

        copy_query = f"""
        CREATE TEMP TABLE {stage_table} (like {schema_table_name});

        COPY {stage_table} ({", ".join(column_list)})
        FROM '{s3_path}'
        IAM_ROLE ''
        REGION 'us-east-2' CSV
        IGNOREHEADER 1
        DATEFORMAT as 'YYYY-MM-DD'
        TIMEFORMAT 'YYYY-MM-DDTHH:MI:SS';

        MERGE INTO {schema_table_name} USING {stage_table} ON {match_conditions} REMOVE DUPLICATES;

        DROP TABLE {stage_table};
        COMMIT;
        """

        session.execute(copy_query)
        session.commit()
        return len(data_frame)
    except Exception as e:
        # Log the error message
        session.rollback()
        test_insert_sql = f"""
        INSERT INTO {schema_table_name}
        ({", ".join(column_list)})
        VALUES({", ".join(map(str, data_frame.iloc[0]))});
        """
        print(f"Debug Query: {test_insert_sql}")
        message = f"Merge into {schema_table_name} failed: {str(e)}"
        raise RuntimeError(message) from e


def merge_main_table(
    session, primary_keys, dumping_schema_table_name, schema_table_name
):
    try:
        match_conditions = [
            f"{schema_table_name}.{field} = {dumping_schema_table_name}.{field}"
            for field in primary_keys
        ]
        match_conditions = " AND ".join(match_conditions)

        copy_query = f"""
        MERGE INTO {schema_table_name} USING {dumping_schema_table_name} ON {match_conditions} REMOVE DUPLICATES;
        COMMIT;
        """
        session.execute(copy_query)
        session.commit()
        print(
            f"uploading and copying to Main Table Redshift successfulyy: {schema_table_name}"
        )
    except Exception as e:
        # Log the error message
        session.rollback()
        message = f"Merge into main table {schema_table_name} failed: {str(e)}"
        raise RuntimeError(message) from e


def get_column_type(value, key=None):
    """Returns the appropriate SQLAlchemy data type based on the value."""

    timestamp_format = "%Y-%m-%dT%H:%M:%S.%f"
    datetime_format = "%Y-%m-%dT%H:%M:%S"

    if key and "description" in key.lower():
        return String(length=4096)
    elif key and key.lower() == "taxisuptodate":
        return Boolean
    elif key and key.lower() == "subaccount":
        return String
    elif key and "date" in key.lower():
        if isinstance(value, str):
            if "." in value:
                try:
                    datetime.strptime(value, timestamp_format)
                    return TIMESTAMP
                except ValueError:
                    pass
            else:
                try:
                    datetime.strptime(value, datetime_format)
                    return Date
                except ValueError:
                    pass
        return String

    if isinstance(value, bool):
        return Boolean
    elif isinstance(value, float):
        return Float
    elif isinstance(value, str):
        try:
            float_value = float(value)
            return String if float_value.is_integer() and float_value != 0.0 else Float
        except ValueError:
            if "." in value:
                try:
                    datetime.strptime(value, timestamp_format)
                    return TIMESTAMP
                except ValueError:
                    pass
            else:
                try:
                    datetime.strptime(value, datetime_format)
                    return Date
                except ValueError:
                    pass

            return String
    else:
        return String


def check_and_create_table(engine, table_name, schema_name, column_list):
    """
    Checks if the table exists, add columns if any new columns are found.creates the table if it doesn't.
    """
    metadata = MetaData()
    try:
        my_table = Table(
            table_name,
            metadata,
            autoload=True,
            autoload_with=engine,
            schema=schema_name,
        )
        print(f"Table {table_name} already exists.")
        # sync columns
        existing_columns = my_table.columns.keys()
        for column, column_type in column_list.items():
            if column.lower() not in existing_columns:
                col = Column(column, column_type, nullable=True, default=None)
                _column_type = col.type.compile(engine.dialect)
                query = f"ALTER TABLE {schema_name}.{table_name} ADD COLUMN {column} {_column_type}"
                print(query, "===")
                engine.execute(query)
                print(
                    f"Column ({column}) added to the table {table_name} successfully."
                )
    except Exception:
        my_table = Table(table_name, metadata, schema=schema_name)
        for column, column_type in column_list.items():
            my_table.append_column(
                Column(column, column_type, nullable=True, default=None)
            )

        metadata.create_all(engine)
        print(f"Table {table_name} created successfully.")


def make_api_call(url, params, max_retries=3):
    """
    Make an API call with rate limiting and retry logic.

    Args:
        url: The API endpoint URL
        params: Query parameters
        max_retries: Maximum number of retry attempts for 401 errors

    Returns:
        List of records or None if failed
    """
    headers = {
        "Accept": "application/json",
        
        "Authorization": "Basic ",
        "Cookie": "Locale=TimeZone=GMTE0000U&Culture=en-US; UserBranch=1",
    }

    for attempt in range(max_retries):
        try:
            # Log the API call details for debugging
            if attempt > 0:
                print(f"[API_RETRY] Attempt {attempt + 1} of {max_retries}")
            print(f"[API_CALL] Making request to: {url}")
            print(f"[API_CALL] Params: {params}")
            print(
                f"[API_CALL] Auth header (last 10 chars): ...{headers['Authorization'][-10:]}"
            )

            # Add rate limiting: 500ms delay between requests to prevent account lockout
            time.sleep(0.5)

            # response = requests.get(url, headers=headers, params=params, timeout=30)
            response = requests.get(url, headers=headers, params=params, timeout=30, verify=False)

            print(f"[API_RESPONSE] Status code: {response.status_code}")

            if response.ok:
                data = response.json().get("value", [])
                if not data:
                    print("[API_RESPONSE] No records found in the response.")
                else:
                    print(f"[API_RESPONSE] Successfully retrieved {len(data)} records")
                return data
            elif response.status_code == 401:
                print(
                    f"[API_ERROR] 401 Unauthorized - Account may be locked or credentials invalid"
                )
                print(
                    f"[API_ERROR] Response body (first 500 chars): {response.text[:500]}"
                )

                # If this is not the last attempt, wait with exponential backoff
                if attempt < max_retries - 1:
                    wait_time = 2**attempt  # 1s, 2s, 4s...
                    print(f"[API_RETRY] Waiting {wait_time} seconds before retry...")
                    time.sleep(wait_time)
                    continue
                else:
                    print(f"[API_ERROR] Max retries reached. Account may be locked.")
                    return None
            else:
                print(
                    f"[API_ERROR] Error fetching data from Acumatica API. Status code: {response.status_code}"
                )
                print(
                    f"[API_ERROR] Response body (first 500 chars): {response.text[:500]}"
                )
                return None

        except Exception as e:
            print(f"[API_EXCEPTION] ERROR at make api call: {e}")
            print(f"[API_EXCEPTION] Full error: {traceback.format_exc()}")

            # If this is not the last attempt, wait before retry
            if attempt < max_retries - 1:
                wait_time = 2**attempt
                print(f"[API_RETRY] Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)
                continue
            else:
                return None

    return None


def fetch_column_list(api_url, params):
    """Fetches column list from Acumatica API."""
    try:
        print(f"[FETCH_COLUMNS] Fetching column metadata from: {api_url}")
        data = make_api_call(api_url, params)

        if data:
            first_record = data[0]
            columns = {
                key: get_column_type(value, key) for key, value in first_record.items()
            }
            print(f"[FETCH_COLUMNS] Successfully fetched {len(columns)} columns")
        else:
            print(f"[FETCH_COLUMNS] No data returned, using empty column list")
            columns = {}

        return columns
    except Exception as e:
        print(
            f"[FETCH_COLUMNS_ERROR] ERROR at fetches column list from Acumatica API: {e}"
        )
        print(f"[FETCH_COLUMNS_ERROR] Full error: {traceback.format_exc()}")


def fetch_and_process_data(api_url, params, column_list, convert_dict):
    """Fetches and processes data from Acumatica API."""
    try:
        data = make_api_call(api_url, params)

        if data:
            new_records_df = pd.DataFrame(data)
            new_records_df = new_records_df[column_list]
            new_records_df = new_records_df.astype(convert_dict)
        else:
            new_records_df = pd.DataFrame()

        return new_records_df
    except Exception as e:
        message = f"Fetch and process data failed: {e}"
        raise RuntimeError(message) from e


def get_columns_list(inquiry_key, timestamp_field):
    convert_key = convert_to_lowercase_and_underscore(inquiry_key)
    inquiry_id = f"acumatica_{convert_key}"
    print("inquiry_id", inquiry_id)
    current_datetime = datetime.now()
    # Get days ago date
    past_days_ago = current_datetime - timedelta(days=7)
    formatted_past_week_ago = past_days_ago.strftime("%Y-%m-%dT00:00:00")
    string_date = current_datetime.strftime("%Y_%m_%d")
    api_url = f"https://acumatica.com/odata//{inquiry_key}"
    params = {
        # '$top': 1,
    }
    timestamp_field = timestamp_field
    if timestamp_field is None:
        params = {
            # '$top': 1,
        }
    else:
        params = {
            "$filter": f"{timestamp_field} gt DateTime'{formatted_past_week_ago}'",
            "$top": 1,
        }
    column_dict = fetch_column_list(api_url, params)
    column_list = list(
        getattr(column_dict, "keys", lambda: [])()
    )  # Safe fallback to empty list

    # convert_dict = {column: object for column in column_list}
    return column_list, inquiry_id, past_days_ago, column_dict, convert_key, api_url


def _quote_identifier(identifier: str) -> str:
    """
    Quote SQL identifiers safely, preserving existing quotes.
    """
    if identifier.startswith('"') and identifier.endswith('"'):
        return identifier
    return '"' + identifier.replace('"', '""') + '"'


def find_missing_in_prod(session, primary_keys, dump_table, prod_table, limit=5):
    """
    Identify records present in the staging/dump table but absent from prod.
    """
    if not primary_keys:
        return {"count": 0, "samples": []}

    dump_alias = "dump_src"
    prod_alias = "prod_src"
    join_clause = " AND ".join(
        f"{prod_alias}.{_quote_identifier(pk)} = {dump_alias}.{_quote_identifier(pk)}"
        for pk in primary_keys
    )
    where_clause = " AND ".join(
        f"{prod_alias}.{_quote_identifier(pk)} IS NULL" for pk in primary_keys
    )
    select_columns = ", ".join(
        f"{dump_alias}.{_quote_identifier(pk)}" for pk in primary_keys
    )

    sample_query = text(
        f"""
        SELECT {select_columns}
        FROM {dump_table} AS {dump_alias}
        LEFT JOIN {prod_table} AS {prod_alias}
          ON {join_clause}
        WHERE {where_clause}
        LIMIT :limit
        """
    )
    count_query = text(
        f"""
        SELECT COUNT(*)
        FROM {dump_table} AS {dump_alias}
        LEFT JOIN {prod_table} AS {prod_alias}
          ON {join_clause}
        WHERE {where_clause}
        """
    )

    sample_rows = [
        tuple(map(str, row))
        for row in session.execute(sample_query, {"limit": limit}).fetchall()
    ]
    total_missing = session.execute(count_query).scalar()

    return {"count": total_missing or 0, "samples": sample_rows}


def find_duplicates(session, table_name, primary_keys, limit=5):
    """
    Detect duplicate primary key combinations in the specified table.
    """
    if not primary_keys:
        return {"count": 0, "samples": []}

    table_alias = "dup_src"
    qualified_cols = [
        f"{table_alias}.{_quote_identifier(pk)}" for pk in primary_keys
    ]
    select_cols = ", ".join(qualified_cols)
    group_cols = ", ".join(qualified_cols)

    sample_query = text(
        f"""
        SELECT {select_cols}, COUNT(*) AS duplicate_count
        FROM {table_name} AS {table_alias}
        GROUP BY {group_cols}
        HAVING COUNT(*) > 1
        ORDER BY duplicate_count DESC
        LIMIT :limit
        """
    )
    count_query = text(
        f"""
        SELECT COUNT(*) FROM (
            SELECT 1
            FROM {table_name} AS {table_alias}
            GROUP BY {group_cols}
            HAVING COUNT(*) > 1
        ) dup
        """
    )

    samples = [
        {"keys": tuple(map(str, row[:-1])), "duplicate_count": int(row[-1])}
        for row in session.execute(sample_query, {"limit": limit}).fetchall()
    ]
    duplicate_groups = session.execute(count_query).scalar()

    return {"count": duplicate_groups or 0, "samples": samples}


def count_ingested_records(session, primary_keys, dump_table, prod_table):
    """
    Count records that exist in both the dump stage table and prod table.
    """
    if not primary_keys:
        return 0

    dump_alias = "ing_dump"
    prod_alias = "ing_prod"
    join_clause = " AND ".join(
        f"{prod_alias}.{_quote_identifier(pk)} = {dump_alias}.{_quote_identifier(pk)}"
        for pk in primary_keys
    )

    query = text(
        f"""
        SELECT COUNT(*)
        FROM {dump_table} AS {dump_alias}
        JOIN {prod_table} AS {prod_alias}
          ON {join_clause}
        """
    )

    count_value = session.execute(query).scalar()
    return count_value or 0


def main_table_dq(session, primary_keys, dumping_schema_table_name, schema_table_name, sample_limit=5):
    """
    Run core data-quality checks comparing stage and prod tables.
    """
    try:
        missing_summary = find_missing_in_prod(
            session,
            primary_keys,
            dumping_schema_table_name,
            schema_table_name,
            limit=sample_limit,
        )
        prod_dup_summary = find_duplicates(
            session, schema_table_name, primary_keys, limit=sample_limit
        )
        stage_dup_summary = find_duplicates(
            session, dumping_schema_table_name, primary_keys, limit=sample_limit
        )

        return {
            "missing_in_prod": missing_summary,
            "prod_duplicates": prod_dup_summary,
            "stage_duplicates": stage_dup_summary,
        }
    except Exception as e:
        session.rollback()
        return {"error": str(e)}


# Function to send an email with CSV attachments
def mailConnection(mail_data):
    sender_email = ""
    sender_password = ""
    receiver_emails = ["", "r"]

    msg = MIMEMultipart()
    msg["From"] = sender_email
    msg["To"] = ", ".join(receiver_emails)
    msg["Subject"] = "Data Quality Check: ETL Status"

    categories = [
        ("Weekly_Incremental", "Weekly Incremental"),
        ("Monthly_Incremental", "Monthly Incremental"),
        ("Yearly_Incremental", "Yearly Incremental"),
        ("Full_Refresh", "Full Refresh"),
    ]

    severity_order = {"success": 0, "warning": 1, "failed": 2}

    def fmt_text(value):
        if value is None:
            return ""
        return html.escape(str(value)).replace("\n", "<br>")

    def fmt_num(value):
        if value in (None, "N/A", "Unavailable"):
            return "N/A"
        if isinstance(value, str):
            stripped = value.replace(",", "").strip()
            if stripped == "":
                return "N/A"
            try:
                numeric = float(stripped)
            except ValueError:
                return value
        else:
            try:
                numeric = float(value)
            except (TypeError, ValueError):
                return str(value)
        if numeric.is_integer():
            return f"{int(numeric):,}"
        formatted = f"{numeric:,.2f}"
        return formatted.rstrip("0").rstrip(".")

    def format_samples(samples, limit=4):
        if not samples:
            return ""
        rendered = []
        for sample in samples[:limit]:
            if isinstance(sample, dict):
                keys = sample.get("keys")
                if isinstance(keys, (list, tuple)):
                    key_text = ", ".join(str(k) for k in keys)
                else:
                    key_text = str(keys)
                dup_count = sample.get("duplicate_count")
                if dup_count is not None:
                    rendered.append(f"{key_text} (x{dup_count})")
                else:
                    rendered.append(key_text)
            elif isinstance(sample, (list, tuple)):
                rendered.append(", ".join(str(x) for x in sample))
            else:
                rendered.append(str(sample))
        if len(samples) > limit:
            rendered.append("…")
        return "; ".join(rendered)

    def build_entry(category_key, table_name, counts):
        entry = {
            "name": table_name,
            "severity": "success",
            "metrics": [],
            "badges": [],
            "notes": [],
        }

        pipeline_steps = counts.get("pipeline_steps") or []
        pipeline_errors = counts.get("pipeline_errors") or []

        def bump(level):
            if severity_order[level] > severity_order[entry["severity"]]:
                entry["severity"] = level

        def add_badge(kind, text):
            if text:
                entry["badges"].append((kind, text))

        if "error" in counts:
            bump("failed")
            error_text = counts.get("error", "Unknown error")
            add_badge("crit", "Load failure")
            entry["notes"].append(error_text)
            if pipeline_errors:
                entry["notes"].append("Pipeline errors: " + "; ".join(pipeline_errors))
            fail_steps = [
                f"{step.get('step')}: {step.get('detail', '')}".strip(": ")
                for step in pipeline_steps
                if step.get("status") == "failed"
            ]
            warn_steps = [
                f"{step.get('step')}: {step.get('detail', '')}".strip(": ")
                for step in pipeline_steps
                if step.get("status") == "warning"
            ]
            if fail_steps:
                entry["notes"].append("Failed steps: " + "; ".join(fail_steps))
            if warn_steps:
                entry["notes"].append("Warnings: " + "; ".join(warn_steps))
            return entry

        if category_key != "Full_Refresh":
            stage_count = counts.get("stage_count")
            main_table_count = counts.get("main_table_count")
            ingested_count = counts.get("ingested_count")

            entry["metrics"].append(f"Stage: {fmt_num(stage_count)}")
            entry["metrics"].append(f"Prod: {fmt_num(main_table_count)}")
            entry["metrics"].append(
                f"Ingested: {fmt_num(ingested_count)}"
                if ingested_count not in (None, "Unavailable")
                else "Ingested: N/A"
            )

            dq_summary = counts.get("dq")
            if not isinstance(dq_summary, dict):
                dq_summary = {}

            if dq_summary.get("error"):
                bump("failed")
                add_badge("crit", f"DQ error: {dq_summary['error']}")
            else:
                missing_info = dq_summary.get("missing_in_prod") or {}
                missing_count = missing_info.get("count")
                if missing_count:
                    bump("warning")
                    add_badge(
                        "warn", f"Missing in prod ({fmt_num(missing_count)})"
                    )
                    missing_samples = format_samples(missing_info.get("samples") or [])
                    if missing_samples:
                        entry["notes"].append(
                            "Missing sample: " + missing_samples
                        )

                prod_duplicates = dq_summary.get("prod_duplicates") or {}
                prod_dup_count = prod_duplicates.get("count")
                if prod_dup_count:
                    bump("warning")
                    add_badge(
                        "warn", f"Prod duplicates ({fmt_num(prod_dup_count)})"
                    )
                    dup_samples = format_samples(prod_duplicates.get("samples") or [])
                    if dup_samples:
                        entry["notes"].append(
                            "Prod duplicate samples: " + dup_samples
                        )

                stage_duplicates = dq_summary.get("stage_duplicates") or {}
                stage_dup_count = stage_duplicates.get("count")
                if stage_dup_count:
                    bump("warning")
                    add_badge(
                        "warn", f"Stage duplicates ({fmt_num(stage_dup_count)})"
                    )
                    stage_samples = format_samples(stage_duplicates.get("samples") or [])
                    if stage_samples:
                        entry["notes"].append(
                            "Stage duplicate samples: " + stage_samples
                        )

            status_text = counts.get("status")
            if status_text and status_text.lower() != "status unavailable":
                entry["notes"].append(status_text)
        else:
            main_table_count = counts.get("main_table_count")
            ingested_count = counts.get("ingested_count")
            entry["metrics"].append(f"Total: {fmt_num(main_table_count)}")
            entry["metrics"].append(
                f"Ingested: {fmt_num(ingested_count)}"
                if ingested_count not in (None, "Unavailable")
                else "Ingested: N/A"
            )

            prod_dup_summary = counts.get("prod_duplicates") or {}
            prod_dup_count = prod_dup_summary.get("count")
            if prod_dup_count:
                bump("warning")
                add_badge(
                    "warn", f"Duplicates ({fmt_num(prod_dup_count)})"
                )
                dup_samples = format_samples(prod_dup_summary.get("samples") or [])
                if dup_samples:
                    entry["notes"].append("Duplicate samples: " + dup_samples)

        fail_steps = [
            f"{step.get('step')}: {step.get('detail', '')}".strip(": ")
            for step in pipeline_steps
            if step.get("status") == "failed"
        ]
        warn_steps = [
            f"{step.get('step')}: {step.get('detail', '')}".strip(": ")
            for step in pipeline_steps
            if step.get("status") == "warning"
        ]

        if pipeline_errors:
            bump("failed")
            add_badge("crit", "Pipeline errors recorded")
            entry["notes"].append("; ".join(pipeline_errors))

        if fail_steps:
            bump("failed")
            add_badge("crit", "Pipeline failures")
            entry["notes"].append("Failed steps: " + "; ".join(fail_steps))
        if warn_steps and entry["severity"] != "failed":
            bump("warning")
            add_badge("warn", "Pipeline warnings")
            entry["notes"].append("Warnings: " + "; ".join(warn_steps))

        if entry["severity"] == "success":
            add_badge("good", "All checks passed")

        return entry

    def render_entry(entry, compact=False):
        severity_class = {
            "success": "clean",
            "warning": "warn",
            "failed": "fail",
        }[entry["severity"]]
        title_text = entry["name"]

        if compact:
            if entry["severity"] == "success":
                title_text = f"✅ {title_text}"
            parts = [f"<li class='table-item {severity_class}'>"]
            parts.append(f"<div class='table-title'>{fmt_text(title_text)}</div>")
            if entry["badges"]:
                parts.append(
                    "<div class='badge-row'>"
                    + "".join(
                        f"<span class='badge {kind}'>{fmt_text(text)}</span>"
                        for kind, text in entry["badges"]
                    )
                    + "</div>"
                )
            if entry["metrics"]:
                parts.append(
                    "<div class='metric-line'>"
                    + " &#8226; ".join(fmt_text(m) for m in entry["metrics"])
                    + "</div>"
                )
            parts.append("</li>")
            return "".join(parts)

        badges_html = "".join(
            f"<span class='badge {kind}'>{fmt_text(text)}</span>"
            for kind, text in entry["badges"]
        )
        metrics_text = (
            " &#8226; ".join(fmt_text(m) for m in entry["metrics"])
            if entry["metrics"]
            else ""
        )

        parts = [f"<li class='table-item {severity_class}'>"]
        parts.append("<details class='table-detail'>")
        summary_content = [f"<span class='table-title'>{fmt_text(title_text)}</span>"]
        if badges_html:
            summary_content.append(
                f"<span class='summary-badges'>{badges_html}</span>"
            )
        if metrics_text:
            summary_content.append(
                f"<span class='metric-summary'>{metrics_text}</span>"
            )
        parts.append("<summary>" + "".join(summary_content) + "</summary>")

        if entry["notes"]:
            notes_html = "<br>".join(fmt_text(note) for note in entry["notes"])
            parts.append(f"<div class='notes'>{notes_html}</div>")

        parts.append("</details></li>")
        return "".join(parts)

    summary_stats = {}
    sections = []

    for category_key, label in categories:
        tables = mail_data.get(category_key, {})
        stats = {"success": 0, "warning": 0, "failed": 0, "total": 0}
        healthy_entries = []
        warn_entries = []
        fail_entries = []

        for table_name, counts in tables.items():
            entry = build_entry(category_key, table_name, counts)
            stats["total"] += 1
            stats[entry["severity"]] += 1
            if entry["severity"] == "success":
                healthy_entries.append(entry)
            elif entry["severity"] == "warning":
                warn_entries.append(entry)
            else:
                fail_entries.append(entry)

        healthy_entries.sort(key=lambda e: e["name"])
        warn_entries.sort(key=lambda e: e["name"])
        fail_entries.sort(key=lambda e: e["name"])

        summary_stats[category_key] = stats
        sections.append(
            {
                "label": label,
                "key": category_key,
                "stats": stats,
                "warnings": warn_entries,
                "errors": fail_entries,
                "healthy": healthy_entries,
            }
        )

    style_block = """
    <style>
      body { font-family: 'Segoe UI', Arial, sans-serif; background: #f6f8fa; color: #1f2933; margin: 0; padding: 24px; }
      .container { max-width: 960px; margin: 0 auto; background: #ffffff; padding: 28px 32px; border-radius: 14px; box-shadow: 0 2px 6px rgba(15, 23, 42, 0.08); }
      h1 { margin-top: 0; margin-bottom: 24px; font-size: 28px; }
      .summary-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 16px; margin-bottom: 32px; }
      .summary-card { border: 1px solid #d0d7de; border-radius: 12px; padding: 16px; background: #fafbfc; }
      .summary-card h3 { margin: 0 0 10px 0; font-size: 16px; }
      .summary-line { margin-bottom: 6px; font-size: 13px; }
      .summary-total { font-size: 13px; color: #4a5568; }
      .status-badge { display: inline-block; padding: 3px 8px; border-radius: 999px; font-size: 12px; font-weight: 600; margin-right: 6px; }
      .status-ok { background: #e6f4ea; color: #1b5e20; }
      .status-warn { background: #fff4e5; color: #8c5300; }
      .status-fail { background: #ffebee; color: #b71c1c; }
      .section-card { border: 1px solid #d0d7de; border-radius: 12px; margin-bottom: 28px; overflow: hidden; }
      .section-header { background: #f1f4f8; padding: 14px 18px; display: flex; justify-content: space-between; align-items: center; font-weight: 600; gap: 16px; }
      .section-meta { font-size: 13px; color: #52606d; }
      .section-body { padding: 18px; background: #ffffff; }
      .table-list-wrapper { overflow-x: auto; }
      .table-list { list-style: none; margin: 0; padding: 0; display: flex; flex-wrap: wrap; gap: 12px; min-width: 220px; }
      .table-item { border: 1px solid #e5e9f0; border-radius: 10px; padding: 12px 14px; background: #ffffff; flex: 0 0 220px; max-width: 260px; box-sizing: border-box; }
      .table-item.warn { border-color: #f4b400; background: #fff8e1; }
      .table-item.fail { border-color: #d93025; background: #fdecea; }
      .table-title { font-weight: 600; margin-bottom: 6px; font-size: 15px; }
      .badge-row { margin-bottom: 6px; }
      .badge { display: inline-block; padding: 2px 7px; border-radius: 999px; font-size: 11px; font-weight: 600; margin-right: 6px; margin-bottom: 4px; background: #edf2f7; color: #1a202c; }
      .badge.good { background: #e6f4ea; color: #1b5e20; }
      .badge.warn { background: #fff4e5; color: #8c5300; }
      .badge.crit { background: #ffebee; color: #b71c1c; }
      .badge.info { background: #e3f2fd; color: #0d47a1; }
      .metric-line { font-size: 13px; color: #364152; }
      .notes { font-size: 12px; color: #52606d; margin-top: 6px; word-break: break-word; max-height: 170px; overflow-y: auto; padding-right: 6px; }
      .all-clear { margin: 0; font-size: 14px; color: #1b5e20; background: #e6f4ea; padding: 10px 14px; border-radius: 8px; display: inline-block; }
      details { margin-top: 14px; }
      details summary { cursor: pointer; font-size: 13px; font-weight: 600; color: #1d4ed8; }
      .healthy-list .table-item { background: #f9fafb; border-color: #e5e9f0; }
      .section-subhead { margin: 18px 0 10px 0; font-size: 14px; font-weight: 600; }
      .section-subhead.warn { color: #8c5300; }
      .section-subhead.fail { color: #b71c1c; }
      .table-detail { margin-top: 0; }
      .table-detail > summary { display: flex; align-items: center; flex-wrap: wrap; gap: 8px; list-style: none; outline: none; font-size: 14px; font-weight: 600; color: #1f2933; cursor: pointer; }
      .table-detail > summary::-webkit-details-marker { display: none; }
      .table-detail > summary::before { content: "▸"; font-size: 12px; color: #636f83; margin-right: 4px; transition: transform 0.2s ease; }
      .table-detail[open] > summary::before { transform: rotate(90deg); }
      .summary-badges .badge { margin-bottom: 0; }
      .metric-summary { font-size: 12px; color: #52606d; }
      .table-detail[open] .table-title { font-weight: 700; }
    </style>
    """

    html_parts = [
        "<!DOCTYPE html>",
        "<html lang='en'>",
        "<head>",
        "<meta charset='UTF-8'>",
        "<meta name='viewport' content='width=device-width, initial-scale=1.0'>",
        "<title>ETL Status</title>",
        style_block,
        "</head>",
        "<body>",
        "<div class='container'>",
        "<h1>ETL Status</h1>",
    ]

    summary_html = ["<div class='summary-grid'>"]
    for category_key, label in categories:
        stats = summary_stats.get(
            category_key, {"success": 0, "warning": 0, "failed": 0, "total": 0}
        )
        summary_html.append(
            f"<div class='summary-card'>"
            f"<h3>{fmt_text(label)}</h3>"
            f"<div class='summary-line'><span class='status-badge status-ok'>{stats['success']} Success</span>"
            f"<span class='status-badge status-warn'>{stats['warning']} Warning</span>"
            f"<span class='status-badge status-fail'>{stats['failed']} Failed</span></div>"
            f"<div class='summary-total'>Total tables: {stats['total']}</div>"
            "</div>"
        )
    summary_html.append("</div>")
    html_parts.extend(summary_html)

    for section in sections:
        label = section["label"]
        stats = section["stats"]
        warn_entries = section["warnings"]
        fail_entries = section["errors"]
        healthy = section["healthy"]
        html_parts.append("<div class='section-card'>")
        meta_text = (
            f"{stats['total']} total · {stats['success']} ok · "
            f"{stats['warning']} warn · {stats['failed']} fail"
        )
        html_parts.append(
            f"<div class='section-header'><span>{fmt_text(label)}</span>"
            f"<span class='section-meta'>{fmt_text(meta_text)}</span></div>"
        )
        html_parts.append("<div class='section-body'>")

        if stats["total"] == 0:
            html_parts.append(
                "<p class='all-clear'>No tables configured for this refresh.</p>"
            )
        else:
            if healthy:
                html_parts.append(
                    f"<details><summary>Show healthy tables ({len(healthy)})</summary>"
                )
                html_parts.append("<div class='table-list-wrapper'><ul class='table-list healthy-list'>")
                for entry in healthy:
                    html_parts.append(render_entry(entry, compact=True))
                html_parts.append("</ul></div></details>")

            if warn_entries:
                html_parts.append("<div class='section-subhead warn'>Warnings</div>")
                html_parts.append("<div class='table-list-wrapper'><ul class='table-list'>")
                for entry in warn_entries:
                    html_parts.append(render_entry(entry))
                html_parts.append("</ul></div>")

            if fail_entries:
                html_parts.append("<div class='section-subhead fail'>Errors</div>")
                html_parts.append("<div class='table-list-wrapper'><ul class='table-list'>")
                for entry in fail_entries:
                    html_parts.append(render_entry(entry))
                html_parts.append("</ul></div>")

            if not warn_entries and not fail_entries:
                html_parts.append(
                    "<p class='all-clear'>✅ All tables healthy in this refresh.</p>"
                )

        html_parts.append("</div></div>")

    html_parts.extend(["</div>", "</body>", "</html>"])

    html_content = "".join(html_parts)
    msg.attach(MIMEText(html_content, "html"))

    smtp_server = "smtp.gmail.com"
    smtp_port = 465
    with open("nul", "w") as null_file, redirect_stdout(null_file):
        with smtplib.SMTP_SSL(smtp_server, smtp_port) as server:
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, receiver_emails, msg.as_string())

    print("Email sent successfully!")


def run_etl_and_generate_report(
    rows, db, session, pipeline_logs=None, excluded_tables=None
):
    pipeline_logs = pipeline_logs or {}
    excluded_tables = set(excluded_tables or [])
    email_data = {
        "Weekly_Incremental": {},
        "Monthly_Incremental": {},
        "Yearly_Incremental": {},
        "Full_Refresh": {},
    }

    try:
        for row in rows:
            original_table_name = row["table_name"]
            if original_table_name in excluded_tables:
                continue

            inquiry = convert_to_lowercase_and_underscore(original_table_name)
            inquiry_key = inquiry
            refresh_type = row["refresh_type"]
            primary_keys = (
                json.loads(row["primary_keys"]) if row["primary_keys"] else []
            )

            schema_table_name = f"raw_data.acumatica_{inquiry_key}"
            dump_schema_table_name = f"dumping_temp_data.acumatica_{inquiry_key}"
            pipeline_entry = pipeline_logs.get(inquiry_key, {})
            pipeline_error = None
            pipeline_errors = []
            if isinstance(pipeline_entry, dict):
                pipeline_steps = list(pipeline_entry.get("steps", []))
                pipeline_errors = list(pipeline_entry.get("errors", []))
                pipeline_error = pipeline_entry.get("error")
            elif isinstance(pipeline_entry, list):
                pipeline_steps = list(pipeline_entry)
            else:
                pipeline_steps = []
            if not pipeline_error and pipeline_errors:
                pipeline_error = "; ".join(
                    [err for err in pipeline_errors if err]
                )

            def record_step(step, status, detail=None):
                pipeline_steps.append(
                    {"step": step, "status": status, "detail": detail}
                )

            try:
                if refresh_type in [
                    "Monthly_Incremental",
                    "Yearly_Incremental",
                    "Weekly_Incremental",
                ]:
                    if pipeline_error:
                        email_data[refresh_type][inquiry_key] = {
                            "error": pipeline_error,
                            "pipeline_steps": pipeline_steps,
                            "pipeline_errors": pipeline_errors,
                        }
                        continue

                    # Get count from the dump table
                    try:
                        sql = f"SELECT count(*) FROM {dump_schema_table_name}"
                        count = session.execute(sql)
                        stage_count = count.scalar()
                        record_step(
                            "stage_table_count",
                            "success",
                            f"{dump_schema_table_name}: {stage_count}",
                        )
                    except Exception as count_err:
                        record_step(
                            "stage_table_count",
                            "failed",
                            str(count_err),
                        )
                        raise

                    # Run DQ checks between stage and prod
                    dq_summary = main_table_dq(
                        session,
                        primary_keys,
                        dump_schema_table_name,
                        schema_table_name,
                    )

                    missing_count = (
                        dq_summary.get("missing_in_prod", {}).get("count", 0)
                        if isinstance(dq_summary, dict)
                        else None
                    )
                    if isinstance(dq_summary, dict) and dq_summary.get("error"):
                        record_step(
                            "dq_checks",
                            "failed",
                            dq_summary.get("error"),
                        )
                    else:
                        record_step(
                            "dq_checks",
                            "success",
                            f"Missing in prod: {missing_count}",
                        )

                    if isinstance(dq_summary, dict) and dq_summary.get("error"):
                        status_detail = f"DQ error: {dq_summary['error']}"
                    elif missing_count is None:
                        status_detail = "Missing in prod: unavailable"
                    elif missing_count == 0:
                        status_detail = "Missing in prod: none"
                    else:
                        status_detail = f"Missing in prod: {missing_count}"

                    # Get count from the main table
                    try:
                        sql = f"SELECT count(*) FROM {schema_table_name}"
                        count = session.execute(sql)
                        main_table_count = count.scalar()
                        record_step(
                            "main_table_count",
                            "success",
                            f"{schema_table_name}: {main_table_count}",
                        )
                    except Exception as main_count_err:
                        record_step(
                            "main_table_count",
                            "failed",
                            str(main_count_err),
                        )
                        raise

                    try:
                        ingested_count = count_ingested_records(
                            session,
                            primary_keys,
                            dump_schema_table_name,
                            schema_table_name,
                        )
                        record_step(
                            "ingested_records",
                            "success",
                            f"{ingested_count} prod rows matched stage",
                        )
                    except Exception as ing_err:
                        ingested_count = None
                        record_step(
                            "ingested_records",
                            "failed",
                            str(ing_err),
                        )
                        try:
                            session.rollback()
                        except Exception:
                            pass

                    email_data[refresh_type][inquiry_key] = {
                        "stage_count": stage_count,
                        "status": status_detail,
                        "main_table_count": main_table_count,
                        "ingested_count": ingested_count,
                        "dq": dq_summary,
                        "pipeline_steps": pipeline_steps,
                        "pipeline_errors": pipeline_errors,
                    }

                elif refresh_type == "Full_Refresh":
                    if pipeline_error:
                        email_data["Full_Refresh"][inquiry_key] = {
                            "error": pipeline_error,
                            "pipeline_steps": pipeline_steps,
                            "pipeline_errors": pipeline_errors,
                        }
                        continue
                    # Get count from the main table
                    main_table_count = None
                    prod_dup_summary = {}

                    try:
                        sql = f"SELECT count(*) FROM {schema_table_name}"
                        count = session.execute(sql)
                        main_table_count = count.scalar()
                        record_step(
                            "main_table_count",
                            "success",
                            f"{schema_table_name}: {main_table_count}",
                        )
                    except Exception as main_count_err:
                        record_step(
                            "main_table_count",
                            "failed",
                            str(main_count_err),
                        )
                        raise

                    ingested_count = main_table_count
                    record_step(
                        "ingested_records",
                        "success",
                        f"{ingested_count} records ingested this refresh",
                    )

                    try:
                        prod_dup_summary = find_duplicates(
                            session, schema_table_name, primary_keys
                        )
                        if prod_dup_summary.get("count"):
                            record_step(
                                "prod_duplicates_check",
                                "warning",
                                f"{prod_dup_summary.get('count')} duplicate groups",
                            )
                        else:
                            record_step(
                                "prod_duplicates_check",
                                "success",
                                "No duplicate groups",
                            )
                    except Exception as dup_err:
                        record_step(
                            "prod_duplicates_check",
                            "failed",
                            str(dup_err),
                        )
                        raise

                    email_data["Full_Refresh"][inquiry_key] = {
                        "main_table_count": main_table_count,
                        "prod_duplicates": prod_dup_summary,
                        "ingested_count": ingested_count,
                        "pipeline_steps": pipeline_steps,
                        "pipeline_errors": pipeline_errors,
                    }

            except Exception as e:
                print(f"Error processing {inquiry_key}: {e}")
                session.rollback()
                email_data[refresh_type][inquiry_key] = {
                    "error": str(e),
                    "pipeline_steps": pipeline_steps,
                    "pipeline_errors": pipeline_errors + [str(e)],
                }

        mailConnection(email_data)

    except Exception as e:
        print(f"Error: {e}")
        session.rollback()


# def remove_duplication_from_stage(session, primary_keys, dump_schema_table_name, schema_table_name):
#     """
#     Remove duplicate records from the stage table based on primary keys.
#     """
#     primary_keys_str = ', '.join(f'"{key}"' for key in primary_keys)
#     print(f"Primary Keys {primary_keys_str}")
#     temp_table = f"temp_{schema_table_name.split('.')[-1]}"

#     print(f"Temp Table: {temp_table}")

#     sql_statement = f"""
#     BEGIN;

#     CREATE TEMP TABLE {temp_table} AS
#     SELECT *, ROW_NUMBER() OVER (PARTITION BY {primary_keys_str}) AS row_num
#     FROM {dump_schema_table_name};

#     DELETE FROM {temp_table} WHERE row_num > 1;

#     ALTER TABLE {temp_table} DROP COLUMN row_num;

#     TRUNCATE TABLE {dump_schema_table_name};

#     INSERT INTO {dump_schema_table_name} SELECT * FROM {temp_table};

#     DROP TABLE {temp_table};

#     COMMIT;
#     """


#     print(f"Query {sql_statement}")
#     try:
#         session.execute(sql_statement)
#         print("Duplicate removal completed successfully.")
#     except Exception as e:
#         session.rollback()
#         print(f"Error during duplicate removal: {e}")
def remove_duplication_from_stage(
    session, primary_keys, dump_schema_table_name, schema_table_name
):
    """
    Clean '.0' from primary key columns and remove duplicate records from the stage table.
    If cleaning fails, fallback to deduplication only.
    """
    try:
        print("\n[STEP 1] Preparing SQL for cleanup and deduplication...")
        primary_keys_str = ", ".join(f'"{key}"' for key in primary_keys)
        print(f"Primary Keys: {primary_keys_str}")
        print(f"Dump Table: {dump_schema_table_name}")
        print(f"Final Table: {schema_table_name}")

        table_name_only = schema_table_name.split(".")[-1]
        temp_table = f"temp_{table_name_only}"

        update_set_clauses = [
            f"\"{key}\" = TRIM(REPLACE(\"{key}\", '.0', ''))" for key in primary_keys
        ]
        where_clauses = [f"\"{key}\" LIKE '%.0'" for key in primary_keys]

        update_sql = f"""
        UPDATE {dump_schema_table_name}
        SET {", ".join(update_set_clauses)}
        WHERE {" OR ".join(where_clauses)};
        """

        dedup_sql = f"""
        BEGIN;

        -- Step 1: Clean primary key columns
        {update_sql}

        -- Step 2: Create temp table with row numbers for deduplication
        CREATE TEMP TABLE {temp_table} AS
        SELECT *, ROW_NUMBER() OVER (PARTITION BY {primary_keys_str}) AS row_num
        FROM {dump_schema_table_name};

        -- Step 3: Delete duplicates
        DELETE FROM {temp_table} WHERE row_num > 1;

        -- Step 4: Drop row_num helper column
        ALTER TABLE {temp_table} DROP COLUMN row_num;

        -- Step 5: Refresh original table with cleaned + deduplicated data
        TRUNCATE TABLE {dump_schema_table_name};

        INSERT INTO {dump_schema_table_name}
        SELECT * FROM {temp_table};

        DROP TABLE {temp_table};

        COMMIT;
        """

        print("\n[STEP 2] Executing deduplication SQL with cleaning...")
        session.execute(dedup_sql)
        print("[SUCCESS] Cleanup and duplicate removal completed successfully.")
    except Exception as e:
        print(f"[ERROR] Cleaning-based deduplication failed: {e}")
        session.rollback()

        # Fallback to deduplication only
        print("\n[STEP 3] Falling back to deduplication without cleaning...")

        fallback_sql = f"""
        BEGIN;

        CREATE TEMP TABLE {temp_table} AS
        SELECT *, ROW_NUMBER() OVER (PARTITION BY {primary_keys_str}) AS row_num
        FROM {dump_schema_table_name};

        DELETE FROM {temp_table} WHERE row_num > 1;

        ALTER TABLE {temp_table} DROP COLUMN row_num;

        TRUNCATE TABLE {dump_schema_table_name};

        INSERT INTO {dump_schema_table_name}
        SELECT * FROM {temp_table};

        DROP TABLE {temp_table};

        COMMIT;
        """

        print(f"[INFO] Executing fallback deduplication query:\n{fallback_sql}")
        try:
            session.execute(fallback_sql)
            print("[SUCCESS] Fallback duplicate removal completed successfully.")
        except Exception as e:
            session.rollback()
            print(f"[ERROR] Fallback deduplication also failed: {e}")
            raise RuntimeError(f"Fallback deduplication failed: {e}") from e


def main():
    db_host, db_username, db_password, db_name = get_secret("redshift-credentials")
    db_port = 5439
    conn_string = f"postgresql://{quote_plus(db_username)}:{quote_plus(db_password)}@{quote_plus(db_host)}:5439/{quote_plus(db_name)}"
    db = create_engine(conn_string)
    con = db.connect()
    session = sessionmaker(bind=db)()

    # Query to fetch metadata
    query = "Select * from dumping_temp_data.etl_metadata where is_active=True"
    print("Executing query:", query)

    # Execute the query and fetch the results
    result = db.execute(query)
    rows = result.fetchall()
    # print("Query Results:")

    records_inserted = {}  # Dictionary to track inserted records for each table
    pipeline_logs = {}

    excluded_tables = {
        # "IN-StockItem",
        # "LS-ConsignmentPaidInvoices",
        # "LS-DistruAdjustmentsVelixo",
        # "VC-BreakdownPL",
        # "VC-LaborDetailedETL-2",
        # "VC-Most Recent Order",
        # "VC-SalesPromoSampleSpendings",
        # "LS-COGS",
        # "PLCustomersGI",
        # "VC-OilUsagebyBatch",
        # "VC-PrdOprMat",
        # "SO-Invoice",
        # "SO-SalesOrder",
        # "VC-DepartmentBudgets",
        # "AR-Invoices and Memos",
        # "VC-Forecast",
        # "VC-OilUsage",
        # "VC-ShipmentDetails",
        # "SO-Shipment",
        #  "LS-ShipBatches",
        # "AP-Bills and Adjustments",
        # "AR-Payments and Applications",
        # "AR-Customers",
        # "AR-Salespersons",
        # "Batch Numbers",
        # "DB Adjustments",
        # "DB-ProdCostCustom",
        # "DB-ProdCostCustomTest",
        # "DB-ProdCostDataSelf",
        # "DB-Production Orders",
        # "LS-ARAgingCustomer",
        # "ls-AROvertime",
        # "LS-BudgetInquiry",
        # "LS-ConsignmentARAging",
        # "LS-ForecastProductionDemand",
        # "LS-ForecastProductionDemandsMSO",
        # "LS-LiveMenuNV2",
        # "LS-MRPSalesOrderDemand",
        # "LS-MRPSalesOrderDemandMSO",
        # "LS-MSOProductionInProcess",
        # "LS-OilBillDueExtended",
        # "LS-LaborOperation",
        # "LS-PKGForecastDemand",
        # "LS-PKGForecastDemandMSO",
        # "LS-PkgOnHandMRP",
        # "LS-ProductionDemands",
        # "LS-ProductionDemandsMSO",
        # "LS-UnbilledOil",
        # "Mktg - Co Marketing - Budtender - Slotting Fee",
        # "OpenSalesOrderDetailMSO",
        # "Riverton Output",
        # "SNR_StockItemsNoOrders",
        # "VC - Sales Prices by Customer",
        # "VC-APAging",
        # "VC-BusinessAccounts",
        # "VC-ConsignmentQuantity",
        # "VC-FinishedGoodsDefaultPrices",
        # "VC-ICOGandDIST",
        # "VC-InvbyInventoryItemNUVA",
        # "VC-InventoryHistory",
        # "VC-LiveMenuNuvata",
        # "VC-PODetailed",
        # "VC-ProductionDemandsMSO",
        # "VC-runoutdatematerial",
        # "VC-SalesRepBudget-Sample",
        # "VC-SKUsNotinStoresMA",
        # "VC-SKUsNotinStoresNV",
        # "LS-HappyTrailsLiveMenu",
        # "VC-InvbyInventoryItemHTRAILS",
        # "GummyTrackerOnHand",
        # "GummyTrackerPlanned",
        # "GummyTrackerinProcess",
        # "GummyTrackerForecast",
        # "LS-CustomerActivity",
        # "LS-PaymentApplicationWithInvoice",
        # "KS-InvoicesbyInvItem",
        # "ls-transferdetails",
        # "ls-uniquecustomers",
        # "ls-ratetypes",
        # "ls-budget",
        # "ls-materialrunoutdatemso",
        # "ls-salesprices",
        # "LS-QuickCheckETL",
        # "Sales Order Detail",
        # "LS-Vehicles",
        # "KS-BOMDetailed",
        # "KS-InvoicePaymentDetails",
        # "GummyTrackerQuarantine",
        # "LS-InvoicedBatchesAIRO",
        # "CR-BusinessAccounts2018R1",
        # "LabResults",
        # "QtyOnHandLocation",
        # "VC-Employees",
        # "VC-MerchInventory",
        # "LS-TransferLeadTime",
        
        # "LS-InvoiceWriteOff",
        # "ImportCompaniesValue",
        # "ImportCompValSO",
        # "LS-NoBatchID",
        # "AM-Material",
        # "AM-Move",
        # "AM-Production Orders"
    } 

    for row in rows:
        try:
            # Extract metadata values from the query result row
            inquiry_key = row["table_name"]
            if inquiry_key in excluded_tables:
                continue
            refresh_type = row["refresh_type"]
            primary_keys = (
                json.loads(row["primary_keys"]) if row["primary_keys"] else []
            )
            timestamp_field = row["timestamp_field"]
            time_interval = row["time_interval"]

            print(f"\n{'=' * 80}")
            print(f"[ETL_START] Processing table: {inquiry_key}")
            print(f"[ETL_START] Refresh type: {refresh_type}")
            print(f"[ETL_START] Timestamp field: {timestamp_field}")
            print(f"{'=' * 80}")

            # Get column list, API details, and other table-specific configurations

            convert_key_placeholder = inquiry_key
            pipeline_entry = pipeline_logs.setdefault(
                convert_key_placeholder, {"steps": [], "errors": []}
            )

            def log_step(step_name, status, detail=None):
                step_record = {"step": step_name, "status": status}
                if detail:
                    step_record["detail"] = detail
                pipeline_entry.setdefault("steps", []).append(step_record)
                if status == "failed":
                    pipeline_entry.setdefault("errors", []).append(
                        detail or step_name
                    )
                    # store first failure as headline error
                    pipeline_entry.setdefault("error", detail or step_name)

            try:
                (
                    column_list,
                    inquiry_id,
                    past_days_ago,
                    column_dict,
                    convert_key,
                    api_url,
                ) = get_columns_list(inquiry_key, timestamp_field)
                convert_dict = {column: object for column in column_list}
                log_step(
                    "fetch_columns",
                    "success",
                    f"{len(column_list)} columns fetched from {inquiry_key}",
                )
                if not column_list:
                    log_step(
                        "fetch_columns",
                        "warning",
                        "No columns returned from API",
                    )
                if convert_key != convert_key_placeholder:
                    pipeline_logs[convert_key] = pipeline_entry
                    pipeline_logs.pop(convert_key_placeholder, None)
            except Exception as fetch_cols_err:
                log_step("fetch_columns", "failed", str(fetch_cols_err))
                pipeline_entry.setdefault("errors", []).append(str(fetch_cols_err))
                pipeline_entry["error"] = str(fetch_cols_err)
                raise

            print(f"[ETL_CONFIG] API URL: {api_url}")
            print(f"[ETL_CONFIG] Columns found: {len(column_list)}")
            if not column_list:
                print(
                    f"[ETL_WARNING] No columns found for {inquiry_key} - API may have failed"
                )

            if refresh_type == "Weekly_Incremental":
                # Handle weekly incremental data refresh
                print("Weekly_Incremental START")
                try:
                    schema_table_name = f"raw_data.{inquiry_id}"
                    dump_schema_table_name = f"dumping_temp_data.{inquiry_id}"
                    try:
                        check_and_create_table(
                            db, inquiry_id, "raw_data", column_dict
                        )
                        log_step(
                            "check_table_prod",
                            "success",
                            f"{schema_table_name}",
                        )
                    except Exception as table_err:
                        log_step(
                            "check_table_prod", "failed", str(table_err)
                        )
                        raise
                    try:
                        check_and_create_table(
                            db, inquiry_id, "dumping_temp_data", column_dict
                        )
                        log_step(
                            "check_table_stage",
                            "success",
                            f"{dump_schema_table_name}",
                        )
                    except Exception as table_err:
                        log_step(
                            "check_table_stage", "failed", str(table_err)
                        )
                        raise

                    # Clear previous data in the table
                    # Truncate the table before starting the refresh
                    try:
                        truncate_table(session, dump_schema_table_name)
                        session.commit()
                        log_step(
                            "truncate_stage",
                            "success",
                            dump_schema_table_name,
                        )
                    except Exception as trunc_err:
                        log_step(
                            "truncate_stage",
                            "failed",
                            str(trunc_err),
                        )
                        try:
                            session.rollback()
                        except Exception:
                            pass
                        raise
                    current_datetime = datetime.now()

                    # Calculate the maximum datetime for the time interval
                    max_field_datetime = current_datetime - timedelta(days=7)
                    if max_field_datetime:
                        start_date = max_field_datetime
                        if isinstance(start_date, date):
                            start_date = datetime.combine(
                                start_date, datetime.min.time()
                            )
                    else:
                        # Default start date if no max_field_datetime is available
                        start_date = datetime.strptime(
                            "2025-01-01T00:00:00", "%Y-%m-%dT%H:%M:%S"
                        )

                    print(f"Starting to fetch all dataset from {start_date}")

                    interval_start = start_date
                    interval_end = start_date + timedelta(days=7)
                    inserted_total = 0

                    while interval_start < current_datetime:
                        interval_start_time = (
                            interval_start - timedelta(days=1)
                        ).strftime("%Y-%m-%dT00:00:00")
                        interval_end_time = interval_end.strftime("%Y-%m-%dT00:00:00")
                        refresh_bucket_path = (
                            f"s3://delete-logic-testing/{convert_key}/{convert_key}.csv"
                        )
                        params = {
                            "$filter": f"{timestamp_field} gt DateTime'{interval_start_time}' and {timestamp_field} lt DateTime'{interval_end_time}'"
                        }

                        # Fetch and process data from the API
                        try:
                            new_records_df = fetch_and_process_data(
                                api_url, params, column_list, convert_dict
                            )
                        except Exception as fetch_err:
                            log_step(
                                "fetch_data",
                                "failed",
                                f"Interval {interval_start_time} - {interval_end_time}: {fetch_err}",
                            )
                            raise
                        interval_start = interval_end
                        interval_end = interval_start + timedelta(days=7)

                        if new_records_df.empty:
                            print(f"No records found for {inquiry_key}. Skipping..")
                            continue

                        # Merge new records into the database table
                        try:
                            inserted = merge_table(
                                session,
                                new_records_df,
                                refresh_bucket_path,
                                inquiry_id,
                                column_list,
                                primary_keys,
                                dump_schema_table_name,
                            )
                            inserted_total += inserted
                        except Exception as merge_err:
                            log_step(
                                "merge_stage",
                                "failed",
                                str(merge_err),
                            )
                            raise
                        if inserted == 0:
                            break

                        # Update the inserted records count for the current table
                        if inquiry_key in records_inserted:
                            records_inserted[inquiry_key] += inserted
                        else:
                            records_inserted[inquiry_key] = inserted

                        print(
                            f"Completed {inquiry_key} for {timestamp_field} Inquiry. {inserted} records inserted"
                        )

                    print(f"Start Removing the duplicates from {inquiry_id}")
                    try:
                        remove_duplication_from_stage(
                            session,
                            primary_keys,
                            dump_schema_table_name,
                            schema_table_name,
                        )
                        log_step("dedupe_stage", "success")
                    except Exception as dedupe_err:
                        log_step("dedupe_stage", "failed", str(dedupe_err))
                        raise
                    try:
                        merge_main_table(
                            session,
                            primary_keys,
                            dump_schema_table_name,
                            schema_table_name,
                        )
                        log_step(
                            "merge_main_table",
                            "success",
                            f"Merged {inserted_total} rows",
                        )
                    except Exception as merge_main_err:
                        log_step(
                            "merge_main_table",
                            "failed",
                            str(merge_main_err),
                        )
                        raise
                    log_step(
                        "merge_stage_total",
                        "success",
                        f"{inserted_total} rows merged into stage",
                    )
                except Exception as e:
                    try:
                        session.rollback()
                    except Exception:
                        pass
                    print(f"Error at weekly increment: ,{e}")
                    log_step("weekly_incremental", "failed", str(e))
                    pipeline_entry.setdefault("errors", []).append(str(e))
                    pipeline_entry["error"] = str(e)

            elif refresh_type == "Monthly_Incremental":
                # Handle Monthly incremental data refresh
                print("Monthly_Incremental START")
                try:
                    schema_table_name = f"raw_data.{inquiry_id}"
                    dump_schema_table_name = f"dumping_temp_data.{inquiry_id}"
                    current_datetime = datetime.now()

                    try:
                        check_and_create_table(
                            db, inquiry_id, "raw_data", column_dict
                        )
                        log_step("check_table_prod", "success", schema_table_name)
                    except Exception as table_err:
                        log_step("check_table_prod", "failed", str(table_err))
                        raise
                    try:
                        check_and_create_table(
                            db, inquiry_id, "dumping_temp_data", column_dict
                        )
                        log_step("check_table_stage", "success", dump_schema_table_name)
                    except Exception as table_err:
                        log_step("check_table_stage", "failed", str(table_err))
                        raise
                    # Truncate the table before starting the refresh
                    try:
                        truncate_table(session, dump_schema_table_name)
                        log_step("truncate_stage", "success", dump_schema_table_name)
                    except Exception as trunc_err:
                        log_step("truncate_stage", "failed", str(trunc_err))
                        try:
                            session.rollback()
                        except Exception:
                            pass
                        raise
                    # Query to fetch the maximum value of the timestamp field in the table
                    field_max_query = (
                        f"SELECT MAX({timestamp_field}) FROM {schema_table_name};"
                    )
                    field_max_results = execute_query(session, field_max_query)
                    log_step(
                        "max_timestamp_query",
                        "success",
                        f"{schema_table_name}",
                    )
                    max_field_datetime = field_max_results[0][0]

                    if max_field_datetime:
                        start_date = max_field_datetime
                        if isinstance(start_date, date):
                            start_date = datetime.combine(
                                start_date, datetime.min.time()
                            )
                    else:
                        # Default start date if no max timestamp is available
                        start_date = datetime.strptime(
                            "2021-01-01T00:00:00", "%Y-%m-%dT%H:%M:%S"
                        )

                    print(f"Starting to fetch all dataset from {start_date}")

                    interval_start = start_date
                    interval_end = start_date + timedelta(days=30)
                    inserted_total = 0

                    while interval_start < current_datetime:
                        interval_start_time = (
                            interval_start - timedelta(days=1)
                        ).strftime("%Y-%m-%dT00:00:00")
                        interval_end_time = interval_end.strftime("%Y-%m-%dT00:00:00")

                        # Define the S3 bucket path for refresh data
                        refresh_bucket_path = f"s3://acumatica-sunderstorm-analytics/weekly_loads/{convert_key}/{convert_key}_new_records{interval_start.strftime('%Y_%m_%d')}.csv"

                        params = {
                            "$filter": f"{timestamp_field} gt DateTime'{interval_start_time}' and {timestamp_field} lt DateTime'{interval_end_time}'"
                        }

                        # Fetch and process data from the API
                        try:
                            new_records_df = fetch_and_process_data(
                                api_url, params, column_list, convert_dict
                            )
                        except Exception as fetch_err:
                            log_step(
                                "fetch_data",
                                "failed",
                                f"Interval {interval_start_time} - {interval_end_time}: {fetch_err}",
                            )
                            raise

                        interval_start = interval_end
                        interval_end = interval_start + timedelta(days=30)

                        if new_records_df.empty:
                            print(f"No records found for {inquiry_key}. Skipping..")
                            continue

                        # Merge new records into the database table
                        try:
                            inserted = merge_table(
                                session,
                                new_records_df,
                                refresh_bucket_path,
                                inquiry_id,
                                column_list,
                                primary_keys,
                                dump_schema_table_name,
                            )
                            inserted_total += inserted
                        except Exception as merge_err:
                            log_step("merge_stage", "failed", str(merge_err))
                            raise
                        if inserted == 0:
                            break

                        # Update the inserted records count for the current table
                        if inquiry_key in records_inserted:
                            records_inserted[inquiry_key] += inserted
                        else:
                            records_inserted[inquiry_key] = inserted

                        print(
                            f"Completed {inquiry_key} for {timestamp_field} Inquiry. {inserted} records inserted"
                        )

                    print(f"Start Removing the duplicates from {inquiry_id}")
                    try:
                        remove_duplication_from_stage(
                            session,
                            primary_keys,
                            dump_schema_table_name,
                            schema_table_name,
                        )
                        log_step("dedupe_stage", "success")
                    except Exception as dedupe_err:
                        log_step("dedupe_stage", "failed", str(dedupe_err))
                        raise
                    try:
                        merge_main_table(
                            session,
                            primary_keys,
                            dump_schema_table_name,
                            schema_table_name,
                        )
                        log_step(
                            "merge_main_table",
                            "success",
                            f"Merged {inserted_total} rows",
                        )
                    except Exception as merge_main_err:
                        log_step(
                            "merge_main_table",
                            "failed",
                            str(merge_main_err),
                        )
                        raise
                    log_step(
                        "merge_stage_total",
                        "success",
                        f"{inserted_total} rows merged into stage",
                    )
                except Exception as e:
                    try:
                        session.rollback()
                    except Exception:
                        pass
                    print(f"Error at Monthly increment: ,{e}")
                    log_step("monthly_incremental", "failed", str(e))
                    pipeline_entry.setdefault("errors", []).append(str(e))
                    pipeline_entry["error"] = str(e)

            elif refresh_type == "Full_Refresh":
                # Handle Full_Refresh data refresh
                print("Full_Refresh START")
                try:
                    schema_table_name = f"raw_data.{inquiry_id}"
                    try:
                        check_and_create_table(
                            db, inquiry_id, "raw_data", column_dict
                        )
                        log_step("check_table_prod", "success", schema_table_name)
                    except Exception as table_err:
                        log_step("check_table_prod", "failed", str(table_err))
                        raise

                    # Handle cases where no timestamp field is defined
                    if timestamp_field is None:
                        # Fetch all data without a timestamp filter
                        try:
                            new_records_df = fetch_and_process_data(
                                api_url, None, column_list, convert_dict
                            )
                            log_step(
                                "fetch_data",
                                "success",
                                f"{len(new_records_df)} records fetched",
                            )
                        except Exception as fetch_err:
                            log_step("fetch_data", "failed", str(fetch_err))
                            raise
                        inserted = 0
                        if not new_records_df.empty:
                            # Truncate the table before inserting new data
                            try:
                                truncate_table(session, schema_table_name)
                                log_step(
                                    "truncate_main",
                                    "success",
                                    schema_table_name,
                                )
                            except Exception as trunc_err:
                                log_step("truncate_main", "failed", str(trunc_err))
                                try:
                                    session.rollback()
                                except Exception:
                                    pass
                                raise
                            refresh_bucket_path = f"s3://acumatica-sunderstorm-analytics/weekly_loads/{convert_key}/{convert_key}.csv"
                            # Upload data to Redshift and copy to the table
                            try:
                                inserted = upload_and_copy_to_redshift(
                                    session,
                                    new_records_df,
                                    refresh_bucket_path,
                                    schema_table_name,
                                    column_list,
                                )
                                log_step(
                                    "copy_to_redshift",
                                    "success",
                                    f"Inserted {inserted} rows",
                                )
                            except Exception as copy_err:
                                log_step(
                                    "copy_to_redshift", "failed", str(copy_err)
                                )
                                raise

                        # Update the inserted records count for the current table
                        if inquiry_key in records_inserted:
                            records_inserted[inquiry_key] += inserted
                        else:
                            records_inserted[inquiry_key] = inserted

                        print(
                            f"Completed {inquiry_key} Inquiry. {inserted} records inserted"
                        )
                    else:
                        # Handle cases with a timestamp field for incremental fetching
                        current_datetime = datetime.now()
                        interval_start = datetime(2021, 1, 1)
                        interval_end = interval_start + timedelta(days=7)

                        # Truncate the table before starting the refresh
                        try:
                            truncate_table(session, schema_table_name)
                            log_step("truncate_main", "success", schema_table_name)
                        except Exception as trunc_err:
                            log_step("truncate_main", "failed", str(trunc_err))
                            try:
                                session.rollback()
                            except Exception:
                                pass
                            raise
                        inserted_total = 0

                        while interval_start < current_datetime:
                            interval_start_time = interval_start.strftime(
                                "%Y-%m-%dT00:00:00"
                            )
                            interval_end_time = interval_end.strftime(
                                "%Y-%m-%dT00:00:00"
                            )

                            # Define the S3 bucket path for the current interval's data
                            refresh_bucket_path = f"s3://acumatica-sunderstorm-analytics/weekly_loads/{convert_key}/{convert_key}_new_records{interval_start.strftime('%Y_%m_%d')}.csv"

                            params = {
                                "$filter": f"{timestamp_field} gt DateTime'{interval_start_time}' and {timestamp_field} lt DateTime'{interval_end_time}'"
                            }

                            # Fetch and process data for the current interval
                            try:
                                new_records_df = fetch_and_process_data(
                                    api_url, params, column_list, convert_dict
                                )
                            except Exception as fetch_err:
                                log_step(
                                    "fetch_data",
                                    "failed",
                                    f"Interval {interval_start_time} - {interval_end_time}: {fetch_err}",
                                )
                                raise

                            # Move to the next interval
                            interval_start = interval_end
                            interval_end = interval_start + timedelta(days=7)

                            if new_records_df.empty:
                                # Skip processing if no records are found for the interval
                                print(f"No records found for {inquiry_key}. Skipping..")
                                continue

                            # Upload data to Redshift and copy to the table
                            try:
                                inserted = upload_and_copy_to_redshift(
                                    session,
                                    new_records_df,
                                    refresh_bucket_path,
                                    schema_table_name,
                                    column_list,
                                )
                                inserted_total += inserted
                            except Exception as copy_err:
                                log_step(
                                    "copy_to_redshift",
                                    "failed",
                                    str(copy_err),
                                )
                                raise

                            # Update the inserted records count for the current table
                            if inquiry_key in records_inserted:
                                records_inserted[inquiry_key] += inserted
                            else:
                                records_inserted[inquiry_key] = inserted

                        print(
                            f"Completed {inquiry_key} for {timestamp_field} Inquiry. {inserted} records inserted"
                        )
                        log_step(
                            "copy_to_redshift",
                            "success",
                            f"Inserted {inserted_total or inserted} rows",
                        )
                except Exception as e:
                    try:
                        session.rollback()
                    except Exception:
                        pass
                    print(f"Error at Full refresh increment: ,{e}")
                    log_step("full_refresh", "failed", str(e))
                    pipeline_entry.setdefault("errors", []).append(str(e))
                    pipeline_entry["error"] = str(e)

            elif refresh_type == "Yearly_Incremental":
                # Handle Yearly incremental data refresh

                print("Yearly_Incremental START")
                try:
                    schema_table_name = f"raw_data.{inquiry_id}"
                    dump_schema_table_name = f"dumping_temp_data.{inquiry_id}"
                    current_datetime = datetime.now()

                    try:
                        check_and_create_table(
                            db, inquiry_id, "raw_data", column_dict
                        )
                        log_step("check_table_prod", "success", schema_table_name)
                    except Exception as table_err:
                        log_step("check_table_prod", "failed", str(table_err))
                        raise
                    try:
                        check_and_create_table(
                            db, inquiry_id, "dumping_temp_data", column_dict
                        )
                        log_step("check_table_stage", "success", dump_schema_table_name)
                    except Exception as table_err:
                        log_step("check_table_stage", "failed", str(table_err))
                        raise

                    # Truncate the table before starting the refresh
                    try:
                        truncate_table(session, dump_schema_table_name)
                        log_step("truncate_stage", "success", dump_schema_table_name)
                    except Exception as trunc_err:
                        log_step("truncate_stage", "failed", str(trunc_err))
                        try:
                            session.rollback()
                        except Exception:
                            pass
                        raise

                    # Set the start date for the yearly interval (365 days ago)
                    interval_start = datetime.now() - timedelta(days=365)
                    # Define the initial interval end date (31 days after the start)
                    interval_end = interval_start + timedelta(31)
                    inserted_total = 0

                    while interval_start < current_datetime:
                        print(interval_start, interval_end)
                        interval_start_time = (
                            interval_start - timedelta(days=1)
                        ).strftime("%Y-%m-%dT00:00:00")  # take care of gt
                        interval_end_time = interval_end.strftime("%Y-%m-%dT00:00:00")

                        interval_start_time_date = interval_start.strftime("%Y_%m_%d")
                        refresh_bucket_path = f"s3://acumatica-sunderstorm-analytics/weekly_loads/{convert_key}/{convert_key}_new_records{interval_start_time_date}.csv"

                        params = {
                            "$filter": f"{timestamp_field} gt DateTime'{interval_start_time}' and {timestamp_field} lt DateTime'{interval_end_time}'"
                        }

                        # Fetch and process data for the current interval
                        try:
                            new_records_df = fetch_and_process_data(
                                api_url, params, column_list, convert_dict
                            )
                        except Exception as fetch_err:
                            log_step(
                                "fetch_data",
                                "failed",
                                f"Interval {interval_start_time} - {interval_end_time}: {fetch_err}",
                            )
                            raise

                        # Move to the next interval
                        interval_start = interval_end
                        interval_end = interval_start + timedelta(days=30)

                        if new_records_df.empty:
                            # Skip processing if no records are found for the interval
                            print(f"No records found for {inquiry_key}. Skipping..")
                            continue

                        # Merge the new records into the target table

                        try:
                            inserted = merge_table(
                                session,
                                new_records_df,
                                refresh_bucket_path,
                                inquiry_id,
                                column_list,
                                primary_keys,
                                dump_schema_table_name,
                            )
                            inserted_total += inserted
                        except Exception as merge_err:
                            log_step("merge_stage", "failed", str(merge_err))
                            raise

                        if inserted == 0:
                            break

                        # Update the inserted records count for the current table
                        if inquiry_key in records_inserted:
                            records_inserted[inquiry_key] += inserted
                        else:
                            records_inserted[inquiry_key] = inserted
                    interval_start = datetime.now() - timedelta(days=365)
                    query = f"""
                        delete from {schema_table_name} where {timestamp_field} >= '{interval_start.strftime("%Y-%m-%d")}';
                        """
                    session.execute(query)
                    session.commit()

                    print(f"Start Removing the duplicates from {inquiry_id}")
                    try:
                        remove_duplication_from_stage(
                            session,
                            primary_keys,
                            dump_schema_table_name,
                            schema_table_name,
                        )
                        log_step("dedupe_stage", "success")
                    except Exception as dedupe_err:
                        log_step("dedupe_stage", "failed", str(dedupe_err))
                        raise
                    try:
                        merge_main_table(
                            session,
                            primary_keys,
                            dump_schema_table_name,
                            schema_table_name,
                        )
                        log_step(
                            "merge_main_table",
                            "success",
                            f"Merged {inserted_total} rows",
                        )
                    except Exception as merge_main_err:
                        log_step(
                            "merge_main_table",
                            "failed",
                            str(merge_main_err),
                        )
                        raise
                    log_step(
                        "merge_stage_total",
                        "success",
                        f"{inserted_total} rows merged into stage",
                    )
                except Exception as e:
                    try:
                        session.rollback()
                    except Exception:
                        pass
                    print(f"Error at Yearly increment: ,{e}")
                    log_step("yearly_incremental", "failed", str(e))
                    pipeline_entry.setdefault("errors", []).append(str(e))
                    pipeline_entry["error"] = str(e)

        # Handle any exceptions that occur during processing
        except Exception as e:
            try:
                session.rollback()
            except Exception:
                pass
            print(f"[ETL_FAILURE] Failed to process {inquiry_key}")
            print(f"[ETL_FAILURE] Error: {str(e)}")
            print(f"[ETL_FAILURE] Full traceback:")
            print(traceback.format_exc())
            raise e

    # Log a summary of records inserted
    print("Summary of records inserted:")
    for table, count in records_inserted.items():
        print(f"Table: {table}, Records Inserted: {count}")

    run_etl_and_generate_report(rows, db, session, pipeline_logs, excluded_tables)
    # Close database connections
    con.close()


main()

