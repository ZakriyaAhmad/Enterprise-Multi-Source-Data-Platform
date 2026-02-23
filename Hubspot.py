import json
import requests
from datetime import datetime, timedelta, timezone

import csv
from datetime import timezone as dt_timezone
import boto3
import pandas as pd
import pg8000
from typing import List
import io
import time
import re
import uuid
import logging
import warnings
# Time‑zone helpers
from zoneinfo import ZoneInfo
PT_ZONE = ZoneInfo("America/Los_Angeles")  # Pacific time; honours DST

# ----------------------------------------------------------------------
# Centralised logger – switch to DEBUG for very chatty output
# ----------------------------------------------------------------------
print(">>> entered hubspot ETL script")  # temporary startup sentinel
import sys

# --- send the standard Python 'logging' output to STDOUT so it appears
# --- in the same Glue CloudWatch *driver‑stdout* stream as print() calls.
for h in logging.root.handlers[:]:
    logging.root.removeHandler(h)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],  # <‑‑ STDOUT, not STDERR
    force=True,
)
logger = logging.getLogger(__name__)

# Silence the pandas “highly fragmented” warning; we eliminate the root
# cause below, but this keeps the console clean if it ever re‑appears.
warnings.filterwarnings("ignore", category=pd.errors.PerformanceWarning)


def dt_to_epoch_ms(dt: datetime) -> int:
    """
    HubSpot Search API expects milliseconds since epoch for all
    filters on DATE/TIMESTAMP properties.  This helper converts a
    timezone‑aware datetime → int ms.
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


# Accept 0–6 fractional‐second digits and either:
#   • a trailing ‘Z’
#   • an explicit ±HH:MM offset
#   • the (broken) “HH:MM” offset that HubSpot sometimes emits without a sign
_ISO_TS_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-]?\d{2}:\d{2})$"
)

def _iso_utc_to_pt(ts: str) -> str:
    """
    Convert an ISO‑8601 timestamp that ends with 'Z' (UTC) to Pacific Time,
    returning the same ISO format but with the proper −07:00/−08:00 offset.
    If *ts* is not a matching timestamp, it is returned unchanged.
    """
    # --- normalise the incoming string so datetime.fromisoformat() can parse it ---
    src = ts
    if src.endswith("Z"):
        # convert trailing ‘Z’ → ‘+00:00’
        src = src[:-1] + "+00:00"
    elif re.match(r".*\d{2}:\d{2}$", src) and not re.match(r".*[+-]\d{2}:\d{2}$", src):
        # HubSpot bug: offset present but sign missing (e.g. “…41.1690:00”).
        # Keep the full fractional seconds and just prefix the missing “+00:00”.
        src = re.sub(r"(\d{2}:\d{2})$", r"+00:00", src)
    if not src or not _ISO_TS_RE.match(src):
        return src
    try:
        dt = datetime.fromisoformat(src).astimezone(PT_ZONE)
        # Preserve millisecond precision; include offset (‑07:00/‑08:00)
        return dt.isoformat(timespec="milliseconds")
    except ValueError:
        return src  # leave untouched if parse fails

def convert_df_dates_to_pt(df: pd.DataFrame) -> None:
    """
    In‑place convert every string column that looks like a UTC ISO timestamp
    (with trailing 'Z') to Pacific Time. Operates column‑wise for speed.
    """
    for col in df.columns:
        if df[col].dtype == "object":
            mask = df[col].str.match(_ISO_TS_RE, na=False)
            if mask.any():
                df.loc[mask, col] = df.loc[mask, col].apply(_iso_utc_to_pt)




from botocore.exceptions import ClientError


ACCESS_TOKEN = ""
HUB_ID = ""

# The schema names for the custom objects in HubSpot.
DAILY_USER_METRICS_SCHEMA_NAME = f"p{HUB_ID}_ht_daily_user_metrics"
GEOFENCE_VISITS_SCHEMA_NAME = f"p{HUB_ID}_ht_visit"
FORM_SUBMISSIONS_SCHEMA_NAME = f"p{HUB_ID}_form_submissions"

# These are the properties of the Daily User Metrics custom objects.
DAILY_USER_METRICS_PROPERTIES = [
    "daily_user_metrics_name",  # YYYY-MM-DD driver_handle
    "distance",  # the cumulative distance traveled in meters
    "driver_handle",  # the worker's driver_handle
    "recorded_at",  # timestamp that the Daily User Metrics object was recorded
    "user_name",  # the name of the worker
    "visits",  # the number of visits recorded
]

# These are the properties of the Geofence Visit custom objects.
GEOFENCE_VISITS_PROPERTIES = [
    "checkin_time",  # the timestamp of the check-in time
    "checkout_time",  # the timestamp of the check-out time
    "distance_traveled_to_destination",  # the distance traveled to the destination in meters
    "embed_url",  # the HyperTrack embed URL
    "geofence_address",  # the address of the geofence
    "geofence_description",  # the description of the geofence
    "geofence_marker_id",  # the geofence marker ID for the visit
    "geofence_name",  # the name of the geofence
    "time_spent_in_minutes",  # the time spent in the geofence in minutes
    "user_name",  # the name of the worker
]

# These are the properties of Form Submissions Custom Object.
FORM_SUBMISSIONS_PROPERTIES = [
    "additional_comments",
    "are_classics_currently_displayed_",
    "are_nanos_currently_displayed_",
    "are_rosin_belts_currently_displayed_",
    "associated_company_id",
    "associated_company_name",
    "belts_picture_upload",
    "business_account_cd",
    "choose_option",
    "classics_picture_upload",
    "combo_of_deal_____what_sku_s__were_the_deal_being_ran_on__",
    "current_display_photo_upload",
    "custom_sleep_display_picture_upload",
    "customer_name",
    "day_of_the_week",
    "day_of_the_week_or_the_demo",
    "did_they_have_any_interesting_questions_",
    "did_you_add_new_signage_to_the_store_",
    "did_you_conduct_a_pop_up_",
    "did_you_conduct_a_pop_up_____photo_upload",
    "did_you_conduct_a_pop_up____kanha_which_campaign__account_form_",
    "did_you_conduct_a_pop_up____what_brand_was_it_for",
    "did_you_conduct_a_pop_up____what_brand_was_it_for__account_form_",
    "did_you_conduct_a_pop_up____which_campaign",
    "did_you_conduct_training_",
    "did_you_conduct_training_____kanha_which_campaign__account_form_",
    "did_you_conduct_training____what_brand_was_it_for__account_form_",
    "did_you_conduct_training___account_form_",
    "did_you_drop_food",
    "did_you_drop_food___other_what_did_you_drop_off_",
    "did_you_drop_off_non_meds_",
    "did_you_drop_off_retail_assets",
    "did_you_drop_retail_assets_",
    "did_you_drop_retail_assets____choose_option",
    "did_you_drop_retail_assets____choose_option__account_form_",
    "did_you_drop_retail_assets___account_form_",
    "did_you_drop_retail_assets__which_campaign",
    "did_you_drop_retail_assets__which_campaign__account_form_",
    "did_you_drop_swag",
    "did_you_drop_swag___how_much_did_you_drop_off_",
    "did_you_drop_swag___other_what_did_you_drop_off_",
    "did_you_drop_swag__what_did_you_drop_off_",
    "did_you_feel_our_attendance_was_successful_at_the_event_",
    "did_you_feel_our_attendance_was_successful_at_the_event___why_as_it_not_successful_",
    "did_you_feel_our_attendance_was_successful_at_the_event___why_was_it_successful_",
    "did_you_install_a_new_display_",
    "did_you_install_new_displays_",
    "did_you_install_new_displays_____choose_option__account_form_",
    "did_you_install_new_displays_____upload_pictures__account_form_",
    "did_you_install_new_displays____which_campaign__account_form_",
    "did_you_install_new_displays__account_form_",
    "did_you_install_new_signage_",
    "did_you_install_new_signage_____choose_option",
    "did_you_install_new_signage_____choose_option__account_form_",
    "did_you_install_new_signage_____pictures_upload",
    "did_you_install_new_signage_____upload_pictures__account_form_",
    "did_you_install_new_signage____which_campaign",
    "did_you_install_new_signage____which_campaign__account_form_",
    "did_you_install_new_signage__account_form_",
    "did_you_work_an_event_",
    "did_you_work_an_event_____what_was_the_deal__",
    "did_you_work_an_event____estimated_number_of_units_sold",
    "did_you_work_an_event____photo_upload",
    "did_you_work_an_event____which_campaign",
    "did_you_work_an_event____which_campaign__account_form_",
    "did_you_work_an_event___day_of_the_week",
    "do_you_feel_our_attendance_was_successful_in_the_event_",
    "do_you_have_a___patients_seen_during_your_demo_",
    "do_you_have_a___patients_seen_during_your_event_",
    "estimated_number_of_units_sold",
    "fx_picture_upload",
    "how_are_minis_currently_displayed_",
    "how_are_we_currently_displayed_",
    "how_long_did_you_work_the_event_",
    "how_long_was_the_whole_event",
    "how_many_budtenders_did_you_train_",
    "how_many_budtenders_does_the_store_have_",
    "how_many_non_meds_did_you_drop_off_",
    "how_many_of_each_item_did_you_drop_off_",
    "how_many_of_these_vendors_were_edibles_companies_",
    "how_many_of_these_vendors_were_edibles_companies____event",
    "how_many_of_those_vendors_were_edible_companies_",
    "how_many_patients_were_in_the_shop_during_the_duration_of_your_demo_",
    "how_many_people_did_you_get_to_follow_us_",
    "how_many_products_in_total_were_sold_during_the_demo_not_including_promo_units",
    "how_many_products_in_total_were_sold_during_the_event_not_including_promo_units",
    "how_many_snacks_did_you_drop_off_",
    "how_many_vendors_were_at_the_event_",
    "how_many_vendors_were_at_the_event_with_you",
    "how_much_did_you_drop_off_",
    "how_was_the_foot_traffic_during_the_pop_up_",
    "how_was_the_foot_traffic_in_the_shop_during_the_demo_",
    "how_was_the_foot_traffic_in_the_shop_during_the_event_",
    "hs_all_accessible_team_ids",
    "hs_all_assigned_business_unit_ids",
    "hs_all_owner_ids",
    "hs_all_team_ids",
    "hs_created_by_user_id",
    "hs_createdate",
    "hs_lastmodifieddate",
    "hs_merged_object_ids",
    "hs_object_id",
    "hs_object_source",
    "hs_object_source_detail_1",
    "hs_object_source_detail_2",
    "hs_object_source_detail_3",
    "hs_object_source_id",
    "hs_object_source_label",
    "hs_object_source_user_id",
    "hs_pinned_engagement_id",
    "hs_read_only",
    "hs_shared_team_ids",
    "hs_shared_user_ids",
    "hs_unique_creation_key",
    "hs_updated_by_user_id",
    "hs_user_ids_of_all_notification_followers",
    "hs_user_ids_of_all_notification_unfollowers",
    "hs_user_ids_of_all_owners",
    "hs_was_imported",
    "hubspot_owner_assigneddate",
    "hubspot_owner_id",
    "hubspot_team_id",
    "if_you_have_an_actual_number_of_products_sold_please_specify",
    "is_classics_currently_displayed_",
    "is_fx_currently_displayed_",
    "is_minis_currently_displayed_",
    "is_minis_currently_displayed______upload_pictures",
    "is_nano_currently_displayed_",
    "is_rosin_belts_currently_displayed_",
    "is_seasonal_currently_displayed_",
    "is_the_custom_sleep_display_currently_displayed_",
    "is_there_a_custom_sleep_display_currently_displayed_",
    "is_there_a_promo_running__",
    "is_vape_currently_displayed_",
    "license_number",
    "nano_picture_upload",
    "other____what_new_signage_did_you_add__",
    "other____what_was_the_deal_",
    "other____what_was_the_deal__did_you_work_an_event____",
    "photo_upload",
    "picture_of_promo_signage",
    "pictures_upload",
    "place_for_photo_of_demo_set_up",
    "place_for_photo_of_display",
    "place_for_photo_of_event_set_up",
    "place_to_upload_up_to_10_pictures",
    "record_name",
    "seasonal_picture_upload",
    "source_form_name",
    "street_address",
    "submission_idempotent_id",
    "vape_picture_upload",
    "was_the_store_completely_out_of_stock_",
    "was_the_store_out_of_stock_on_any_sku_s__",
    "was_there_signage_promoting_the_deal_",
    "were_you_able_to_get_anyone_to_follow_us_on_social_media",
    "what_brand_was_it_for",
    "what_brand_was_it_for___if_kanha_which_campaign",
    "what_category_was_on_promo",
    "what_did_you_drop_off_",
    "what_did_you_train_the_budtenders_on_",
    "what_display_did_you_install_",
    "what_is_the_specific_promo_running__",
    "what_new_signage_did_you_add_",
    "what_platform_did_you_get_them_to_follow_us_on_",
    "what_retail_asset_did_you_drop_off",
    "what_sku_of_non_meds_did_you_drop_off_",
    "what_sku_product_launch_was_this_signage_related_to_",
    "what_sku_s__was_the_store_out_of_stock_on_",
    "what_sku_s__were_the_deal_being_ran_on_",
    "what_snacks_did_you_drop_off_",
    "what_swag_items_did_you_drop_off_",
    "what_was_the_deal",
    "what_was_the_event_for_",
    "what_was_the_event_name_",
    "what_were_their_names_and_positions_",
    "which_campaign",
    "why_do_you_feel_if_was_successful",
    "why_do_you_feel_it_was_unsuccessful",
]

# These are the default properties that HubSpot includes in all objects.
# This constant is only for reference and is not used by the code.
HUBSPOT_DEFAULT_PROPERTIES = [
    "hs_createdate",  # the timestamp that the object was created in HubSpot
    "hs_lastmodifieddate",  # the timestamp that the object was last modified in HubSpot
    "hs_object_id",  # the object ID in HubSpot
]


def upload_df_s3(df, s3_uri: str):
    """
    Upload a DataFrame to S3 as a well‑quoted CSV (handles commas/newlines).
    """
    logger.debug("Uploading DataFrame to %s (%d rows)", s3_uri, len(df))
    match = re.match(r"s3://([^/]+)/(.+)", s3_uri)
    if not match:
        raise ValueError(f"Invalid S3 URI: {s3_uri}")
    bucket, key = match.groups()

    csv_buffer = io.StringIO()
    df.to_csv(
        csv_buffer,
        index=False,
        quoting=csv.QUOTE_ALL,
        quotechar='"',
        escapechar="\\",
        line_terminator="\n",
    )
    csv_buffer.seek(0)

    logger.debug("PUT to S3 bucket=%s key=%s", bucket, key)
    boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())


def get_secret(secret_name):
    """Retrieves secrets from AWS Secrets Manager."""

    region_name = "us-west-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        logger.error("Error retrieving secret %s: %s", secret_name, e)
        raise e

    secret = get_secret_value_response["SecretString"]
    secret_data = json.loads(secret)

    return (
        secret_data["Host"],
        secret_data["Username"],
        secret_data["Password"],
        secret_data["Database"],
    )


def upload_and_merge_into_redshift(cursor, data_frame, table_name, primary_keys):
    logger.info("Start Redshift load for %s – %d rows", table_name, len(data_frame))
    query = ""  # initialise so it's always defined for exception printing
    try:
        schema_table_name = f"raw_data.hubspot_{table_name}"
        stage_table = f"hubspot_{table_name}_stage"
        s3_path = f"s3://analytics/hubspot/{table_name}.csv"

        # ------------------------------------------------------------------
        # Ensure the DataFrame exactly matches the Redshift table structure
        # (avoids NOT NULL copy errors when new columns are added).
        # ------------------------------------------------------------------
        cursor.execute(f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'raw_data'
              AND table_name  = 'hubspot_{table_name}'
            ORDER BY ordinal_position
        """)
        target_columns = [row[0] for row in cursor.fetchall()]

        # ------------------------------------------------------------------
        # Ensure the frame has *every* column the target table expects,
        # in the exact same order, **without** creating hundreds of blocks.
        # Build a fresh DataFrame in one go with the exact column ordering.
        # This keeps the underlying memory contiguous and avoids the
        # “DataFrame is highly fragmented” performance warning.
        # ------------------------------------------------------------------
        data_frame = (
            pd.DataFrame(data_frame, columns=target_columns).fillna(
                pd.NA
            )  # ensure missing cols start as <NA>
        )

        # ── final pass to normalise any UTC timestamps, incl. ones we just added ──
        convert_df_dates_to_pt(data_frame)

        # ------------------------------------------------------------------
        # Fill NULLs for NOT NULL columns based on data type and provide intelligent defaults for bookkeeping columns
        # ------------------------------------------------------------------
        cursor.execute(f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'raw_data'
              AND table_name  = 'hubspot_{table_name}'
              AND is_nullable = 'NO'
        """)
        not_null_cols = cursor.fetchall()  # list of (col, type)
        for col, sql_type in not_null_cols:
            if col not in data_frame.columns:
                data_frame[col] = pd.NA

            # Special handling for bookkeeping columns
            if col == "_airbyte_raw_id":
                data_frame[col] = data_frame[col].fillna(
                    pd.Series(
                        [str(uuid.uuid4()) for _ in range(len(data_frame))],
                        index=data_frame.index,
                    )
                )
                continue  # already satisfied
            if col == "_airbyte_meta":
                data_frame[col] = data_frame[col].fillna('{"source":"hubspot"}')
                continue
            if col == "_airbyte_extracted_at":
                now_iso = (
                    datetime.now(dt_timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
                    + "Z"
                )
                data_frame[col] = data_frame[col].fillna(now_iso)
                continue

            # Generic fall‑back based on SQL type
            if sql_type in ("character varying", "text"):
                data_frame[col] = data_frame[col].fillna("")
            elif sql_type in (
                "integer",
                "bigint",
                "smallint",
                "decimal",
                "numeric",
                "double precision",
            ):
                data_frame[col] = data_frame[col].fillna(0)
            elif sql_type == "boolean":
                data_frame[col] = data_frame[col].fillna(False)
            elif sql_type in (
                "date",
                "timestamp without time zone",
                "timestamp with time zone",
            ):
                now_iso = (
                    datetime.now(dt_timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
                    + "Z"
                )
                data_frame[col] = data_frame[col].fillna(now_iso)
            else:
                data_frame[col] = data_frame[col].fillna("")

        upload_df_s3(data_frame, s3_path)

        columns = ", ".join(target_columns)
        match_conditions = [
            f"{schema_table_name}.{field} = {stage_table}.{field}"
            for field in primary_keys
        ]
        match_conditions = " AND ".join(match_conditions)

        query = f"""
        CREATE TEMP TABLE {stage_table} (like {schema_table_name});

        COPY {stage_table} ({columns})
        FROM '{s3_path}'
        IAM_ROLE ''
        REGION 'us-east-2'
        CSV
        QUOTE '"'
        IGNOREHEADER 1
        DATEFORMAT 'auto'
        TIMEFORMAT 'auto';

        MERGE INTO {schema_table_name} USING {stage_table} ON {match_conditions} REMOVE DUPLICATES;

        DROP TABLE {stage_table};
        COMMIT;
        """
        cursor.execute(query)
        logger.info("Redshift MERGE for %s completed OK", table_name)
        return len(data_frame)
    except Exception as e:
        logger.error(
            "Redshift MERGE failed for %s: %s\nQuery:\n%s", table_name, str(e), query
        )


def data_parser(data):
    rows = []

    for item in data:
        properties = item.pop("properties", {})
        properties.pop("embed_url", None)

        item.update(properties)
        rows.append(item)

    return rows


# Uses the HubSpot CRM Search API to fetch custom objects.
# See: https://developers.hubspot.com/docs/api/crm/search
def fetch_all_objects(
    schema_name,
    properties,
    from_time_utc_0_timestamp=None,
    to_time_utc_0_timestamp=None,
    time_property_to_search_by="hs_lastmodifieddate",
):
    """
    Fetches all objects of the specified schema name within the specified time range.
    Note that the time range is inclusive of the start time and exclusive of the end time.
    All filter values must be epoch milliseconds (int), per HubSpot Search API v3 spec.
    """

    logger.info(
        "Fetching %s objects (%s → %s)",
        schema_name,
        from_time_utc_0_timestamp,
        to_time_utc_0_timestamp,
    )

    # Base URL for the custom objects
    url = f"https://api.hubapi.com/crm/v3/objects/{schema_name}/search"

    # Headers with access token
    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }

    # HubSpot (April 2025) enforces a hard ≤100 property cap.
    # If we need more than that, request *all* properties instead.
    if len(properties) > 95:
        payload = {
            "limit": 100,
            "includeAllProperties": True,
        }
    else:
        payload = {
            "limit": 100,
            "properties": properties,
        }

    # Create filters for the query
    filters = []
    if from_time_utc_0_timestamp:
        filters.append(
            {
                "propertyName": time_property_to_search_by,
                "operator": "GTE",  # Greater than or equal to
                "value": from_time_utc_0_timestamp,
            }
        )
    if to_time_utc_0_timestamp:
        filters.append(
            {
                "propertyName": time_property_to_search_by,
                "operator": "LTE",  # Less than or equal to
                "value": to_time_utc_0_timestamp,
            }
        )
    if filters:
        payload["filterGroups"] = [{"filters": filters}]

    # Initialize a list to hold all objects
    all_objects = []

    while True:
        # Make the request to the HubSpot CRM Search API
        response = requests.post(url, headers=headers, json=payload)

        # Handle HubSpot second‑level throttling
        if response.status_code == 429:
            # Respect 'Retry‑After' if present, otherwise wait 2 seconds and retry
            retry_after = int(response.headers.get("Retry-After", "2"))
            logger.warning(
                "429 throttle for %s – sleeping %ds", schema_name, retry_after
            )
            time.sleep(retry_after)
            continue

        # Check if the request was successful
        if response.status_code != 200:
            logger.error("HubSpot error %s: %s", response.status_code, response.text)
            break

        data = response.json()
        new_data = data_parser(data.get("results", []))
        all_objects.extend(new_data)

        # Handle pagination. The 'after' cursor is used to get the next page of results.
        after = data.get("paging", {}).get("next", {}).get("after")
        if after is None:
            break  # Exit loop if no more pages

        # Update the 'after' cursor
        payload["after"] = after

        # Tiny sleep to stay safely below the 10‑req‑per‑second “SECONDLY” policy
        time.sleep(0.12)

    return all_objects


def fetch_recent_objects_list(
    schema_name: str,
    properties: List[str],
    start_ms: int,
    end_ms: int,
    time_property: str = "hs_lastmodifieddate",
) -> list[dict]:
    """
    Fallback loader for very wide custom‑object schemas (›100 properties)
    where the Search API often omits most properties.  It walks the plain
    GET /objects/{schema} list endpoint, then keeps only those rows whose
    *time_property* (ISO‑8601) falls inside the [start_ms, end_ms] window.
    """
    url     = f"https://api.hubapi.com/crm/v3/objects/{schema_name}"
    headers = {"Authorization": f"Bearer {ACCESS_TOKEN}"}
    params  = {"limit": 100, "archived": "false"}
    if properties:
        params["properties"] = properties    # requests handles list‑values

    rows: list[dict] = []
    while True:
        r = requests.get(url, headers=headers, params=params, timeout=60)
        if r.status_code == 429:                     # secondary throttle
            time.sleep(int(r.headers.get("Retry-After", "2")))
            continue
        r.raise_for_status()

        body = r.json()
        for itm in body.get("results", []):
            props = itm.pop("properties", {})
            ts_str = props.get(time_property)
            # keep row if timestamp is absent OR within window
            keep = True
            if ts_str:
                try:
                    ts_ms = int(
                        datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                        .timestamp() * 1000
                    )
                    keep = start_ms <= ts_ms <= end_ms
                except ValueError:
                    pass       # malformed → keep for safety
            if keep:
                props.pop("embed_url", None)
                itm.update(props)
                rows.append(itm)

        after = body.get("paging", {}).get("next", {}).get("after")
        if not after:
            break
        params["after"] = after
        time.sleep(0.12)       # stay <10 req/s
    return rows

inquiries = [
    (
        "visits",
        GEOFENCE_VISITS_SCHEMA_NAME,
        GEOFENCE_VISITS_PROPERTIES,
        "hs_lastmodifieddate",
    ),
    (
        "daily_user_metric",
        DAILY_USER_METRICS_SCHEMA_NAME,
        DAILY_USER_METRICS_PROPERTIES,
        "recorded_at",
    ),
    (
        "form_submissions",
        FORM_SUBMISSIONS_SCHEMA_NAME,
        FORM_SUBMISSIONS_PROPERTIES,
        "hs_lastmodifieddate",
    ),
]


def process_inquiry(cursor, inquiry):
    (table_name, schema_name, properties, time_property_to_search_by) = inquiry
    logger.info("Processing %s...", table_name)

    today = datetime.utcnow().replace(tzinfo=timezone.utc)
    days_10_ago = today - timedelta(days=10)

    # HubSpot wants epoch milliseconds
    start_ts = dt_to_epoch_ms(days_10_ago)
    end_ts = dt_to_epoch_ms(today)

    # Choose loader: wide objects fall back to list‑endpoint workaround
    # data = fetch_all_objects(
    #     schema_name, properties, start_ts, end_ts, time_property_to_search_by
    # )
    if len(properties) > 95:
        data = fetch_recent_objects_list(
            schema_name,
            properties,
            start_ts,
            end_ts,
            time_property_to_search_by,
        )
    else:
        data = fetch_all_objects(
            schema_name,
            properties,
            start_ts,
            end_ts,
            time_property_to_search_by,
        )

    res = None
    if data:
        df = pd.DataFrame(data)
        # ── shift all HubSpot timestamps from UTC → Pacific ──
        convert_df_dates_to_pt(df)
        res = upload_and_merge_into_redshift(cursor, df, table_name, ["id"])
    logger.info("Loaded %d new %s records", res or 0, table_name)
    return res


def main():
    print(">>> main() entered")
    logger.info("main() entered")

    try:
        db_host, db_username, db_password, db_name = get_secret("redshift-credentials")
        print(">>> got Redshift secret OK")

        conn = pg8000.connect(
            host=db_host,
            port=5439,
            database=db_name,
            user=db_username,
            password=db_password,
        )
        print(">>> Redshift connection established")

        cursor = conn.cursor()

        for inquiry in inquiries:
            print(f">>> processing {inquiry[0]}")
            result = process_inquiry(cursor, inquiry)

        cursor.close()
        conn.close()
        print(">>> ETL finished, connection closed")

    except Exception as exc:
        logger.exception("ETL run failed: %s", exc)
        print(f">>> ETL run failed: {exc}")
        raise


if __name__ == "__main__":
    main()




####################################### 
####################################### 
####################################### 
#FULL REFRESH SCRIPT THAT YOU CAN RUN IN PYTHON IN ANY IDE - ANTHONY. it will update the following 3 tables: hubspot_visits, hubspot_daily_user_metric, hubspot_form_submissions
####################################### 
####################################### 
####################################### 
# # #!/usr/bin/env python
# # """
# #!/usr/bin/env python
# """
# One-time full refresh of HubSpot custom-object tables.
#   • lands *all* data in audit_hubspot schema
#   • compares row counts and per-row hashes against production
#   • prints a diff, then exits
# Nothing in raw_data.* is touched.
# """

# import os, io, csv, re, time, json, uuid, logging, itertools, hashlib
# from datetime import datetime, timezone
# from datetime import timezone as dt_timezone
# from datetime import datetime as _dt
# from typing import List, Optional
# from zoneinfo import ZoneInfo  # ← convert UTC → PT

# import boto3, requests, pg8000, pandas as pd
# from botocore.exceptions import ClientError

# # ────────────────────────── cfg ──────────────────────────
# ACCESS_TOKEN = (
#     os.getenv("HUBSPOT_ACCESS_TOKEN") or ""
# )
# HUB_ID = "23759548"

# # Tunables for large-object exports
# HTTP_TIMEOUT = 120  # seconds – allow slower pages to finish
# PAGE_LIMIT = 50  # rows per page – smaller payloads, fewer resets

# # Retry tunables for HubSpot API paging
# MAX_RETRIES = 6  # max attempts for a single API page
# BACKOFF_BASE = 3  # initial back‑off seconds (doubles each retry)

# # ------------------------------------------------------------------
# # Explicit property lists for each custom object (≤100 each so the
# # Search API handles them cleanly).
# # ------------------------------------------------------------------
# DAILY_USER_METRICS_PROPERTIES = [
#     "daily_user_metrics_name",
#     "distance",
#     "driver_handle",
#     "recorded_at",
#     "user_name",
#     "visits",
#     "hs_createdate",
#     "hs_lastmodifieddate",
#     "hs_object_id",
# ]

# GEOFENCE_VISITS_PROPERTIES = [
#     "checkin_time",
#     "checkout_time",
#     "distance_traveled_to_destination",
#     "embed_url",
#     "geofence_address",
#     "geofence_description",
#     "geofence_marker_id",
#     "geofence_name",
#     "time_spent_in_minutes",
#     "user_name",
#     "hs_createdate",
#     "hs_lastmodifieddate",
#     "hs_object_id",
# ]

# FORM_SUBMISSIONS_PROPERTIES = [
#     # (same list as incremental ETL – 100 + items)  # keep comment
#     "additional_comments",
#     "are_classics_currently_displayed_",
#     "are_nanos_currently_displayed_",
#     "are_rosin_belts_currently_displayed_",
#     "associated_company_id",
#     "associated_company_name",
#     "belts_picture_upload",
#     "business_account_cd",
#     "choose_option",
#     "classics_picture_upload",
#     "combo_of_deal_____what_sku_s__were_the_deal_being_ran_on__",
#     "current_display_photo_upload",
#     "custom_sleep_display_picture_upload",
#     "customer_name",
#     "day_of_the_week",
#     "day_of_the_week_or_the_demo",
#     "did_they_have_any_interesting_questions_",
#     "did_you_add_new_signage_to_the_store_",
#     "did_you_conduct_a_pop_up_",
#     "did_you_conduct_a_pop_up_____photo_upload",
#     "did_you_conduct_a_pop_up____kanha_which_campaign__account_form_",
#     "did_you_conduct_a_pop_up____what_brand_was_it_for",
#     "did_you_conduct_a_pop_up____what_brand_was_it_for__account_form_",
#     "did_you_conduct_a_pop_up____which_campaign",
#     "did_you_conduct_training_",
#     "did_you_conduct_training_____kanha_which_campaign__account_form_",
#     "did_you_conduct_training____what_brand_was_it_for__account_form_",
#     "did_you_conduct_training___account_form_",
#     "did_you_drop_food",
#     "did_you_drop_food___other_what_did_you_drop_off_",
#     "did_you_drop_off_non_meds_",
#     "did_you_drop_off_retail_assets",
#     "did_you_drop_retail_assets_",
#     "did_you_drop_retail_assets____choose_option",
#     "did_you_drop_retail_assets____choose_option__account_form_",
#     "did_you_drop_retail_assets___account_form_",
#     "did_you_drop_retail_assets__which_campaign",
#     "did_you_drop_retail_assets__which_campaign__account_form_",
#     "did_you_drop_swag",
#     "did_you_drop_swag___how_much_did_you_drop_off_",
#     "did_you_drop_swag___other_what_did_you_drop_off_",
#     "did_you_drop_swag__what_did_you_drop_off_",
#     "did_you_feel_our_attendance_was_successful_at_the_event_",
#     "did_you_feel_our_attendance_was_successful_at_the_event___why_as_it_not_successful_",
#     "did_you_feel_our_attendance_was_successful_at_the_event___why_was_it_successful_",
#     "did_you_install_a_new_display_",
#     "did_you_install_new_displays_",
#     "did_you_install_new_displays_____choose_option__account_form_",
#     "did_you_install_new_displays_____upload_pictures__account_form_",
#     "did_you_install_new_displays____which_campaign__account_form_",
#     "did_you_install_new_displays__account_form_",
#     "did_you_install_new_signage_",
#     "did_you_install_new_signage_____choose_option",
#     "did_you_install_new_signage_____choose_option__account_form_",
#     "did_you_install_new_signage_____pictures_upload",
#     "did_you_install_new_signage_____upload_pictures__account_form_",
#     "did_you_install_new_signage____which_campaign",
#     "did_you_install_new_signage____which_campaign__account_form_",
#     "did_you_install_new_signage__account_form_",
#     "did_you_work_an_event_",
#     "did_you_work_an_event_____what_was_the_deal__",
#     "did_you_work_an_event____estimated_number_of_units_sold",
#     "did_you_work_an_event____photo_upload",
#     "did_you_work_an_event____which_campaign",
#     "did_you_work_an_event____which_campaign__account_form_",
#     "did_you_work_an_event___day_of_the_week",
#     "do_you_feel_our_attendance_was_successful_in_the_event_",
#     "do_you_have_a___patients_seen_during_your_demo_",
#     "do_you_have_a___patients_seen_during_your_event_",
#     "estimated_number_of_units_sold",
#     "fx_picture_upload",
#     "how_are_minis_currently_displayed_",
#     "how_are_we_currently_displayed_",
#     "how_long_did_you_work_the_event_",
#     "how_long_was_the_whole_event",
#     "how_many_budtenders_did_you_train_",
#     "how_many_budtenders_does_the_store_have_",
#     "how_many_non_meds_did_you_drop_off_",
#     "how_many_of_each_item_did_you_drop_off_",
#     "how_many_of_these_vendors_were_edibles_companies_",
#     "how_many_of_these_vendors_were_edibles_companies____event",
#     "how_many_of_those_vendors_were_edible_companies_",
#     "how_many_patients_were_in_the_shop_during_the_duration_of_your_demo_",
#     "how_many_people_did_you_get_to_follow_us_",
#     "how_many_products_in_total_were_sold_during_the_demo_not_including_promo_units",
#     "how_many_products_in_total_were_sold_during_the_event_not_including_promo_units",
#     "how_many_snacks_did_you_drop_off_",
#     "how_many_vendors_were_at_the_event_",
#     "how_many_vendors_were_at_the_event_with_you",
#     "how_much_did_you_drop_off_",
#     "how_was_the_foot_traffic_during_the_pop_up_",
#     "how_was_the_foot_traffic_in_the_shop_during_the_demo_",
#     "how_was_the_foot_traffic_in_the_shop_during_the_event_",
#     "hs_all_accessible_team_ids",
#     "hs_all_assigned_business_unit_ids",
#     "hs_all_owner_ids",
#     "hs_all_team_ids",
#     "hs_created_by_user_id",
#     "hs_createdate",
#     "hs_lastmodifieddate",
#     "hs_merged_object_ids",
#     "hs_object_id",
#     "hs_object_source",
#     "hs_object_source_detail_1",
#     "hs_object_source_detail_2",
#     "hs_object_source_detail_3",
#     "hs_object_source_id",
#     "hs_object_source_label",
#     "hs_object_source_user_id",
#     "hs_pinned_engagement_id",
#     "hs_read_only",
#     "hs_shared_team_ids",
#     "hs_shared_user_ids",
#     "hs_unique_creation_key",
#     "hs_updated_by_user_id",
#     "hs_user_ids_of_all_notification_followers",
#     "hs_user_ids_of_all_notification_unfollowers",
#     "hs_user_ids_of_all_owners",
#     "hs_was_imported",
#     "hubspot_owner_assigneddate",
#     "hubspot_owner_id",
#     "hubspot_team_id",
#     "if_you_have_an_actual_number_of_products_sold_please_specify",
#     "is_classics_currently_displayed_",
#     "is_fx_currently_displayed_",
#     "is_minis_currently_displayed_",
#     "is_minis_currently_displayed______upload_pictures",
#     "is_nano_currently_displayed_",
#     "is_rosin_belts_currently_displayed_",
#     "is_seasonal_currently_displayed_",
#     "is_the_custom_sleep_display_currently_displayed_",
#     "is_there_a_custom_sleep_display_currently_displayed_",
#     "is_there_a_promo_running__",
#     "is_vape_currently_displayed_",
#     "license_number",
#     "nano_picture_upload",
#     "other____what_new_signage_did_you_add__",
#     "other____what_was_the_deal_",
#     "other____what_was_the_deal__did_you_work_an_event____",
#     "photo_upload",
#     "picture_of_promo_signage",
#     "pictures_upload",
#     "place_for_photo_of_demo_set_up",
#     "place_for_photo_of_display",
#     "place_for_photo_of_event_set_up",
#     "place_to_upload_up_to_10_pictures",
#     "record_name",
#     "seasonal_picture_upload",
#     "source_form_name",
#     "street_address",
#     "submission_idempotent_id",
#     "vape_picture_upload",
#     "was_the_store_completely_out_of_stock_",
#     "was_the_store_out_of_stock_on_any_sku_s__",
#     "was_there_signage_promoting_the_deal_",
#     "were_you_able_to_get_anyone_to_follow_us_on_social_media",
#     "what_brand_was_it_for",
#     "what_brand_was_it_for___if_kanha_which_campaign",
#     "what_category_was_on_promo",
#     "what_did_you_drop_off_",
#     "what_did_you_train_the_budtenders_on_",
#     "what_display_did_you_install_",
#     "what_is_the_specific_promo_running__",
#     "what_new_signage_did_you_add_",
#     "what_platform_did_you_get_them_to_follow_us_on_",
#     "what_retail_asset_did_you_drop_off",
#     "what_sku_of_non_meds_did_you_drop_off_",
#     "what_sku_product_launch_was_this_signage_related_to_",
#     "what_sku_s__was_the_store_out_of_stock_on_",
#     "what_sku_s__were_the_deal_being_ran_on_",
#     "what_snacks_did_you_drop_off_",
#     "what_swag_items_did_you_drop_off_",
#     "what_was_the_deal",
#     "what_was_the_event_for_",
#     "what_was_the_event_name_",
#     "what_were_their_names_and_positions_",
#     "which_campaign",
#     "why_do_you_feel_if_was_successful",
#     "why_do_you_feel_it_was_unsuccessful",
# ]

# SCHEMAS = {
#     "visits": (
#         f"p{HUB_ID}_ht_visit",
#         GEOFENCE_VISITS_PROPERTIES,
#     ),
#     "daily_user_metric": (
#         f"p{HUB_ID}_ht_daily_user_metrics",
#         DAILY_USER_METRICS_PROPERTIES,
#     ),
#     "form_submissions": (
#         f"p{HUB_ID}_form_submissions",
#         FORM_SUBMISSIONS_PROPERTIES,
#     ),
# }

# AUDIT_SCHEMA = "audit_hubspot"  # created if it doesn’t exist
# S3_BUCKET = "acumatica-sunderstorm-analytics"
# AWS_ROLE = ""
# REGION = "us-east-2"

# logging.basicConfig(
#     level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
# )
# log = logging.getLogger("fullrefresh")


# # ─────────────────── timezone helpers (UTC → America/Los_Angeles) ───────────────────
# _ISO_TS_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$")


# def _iso_utc_to_pt(ts: str) -> str:
#     """
#     Convert an ISO‑8601 timestamp that ends with 'Z' (UTC) to Pacific Time,
#     returning the same ISO format but with the proper −07:00/−08:00 offset.
#     If *ts* is not a matching timestamp, it is returned unchanged.
#     """
#     if not ts or not _ISO_TS_RE.match(ts):
#         return ts
#     try:
#         dt = datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(
#             ZoneInfo("America/Los_Angeles")
#         )
#         # keep milliseconds; include offset so intent is explicit
#         return dt.isoformat(timespec="milliseconds")
#     except ValueError:
#         return ts  # leave untouched on any parse error


# def convert_df_dates_to_pt(df: pd.DataFrame) -> None:
#     """
#     In‑place convert every string column that looks like a UTC ISO timestamp
#     (with trailing 'Z') to Pacific Time.  Operates column‑wise for speed.
#     """
#     for col in df.columns:
#         if df[col].dtype == "object":
#             mask = df[col].str.match(_ISO_TS_RE, na=False)
#             if mask.any():
#                 df.loc[mask, col] = df.loc[mask, col].apply(_iso_utc_to_pt)


# # ───────────────────── helpers (same as ETL) ─────────────────────


# def get_secret(secret_name: str):
#     """Retrieve Redshift creds from AWS Secrets Manager (same helper the ETL uses)."""
#     region_name = "us-west-1"
#     session = boto3.session.Session()
#     client = session.client("secretsmanager", region_name=region_name)
#     try:
#         resp = client.get_secret_value(SecretId=secret_name)
#     except ClientError as exc:
#         raise RuntimeError(f"Unable to read secret {secret_name}: {exc}") from exc
#     s = json.loads(resp["SecretString"])
#     return s["Host"], s["Username"], s["Password"], s["Database"]


# def hubspot_all(schema_name: str, props: Optional[List[str]] = None) -> list[dict]:
#     """Fetch *all* rows for a custom object via Search API paging."""
#     url = f"https://api.hubapi.com/crm/v3/objects/{schema_name}/search"
#     headers = {
#         "Authorization": f"Bearer {ACCESS_TOKEN}",
#         "Content-Type": "application/json",
#     }
#     # Decide whether to request an explicit property list or "all".
#     if props and len(props) <= 95:  # Search API max‑100 prop limit
#         payload = {
#             "limit": PAGE_LIMIT,
#             "properties": props,
#             "sorts": [{"propertyName": "hs_object_id", "direction": "ASCENDING"}],
#         }
#     else:
#         # Too many props → ask for *all* (Search API 400 risk; we'll trap below)
#         payload = {
#             "limit": PAGE_LIMIT,
#             "includeAllProperties": True,
#             "sorts": [{"propertyName": "hs_object_id", "direction": "ASCENDING"}],
#         }
#     out = []
#     retries = 0

#     while True:
#         try:
#             r = requests.post(url, headers=headers, json=payload, timeout=HTTP_TIMEOUT)
#         except requests.exceptions.RequestException as exc:
#             log.warning(
#                 "Network error for %s search page – %s; retrying in %s s",
#                 schema_name,
#                 exc,
#                 BACKOFF_BASE * (2**retries),
#             )
#             time.sleep(BACKOFF_BASE * (2**retries))
#             retries += 1
#             if retries >= MAX_RETRIES:
#                 raise
#             continue
#         if r.status_code == 400:
#             log.warning(
#                 "Search API 400 for %s – falling back to list endpoint", schema_name
#             )
#             return hubspot_all_list(schema_name, props)
#         if r.status_code == 429:
#             retries += 1
#             if retries >= MAX_RETRIES:
#                 raise RuntimeError(f"Exceeded retries for {schema_name}")
#             time.sleep(BACKOFF_BASE * (2 ** (retries - 1)))
#             continue
#         if (
#             r.status_code == 502
#         ):  # transient Bad Gateway occasionally returned by HubSpot
#             log.warning(
#                 "502 Bad Gateway for %s – retrying in %s s",
#                 schema_name,
#                 BACKOFF_BASE * (2**retries),
#             )
#             retries += 1
#             if retries >= MAX_RETRIES:
#                 raise RuntimeError(f"Exceeded retries for {schema_name}")
#             time.sleep(BACKOFF_BASE * (2 ** (retries - 1)))
#             continue
#         r.raise_for_status()
#         data = r.json()
#         retries = 0
#         for item in data.get("results", []):
#             props = item.pop("properties", {})
#             item.update(props)
#             out.append(item)
#         after = data.get("paging", {}).get("next", {}).get("after")
#         if not after:
#             return out
#         payload["after"] = after
#         time.sleep(0.12)


# # Fallback loader: walks the plain GET /objects/{schema} list endpoint.
# def hubspot_all_list(schema_name: str, props: Optional[List[str]] = None) -> list[dict]:
#     """
#     Fallback loader that walks the plain GET /objects/{schema} list
#     endpoint.  It returns the same flattened rows our Search‑API helper
#     produces, but avoids the Search‑API request‑validation quirks that
#     occasionally yield 400s for very large custom‑object tables.
#     """
#     url = f"https://api.hubapi.com/crm/v3/objects/{schema_name}"
#     headers = {"Authorization": f"Bearer {ACCESS_TOKEN}"}
#     params = {"limit": PAGE_LIMIT, "archived": "false"}
#     if props:
#         # HubSpot allows multiple `properties` query parameters; requests
#         # handles list‑values by expanding them.
#         params["properties"] = props
#     out: list[dict] = []
#     retries = 0

#     while True:
#         try:
#             r = requests.get(url, headers=headers, params=params, timeout=HTTP_TIMEOUT)
#         except requests.exceptions.RequestException as exc:
#             log.warning(
#                 "Network error for %s list page – %s; retrying in %s s",
#                 schema_name,
#                 exc,
#                 BACKOFF_BASE * (2**retries),
#             )
#             time.sleep(BACKOFF_BASE * (2**retries))
#             retries += 1
#             if retries >= MAX_RETRIES:
#                 raise
#             continue
#         if r.status_code == 429:  # secondary throttle
#             retries += 1
#             if retries >= MAX_RETRIES:
#                 raise RuntimeError(f"Exceeded retries for {schema_name}")
#             time.sleep(BACKOFF_BASE * (2 ** (retries - 1)))
#             continue
#         if (
#             r.status_code == 502
#         ):  # transient Bad Gateway occasionally returned by HubSpot
#             log.warning(
#                 "502 Bad Gateway for %s – retrying in %s s",
#                 schema_name,
#                 BACKOFF_BASE * (2**retries),
#             )
#             retries += 1
#             if retries >= MAX_RETRIES:
#                 raise RuntimeError(f"Exceeded retries for {schema_name}")
#             time.sleep(BACKOFF_BASE * (2 ** (retries - 1)))
#             continue
#         r.raise_for_status()
#         data = r.json()
#         retries = 0
#         for item in data.get("results", []):
#             props = item.pop("properties", {})
#             item.update(props)
#             out.append(item)
#         after = data.get("paging", {}).get("next", {}).get("after")
#         if not after:
#             return out
#         params["after"] = after
#         time.sleep(0.12)  # stay below 10 rps ceiling


# def rs_conn():
#     host, user, pwd, db = get_secret("redshift-credentials")
#     return pg8000.connect(host=host, user=user, password=pwd, database=db, port=5439)


# def upload_df_s3(df: pd.DataFrame, key: str):
#     buf = io.StringIO()
#     df.to_csv(buf, index=False, quoting=csv.QUOTE_ALL, quotechar='"', escapechar="\\")
#     boto3.client("s3").put_object(Bucket=S3_BUCKET, Key=key, Body=buf.getvalue())


# def hash_row(row: dict, ignore: set[str] = {"hs_lastmodifieddate"}) -> str:
#     stable = {k: v for k, v in row.items() if k not in ignore}
#     return hashlib.md5(
#         json.dumps(stable, sort_keys=True, default=str).encode()
#     ).hexdigest()


# # ────────────────────── DataFrame helper ──────────────────────
# def frame_like_table(
#     cur, table_schema: str, table_name: str, source_rows: list[dict]
# ) -> pd.DataFrame:
#     """
#     Build a DataFrame whose columns EXACTLY match the target Redshift
#     table, but **without** dropping any data that arrived from HubSpot.

#     1.  Pull the target column list from Redshift.
#     2.  Let pandas first create a frame from all source keys
#         (keeps every property HubSpot returned).
#     3.  Add any missing columns that exist in Redshift but not in the
#         source payload so COPY won't complain.
#     4.  Re‑order the frame to the exact Redshift column sequence.
#     5.  Fill NOT‑NULL columns with safe defaults (same logic as before).
#     """
#     cur.execute(
#         """
#         SELECT column_name, data_type, is_nullable
#         FROM information_schema.columns
#         WHERE table_schema = %s AND table_name = %s
#         ORDER BY ordinal_position
#         """,
#         (table_schema, table_name),
#     )
#     cols_meta = cur.fetchall()
#     target_cols = [c[0] for c in cols_meta]

#     # Step 2 – keep **all** inbound columns first
#     df = pd.DataFrame(source_rows)

#     # Step 3 – add any Redshift columns that HubSpot didn’t send
#     missing_cols = [c for c in target_cols if c not in df.columns]
#     if missing_cols:
#         df = pd.concat(
#             [df, pd.DataFrame({c: pd.NA for c in missing_cols}, index=df.index)],
#             axis=1,
#         )

#     # Step 4 – exact ordering
#     df = df[target_cols]

#     # Step 5 – previous NOT‑NULL default logic (unchanged)
#     for col, sql_type, nullable in cols_meta:
#         # Skip nullable columns **except** the _airbyte bookkeeping columns
#         if nullable == "YES" and not col.startswith("_airbyte_"):
#             continue
#         if col == "_airbyte_raw_id":
#             df[col] = df[col].fillna(
#                 pd.Series([str(uuid.uuid4()) for _ in range(len(df))], index=df.index)
#             )
#             continue
#         if col == "_airbyte_meta":
#             df[col] = df[col].fillna('{"source":"hubspot"}')
#             continue
#         if col == "_airbyte_extracted_at":
#             now_iso = (
#                 datetime.now(dt_timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
#                 + "Z"
#             )
#             df[col] = df[col].fillna(now_iso)
#             continue

#         # Generic fall‑backs
#         if sql_type in ("character varying", "text"):
#             df[col] = df[col].fillna("")
#         elif sql_type in (
#             "integer",
#             "bigint",
#             "smallint",
#             "decimal",
#             "numeric",
#             "double precision",
#         ):
#             df[col] = df[col].fillna(0)
#         elif sql_type == "boolean":
#             df[col] = df[col].fillna(False)
#         else:  # date / timestamp / others
#             now_iso = (
#                 datetime.now(dt_timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
#                 + "Z"
#             )
#             df[col] = df[col].fillna(now_iso)

#     # Convert all UTC timestamps to Pacific Time before writing to S3/Redshift
#     convert_df_dates_to_pt(df)
#     return df


# # ───────────────────────── main flow ─────────────────────────
# def stage_full_tables(cur) -> dict[str, int]:
#     """Load full HubSpot data into audit_hubspot.*, return row counts."""
#     cur.execute(f"CREATE SCHEMA IF NOT EXISTS {AUDIT_SCHEMA}")
#     counts = {}
#     for tbl, (hs_schema, _props) in SCHEMAS.items():
#         rs_tbl = f"{AUDIT_SCHEMA}.hubspot_{tbl}"
#         log.info("→ %s – pulling from HubSpot", tbl)
#         if tbl in ("visits", "form_submissions"):
#             data = hubspot_all_list(hs_schema, _props)
#         else:
#             data = hubspot_all(hs_schema, _props)
#         cur.execute(f"DROP TABLE IF EXISTS {rs_tbl}")
#         cur.execute(f"CREATE TABLE {rs_tbl} (LIKE raw_data.hubspot_{tbl})")
#         # Build DataFrame perfectly aligned with the audit table
#         df = frame_like_table(cur, AUDIT_SCHEMA, f"hubspot_{tbl}", data)
#         counts[tbl] = len(df)

#         key = f"hubspot/audit/{tbl}.csv"
#         upload_df_s3(df, key)
#         cols = ", ".join(df.columns)
#         # --------------------------- bulk load ---------------------------
#         copy = f"""
#         COPY {rs_tbl} ({cols})
#         FROM 's3://{S3_BUCKET}/{key}'
#         IAM_ROLE '{AWS_ROLE}'
#         REGION '{REGION}'
#         CSV
#         QUOTE '"'
#         IGNOREHEADER 1
#         TRUNCATECOLUMNS
#         ACCEPTINVCHARS
#         BLANKSASNULL
#         EMPTYASNULL
#         DATEFORMAT 'auto'
#         TIMEFORMAT 'auto';
#         COMMIT;
#         """

#         try:
#             cur.execute(copy)
#             # ── sanity‑check: did COPY actually load anything? ──
#             cur.execute(f"SELECT COUNT(*) FROM {rs_tbl}")
#             loaded_rows = cur.fetchone()[0]
#             if loaded_rows == 0:
#                 log.error("COPY finished but inserted 0 rows into %s", rs_tbl)
#                 cur.execute(
#                     """
#                     SELECT starttime, line_number, position,
#                            raw_field_value, err_reason
#                     FROM   stl_load_errors
#                     WHERE  filename LIKE %s
#                     ORDER  BY starttime DESC
#                     LIMIT  25
#                     """,
#                     (f"%{key}",),
#                 )
#                 for err in cur.fetchall():
#                     log.error("LOAD_ERR %s", err)
#                 raise RuntimeError(
#                     f"COPY reported success but loaded 0 rows into {rs_tbl}; "
#                     "see LOAD_ERR lines above."
#                 )
#         except Exception as exc:
#             # surface the first few load‑error rows for quick diagnosis
#             log.error("COPY into %s failed: %s", rs_tbl, exc)
#             cur.execute(
#                 """
#                 SELECT starttime, line_number, position, raw_field_value, err_reason
#                 FROM stl_load_errors
#                 WHERE filename LIKE %s
#                 ORDER BY starttime DESC
#                 LIMIT 15
#             """,
#                 (f"%{key}",),
#             )
#             for row in cur.fetchall():
#                 log.error("LOAD_ERR %s", row)
#             raise
#         log.info("   loaded %s rows → %s", len(df), rs_tbl)
#     return counts


# def compare(cur, audit_counts: dict[str, int]):
#     """Print row-count diff and first mismatching IDs / hashes."""
#     width = max(len(t) for t in SCHEMAS) + 2
#     print("\nHubSpot object counts:")
#     print(f"{'Object':{width}} HS cnt   audit_hubspot cnt")
#     for tbl in SCHEMAS:
#         hs_cnt = audit_counts[tbl]
#         cur.execute(f"SELECT COUNT(*) FROM {AUDIT_SCHEMA}.hubspot_{tbl}")
#         aud_cnt = cur.fetchone()[0]
#         print(f"{tbl:{width}} {hs_cnt:>7,}   {aud_cnt:>7,}")

#     # deeper diff – IDs and hash mismatches
#     for tbl in SCHEMAS:
#         prod = f"raw_data.hubspot_{tbl}"
#         audit = f"{AUDIT_SCHEMA}.hubspot_{tbl}"

#         cur.execute(f"SELECT id FROM {audit}")
#         audit_ids = {r[0] for r in cur.fetchall()}
#         cur.execute(f"SELECT id FROM {prod}")
#         prod_ids = {r[0] for r in cur.fetchall()}

#         missing_prod = audit_ids - prod_ids
#         missing_audit = prod_ids - audit_ids

#         log.warning(
#             "%s – missing_prod=%d  missing_audit=%d",
#             tbl,
#             len(missing_prod),
#             len(missing_audit),
#         )
#         if missing_prod or missing_audit:
#             print(
#                 json.dumps(
#                     {
#                         "missing_in_prod": list(missing_prod)[:10],
#                         "missing_in_audit": list(missing_audit)[:10],
#                     },
#                     indent=2,
#                 )
#             )


# def main():
#     with rs_conn() as conn:
#         cur = conn.cursor()
#         counts = stage_full_tables(cur)
#         compare(cur, counts)


# if __name__ == "__main__":
#     main()

    
    
    