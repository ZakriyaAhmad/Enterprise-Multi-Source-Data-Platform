# remove the watermark JSON from the default profile’s S3 bucket
#! aws s3 rm s3://shopify-kanhalife/shopify/etl_state.json

from __future__ import (
    annotations,
)  # ensures type‑hint "|" works under Python 3.9 (Glue)

# pip install boto3
# pip install requests
# pip install pg8000
# pip install pandas
# pip install tenacity
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


def _to_pst(series: pd.Series) -> pd.Series:
    """Return a series of ISO timestamps converted from UTC to PST, formatted as naive strings."""
    return (
        pd.to_datetime(series, utc=True, errors="coerce")
        .dt.tz_convert(_PST)
        .dt.strftime("%Y-%m-%d %H:%M:%S")
    )


# ──────────── EMBEDDED CREDENTIALS (remove before production) ────────────
SHOP = ""  # sub‑domain only
API_VERSION = "2024-04"  # Admin API version
ACCESS_TOKEN = ""

REDSHIFT_HOST = ""
REDSHIFT_PORT = 5439
REDSHIFT_DATABASE = "dev"
REDSHIFT_USER = "admin"
REDSHIFT_PASSWORD = "!"

S3_BUCKET = ""
AWS_REGION = "us-east-2"
IAM_ROLE = ""
RAW_SCHEMA = "raw_data"
STATE_KEY = ""


# ──────────── EASY‑EDIT DISCOUNT CONFIG  ────────────
# Central place to update bundle recipes or discount rates without
# hunting through the code.  Edit constants below, redeploy Glue job.
BUNDLE_RECIPES = {
    "NANO_BUNDLE": [
        "Hybrid Watermelon",
        "Sativa Cran-Pomegranate",
        "Indica Grape",
    ],
    "ESSENTIALS_FX_PACK": [
        "FX Sleep",
        "FX Restore",
        "FX Energy",
    ],
}
DEFAULT_BUNDLE_PCT = 0.20  # 20 % off bundle components
SUBSCRIPTION_PCT = 0.20  # default % off for any selling‑plan row
# ────────────────────────────────────────────────────────────────

LOOKBACK_YEARS = 10  # for first run if no state

# ──────────── Shopify GraphQL helper (replaces REST for orders) ────────────
GQL_ENDPOINT = f"https://{SHOP}.myshopify.com/admin/api/graphql.json"
HEADERS_GQL = {
    "X-Shopify-Access-Token": ACCESS_TOKEN,
    "Content-Type": "application/json",
    "Accept": "application/json",
}


@retry(wait=wait_exponential(multiplier=2, min=2, max=60), stop=stop_after_attempt(5))
def shopify_gql(
    query: str, variables: Optional[Dict[str, Any]] | None = None
) -> Dict[str, Any]:
    """POST a GraphQL query with basic retry and error handling."""
    resp = requests.post(
        GQL_ENDPOINT,
        headers=HEADERS_GQL,
        json={"query": query, "variables": variables or {}},
        timeout=30,
        verify=False,  # TODO: drop verify=False in prod
    )
    if resp.status_code == 429:
        time.sleep(int(resp.headers.get("Retry-After", "2")))
    resp.raise_for_status()
    body = resp.json()
    if "errors" in body:
        raise RuntimeError(json.dumps(body["errors"], indent=2))
    return body["data"]


ORDERS_QUERY_GQL = """
query ($firstOrders:Int!,$firstLines:Int!,$cursor:String,$search:String!){
  orders(first:$firstOrders,after:$cursor,query:$search){
    pageInfo{hasNextPage endCursor}
    edges{
      node{
        id
        name
        channelInformation {
          app {
            title
          }
        }
        createdAt processedAt updatedAt closedAt cancelledAt cancelReason
        financialStatus:displayFinancialStatus
        fulfillmentStatus:displayFulfillmentStatus
        currencyCode tags
        totalDiscountsSet     {shopMoney{amount}}
        subtotalPriceSet      {shopMoney{amount}}
        totalShippingPriceSet {shopMoney{amount currencyCode}}
        totalTaxSet           {shopMoney{amount}}
        totalPriceSet         {shopMoney{amount}}
        totalRefundedSet { shopMoney { amount } }
        refunds{ id note }
        discountApplications(first:100){
          nodes{
            __typename
            ... on ManualDiscountApplication{title}
            ... on DiscountCodeApplication{code}
            ... on ScriptDiscountApplication{title}
            ... on AutomaticDiscountApplication{title}
          }
        }
        customer{
          id email firstName lastName
          emailMarketingConsent{marketingState}
        }
        lineItems(first:$firstLines){
          edges{
            node{
              id product{id} sku title name quantity currentQuantity unfulfilledQuantity
              originalUnitPriceSet{shopMoney{amount}}
              totalDiscountSet    {shopMoney{amount}}
              sellingPlan{name}
              lineItemGroup{__typename}
              variant{id title requiresComponents}
            }
          }
        }
        shippingLines(first: 10){
          edges{
            node{
              originalPriceSet   { shopMoney { amount currencyCode } }
              discountedPriceSet { shopMoney { amount currencyCode } }
              discountAllocations{
                allocatedAmountSet { shopMoney { amount currencyCode } }
                discountApplication{
                  __typename
                  ... on DiscountCodeApplication { code }
                  ... on ManualDiscountApplication { title }
                  targetType      # LINE_ITEM or SHIPPING_LINE
                }
              }
            }
          }
        }
      }
    }
  }
}
"""


def _get(obj: Optional[dict], *path):
    for p in path:
        obj = obj.get(p) if obj else None
    return obj


def _strip_gid(gid: Optional[str]) -> Optional[str]:
    return gid.split("/")[-1] if gid else None


def _shipping_discount(order: dict) -> float:
    """
    Sum every discount allocation that Shopify flags as targeting the SHIPPING_LINE.
    """
    disc = 0.0
    for sl_edge in order.get("shippingLines", {}).get("edges", []):
        sl = sl_edge["node"]
        for alloc in sl.get("discountAllocations", []):
            if alloc["discountApplication"]["targetType"] == "SHIPPING_LINE":
                disc += float(
                    _get(alloc, "allocatedAmountSet", "shopMoney", "amount") or 0
                )
    return round(disc, 2)


def _flatten(order: dict, li: dict, shipping_disc: float) -> dict:
    codes = [
        d.get("code") or d.get("title")
        for d in order["discountApplications"]["nodes"]
        if d.get("code") or d.get("title")
    ]
    variant = li.get("variant") or {}
    group = li.get("lineItemGroup")
    role = (
        "Bundle"
        if variant.get("requiresComponents")
        else ("Component" if group else None)
    )
    cust = order.get("customer") or {}
    refunds_list = order.get("refunds") or []
    refund_count = len(refunds_list)
    total_refund_amount = float(
        _get(order, "totalRefundedSet", "shopMoney", "amount") or 0
    )
    refund_notes = "; ".join([r.get("note") for r in refunds_list if r.get("note")])
    chan = order.get("channelInformation") or {}

    return {
        # Order‑level
        "id": _strip_gid(order["id"]),
        "order_number": int(order["name"].lstrip("#")),
        "created_at": order.get("createdAt"),
        "processed_at": order.get("processedAt"),
        "updated_at": order.get("updatedAt"),
        "closed_at": order.get("closedAt"),
        "cancelled_at": order.get("cancelledAt"),
        "cancel_reason": order.get("cancelReason"),
        "financial_status": order.get("financialStatus"),
        "fulfillment_status": order.get("fulfillmentStatus"),
        "customer_id": _strip_gid(cust.get("id")),
        "customer_email": cust.get("email"),
        "customer_first_name": cust.get("firstName"),
        "customer_last_name": cust.get("lastName"),
        "total_discounts": _get(order, "totalDiscountsSet", "shopMoney", "amount"),
        "subtotal_price": _get(order, "subtotalPriceSet", "shopMoney", "amount"),
        "total_shipping_price_set_shop_money_amount": _get(
            order, "totalShippingPriceSet", "shopMoney", "amount"
        ),
        "total_shipping_price_set_shop_money_currency_code": _get(
            order, "totalShippingPriceSet", "shopMoney", "currencyCode"
        ),
        "total_tax": _get(order, "totalTaxSet", "shopMoney", "amount"),
        "total_price": _get(order, "totalPriceSet", "shopMoney", "amount"),
        "currency": order.get("currencyCode"),
        "total_refund_amount": total_refund_amount,
        "refund_count": refund_count,
        "refund_notes": refund_notes,
        "discount_codes": ",".join(codes),
        "tags": order.get("tags"),
        "channel_app_name": _get(chan, "app", "title"),
        "shipping_discount": shipping_disc,
        # Line‑item‑level
        "line_item_id": _strip_gid(li.get("id")),
        "line_item_product_id": _strip_gid(_get(li, "product", "id")),
        "line_item_sku": li.get("sku"),
        "line_item_title": li.get("title"),
        "line_item_variant_title": variant.get("title"),
        "line_item_name": li.get("name"),
        "line_item_variant_id": _strip_gid(variant.get("id")),
        "line_item_quantity": li.get("quantity"),
        "line_item_current_quantity": li.get("currentQuantity"),
        "line_item_fulfillable_quantity": li.get("unfulfilledQuantity"),
        "line_item_price": _get(li, "originalUnitPriceSet", "shopMoney", "amount"),
        "line_item_total_discount": _get(li, "totalDiscountSet", "shopMoney", "amount"),
        # Extras
        "selling_plan_name": _get(li, "sellingPlan", "name"),
        "bundle_role": role,
    }


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

    # derive total_line_items_price if missing
    if {"line_item_price", "line_item_quantity"}.issubset(df.columns):
        df["__ext"] = pd.to_numeric(
            df["line_item_price"], errors="coerce"
        ) * pd.to_numeric(df["line_item_quantity"], errors="coerce")
        order_totals = df.groupby("id")["__ext"].sum()
        df["total_line_items_price"] = df["id"].map(order_totals)
        df.drop(columns="__ext", inplace=True)
        # ---- Enforce canonical column order so strategic grouping is preserved
        if "total_line_items_price" in df.columns:
            ordered = [c for c in ORDER_COLS if c in df.columns]
            unordered = [c for c in df.columns if c not in ordered]
            df = df[ordered + unordered]
        # ensure shipping_discount numeric & NaN‑safe
        df["shipping_discount"] = pd.to_numeric(
            df["shipping_discount"], errors="coerce"
        ).fillna(0)

    # --- identify embedded‑discount lines (20 % subs & bundles) ----------
    # treat NaN, "0", "nan", "None" (any case) as empty ⇒ not a subscription
    df["is_subscription"] = (
        df["selling_plan_name"]
        .fillna("")
        .astype(str)
        .str.strip()
        .replace(
            {
                "0": "",
                "nan": "",
                "NaN": "",
                "None": "",
                "none": "",
            }
        )
        != ""
    )
    df["is_bundle_component"] = df["bundle_role"] == "Component"
    df["adj_discount_pct"] = np.where(
        df["is_subscription"] | df["is_bundle_component"],
        SUBSCRIPTION_PCT,  # applies same 20 % to subs
        0.0,
    )
    df.loc[df["is_bundle_component"], "adj_discount_pct"] = DEFAULT_BUNDLE_PCT

    # --- bundle recipe detection -----------------------------------------
    # clean component titles (strip trailing " - 1 Pack", etc.)
    df["__clean_title"] = (
        df["line_item_title"].astype(str).str.split(" - ").str[0].str.strip()
    )

    def _detect_bundle(group: pd.DataFrame) -> str:
        titles = set(group.loc[group["is_bundle_component"], "__clean_title"])
        tags = [
            k for k, recipe in BUNDLE_RECIPES.items() if set(recipe).issubset(titles)
        ]
        return "|".join(sorted(tags)) if tags else "0"

    df["bundle_names"] = df["id"].map(
        df.groupby("id", group_keys=False).apply(_detect_bundle)
    )

    # numeric helper for price
    price_numeric = pd.to_numeric(df["line_item_price"], errors="coerce").fillna(0)
    # keep the raw discounted price for potential auditing
    df["raw_unit_price"] = price_numeric
    df["line_item_price_full"] = (price_numeric / (1 - df["adj_discount_pct"])).round(2)

    # --- generic rounding for bundle components --------------------------
    # Round the inferred full price **to the nearest whole dollar** so minor
    # Shopify proration cents (27.91 / 27.92) become a clean 28.00 while FX
    # stays 30.00.  Keeps the logic generic for any future bundle SKUs.
    bundle_mask = df["is_bundle_component"]
    df.loc[bundle_mask, "line_item_price_full"] = df.loc[
        bundle_mask, "line_item_price_full"
    ].round(0)

    # re‑compute the adjustment based on the rounded price
    df["line_item_discount_adj"] = (df["line_item_price_full"] - price_numeric).round(2)

    # discount adjustment for the *entire line* (adj per unit × quantity)
    df["line_item_discount_adj_total"] = (
        df["line_item_discount_adj"] * df["line_item_quantity"]
    ).round(2)

    # bump the line‑level discount column (use total adjustment)
    li_disc_numeric = pd.to_numeric(
        df["line_item_total_discount"], errors="coerce"
    ).fillna(0)
    df["line_item_total_discount"] = (
        li_disc_numeric + df["line_item_discount_adj_total"]
    ).round(2)

    # overwrite line_item_price with the *full* list price
    df["line_item_price"] = df["line_item_price_full"]
    # `line_item_price_full` has served its purpose; drop to keep the table thin
    df.drop(columns="line_item_price_full", inplace=True, errors="ignore")

    # --- pack-size handling ------------------------------------------------
    # Derive pack_size from variant title like "4 Pack" or "2 pack"; default to 1
    df["pack_size"] = (
        df["line_item_variant_title"]
        .fillna("")
        .astype(str)
        .str.extract(r"(\d+)\s*[Pp]ack")[0]
        .astype(float)
    ).fillna(1)

    # Calculate true unit counts and unit-level pricing
    df["unit_quantity"] = df["line_item_quantity"] * df["pack_size"]
    df["unit_price"] = (
        pd.to_numeric(df["line_item_price"], errors="coerce") / df["pack_size"]
    ).round(2)

    # --- allocate order‑level promo discounts down to lines ---------------
    log.info("Allocating promo-code dollars across %d orders", df["id"].nunique())
    # Shopify keeps promo‑code dollars only in total_discounts; allocate the
    # undistributed portion proportionally by list price × quantity.
    # ── two parallel totals ───────────────────────────────────
    # • __ext_net  : price Shopify discounted against
    # • __ext_full : restored list price (for later analytics only)
    df["__ext_net"] = price_numeric * df["line_item_quantity"]  # net basis
    df["__ext_full"] = df["line_item_price"].astype(float) * df["line_item_quantity"]

    # per‑order sums we need
    order_sums = (
        df.groupby("id")[
            [
                "__ext_net",
                "line_item_discount_adj_total",
                "total_discounts",
                "shipping_discount",
            ]
        ]
        .agg(
            {
                "__ext_net": "sum",
                "line_item_discount_adj_total": "sum",
                "total_discounts": "first",  # same on every row
                "shipping_discount": "first",
            }
        )
        .rename(
            columns={
                "__ext_net": "gross_order",
                "line_item_discount_adj_total": "hidden_sum",
                "total_discounts": "order_disc_str",
            }
        )
    )

    # cast order‑level discount once to numeric
    order_sums["order_disc"] = pd.to_numeric(
        order_sums["order_disc_str"], errors="coerce"
    ).fillna(0)

    # --- Determine promo‑code dollars that still need allocating -------------
    # Sum of discounts already present on lines *before* promo allocation
    line_disc_prealloc = (
        df.groupby("id")["line_item_total_discount"].sum().rename("line_disc_prealloc")
    )
    # Attach to per‑order summary
    order_sums = order_sums.join(line_disc_prealloc)
    # Dollars already embedded on lines include hidden 20 % (hidden_sum) plus any
    # Shopify‑recorded line discounts (e.g., BOGO 100 % off).  Only the remainder
    # needs to be allocated.
    order_sums["promo_pool"] = (
        order_sums["order_disc"]
        - order_sums["shipping_discount"]  # ‼️ new – keep freight promos out
        - (order_sums["line_disc_prealloc"] - order_sums["hidden_sum"])
    ).clip(lower=0)

    # map the promo pool and gross order back to each line
    df["__promo_pool"] = df["id"].map(order_sums["promo_pool"])
    df["__gross_order"] = df["id"].map(order_sums["gross_order"])

    # allocate promo dollars proportionally by price weight, but keep the penny
    df["__raw_alloc"] = df["__promo_pool"] * df["__ext_net"] / df["__gross_order"]
    # round each line to cents
    df["promo_alloc"] = df["__raw_alloc"].round(2)
    # figure out the remainder lost to rounding per order
    remainder = (
        df.groupby("id")["__raw_alloc"].sum() - df.groupby("id")["promo_alloc"].sum()
    ).round(2)
    df["__remainder"] = df["id"].map(remainder)
    # add the leftover penny (<= 0.01) to the last line of each order so the sums foot
    df.loc[df.groupby("id").tail(1).index, "promo_alloc"] += df.loc[
        df.groupby("id").tail(1).index, "__remainder"
    ]
    # clean up helper columns
    df.drop(columns=["__raw_alloc", "__remainder"], inplace=True, errors="ignore")

    # add the promo allocation to line‑level discount
    df["line_item_total_discount"] = (
        pd.to_numeric(df["line_item_total_discount"], errors="coerce").fillna(0)
        + df["promo_alloc"]
    ).round(2)

    # --- update order‑level aggregates to reflect the adjustments --------
    order_aggr = (
        df.groupby("id")[["__ext_full", "line_item_discount_adj_total", "promo_alloc"]]
        .sum()
        .round(2)
        .rename(
            columns={
                "__ext_full": "full_price_sum",
                "line_item_discount_adj_total": "hidden_sum",
                "promo_alloc": "promo_sum",
            }
        )
    )
    df["total_line_items_price"] = df["id"].map(order_aggr["full_price_sum"])

    # overwrite order-level discount with full (hidden + order-level promo) value
    df["total_discounts"] = df["id"].map(
        (order_aggr["hidden_sum"] + order_sums["order_disc"]).round(2)
    )

    # cleanup helper columns
    df.drop(
        columns=[
            "__ext_full",
            "line_item_discount_adj_total",
            "promo_alloc",
            "__promo_pool",
            "__gross_order",
        ],
        inplace=True,
        errors="ignore",
    )

    # --- append marker codes so BI can see the source of discount --------
    def _append_marker(row):
        codes_raw = (
            ""
            if str(row["discount_codes"]).strip(" 0[]") in ("", "nan")
            else str(row["discount_codes"])
        )
        codes = [c for c in codes_raw.split(",") if c]
        if row["is_subscription"]:
            codes.append("SUB20")
        if row["is_bundle_component"]:
            codes.append("BUNDLE20")
        # de‑duplicate while preserving order
        seen = set()
        return ",".join([c for c in codes if not (c in seen or seen.add(c))]) or "0"

    df["discount_codes"] = df.apply(_append_marker, axis=1)

    # --- append bundle names to the 'tags' column ------------------------
    def _merge_tags(row):
        base_raw = (
            "" if str(row["tags"]).strip(" 0[]") in ("", "nan") else str(row["tags"])
        )
        tag_list = [t.strip() for t in base_raw.split(",") if t.strip()]
        if row.get("bundle_names") not in ("0", None, np.nan):
            for t in row["bundle_names"].split("|"):
                if t and t not in tag_list:
                    tag_list.append(t)
        return ",".join(tag_list) if tag_list else "0"

    df["tags"] = df.apply(_merge_tags, axis=1)

    # drop helper cols we don't want in the final table
    df.drop(columns=["__clean_title", "raw_unit_price"], inplace=True, errors="ignore")
    # --- convert timestamp columns from UTC to PST ------------------------
    for col in [
        "created_at",
        "processed_at",
        "updated_at",
        "closed_at",
        "cancelled_at",
    ]:
        if col in df.columns:
            df[col] = _to_pst(df[col])
    # ---------------------------------------------------------------------
    return df


# ──────────── End Shopify GraphQL helper ────────────


###############################################################################
#  FINANCIAL / QUANTITY FIELD GLOSSARY
#
#  This ETL writes every money value as a *string* (varchar) so Redshift loads
#  are tolerant of bad rows; downstream models should cast to DECIMAL(18,2).
#  All prices/amounts are **in the shop’s transaction currency** (USD today).
#
#  ─── Per‑Line Columns ───────────────────────────────────────────────────────
#  • line_item_price
#       The undiscounted LIST price *per retail pack* **after we undo** any
#       implicit 20 % bundle/subscription markdown Shopify bakes in.
#       Example: FX Sleep sells for $30 but the API might return $24; we
#       restore $30 here.  Not multiplied by quantity.
#
#  • line_item_quantity
#       The number of packs purchased for this SKU in the order row.
#
#  • pack_size
#       Units per pack parsed from variant title (“4 Pack” → 4).  Defaults to
#       1 when pattern not present or single‑serving SKU.
#
#  • unit_quantity
#       The *true* number of retail units leaving the warehouse:
#          unit_quantity = line_item_quantity × pack_size
#
#  • unit_price
#       List price per *unit*:
#          unit_price = line_item_price ÷ pack_size
#
#  • line_item_total_discount
#       The **total** discount dollars applied to this line.  It is:
#         native Shopify discounts  +  recovered hidden 20 % × quantity
#
#  ─── Per‑Order Columns (duplicated on every row) ────────────────────────────
#  • total_line_items_price
#       Gross order subtotal at LIST price:
#         Σ(line_item_price × line_item_quantity) across the order.
#
#  • total_discounts
#       All discounts that reduce revenue:
#         native Shopify discounts + recovered hidden 20 %.
#       Therefore: subtotal_price = total_line_items_price – total_discounts
#
#  • subtotal_price
#       Net merchandise subtotal shown to the buyer (list – discounts).
#
#  • total_shipping_price_set_shop_money_amount
#       Shipping charged to the buyer.
#
#  • total_tax
#       Sales tax collected.
#
#  • total_price
#       Cash collected from customer:
#         subtotal_price + shipping + tax – refunds (if any).
#
#  ─── Program Flags & Tags (metadata, not money) ────────────────────────────
#  • is_subscription            – True if selling_plan_name present.
#  • is_bundle_component        – True if bundle_role == "Component".
#  • adj_discount_pct           – % discount we had to back‑calculate (20 % today).
#  • bundle_names               – Pipe‑delimited string of bundle tags detected
#                                 (e.g. "NANO_BUNDLE|ESSENTIALS_FX_PACK").
#
#  These flags drive the price‑restoration logic and feed BI dimensions.
###############################################################################
ORDER_COLS = [
    # ------- Keys & Timestamps -------
    "id",
    "order_number",  # numeric order number
    "name",  # Shopify “#XXXX” string
    "created_at",
    "processed_at",
    "updated_at",
    "closed_at",
    "cancelled_at",
    "cancel_reason",
    # ------- Status -------
    "financial_status",
    "fulfillment_status",
    # ------- Customer -------
    "customer_id",
    "customer_email",
    "buyer_accepts_marketing",
    # ------- Pricing (shop currency) -------
    "total_line_items_price",  # gross subtotal (pre‑discount)
    "total_discounts",  # discount amount
    "subtotal_price",  # net subtotal (post‑discount)
    "total_shipping_price_set_shop_money_amount",  # shipping amount
    "total_shipping_price_set_shop_money_currency_code",
    "shipping_discount",
    "total_tax",
    "total_price",  # amount paid
    "currency",
    # ------- Attribution / Context -------
    "discount_codes",
    "tags",
    "channel_app_name",
    "landing_site",
    "referring_site",
    # ------- Line‑item detail -------
    "line_item_id",
    "line_item_product_id",
    "line_item_sku",
    "line_item_title",
    "line_item_variant_title",
    "line_item_name",  # includes “Pack” size
    "line_item_variant_id",
    "line_item_quantity",
    "line_item_current_quantity",
    "line_item_fulfillable_quantity",
    "line_item_price",
    "line_item_total_discount",
]

STREAMS = {
    "orders": ("orders.json", ["id", "line_item_id"], "updated_at_min"),
    "customers": ("customers.json", ["id"], "updated_at_min"),
    #! always pull a full snapshot for products so deletions are detected
    "products": ("products.json", ["id"], None),
}


# ---------- logging config (friendly for AWS Glue / CloudWatch) ----------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],  # STDOUT, not STDERR
    force=True,
)
# Turn down noisy third‑party libraries unless LOG_LEVEL is DEBUG
if LOG_LEVEL != "DEBUG":
    for noisy in ("botocore", "boto3", "urllib3"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

# Module-level logger for info/debug
log = logging.getLogger(__name__)


# Shortcut for console‑style section banners
def banner(msg: str):
    logging.info("\n" + "-" * 70 + "\n" + msg + "\n" + "-" * 70)


# -------------------------------  HELPERS  --------------------------------
s3 = boto3.client("s3", region_name=AWS_REGION)


def read_state():
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=STATE_KEY)
        return json.load(obj["Body"])["last_run"]
    except s3.exceptions.NoSuchKey:
        return None


def write_state(ts_iso):
    s3.put_object(
        Bucket=S3_BUCKET, Key=STATE_KEY, Body=json.dumps({"last_run": ts_iso}).encode()
    )


@retry(wait=wait_exponential(2, 60), stop=stop_after_attempt(5))
def shopify_get(url, params):
    r = requests.get(
        url,
        headers={"X-Shopify-Access-Token": ACCESS_TOKEN},
        params=params,
        timeout=30,
        verify=False,
    )  # TODO: remove verify=False once SSL fixed
    if r.status_code == 429:
        time.sleep(int(r.headers.get("Retry-After", "2")))
    r.raise_for_status()
    return r


def paginate(endpoint, since_iso):
    url = f"https://{SHOP}.myshopify.com/admin/api/{API_VERSION}/{endpoint}"
    params = {"limit": 250}
    # orders endpoint returns only *open* orders unless we ask for more
    if endpoint.startswith("orders"):
        params["status"] = "any"  # include closed, cancelled, archived
    if since_iso:
        params["updated_at_min"] = since_iso
    while url:
        resp = shopify_get(url, params)
        stream = endpoint.split(".")[0]  # derive stream name from endpoint
        logging.info(
            "Page fetched %s  –  items: %s",
            endpoint,
            len(resp.json().get(stream, resp.json())),
        )
        yield resp.json()
        link_hdr = resp.headers.get("Link", "")
        next_url = None
        # Link header can contain both 'prev' and 'next' – pick the next‑page URL explicitly
        for part in link_hdr.split(","):
            if 'rel="next"' in part:
                next_url = part.split(";")[0].strip(" <>")
                break
        url = next_url
        params = {}  # cursor URL already carries params


def to_dataframe(stream, payload):
    logging.info(
        "Normalising payload for stream '%s' – raw count: %d",
        stream,
        len(payload.get(stream, [])),
    )
    df = pd.json_normalize(payload.get(stream, []), sep="_")
    logging.info(
        "DataFrame for %s → rows %d, columns %d", stream, len(df), len(df.columns)
    )
    if stream == "orders" and not df.empty:
        df = df.explode("line_items")
        df = pd.concat(
            [
                df.drop(columns="line_items").reset_index(drop=True),
                pd.json_normalize(df["line_items"]).add_prefix("line_item_"),
            ],
            axis=1,
        )
        # --- Ensure line_item_id is ALWAYS a non‑null string -----------------
        if "line_item_id" not in df.columns:
            df["line_item_id"] = "0"
        else:
            df["line_item_id"] = (
                df["line_item_id"]
                .fillna(0)  # NaN -> 0
                .astype(str)
                .str.strip()
                .replace({"": "0", "nan": "0", "None": "0"})
                .str.replace(r"\.0$", "", regex=True)  # 123.0 -> 123
            )
        # --- Keep whitelist *plus* any nested selling‑plan fields -------------
        for col in ORDER_COLS:
            if col not in df.columns:
                df[col] = None

        df = df[ORDER_COLS]
    return df


def ensure_target_table(cur, target: str, cols: list[str], pk_cols: list[str]):
    """
    Creates the landing table if missing, or adds any new columns.
    Everything starts as varchar for maximum flexibility.
    """
    schema, tbl = target.split(".")
    # does the table exist?
    cur.execute(
        """
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = %s AND table_name = %s
    """,
        (schema, tbl),
    )
    if cur.fetchone() is None:
        col_defs = ", ".join([f'"{c}" varchar' for c in cols])
        pk = ", ".join([f'"{c}"' for c in pk_cols])
        cur.execute(f"CREATE TABLE {target} ({col_defs}, PRIMARY KEY ({pk}))")
        logging.info("Created landing table %s with %d columns", target, len(cols))
        return

    # widen existing table if new Shopify fields appear
    cur.execute(
        """
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
    """,
        (schema, tbl),
    )
    existing = {r[0] for r in cur.fetchall()}
    missing = [c for c in cols if c not in existing]
    for m in missing:
        cur.execute(f'ALTER TABLE {target} ADD COLUMN "{m}" varchar')
    if missing:
        logging.info("Added columns to %s – %s", target, ", ".join(missing))


def upload_merge(cur, df, stream, pk_cols, target):
    """
    Stage the DataFrame to S3 and MERGE it into Redshift.
    Creates a temp table that matches *exactly* the incoming DataFrame and
    uses tolerant COPY options so bad UTF‑8 or long strings don't abort the load.
    """
    # ── 1️⃣ Upload CSV to S3 ───────────────────────────────────────────────
    tmp = tempfile.NamedTemporaryFile("w", delete=False)
    # Write CSV with explicit NaN representation so BLANKSASNULL doesn’t turn empty fields into NULL
    df.to_csv(tmp.name, index=False, quoting=csv.QUOTE_MINIMAL, na_rep="0")
    key = f"shopify/{stream}/{uuid.uuid4()}.csv"
    s3.upload_file(tmp.name, S3_BUCKET, key)
    s3_uri = f"s3://{S3_BUCKET}/{key}"
    logging.info("S3 stage file written: %s  (rows %d)", s3_uri, len(df))
    stage = f"{stream}_stage"

    # ── 2️⃣ Dynamic stage table DDL (quoted identifiers) ──────────────────
    # Build superset of target table columns
    schema, tbl = target.split(".")
    cur.execute(
        """
        SELECT column_name
          FROM information_schema.columns
         WHERE table_schema = %s
           AND table_name = %s
         ORDER BY ordinal_position
        """,
        (schema, tbl),
    )
    all_cols = [row[0] for row in cur.fetchall()]
    # Stage table must match target column count/order for MERGE … REMOVE DUPLICATES
    stage_cols = all_cols  # keep exact target columns only
    # Prepare copy list for DataFrame columns only
    df_cols = df.columns.tolist()
    quoted_df = [f'"{c}"' for c in df_cols]
    col_list = ", ".join(quoted_df)

    # Ensure target has all incoming columns; add any new ones on the fly
    missing_in_stage = [c for c in df_cols if c not in all_cols]
    for col in missing_in_stage:
        cur.execute(f'ALTER TABLE {target} ADD COLUMN "{col}" varchar')
        logging.info("Added missing column %s to %s", col, target)
    if missing_in_stage:
        # refresh column list now that target is widened
        all_cols.extend(missing_in_stage)
        stage_cols = all_cols

    # Build the final DDL string *after* target may have been widened
    col_defs = ", ".join([f'"{c}" varchar' for c in stage_cols])

    pk_match = " AND ".join([f'{target}."{c}" = {stage}."{c}"' for c in pk_cols])
    # Build a NULL‑cleanup step only when the DataFrame actually has the column
    sentinel_update_sql = ""
    if "line_item_id" in df.columns:
        sentinel_update_sql = f"""
        -- Backfill any NULLs that COPY interpreted
        UPDATE {stage}
           SET "line_item_id" = '0'
         WHERE "line_item_id" IS NULL;
        """

    copy_opts = """
      CSV IGNOREHEADER 1
      ACCEPTINVCHARS
      TRUNCATECOLUMNS
      BLANKSASNULL
      EMPTYASNULL
      MAXERROR 1000
    """

    # Build SQL differently for products (full replace) vs other streams (upsert)
    if stream == "products":
        sql = f"""
        BEGIN;
        DROP TABLE IF EXISTS {stage};
        CREATE TEMP TABLE {stage} ({col_defs});
        COPY {stage} ({col_list})
          FROM '{s3_uri}'
          IAM_ROLE '{IAM_ROLE}'
          REGION '{AWS_REGION}'
          {copy_opts};
        {sentinel_update_sql}
        TRUNCATE TABLE {target};
        INSERT INTO {target} SELECT * FROM {stage};
        COMMIT;
        """
    else:
        sql = f"""
        BEGIN;
        DROP TABLE IF EXISTS {stage};
        CREATE TEMP TABLE {stage} ({col_defs});
        COPY {stage} ({col_list})
          FROM '{s3_uri}'
          IAM_ROLE '{IAM_ROLE}'
          REGION '{AWS_REGION}'
          {copy_opts};
        {sentinel_update_sql}
        MERGE INTO {target}
          USING {stage}
          ON {pk_match}
          REMOVE DUPLICATES;
        COMMIT;
        """
    logging.debug(
        "Executing MERGE for stream %s with %d columns (target %s)",
        stream,
        len(df.columns),
        target,
    )

    try:
        cur.execute(sql)
    except Exception:
        # dump first few load errors so they're visible in CloudWatch
        cur.execute(
            "SELECT line_number, colname, err_reason "
            "FROM stl_load_errors ORDER BY starttime DESC LIMIT 5"
        )
        for ln, col, err in cur.fetchall():
            logging.error("LOAD ERR line %s col %s : %s", ln, col, err)
        logging.error(
            "FAILED SQL snippet:\n%s",
            textwrap.shorten(sql, width=600, placeholder=" [...] "),
        )
        raise


# --------------------------  AUDIT TABLE UTILS  ---------------------------
AUDIT_TABLE = "raw_data.etl_run_audit"


def audit_start(cur, run_id, stream):
    cur.execute(
        f"""
        INSERT INTO {AUDIT_TABLE}
            (run_id, stream, started_at, status)
        VALUES (%s, %s, GETDATE(), 'running')
    """,
        (run_id, stream),
    )


def audit_finish(cur, run_id, stream, rows_ext, rows_ld, status="success"):
    cur.execute(
        f"""
        UPDATE {AUDIT_TABLE}
           SET finished_at   = GETDATE(),
               rows_extracted = %s,
               rows_loaded    = %s,
               status         = %s
         WHERE run_id = %s AND stream = %s
    """,
        (rows_ext, rows_ld, status, run_id, stream),
    )


# -------------------------------  MAIN  -----------------------------------
def main():
    run_id = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S")
    last_run = read_state()
    if last_run:
        # Pull 5 minutes further back than the saved watermark so that
        # any orders modified seconds after the previous run are re‑fetched.
        since = (
            dt.datetime.fromisoformat(last_run) - dt.timedelta(minutes=5)
        ).isoformat()
    else:
        since = (
            dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=365 * LOOKBACK_YEARS)
        ).isoformat()

    banner(f"ETL RUN {run_id}  –  starting at watermark {since}")

    logging.info("Watermark (pulling changes since): %s", since)

    conn = pg8000.connect(
        host=REDSHIFT_HOST,
        port=REDSHIFT_PORT,
        database=REDSHIFT_DATABASE,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD,
    )
    cur = conn.cursor()

    max_seen = since
    grand_total = 0
    processed_tables: set[str] = set()  # make sure we only check/alter each table once
    try:
        for stream, (endpoint, pk_cols, _) in STREAMS.items():
            rows_loaded_total = 0
            # store all streams under the same naming convention
            target_table = f"{RAW_SCHEMA}.shopify_{stream}"
            audit_start(cur, run_id, stream)
            # For products we fetch a full snapshot (Shopify doesn't send deletions in incremental feed)
            since_for_stream = since if stream != "products" else None
            # ---- GraphQL path for orders ----------------------------------
            if stream == "orders":
                df = fetch_orders_dataset(
                    first_orders=250, first_lines=50, since_iso=since_for_stream
                )
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
                # finish audit for this stream and move on
                audit_finish(cur, run_id, stream, rows_loaded_total, rows_loaded_total)
                logging.info("%s → %s rows upserted", stream, rows_loaded_total)
                grand_total += rows_loaded_total
                continue
            else:
                for page in paginate(
                    endpoint, since_for_stream
                ):  # REST path (customers/products only)
                    df = to_dataframe(stream, page)
                    if df.empty:
                        continue
                    if target_table not in processed_tables:
                        ensure_target_table(
                            cur, target_table, list(df.columns), pk_cols
                        )
                        processed_tables.add(target_table)
                    upload_merge(cur, df, stream, pk_cols, target_table)
                    rows_loaded_total += len(df)
                    page_max = df["updated_at"].max() if "updated_at" in df else None
                    if page_max:
                        max_seen = max(max_seen, str(page_max))
                audit_finish(cur, run_id, stream, rows_loaded_total, rows_loaded_total)
                logging.info("%s → %s rows upserted", stream, rows_loaded_total)
                grand_total += rows_loaded_total
        banner(f"All streams finished ✔  rows loaded (total): {grand_total}")
        conn.commit()
        write_state(max_seen)
        logging.info("New watermark saved: %s", max_seen)
    except Exception as e:
        logging.exception("ETL run failed")
        audit_finish(cur, run_id, stream, 0, 0, status="error")
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


# ------------------------  QUICK CONNECTIVITY TESTS -----------------------
def test_shopify():
    try:
        r = requests.get(
            f"https://{SHOP}.myshopify.com/admin/api/{API_VERSION}/shop.json",
            headers={"X-Shopify-Access-Token": ACCESS_TOKEN},
            timeout=20,
            verify=False,
        )
        r.raise_for_status()
        print("✅ Shopify OK —", r.json()["shop"]["name"])
    except Exception as e:
        print("❌ Shopify failed:", e)


def test_redshift():
    try:
        pg8000.connect(
            host=REDSHIFT_HOST,
            port=REDSHIFT_PORT,
            database=REDSHIFT_DATABASE,
            user=REDSHIFT_USER,
            password=REDSHIFT_PASSWORD,
        ).close()
        print("✅ Redshift OK")
    except Exception as e:
        print("❌ Redshift failed:", e)


if __name__ == "__main__":
    test_shopify()
    test_redshift()
    main()
