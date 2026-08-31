import os
import base64
from sticky_auth import resolve_sticky_auth
import json
import asyncio
from typing import List
from google.cloud import bigquery
import pandas as pd
from datetime import timedelta, datetime
from zoneinfo import ZoneInfo
from stickyclient import StickyAPIClient

# --- Configuration ---
# Env vars are set in Cloud Run Job. Falls back to defaults for local runs.
GCP_PROJECT = os.environ.get("GCP_PROJECT", "variant-finance-data-project")
BQ_DATASET = os.environ.get("BQ_DATASET", "Sticky_Data")


# Per-entity config. Credentials are fetched from Secret Manager at runtime
# using the Entity code (see sticky_auth.py for the code->secret mapping).
sticky_info = [
    {"company": "pdfdotnet",          "Entity": "PD", "table": "Sticky_data_API_original_PD_Incremental",       "start_date": "11/20/2025", "start_time": "00:00:00"},
    {"company": "mindworksllc",       "Entity": "CT", "table": "Sticky_data_API_original_CT_Incremental",       "start_date": "11/20/2025", "start_time": "00:00:00"},
    {"company": "brainable",          "Entity": "AT", "table": "Sticky_data_API_original_AT_Incremental",       "start_date": "11/20/2025", "start_time": "00:00:00"},
    {"company": "contractsdotnetllc", "Entity": "CN", "table": "Sticky_data_API_original_CN_Incremental",       "start_date": "11/20/2025", "start_time": "00:00:00"},
    {"company": "formsourcellc",      "Entity": "FS", "table": "Sticky_data_API_original_FS_Incremental",       "start_date": "11/20/2025", "start_time": "00:00:00"},
    {"company": "jobflowllc",         "Entity": "JF", "table": "test_Sticky_data_API_original_JF_Incremental",  "start_date": "11/12/2025", "start_time": "00:00:00"},
]


async def run_maintainance():
    """Loads a list of dictionaries into a BigQuery table."""

    # Cloud Run Job uses Application Default Credentials automatically.
    bq_client = bigquery.Client(project=GCP_PROJECT)

    for info in sticky_info:

        company = info["company"]
        table = info["table"]
        query = f"SELECT DISTINCT order_id FROM variant-finance-data-project.Sticky_Data.{table} WHERE order_id IS NOT NULL;"
        df = bq_client.query(query).to_dataframe()

        print(df.head())
        print(f"\nShape: {df.shape}")
        print(f"Total rows: {len(df)}")
        print(f"Total columns: {len(df.columns)}")
        print(f"Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")

        # Fetch creds from Secret Manager for this entity, then re-encode as
        # base64 "user:pass" for the existing StickyAPIClient interface.
        user, pw = resolve_sticky_auth(entity=info["Entity"])
        cred_b64 = base64.b64encode(f"{user}:{pw}".encode()).decode()
        sticky_client = StickyAPIClient(cred_b64, company)

        end_datetime = datetime.now(ZoneInfo("America/New_York")) - timedelta(hours=3)
        end_date = end_datetime.strftime("%m/%d/%Y")
        end_time = end_datetime.strftime("%H:%M:%S")

        order_ids, total_orders_found = await sticky_client.call_order_find_complete(
            start_date=info["start_date"],
            end_date=end_date,
            start_time=info["start_time"],
            end_time=end_time,
            date_type="create",
            criteria="all",
            search_type="all"
        )
        print(f"Total orders found in Sticky.io: {total_orders_found}")
        print(f"Total unique order IDs retrieved: {len(order_ids)}")

        bq_order_ids = set(df['order_id'].astype(str).tolist())
        sticky_order_ids = set(str(oid) for oid in order_ids)
        missing_in_bq = sticky_order_ids - bq_order_ids
        missing_in_sticky = bq_order_ids - sticky_order_ids

        print(f"\n---{company} Comparison Results ---")
        print(f"{company}: Orders in Sticky.io but MISSING in BigQuery: {len(missing_in_bq)}")
        print(f"{company}: Orders in BigQuery but MISSING in Sticky.io: {len(missing_in_sticky)}")

        if missing_in_bq:
            existing_query = f"""
                SELECT DISTINCT order_id 
                FROM `variant-finance-data-project.Sticky_Data.missing_orders` 
                WHERE company = '{company}' AND order_id IS NOT NULL
            """
            existing_df = bq_client.query(existing_query).to_dataframe()
            existing_order_ids = set(existing_df['order_id'].astype(str).tolist())

            new_missing_orders = missing_in_bq - existing_order_ids

            print(f"{company}: Already in missing_orders table: {len(missing_in_bq) - len(new_missing_orders)}")
            print(f"{company}: New orders to insert: {len(new_missing_orders)}")

            if new_missing_orders:
                df_to_insert = pd.DataFrame({
                    "order_id": list(new_missing_orders),
                    "company": company
                })

                table_id = "variant-finance-data-project.Sticky_Data.missing_orders"

                job_config = bigquery.LoadJobConfig(
                    write_disposition=bigquery.WriteDisposition.WRITE_APPEND
                )

                job = bq_client.load_table_from_dataframe(df_to_insert, table_id, job_config=job_config)
                job.result()

                print(f"{company}: Successfully inserted {len(new_missing_orders)} missing orders")

            else:
                print(f"{company}: All missing orders already exist in the table")
        else:
            print(f"{company}: No missing orders to insert")

        await sticky_client.session.close()

    print("Maintenance run complete.")


if __name__ == "__main__":
    asyncio.run(run_maintainance())
