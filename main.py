import os
import yaml
import base64
import json
import asyncio
from datetime import date
from typing import Optional, List, Dict, Any
import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from fastapi import FastAPI, HTTPException,Body
import io
from google.ads.googleads.client import GoogleAdsClient
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
from datetime import date, timedelta, timezone, datetime
from zoneinfo import ZoneInfo
from stickyclient import StickyAPIClient
# --- Configuration ---
# In Cloud Run, these will be set as environment variables.
# For local testing, you can uncomment and set them here.
os.environ["GCP_PROJECT"] = "variant-finance-data-project"
os.environ["BQ_DATASET"] = "Sticky_Data"
os.environ["SERVICE_ACCOUNT_FILE"] = "./variant-finance-datebase-9fa2a65f1ff6.json"
# Get environment variables
SERVICE_ACCOUNT_FILE = os.environ.get("SERVICE_ACCOUNT_FILE")
GCP_PROJECT = os.environ.get("GCP_PROJECT")
BQ_DATASET = os.environ.get("BQ_DATASET")
SVC ="ewogICJ0eXBlIjogInNlcnZpY2VfYWNjb3VudCIsCiAgInByb2plY3RfaWQiOiAidmFyaWFudC1maW5hbmNlLWRhdGEtcHJvamVjdCIsCiAgInByaXZhdGVfa2V5X2lkIjogIjEwNjY5YjNmZGY0ZmU2MWJiNjJiMjE0MWFjY2M1YTA0NzI0NzhmNjgiLAogICJwcml2YXRlX2tleSI6ICItLS0tLUJFR0lOIFBSSVZBVEUgS0VZLS0tLS1cbk1JSUV2UUlCQURBTkJna3Foa2lHOXcwQkFRRUZBQVNDQktjd2dnU2pBZ0VBQW9JQkFRRHJ6TlRvK0dsNnZ2SldcbmdBZFlVczE0dmlVdUMzS0svc3cyd3NCWjVmTnlvK3JjTWY1NTlxM2JlWnR4R2toUllsUDRKYStxWXVqWVZoTDFcbmRMT09IbXVJb0JzN2ZKNWM0a2FXanVVRStYNWV4d3lvTjVycFNjYm82MUYyYmx4QzBrWllick51KytMYkUzcmZcbmk1QitKcGpkT052VkNHVnVLQk40eFFvaVc0WnJndXN1M2FlNmNkS0U4aS9SN0FDUitPb0dqTWg5dEI0WldFeXVcbmw0OGhEYkhKU0poOGRJaFhKRStteklLK1Q5d1Ntclc5dkNCZ2IyazhwNjdkdjFnWDFzMnJWQlZCR2xWUVdyUXRcbklxSzNySWJzRUxMMzF5OFhHazlpWldNM0oyMXI4aENsWDhBS0pHYkVkQ2pzUUJuNHhuai9Tei9RbXUvbXUvYnhcbm5CZEZiVjBGQWdNQkFBRUNnZ0VBRGdVNEhLRVdubnlIaXNLaWpTc2hPZjV1VmdKS3ZYNkFkSG9ZZDAvdnJYK1hcbkdhQWtYaXFmZEVjVENjTFREWG0vL2VkNXZqTVMzcmdoZVBSSEw5cFpzUDQ2R0V1azIrZDlaSHJiSGJSYkFmWXFcblo3OGtxQjNocEp4SFUvZ2tacm03Z29zVWdyTVo3a1pHZmsrOVYrN2lGSGRLeE93eW9iM2l5SUhJeERtMmNLSnpcbk1UTFBpZjZBVzBkV3NnZWhlS09pblg2THMvQXZYMUdPbVNtTVJ3RkNuVHF0K2Q4eE5JVm9JaGxmSER6RENPQWVcbmpNMitaazdXZHRiVmQ2QmxrSWliTjROYm1YU3hUSUFuemk0NTZJSG1VVVZtL09NdkR4OGhVR3Avc25YUUJtUXlcbmxvMFNOUEgyUHg1cXFoWU1rWmZ5R3pHcEdKcHpVcTdYaGxub1ZHQTVFUUtCZ1FEN2VtNGJYeUNUZDZWb3hXV2VcbjBhbEMyZlpUUUdwQU5rdmRBc0VxVWwxTS9iU3dycnBxa0pta3lnazRTdzZwSXN3WmxoOGVnVXRPY2JwbkJFSUVcbmNjSTJadHBNa2RtcUxkT1BzeG56TER1ZjVxa1MyMC9IcEpwVHduY0JxUnZpMmVXWEdablFGT0ZXK3UrZ0pqZ0ZcbnFoK1lDWUVTK3lXcFdVUVp5WitPaXhQZzhRS0JnUUR3Q2p2Mkk4VFpDblpRdlU2bncrSnpMeExYQjFPcStQSkRcbkRLV2lrd2pwTkt2ei9KOXNIc09FV1FyaXJUVHR5RzNEVEUwd3BNVnJUc0k2MC9hdjZCZnN4M09tZ0k1OEhlcHdcbkhxWW5GRWdSVFRJaHp3ZU1GZDkxb3BxcEtudUZUU3NqdjFWN2FhVzVZQ0dIVDhnVTJ6VC9tUmVIZmpvOGdrR2FcbkdkbVNjdDU5VlFLQmdRRHVoU0VLTlIvZ3Z3clVaT1lOelM2TmljNXBDQisrNThEc3owQUh0RGRxWHZpUzNDZFVcbkMvS3VxakkwZ254VlQvdm1DTTFiVWFicnNGTHNnczFiQ2NyN2JuSi9UWmIySXFFWEd2angvSEpSSjZZVmpJNE9cbi9jQ2kwVCt2QTRhL2s0eC8xSGhmTkc3RzRSdUcrcmtJSm1QeEFKSzhQaGxxbHBCUkpUdUJKOGlqQVFLQmdGanhcbnNkNDJ5czRSamwzRWg4eXFUTktaY3NXeXRWSDVCT3ZMVitTeHp1OTYwT3lMZ3hjeEh3bC9aUVV4WVJkcTJTRXdcbnVMbDVsSjE2aFlYKzNMMjVwb1BhTkFSU1JubS9MQXQzaitHVEpsRWk1WnlaZGhaMlZHTG1hYUNkV1QrL3BHaU9cbmtVSTFsMjdsTEFkVGpMUU50Y213RklQa1JmZjkzQWtaNHdEZEI0d3hBb0dBTlJ2QUZXMnpuTkdqTXBMZTFadVZcbmxTUjJzZk0yQTVlbzdLUTNjNTFOV2lMOENzK0FvcU1BeVVzVnBvZndDWGFJMlJxajVFVVFZRlV5SUl1RUJMb0Fcbmx6TlRET2RsdVhPTUNOUDV1Mk9LZmRhMWxBVmd6TFFqN1JNNWlyVjZ5TXBBL2tDKytRRGkyUVZRaFRUeDArWnRcbk45WDhQN3diY29QVWNPNXUrakM1R1RZPVxuLS0tLS1FTkQgUFJJVkFURSBLRVktLS0tLVxuIiwKICAiY2xpZW50X2VtYWlsIjogInN0aWNreS1tYWludGFpbmVyQHZhcmlhbnQtZmluYW5jZS1kYXRhLXByb2plY3QuaWFtLmdzZXJ2aWNlYWNjb3VudC5jb20iLAogICJjbGllbnRfaWQiOiAiMTEwNTQxMzU4MjIzODQ3NjM5MjMyIiwKICAiYXV0aF91cmkiOiAiaHR0cHM6Ly9hY2NvdW50cy5nb29nbGUuY29tL28vb2F1dGgyL2F1dGgiLAogICJ0b2tlbl91cmkiOiAiaHR0cHM6Ly9vYXV0aDIuZ29vZ2xlYXBpcy5jb20vdG9rZW4iLAogICJhdXRoX3Byb3ZpZGVyX3g1MDlfY2VydF91cmwiOiAiaHR0cHM6Ly93d3cuZ29vZ2xlYXBpcy5jb20vb2F1dGgyL3YxL2NlcnRzIiwKICAiY2xpZW50X3g1MDlfY2VydF91cmwiOiAiaHR0cHM6Ly93d3cuZ29vZ2xlYXBpcy5jb20vcm9ib3QvdjEvbWV0YWRhdGEveDUwOS9zdGlja3ktbWFpbnRhaW5lciU0MHZhcmlhbnQtZmluYW5jZS1kYXRhLXByb2plY3QuaWFtLmdzZXJ2aWNlYWNjb3VudC5jb20iLAogICJ1bml2ZXJzZV9kb21haW4iOiAiZ29vZ2xlYXBpcy5jb20iCn0K"

app = FastAPI(title="Sticky.io missing orders", version="1.0.0")

sticky_info = [
    {
                "company": "pdfdotnet",
                "table": "Sticky_data_API_original_PD_Incremental",
                "cred": "RGF0YWFwcHM6QWpBbWpIMkRQZk15bg==",
                "start_date" : "11/20/2025",
                "start_time" : "00:00:00"
    },
    {
                "company": "mindworksllc",
                "table": "Sticky_data_API_original_CT_Incremental",
                "cred": "RGF0YWFwcHM6VlNVdjJ4Q1NFRHZlQVc=",
                "start_date" : "11/20/2025",
                "start_time" : "00:00:00"
    },
    {
                "company": "brainable",
                "table": "Sticky_data_API_original_AT_Incremental",
                "cred": "RGF0YWFwcHM6VGNzTlNTWXhFcUc2ZQ==",
                "start_date" : "11/20/2025",
                "start_time" : "00:00:00"
    },
    {
                "company": "contractsdotnetllc",
                "table": "Sticky_data_API_original_CN_Incremental",
                "cred": "RGF0YWFwcHM6YkhHVmo2ektranVQNGo=",
                "start_date" : "11/20/2025",
                "start_time" : "00:00:00"
    },
    {
                "company": "formsourcellc",
                "table": "Sticky_data_API_original_FS_Incremental",
                "cred": "RGF0YWFwcHM6OW1CU2JVVG1jOXU0WlA=",
                "start_date" : "11/20/2025",
                "start_time" : "00:00:00"
    },
    {
                "company": "variantdiet",
                "table": "Sticky_data_API_original_DT_Incremental",
                "cred": "RGF0YWFwcHM6U2JGdDlHcEp2Z0REcw==",
                "start_date" : "11/20/2025",
                "start_time" : "00:00:00"
    },
    {
                "company": "jobflowllc",
                "table": "test_Sticky_data_API_original_JF_Incremental",
                "cred": "RGF0YWFwcHM6SzZHV1lUQTdFcjIyc2Y=",
                "start_date" : "11/12/2025",
                "start_time" : "00:00:00"
    }
    
]

@app.get("/run_maintainance")
async def run_maintainance():
    """Loads a list of dictionaries into a BigQuery table."""

    service_account_info = json.loads(base64.b64decode(SVC).decode('utf-8'))
    bq_credentials = service_account.Credentials.from_service_account_info(service_account_info)
    bq_client = bigquery.Client(project=GCP_PROJECT, credentials=bq_credentials)
    # Direct to DataFrame

    for info in sticky_info:
        
        company = info["company"]
        query = f"SELECT DISTINCT order_id FROM variant-finance-data-project.Sticky_Data.{info["table"]} WHERE order_id IS NOT NULL;"
        df = bq_client.query(query).to_dataframe()

        print(df.head())
        # Size information
        print(f"\nShape: {df.shape}")                    # (rows, columns)
        print(f"Total rows: {len(df)}")                  # Row count
        print(f"Total columns: {len(df.columns)}")       # Column count
        print(f"Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")  # Memory in MB

        sticky_client = StickyAPIClient(info["cred"],company)

        end_datetime = datetime.now(ZoneInfo("America/New_York")) - timedelta(hours=3)
        end_date = end_datetime.strftime("%m/%d/%Y")
        end_time = end_datetime.strftime("%H:%M:%S")

        order_ids, total_orders_found = await sticky_client.call_order_find_complete(
                start_date=info["start_date"],
                end_date= end_date,
                start_time= info["start_time"],
                end_time= end_time,
                date_type="create",
                criteria="all",
                search_type="all"
            )
        print(f"Total orders found in Sticky.io: {total_orders_found}")
        print(f"Total unique order IDs retrieved: {len(order_ids)}")

        # --- Find missing orders ---
        # Convert df column to a set (assuming column name is 'order_id')
        bq_order_ids = set(df['order_id'].astype(str).tolist())
        # Convert sticky order_ids to a set of strings for consistent comparison
        sticky_order_ids = set(str(oid) for oid in order_ids)
        # Orders in Sticky.io but NOT in BigQuery
        missing_in_bq = sticky_order_ids - bq_order_ids
        # Orders in BigQuery but NOT in Sticky.io (optional, for completeness)
        missing_in_sticky = bq_order_ids - sticky_order_ids

        print(f"\n---{company} Comparison Results ---")
        print(f"{company}: Orders in Sticky.io but MISSING in BigQuery: {len(missing_in_bq)}")
        print(f"{company}: Orders in BigQuery but MISSING in Sticky.io: {len(missing_in_sticky)}")

        if missing_in_bq:
        # --- Check for existing records in missing_orders table ---
            existing_query = f"""
                SELECT DISTINCT order_id 
                FROM `variant-finance-data-project.Sticky_Data.missing_orders` 
                WHERE company = '{company}' AND order_id IS NOT NULL
            """
            existing_df = bq_client.query(existing_query).to_dataframe()
            existing_order_ids = set(existing_df['order_id'].astype(str).tolist())

            # Filter out already existing order_ids
            new_missing_orders = missing_in_bq - existing_order_ids

            print(f"{company}: Already in missing_orders table: {len(missing_in_bq) - len(new_missing_orders)}")
            print(f"{company}: New orders to insert: {len(new_missing_orders)}")

            if new_missing_orders:
                rows_to_insert = [
                    {"order_id": order_id, "company": company}
                    for order_id in new_missing_orders
                ]

                table_id = "variant-finance-data-project.Sticky_Data.missing_orders"
                errors = bq_client.insert_rows_json(table_id, rows_to_insert)

                if errors:
                    print(f"{company}: Errors inserting rows: {errors}")
                else:
                    print(f"{company}: Successfully inserted {len(rows_to_insert)} missing orders into BigQuery")
            else:
                print(f"{company}: All missing orders already exist in the table")
        else:
            print(f"{company}: No missing orders to insert")
        
        await sticky_client.session.close()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=1234)

#import base64
#import json
#
## Read your service account JSON file and convert to base64
#with open("./variant-finance-datebase-9fa2a65f1ff6.json", "r") as f:
#    json_content = f.read()
#
#base64_encoded = base64.b64encode(json_content.encode('utf-8')).decode('utf-8')
#print(base64_encoded)
