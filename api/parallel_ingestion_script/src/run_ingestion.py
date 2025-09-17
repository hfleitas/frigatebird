import os
import uuid
import pprint
from dotenv import load_dotenv
from datetime import datetime, timezone

import asyncio
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder

from src.core.ingestion_engine import Ingestor
from src.core.chunk_reprocessor import Reprocessor

load_dotenv()

bootstrap = {
    "adx_cluster_uri": "https://kvc-t4efc4mq1p8d6sdfm5.southcentralus.kusto.windows.net",
    "adx_ingest_uri": "https://ingest-kvc-t4efc4mq1p8d6sdfm5.southcentralus.kusto.windows.net",
    "adx_database": "db1",
    "defender_resource_uri":"https://api.security.microsoft.com",
    "defender_hunting_api_url": "https://api.security.microsoft.com/api/advancedhunting/run",
    "config_table": "meta_MigrationConfiguration",
    "config_view": "vw_meta_LatestMigrationConfiguration",
    "audit_table": "meta_MigrationAudit",
    "chunk_audit_table": "meta_ChunkIngestionFailures",
    "chunk_audit_view": "vw_meta_LatestChunkIngestionFailures",
    "max_concurrent_tasks": 5,
    "max_thread_workers": 8,
    "chunk_size": 25000,
    "clientId": os.getenv("AZURE_CLIENT_ID"),
    "clientSecret": os.getenv("AZURE_CLIENT_SECRET"),
    "tenantId": os.getenv("AZURE_TENANT_ID"),
}

def setup_kusto_clients(bootstrap):
    kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(
        bootstrap["adx_cluster_uri"],
        bootstrap["clientId"],
        bootstrap["clientSecret"],
        bootstrap["tenantId"]
    )
    
    kusto_client = KustoClient(kcsb)
    
    return kusto_client

def fetch_migration_config(kusto_client, bootstrap):
    print("[INFO] --> Fetching migration configuration from ADX...")
    
    response_config = kusto_client.execute(
        bootstrap["adx_database"], 
        bootstrap["config_view"]
    )
    
    print(f"[INFO] --> Retrieved configuration for migration")

    return response_config

async def main():

    print("="*100)
    print("STARTING CHUNK REPROCESSING")
    print("="*100)
    
    reprocess_handler = Reprocessor(
        bootstrap=bootstrap,
        max_concurrent_tasks=bootstrap["max_concurrent_tasks"],
        chunk_size=bootstrap["chunk_size"]
    )

    try:
        rp_summary = await reprocess_handler.reprocess_failed_chunks()
        pprint.pprint(rp_summary)
    except Exception as e:
        print(f"[ERROR] --> Exception during reprocessing: {e}")
        rp_summary = {"status": "error", "message": str(e)}
    finally:
        reprocess_handler.thread_pool.shutdown(wait=True)

    print("="*100)
    print("STARTING MAIN INGESTION")
    print("="*100)

    now = datetime.now(timezone.utc)
    kusto_ingest_datetime = now.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    bootstrap["ingestion_start_time"] = kusto_ingest_datetime

    bootstrap["ingestion_id"] = str(uuid.uuid4())

    kusto_client = setup_kusto_clients(bootstrap)

    response_config = fetch_migration_config(kusto_client, bootstrap)

    table = response_config.primary_results[0]
    table_configs = [row.to_dict() for row in table if (row["IsActive"] and not (row["LoadType"] == "Full" and row["HighWatermark"]))]
    if table_configs:
        print(f"[INFO] --> Found {len(table_configs)} active tables for migration")
        try:
            ingestion_handler = Ingestor(
                bootstrap=bootstrap,
                max_concurrent_tasks=bootstrap["max_concurrent_tasks"],
                max_thread_workers=bootstrap["max_thread_workers"],
                chunk_size=bootstrap["chunk_size"]
            )

            p_summary = await ingestion_handler.process_all_tables(table_configs)
            pprint.pprint(p_summary)
            return {"reprocessing_summary": rp_summary, "processing_summary": p_summary}
        except Exception as e:
            return f"[ERROR] --> Exception during processing: {e}"
        finally:
            ingestion_handler.thread_pool.shutdown(wait=True)
    else:
        return "[INFO] --> No active tables found for migration"
