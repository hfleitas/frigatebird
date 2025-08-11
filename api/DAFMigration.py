import json
import urllib.request
import urllib.parse
import requests
from datetime import datetime, timezone
from io import StringIO
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.data_format import DataFormat
from azure.kusto.ingest import (
    IngestionProperties,
    IngestionMappingKind,
    QueuedIngestClient
)

#--------------------Prerequisite-------------------------
# Step-1: !pip install azure-kusto-data
# Step-2: !pip install azure-kusto-ingest
#---------------------------------------------

# ---------------------------------------------------------------
# [NEW] Pull config from ADX DefenderAdxConfig dynamically
# ---------------------------------------------------------------
bootstrap = {
    "adx_cluster_uri": "https://kvc-w854a6sxcqm4fsa9qg.northeurope.kusto.windows.net",
    "defender_resource_uri":"https://api.securitycenter.microsoft.com",
    "adx_database": "frigatebird_db",
    "config_table": "DefenderAdxConfig",
    "clientId": "",
    "clientSecret": "",
    "tenantId": "",
    "adx_ingest_uri": "https://ingest-kvc-w854a6sxcqm4fsa9qg.northeurope.kusto.windows.net"
}

# Connect to ADX cluster to execute query
kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(
    bootstrap["adx_cluster_uri"],
    bootstrap["clientId"],
    bootstrap["clientSecret"],
    bootstrap["tenantId"]
)

# Connect to ADX cluster for Ingestion of TVM Tables
kcsb_ingest = KustoConnectionStringBuilder.with_aad_application_key_authentication(
    bootstrap["adx_ingest_uri"],
    bootstrap["clientId"],
    bootstrap["clientSecret"],
    bootstrap["tenantId"]
)

ingest_client = QueuedIngestClient(kcsb_ingest)
kusto_client = KustoClient(kcsb)

adx_ingest_uri = f"{bootstrap['adx_cluster_uri']}/v1/rest/mgmt"
aad_token_url = f"https://login.windows.net/{bootstrap['tenantId']}/oauth2/token"

# Fetch latest rows from ConfigTable
response_config = kusto_client.execute(bootstrap["adx_database"], "VwLatestConfig")

for Table in response_config.primary_results:   # return single table like config
    for row in Table.rows:   # return all rows from config
        defender_api_url = ""
        WatermarkColumn=""
        last_modified_time = row["LastModifiedTime"]
        source_tbl = row["SourceTable"]
        WatermarkColumn = row["WatermarkColumn"]
        destination_tbl = row["DestinationTable"]
        defender_api_url = row['APIEndpoint']
        APIEndPoint = row['APIEndpoint']
        LastModifiedTimeInString=row['LastModifiedTimeInString']
 
        watermark_last_modified_time = last_modified_time.replace(tzinfo=timezone.utc)
        formatted_ts = watermark_last_modified_time.strftime("%Y-%m-%dT%H:%M:%S")+"."+LastModifiedTimeInString.split(".")[1]
        if WatermarkColumn!="None":
            defender_api_url += f"?$filter={WatermarkColumn} gt {formatted_ts}"
        else:
          WatermarkColumn="ingestion_time()"   # in case of no date column exist
          defender_api_url += f"?$filter={WatermarkColumn} gt {formatted_ts}"
     
        print("API Resource URL:"+ defender_api_url)

        # Step 1: Get AAD Token for Defender
        body = {
            'resource': bootstrap["defender_resource_uri"],
            'client_id': bootstrap["clientId"],
            'client_secret': bootstrap["clientSecret"],
            'grant_type': 'client_credentials'
        }
        defender_token_req = urllib.request.Request(aad_token_url, urllib.parse.urlencode(body).encode("utf-8"))
        response = urllib.request.urlopen(defender_token_req)
        aad_token = json.loads(response.read())["access_token"]

        print("Defender Token acquired")

        # Step 2: Call Microsoft Defender API
        headers = {
            "Authorization": f"Bearer {aad_token}"
        }
        response = requests.get(defender_api_url, headers=headers)
        if response.status_code == 200:
            apijson = response.json()
            print(f"{source_tbl} retrieved successfully")

            # Step 3: Get AAD Token for ADX
            adx_token_url = f"https://login.microsoftonline.com/{bootstrap['tenantId']}/oauth2/token"
            adx_token_body = {
                "grant_type": "client_credentials",
                "client_id": bootstrap["clientId"],
                "client_secret": bootstrap["clientSecret"],
                "resource": bootstrap["adx_cluster_uri"]
            }
            adx_req = urllib.request.Request(
                adx_token_url,
                urllib.parse.urlencode(adx_token_body).encode("utf-8")
            )
            adx_response = urllib.request.urlopen(adx_req)
            adx_token = json.loads(adx_response.read())["access_token"]
            print("ADX Token acquired")

            # Step 4: Prepare ingest command
            adx_headers = {
                "Authorization": f"Bearer {adx_token}",
                "Content-Type": "application/json"
            }

            records = apijson["value"]
            if len(apijson.get("value", [])) != 0:
              data_as_str = "\n".join(json.dumps(r) for r in records)
              data_stream = StringIO(data_as_str)
              #print("Raw Json Pay Load" + data_as_str)

              ingestion_props = IngestionProperties(
                  database=bootstrap["adx_database"],
                  table=destination_tbl,
                  data_format=DataFormat.JSON,
                  ingestion_mapping_kind=IngestionMappingKind.JSON,
                  ingestion_mapping_reference="DynamicJsonMap"  # Pre-created mapping in ADX
              )
              ingest_client.ingest_from_stream(data_stream, ingestion_props)
              if WatermarkColumn!="None":
                now = max(item[f"{WatermarkColumn}"] for item in apijson["value"])
              else:
                now = datetime.utcnow()
              ingest_command = f"""
            .set-or-append {bootstrap['config_table']} <|
            datatable (
                APIEndpoint: string,
                SourceTable: string,
                DestinationTable: string,
                WatermarkColumn: string,
                LastModifiedTime: datetime
            )
            [
                '{APIEndPoint}',
                '{source_tbl}',
                '{destination_tbl}',
                '{WatermarkColumn}',
                datetime('{now}')
            ]
            """
              config_update_payload = {
                  "db": bootstrap["adx_database"],
                  "csl": ingest_command
              }
              config_response = requests.post(adx_ingest_uri, headers=adx_headers, json=config_update_payload)
              if config_response.status_code == 200:
                  print(f"Updated config for {source_tbl}")
              else:
                  print(f"Failed to update config for {source_tbl}")
                  print(config_response.text)

        else:
            print(f"Failed to retrieve {source_tbl}. Status code: {response.status_code}")
            print(response.text)
            

