import json
import urllib.request
import urllib.parse
import requests
from datetime import datetime, timezone, timedelta
import time
from io import StringIO
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.data_format import DataFormat
from azure.kusto.ingest import (
    IngestionProperties,
    IngestionMappingKind,
    QueuedIngestClient
)

#--------------------Prerequisite-------------------------
# !pip install azure-kusto-data
# !pip install azure-kusto-ingest
#---------------------------------------------------------

# -------------------- API Limit Config -------------------
API_LIMIT = 45     # e.g. 45 requests/min (adjust if MS Defender updates)
RESET_WINDOW = 60   # seconds
CALL_COUNT = 0
last_reset_time = time.time()

MIN_PAGE_SIZE = 1000   # minimum rows per API call
MAX_PAGE_SIZE = 10000  # maximum rows per API call
PAGE_SIZE = MAX_PAGE_SIZE


def api_limiter():
  """Enforces API request limits"""
  global CALL_COUNT, last_reset_time
  now = time.time()
  # Reset window if 60 seconds of window expired
  if now - last_reset_time > RESET_WINDOW:
      CALL_COUNT = 0
      last_reset_time = now
  # If limit reached, sleep until reset
  if CALL_COUNT >= API_LIMIT:
      sleep_time = RESET_WINDOW - (now - last_reset_time)
      print(f"[LIMIT] API limit hit. Sleeping {sleep_time:.2f} sec...")
      time.sleep(sleep_time)
      CALL_COUNT = 0
      last_reset_time = time.time()

  CALL_COUNT += 1


def defender_api_post(url, headers, payload):
  """Wrapper for Defender API call with retry/throttle handling"""
  global PAGE_SIZE
  api_limiter()
  resp = requests.post(url, headers=headers, json=payload)

  if resp.status_code == 429:  # throttled
      retry_after = int(resp.headers.get("Retry-After", "10"))
      print(f"[THROTTLE] 429 Too Many Requests. Retrying after {retry_after} sec...")

      # back off page size if throttled
      if PAGE_SIZE > MIN_PAGE_SIZE:
          PAGE_SIZE = max(PAGE_SIZE // 2, MIN_PAGE_SIZE)
          print(f"[BACKOFF] Reducing page size to {PAGE_SIZE}")

      time.sleep(retry_after)
      return defender_api_post(url, headers, payload)

  if resp.status_code >= 500:
      # transient server issue → wait & retry
      print(f"[SERVER ERROR] {resp.status_code}, retrying in 15s...")
      time.sleep(15)
      return defender_api_post(url, headers, payload)

  resp.raise_for_status()
  return resp

def GetADXAndDefenderTokenWithIngestURI():
  adx_ingest_uri = f"{bootstrap['adx_cluster_uri']}/v1/rest/mgmt"
  aad_token_url = f"https://login.windows.net/{bootstrap['tenantId']}/oauth2/token"
  # Get Defender Token
  body = {
      'resource':  bootstrap["defender_resource_uri"],
      'client_id': bootstrap["clientId"],
      'client_secret': bootstrap["clientSecret"],
      'grant_type': 'client_credentials'
  }
  defender_token_req = urllib.request.Request(
      aad_token_url,
      urllib.parse.urlencode(body).encode("utf-8")
  )
  aad_token = json.loads(urllib.request.urlopen(defender_token_req).read())["access_token"]
  print("Defender Token acquired")

  # Get ADX Token (for config & audit updates)
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
  adx_token = json.loads(urllib.request.urlopen(adx_req).read())["access_token"]
  print("Adx Token acquired")
  return aad_token, adx_token,adx_ingest_uri

def ingest_defender_data(bootstrap: dict):
    """
    Fetches data from Microsoft Defender API in paginated batches and ingests into Azure Data Explorer.
    Includes dynamic API limit handling and page-size backoff.
    """

    # Step-1 Connect to ADX
    kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(
        bootstrap["adx_cluster_uri"],
        bootstrap["clientId"],
        bootstrap["clientSecret"],
        bootstrap["tenantId"]
    )
    kcsb_ingest = KustoConnectionStringBuilder.with_aad_application_key_authentication(
        bootstrap["adx_ingest_uri"],
        bootstrap["clientId"],
        bootstrap["clientSecret"],
        bootstrap["tenantId"]
    )

    ingest_client = QueuedIngestClient(kcsb_ingest)
    kusto_client = KustoClient(kcsb)
    # Step-2 Get Defender and ADX Token
    aad_token, adx_token,adx_ingest_uri = GetADXAndDefenderTokenWithIngestURI()
    Initial_Token_Generation_time = time.time()
    # Step-3 Fetch latest config
    response_config = kusto_client.execute(bootstrap["adx_database"], "VwLatestMigrationConfiguration")

    for table in response_config.primary_results:
        for row in table.rows:
            load_type=""
            source_tbl = row["SourceTable"]
            destination_tbl = row["DestinationTable"]
            watermark_column = row["WatermarkColumn"]
            last_modified_time = row["LastRefreshedTime"]
            last_modified_str = row['LastRefreshedTimeInString']
            load_type = row['LoadType']

            # Check Condition for Incremental Load after First Full Load
            if load_type=="Full" and last_modified_str !="":
              load_type="Incr"
            # Prepare watermark timestamp
            if last_modified_str != "":
              watermark_last_modified_time = last_modified_time.replace(tzinfo=timezone.utc)
              formatted_ts = watermark_last_modified_time.strftime("%Y-%m-%dT%H:%M:%S") + "." + last_modified_str.split(".")[1]
            elif last_modified_str == "":
              formatted_ts = datetime(1900, 1, 1).strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"

            try:
                # Step 4: Pagination Loop
                global PAGE_SIZE
                PAGE_SIZE = MAX_PAGE_SIZE  # reset before each table
                current_start = 1
                more_data = True
                TotalRecordCount=0

                while more_data:
                    current_end = current_start + PAGE_SIZE - 1
                    currentRecordCount=0
                    if watermark_column != "None":
                        kql_query = f"""
                        {source_tbl}
                        | where {watermark_column} > datetime({formatted_ts})
                        | order by {watermark_column} desc
                        | serialize rownum = row_number()
                        | where rownum between ({current_start} .. {current_end})
                        | project-away rownum
                        """
                    else:
                        kql_query = f"""
                        {source_tbl}
                        | extend IngestionTime = ingestion_time()
                        | where ingestion_time() > datetime({formatted_ts})
                        | order by IngestionTime desc
                        | serialize rownum = row_number()
                        | where rownum between ({current_start} .. {current_end})
                        | project-away rownum
                        """

                    headers = {
                        "Authorization": f"Bearer {aad_token}",
                        "Content-Type": "application/json"
                    }
                    payload = {"Query": kql_query}
                    response = defender_api_post(bootstrap['defender_hunting_api_url'], headers, payload)
                    apijson = response.json()
                    records = apijson.get("Results", [])

                    #print(kql_query)

                    # Check HTTP status first
                    if response.status_code == 403 and "Quota" in response.text:
                      raise Exception("API quota exceeded — wait until reset")
                    elif response.status_code == 400 and "Query result size exceeded" in response.text:
                        raise Exception("Query result too large — reduce batch size or timeframe")
                    elif response.status_code != 200 and response.status_code !=429:
                        raise Exception(f"API error {response.status_code}: {response.text}")

                    # Parse JSON and check Defender 'Errors'
                    apijson = response.json()
                    if "Errors" in apijson and apijson["Errors"]:
                        raise Exception(f"Defender API error: {apijson['Errors']}")

                    records = apijson.get("Results", [])

                    if not records:
                        print(f"No more records for {source_tbl} onward row {current_start}")
                        more_data = False
                        break

                    # Step 5: Ingest current batch
                    # Convert rows to string buffer for ingestion
                    data_as_str = "\n".join(json.dumps(r) for r in records)
                    data_stream = StringIO(data_as_str)
                    ingestion_props = IngestionProperties(
                        database=bootstrap["adx_database"],
                        table=destination_tbl,
                        data_format=DataFormat.JSON,
                        ingestion_mapping_kind=IngestionMappingKind.JSON,
                        ingestion_mapping_reference="DynamicJsonMap"
                    )
                    ingest_client.ingest_from_stream(data_stream, ingestion_props)
                    print(f"Ingested rows in the range: {current_start} to {current_end} for {source_tbl}")

                    # Update watermark
                    if watermark_column != "None":
                        maxtimestamp = max(item[f"{watermark_column}"] for item in records)
                    else:
                        maxtimestamp = max(item["IngestionTime"] for item in records)

                    # Step-6 Update config table
                    ingest_command = f"""
                    .set-or-append {bootstrap['config_table']} <|
                    datatable (
                        SourceTable: string,
                        DestinationTable: string,
                        WatermarkColumn: string,
                        LastRefreshedTime: datetime,
                        LoadType: string,
                        IsActive: bool
                    )
                    [
                        '{source_tbl}',
                        '{destination_tbl}',
                        '{watermark_column}',
                        datetime('{maxtimestamp}'),
                        '{load_type}',
                        true
                    ]
                    """
                    adx_headers = {"Authorization": f"Bearer {adx_token}", "Content-Type": "application/json"}
                    config_update_payload = {"db": bootstrap["adx_database"], "csl": ingest_command}
                    requests.post(adx_ingest_uri, headers=adx_headers, json=config_update_payload)

                    current_start += PAGE_SIZE  # move to next page
                    currentRecordCount=len(records)
                    TotalRecordCount+=currentRecordCount
                    Token_Generation_time_now = time.time()
                    # Call for Tokens when initial token generation time abt to exceed 1 hours limit
                    if(Token_Generation_time_now - Initial_Token_Generation_time>3500):
                      aad_token, adx_token,adx_ingest_uri = GetADXAndDefenderTokenWithIngestURI()

                status = "Success"
                FailuerMessgae = ""

            except Exception as e:
                status = "Failed"
                TotalRecordCount=0
                FailuerMessgae = str(e).replace("'", "''").replace("\n", " ")
                print(f"Data ingestion error for {source_tbl}: {FailuerMessgae}")

            # --------Step-7--Audit Table Logging ----------------
            now = datetime.utcnow()
            ingest_audit_command = f"""
            .set-or-append {bootstrap['audit_table']} <|
            datatable (
                TableName: string,
                Timestamp: datetime,
                Status: string,
                FailureReason: string,
                RecordCount: int
            )
            [
                '{destination_tbl}',
                datetime('{now}'),
                '{status}',
                '{FailuerMessgae}',
                {TotalRecordCount}
            ]
            """
            try:
                adx_headers = {"Authorization": f"Bearer {adx_token}", "Content-Type": "application/json"}
                audit_payload = {"db": bootstrap["adx_database"], "csl": ingest_audit_command}
                requests.post(adx_ingest_uri, headers=adx_headers, json=audit_payload)
                print(f"Audit log inserted for table: {destination_tbl}")
            except Exception as e:
                print(f"[AUDIT ERROR] Failed to insert audit log: {str(e)}")


# Example bootstrap config
bootstrap = {
    "adx_cluster_uri": "https://kvc-w854a6sxcqm4fsa9qg.northeurope.kusto.windows.net",
    "defender_resource_uri": "https://api.security.microsoft.com",
    "adx_database": "frigatebird_db",
    "config_table": "MigrationConfiguration",
    "clientId": "",
    "clientSecret": "",
    "tenantId": "",
    "adx_ingest_uri": "https://ingest-kvc-w854a6sxcqm4fsa9qg.northeurope.kusto.windows.net",
    "defender_hunting_api_url": "https://api.security.microsoft.com/api/advancedhunting/run",
    "audit_table": "MigrationAuditLog"
}

# Run ingestion
ingest_defender_data(bootstrap)
