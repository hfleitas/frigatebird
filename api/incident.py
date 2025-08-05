import json
import urllib.request
import urllib.parse
import requests

# Azure AD credentials
tenantId = '' # Paste your directory (tenant) ID here
clientId = '' # Paste your application (client) ID here
appSecret = '' # Paste your own app secret here to test, then store it in a safe place, such as the Azure Key Vault!

# Microsoft Defender Token URL
token_url = f"https://login.windows.net/{tenantId}/oauth2/token"
resourceAppIdUri = 'https://api.security.microsoft.com'

# ADX details
ADX_CLUSTER_URI = 'https://kvc-w854a6sxcqm4fsa9qg.northeurope.kusto.windows.net'
ADX_DATABASE = 'frigatebird_db'
ADX_TABLE = 'IncidentIngest'

#ADX_INGEST_URI = f"https://ingest-kvc-w854a6sxcqm4fsa9qg.northeurope.kusto.windows.net"

# ADX ingestion endpoint (management endpoint)
ADX_INGEST_URI=f"{ADX_CLUSTER_URI}/v1/rest/mgmt"
# Step 1: Get AAD token for Defender
body = {
    'resource': resourceAppIdUri,
    'client_id': clientId,
    'client_secret': appSecret,
    'grant_type': 'client_credentials'
}
data = urllib.parse.urlencode(body).encode("utf-8")
req = urllib.request.Request(token_url, data)
response = urllib.request.urlopen(req)
jsonResponse = json.loads(response.read())
aadToken = jsonResponse["access_token"]

# Step 2: Call Defender API
headers = {
    "Authorization": f"Bearer {aadToken}"
}
response = requests.get("https://api.security.microsoft.com/api/incidents", headers=headers)

if response.status_code == 200:
    incidents = response.json()
    print("Incidents retrieved successfully")

    # Flatten or transform if needed, else directly ingest as JSON
    formatted_json = json.dumps(incidents["value"], indent=10)
    #print(formatted_json)
    # Step 3: Get AAD token for ADX
    adx_token_url = f"https://login.microsoftonline.com/{tenantId}/oauth2/token"
    adx_token_body = {
        "grant_type": "client_credentials",
        "client_id": clientId,
        "client_secret": appSecret,
        "resource": ADX_CLUSTER_URI # Corrected resource URI
    }
    adx_data = urllib.parse.urlencode(adx_token_body).encode("utf-8")
    adx_req = urllib.request.Request(adx_token_url, adx_data)
    adx_response = urllib.request.urlopen(adx_req)
    adx_token = json.loads(adx_response.read())["access_token"]

    # Step 4: Prepare ADX ingest command
    adx_headers = {
        "Authorization": f"Bearer {adx_token}",
        "Content-Type": "application/json"
    }
   
      # command = f'''
      #         .ingest inline into table {ADX_TABLE} <|
      #         {formatted_json}
      #     '''
    
  # After fetching incidents...
    records = incidents["value"]   # this is a list of dictionaries

    # Convert each item into a Kusto dynamic() literal
    rows = [f"dynamic('{json.dumps(r)}')" for r in records]
    formatted_rows = ",\n".join(rows)

    command = f"""
    .set-or-append {ADX_TABLE} <|
    datatable (RawPayload: dynamic)
    [
    {formatted_rows}
    ]
    """
    adx_payload = {
        "db": ADX_DATABASE,
        "csl": command
    }
    ingest_response = requests.post(ADX_INGEST_URI, headers=adx_headers, json=adx_payload)
    if ingest_response.status_code == 200:
        print("Data ingested into ADX successfully.")
    else:
        print(f"Failed to ingest data. Status code: {ingest_response.status_code}")
        print(ingest_response.text)
else:
    print(f"Failed to retrieve incidents. Status code: {response.status_code}")
    print(response.text)
