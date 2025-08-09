from azure.kusto.data import KustoConnectionStringBuilder, DataFormat
from azure.kusto.ingest import IngestionProperties, QueuedIngestClient
import gzip
import json
import io
 
clusterPath = https://ingest-atef.westeurope.kusto.windows.net
appId = "X"
appKey = "X"
appTenant = "X"
dbName = "test_db"
tableName = "RecordsData"
 
csb = KustoConnectionStringBuilder.with_aad_application_key_authentication(
    clusterPath,
    appId,
    appKey,
    appTenant
)
client = QueuedIngestClient(csb)

ingestionProperties = IngestionProperties(
    database=dbName,
    table=tableName,
    data_format=DataFormat.MULTIJSON
)

fileSrc = 'records.json.gz'

with gzip.open(fileSrc) as inputFile:
    data = json.load(inputFile)
    serialized_records = json.dumps(data['Records'])
    str_stream = io.StringIO(serialized_records)
    client.ingest_from_stream(str_stream, ingestion_properties=ingestionProperties)
