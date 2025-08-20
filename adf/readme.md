# ADF
This folder contains an ADF pipeline for copying a set of tables from Microsoft Defender (XDR) to Azure Data Explorer (ADX) for long-term retention, etc.


## Deployment Guide - TVM Export
This guide intends to export Defender TVM tables not supported natively by the Defender Streaming API export process.

The repo has 2 methods of exporting TVM tables. 
- Using Azure Data Factory: A pipeline template provided by Microsoft that has several considerations. Pipeline uses a single copy activity per table instead of a meta-driven approach for throughput and does not have logic for incremental processing. The template would be copying a full snapshopt of the source table daily; therefore, duplicating data in ADX and throttled by the defender API limits granted the size of the source tables for a large enviroments. ADF is great for orchestrating ETLs using a no-code UI if customers prefer or need such tool. At the moment this is NOT a recommended deployment  method because it will not work in its current state for the volume of large tvm tables, unless an updated template is provided and the no-code UI tool is preferred by the customer.

- Using scripted approach (**RECOMMENDED**): The Cloud Accelerate Factory team has developed and tested a python script to use a meta-driven approach for the export, supports exporting within the Defender API limits, full and incremental loads on a scheduled cadence. You can deploy the python script as an azure runbook or function and schedule it to run on the desired cadence the customer requires. At the moment of this commit, the python script processes chunks sequentially; we will send a PR later this week that processes the chunks in-parallel for faster throughput and additional deployment guidance to the repo. You should not modify anything other than the api endpoint, if using an Azure Gov cloud region, on the python script.
  
Run these steps in order to deploy it:
1. Create the app registration and grant it read permissions as Delegated & Application for Microsoft Threat Protection & WindowsDefenderATP. Permissions granted during our tests are documented in this repo's [api](/api) folder, see [readme.md](/api/readme.md). Most of which do require Admin consent.
2. [TVMExportConfig.kql](/kql/TVMExportConfig.kql) (enter your App Registration ID and Tenant ID).
3. [TVMExportConfigInsert.kql](/kql/TVMExportConfigInsert.kql).
4. Schedule to run the python script as desired ie. Azure Runbook or Azure Function [DAFMigration.py](/api/DAFMigration.py).
5. Validate the new TVM raw tables are populated in ADX using counts, take 100, etc. as for previous export validation checks. You can also monitor the migration config and audit log tables.
6. Apply desired or budgeted retention and caching policies to these Raw layer tables same as other Raw layer tables.
7. Proceed to expand the 9 exported as raw payloads by authoring the kql functions enabling the update policy to target tables.


## Reference Links
- https://security.microsoft.com
- https://learn.microsoft.com/en-us/defender-xdr/api-hello-world
- https://learn.microsoft.com/en-us/defender-endpoint/api/run-advanced-query-api#limitations
- https://learn.microsoft.com/en-us/defender-xdr/advanced-hunting-devicetvmbrowserextensions-table
- https://learn.microsoft.com/en-us/defender-xdr/advanced-hunting-best-practices
- https://admin.microsoft.com
- https://entra.microsoft.com
- https://intune.microsoft.com
