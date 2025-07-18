# Migrate LA to ADX

## References
- [Migrate-LA-to-ADX.ps1](https://github.com/Azure/Azure-Sentinel/blob/master/Tools/AzureDataExplorer/Migrate-LA-to-ADX.ps1)
- [ILZ_Migrate-LA-to-ADX-working.ps1](ILZ_Migrate-LA-to-ADX-working.ps1)
  
## Key Differences

1. The script `ILZ_Migrate-LA-to-ADX-working.ps1` uses Azure Gov endpoints instead of Azure Commercial endpoints.
2. Deploys Premium Namespaces with at least 4 PUs, instead of Standard Namespaces, and sets Eventhub partitions to at least 8 to avoid throttling and increase through-put.
3. Deploys 20 tables per LAW export rule, due to [LAW Export limit](http://aka.ms/LADataExport#limitations) of 10 rules per workspace. Therefore one hub per table and 20 hubs per namespace. ie. If 100 tables need to be exported from LAW then 5 rules get created with 5 namespaces and 20 hubs per namespace. 
4. List of tables in `ADXSupportedTables.json` have been pre-chosen for a specific customer deployement. This file can be edited as needed.
5. Policies that allow for troubleshooting and comply with presidential mandate are applied for specific ADX cluster and tables.
