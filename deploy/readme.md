# Migrate LA to ADX

## References
- [Migrate-LA-to-ADX.ps1](https://github.com/Azure/Azure-Sentinel/blob/master/Tools/AzureDataExplorer/Migrate-LA-to-ADX.ps1)
- [ILZ_Migrate-LA-to-ADX-working.ps1](ILZ_Migrate-LA-to-ADX-working.ps1)
  
## Architecture Comparison

Both scripts deploy a data integration pipeline from Azure Log Analytics (LA) to Azure Data Explorer (ADX) for long-term data retention. The core components include:
- **Source:** Log Analytics Workspace tables.
- **Export Mechanism:** LA data export rules that send data to Event Hubs.
- **Intermediate Layer:** Event Hub Namespaces (with topics automatically created per table).
- **Target:** ADX database with raw tables, mappings, expanded tables, functions, and update policies.
- **Connections:** ADX data connections to ingest from Event Hubs.

However, the architectures differ in scale, isolation, performance tuning, cloud environment, and resource configurations. The original script ("Migrate-LA-to-ADX.ps1") deploys a shared, cost-optimized architecture suitable for commercial Azure clouds, grouping up to 10 tables per Event Hub Namespace for efficiency. The modified script ("ILZ_Migrate-LA-to-ADX-working.ps1") deploys a more isolated, premium architecture tailored for Azure Government (US Gov) clouds, with one Event Hub per table, enhanced caching/retention, and premium features for higher throughput and security compliance.

## Key architectural differences:
- **Event Hub Structure:** Original uses shared namespaces (1 per 10 tables) with Standard SKU; Modified uses dedicated namespaces (1 per table) with Premium SKU.
- **Scalability and Isolation:** Original shares resources across tables for cost savings but potential contention; Modified provides per-table isolation for better performance and fault isolation.
- **Performance Tuning:** Modified includes explicit caching (hot data policies) and retention adjustments in ADX, plus Event Hub partition increases; Original lacks these.
- **Cloud Environment:** Original assumes commercial Azure; Modified uses US Gov endpoints (e.g., management.usgovcloudapi.net).
- **Naming and Provisioning:** Original uses random naming; Modified uses deterministic, table-specific naming.
- **Data Flow Granularity:** Original exports multiple tables to one hub; Modified exports one table per hub.
- **Retention and Caching:** Modified sets ADX raw table retention to 7 days (soft delete enabled) and caching to 1 day hot, main table to 365 days hot; Original sets raw table soft delete to 0 days with no explicit caching.
- **Transactional Behavior:** Original uses transactional updates in ADX policy; Modified uses non-transactional for potentially faster ingestion.
- **Schema Mapping:** Modified supports more data types (e.g., bool, guid, timespan, decimal) for broader compatibility; Original is limited to basic types.
- **Event Hub Config:** Modified sets 8 partitions per hub for parallelism; Original uses defaults.

Below are text-based diagrams (using Mermaid syntax for clarity; these can be rendered in tools like Mermaid Live for visuals). They illustrate the high-level data flow and resource topology.

**Original Script Architecture Diagram (Shared, Commercial Cloud)**
<img width="1009" height="174" alt="image" src="https://github.com/user-attachments/assets/12c6e8f7-6d90-4007-9921-5481c647eb19" />

This architecture emphasizes cost-efficiency with shared Event Hubs, suitable for moderate-scale workloads in commercial Azure. Data from multiple LA tables flows into fewer hubs, potentially reducing costs but increasing dependency between tables.

**Modified Script Architecture Diagram (Isolated, Gov Cloud)**
<img width="1008" height="175" alt="image" src="https://github.com/user-attachments/assets/0ed56598-04a4-4241-87dc-c8b57aba3aef" />

This architecture prioritizes isolation and performance for compliance-heavy environments like US Gov, with dedicated premium resources per table. It supports higher parallelism and customized retention, ideal for sensitive or high-volume data.

### Overall Assessment: 
The modified architecture is more robust for enterprise/government use cases, offering better isolation, compliance (Gov endpoints), and tuning at higher cost (Premium SKU). The original is simpler and cheaper for general commercial workloads but may face scalability issues with many tables.

### Comparison 2: Script Operation
The scripts share the same core logic: querying LA tables, validating support, creating ADX artifacts, provisioning Event Hubs, setting export rules, and connecting to ADX. Both use Az PowerShell modules, REST APIs for rules/connections, and Kusto CLI for ADX commands. However, the modified script ("ILZ_Migrate-LA-to-ADX-working.ps1") introduces customizations for Azure Government, per-table isolation, enhanced error handling, and architecture-specific tweaks. Below is a detailed, exhaustive list of differences, categorized for clarity.

1. General Metadata and Notes
- Original: Notes last edited "14 July 2021"
- Modified: Added "Modified for use in AF ILZ July 2025"

2. Static Values and Endpoints
- Original: Uses commercial Azure endpoints implicitly (e.g., via Az modules).
- Modified: Adds a JSON object $endpointsJson with US Gov cloud endpoints (e.g., "activeDirectory": "https://login.microsoftonline.us", "resourceManager": "https://management.usgovcloudapi.net/"). Uses these in API URIs (e.g., $az.resourceManager).
- Recommendation would be to build-in to the Az modules (see Get-AzEnvironment) features here rather than this approach

3. Helper Functions
- Original: Includes Write-Log, Get-RequiredModules, Split-ArrayBySize, Split-Array, Start-SleepMessage.
- Modified: Same as original, but adds Get-RandomSuffix (generates alphanumeric string; not actually used in the script—leftover from development).

4. Main Functions: Invoke-KustoCLI
- No differences; identical.

5. Main Functions: New-AdxRawMappingTables
- **Schema Query:** Original uses | project ColumnName, DataType; Modified uses | project ColumnName, DataType, ColumnType.
- **Type Mapping:**
  - Original: Limited conversions (datetime to 'datetime'/todatetime; others to 'string'/tostring).
  - Modified: Expanded switch on ColumnType (lowercase): Adds cases for 'dynamic' (todynamic), 'bool' (bool/tobool), 'guid' (guid/toguid), 'timespan' (timespan/totimespan), 'int' (int/toint), 'long' (long/tolong), 'real' (real/todouble), 'decimal' (decimal/todecimal). Default: 'string'/tostring.
- **Retention Policy:** Original: `.alter-merge table ... retention softdelete = 0d`.
  - Modified: `.alter table ... policy retention '{{"SoftDeletePeriod" : "7.00:00:00", "Recoverability" : "Enabled"}}'` (7 days, recoverability enabled).
- **Caching Policies:** Original: None.
  - Modified: Adds `.alter table ... policy caching hot = 1d` for raw table; `.alter table ... policy caching hot = 365d` for main table.
- **Policy Update:** Original: IsEnabled: "True", "IsTransactional": true.
  - Modified: IsEnabled: true, `"IsTransactional": false` (non-transactional for potentially faster, less strict ingestion).

6. Main Functions: New-EventHubNamespace
- **Namespace Creation:**
  - Original: Names as "$LogAnalyticsWorkspaceName-$randomNumber"; Standard SKU, Capacity 12, EnableAutoInflate, MaxThroughputUnits 20; Created in LA's resource group.
  - Modified: Names as "$AdxDBName-$tableName" (deterministic); Premium SKU, Capacity 4; No auto-inflate or max TU; Created in ADX's resource group.
Array Slicing: Original slices by 10 (for shared hubs); Modified slices by 1 (for per-table hubs).

7. Main Functions: New-LaDataExportRule:
- Rule Name: Original: "$LogAnalyticsWorkspaceName-$randomNumber" (random).
  - Modified: "ilz-sentinel-adx-exp-$tableName" (specific to table).
Tables per Rule: Original: Multiple tables (joined as string); Uses LA resource group for Event Hub.
  - Modified: Single table per rule (["$tableName"]); Uses ADX resource group for Event Hub.
- API Access Token: Original: (Get-AzAccessToken).Token.
  - Modified: Secure string conversion using Marshal.SecureStringToBSTR and PtrToStringBSTR for token handling.
- Headers: Original: Dictionary; Modified: Hashtable literal.
- Body: Modified hardcodes single table; removes count increment if not succeeded (but simplified loop).

8. Main Functions: New-ADXDataConnectionRules:
- Provider Registration: Same.
- Event Hub Retrieval: Original: From LA resource group.
  - Modified: From ADX resource group.
- Partition Count: Original: No set.
  - Modified: Sets Set-AzEventHub ... -PartitionCount 8 for each hub.
- API URI and Token: Original: Commercial "https://management.azure.com"; plain token.
  - Modified: Uses $az.resourceManager; secure token conversion as above.
- Headers: Original: Dictionary; Modified: Hashtable.
- Body: Identical, but contextually per-table due to upstream changes.
  
9. Driver Program (Main Execution Flow)
- Module Requirements: Same.
- PowerShell Version Check: Same.
- Logging and Prompts: Same, but modified notes July 2025.
- Context and Subscription: Same.
- Workspace Retrieval: Same.
- Table Selection: Same logic for all vs. selected tables.
- ADX Table Creation: Calls modified New-AdxRawMappingTables.
- Create/Update Question: Same prompt.
- Event Hubs Creation: Original slices by 10; Modified by 1.
- Export Rule Creation: Calls modified version.
- Data Connection: Same question; if yes, waits 30 min and calls modified version.
- Update Path: If updating schemas (yes), skips to log "Table schemas has been updated".

10. Other Operational Differences
- Error Handling: Modified has more specific logging in catches (e.g., status code/description in data connections).
- Verbose Output: Similar, but adjusted for new params (e.g., Premium SKU in Event Hub creation).
- File Paths and Cleanup: Same.
- Dependencies: Both require Az modules, but modified assumes Gov cloud context.
- Performance: Modified may take longer for many tables due to per-table provisioning but offers better runtime isolation.

In summary, the modified script is adapted for a government cloud, per-table architecture with premium features, while the original is more generic and shared. The changes enhance compliance, performance, and determinism but increase complexity and potential costs.
