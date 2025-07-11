<#       
    THE SCRIPT IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SCRIPT OR THE USE OR OTHER DEALINGS IN THE
	SOFTWARE.

    .SYNOPSIS
        This PowerShell script integrates given Log Analytics Workspace tables data into Azure Data Explorer for long term retention. 
        For more information on how to use this script please visit: https://github.com/Azure/Azure-Sentinel/tree/master/Tools/AzureDataExplorer

    .DESCRIPTION
        It performs the following actions:
            1. Queries the Log Analytics workspace tables.
            2. Validates table names against data export supported tables AdxSupportedTables.json.
            3. Creates target Table, Raw and Mapping in Azure Data Explorer.
            4. Creates Event Hub namespaces (Standard) by dividing #1 tables by 10.
            5. Creates data export rules via Azure REST API on Log Analytics workspace.
            6. Creates data connection rules in Azure Data Explorer (ADX) database.
    
    .PARAMETER LogAnalyticsWorkSpaceName
        Enter the Log Analytics workspace name (required)
    
    .PARAMETER LogAnalyticsResourceGroupName
        Enter the Resource Group name of Log Analytics workspace (required)

    .PARAMETER AdxResourceGroupName
        Enter the Resource Group name of Azure Data Explorer (ADX) (required)

    .PARAMETER AdxClusterURL
        Enter the Resource Group name of Azure Data Explorer (ADX) Cluster URL (required)

    .PARAMETER AdxDatabaseName
        Enter the Resource Group name of Azure Data Explorer (ADX) Database Name (required)

    .NOTES
        Modified for use in AF ILZ
        July 2025

        AUTHOR: Sreedhar Ande
        LASTEDIT: 14 July 2021

    .EXAMPLE
        .\Migrate-LA-to-ADX.ps1 -LogAnalyticsResourceGroup la-resgrp1 -LogAnalyticsWorkspaceName la-workspace-1 `
        -AdxResourceGroup adx-resgrp1 -AdxClusterURL "https://adxcluster1.eastus2.kusto.windows.net" -AdxDBName AdxClusterDb1
#>

#region UserInputs

param(
    [parameter(Mandatory = $true, HelpMessage = "Enter the resource group location for the Log Analytics workspace.")]
    [string]$LogAnalyticsResourceGroup,

    [parameter(Mandatory = $true, HelpMessage = "Enter the Log Analytics workspace name from which to export data.")]
    [string]$LogAnalyticsWorkspaceName,

    [parameter(Mandatory = $true, HelpMessage = "Enter the resource group location for the existing Azure Data Explorer (ADX) cluster for which to export data.")]
    [string]$AdxResourceGroup,

    [parameter(Mandatory = $true, HelpMessage = "Enter the Azure Data Explorer (ADX) cluster URL.")]
    [string]$AdxClusterURL,

    [parameter(Mandatory = $true, HelpMessage = "Enter the Azure Data Explorer (ADX) cluster database name.")]
    [string]$AdxDBName
) 



#endregion UserInputs
      
#region StaticValues

[string]$AdxEngineUrl = "$AdxClusterURL/$AdxDBName"
[string]$KustoToolsPackage = "microsoft.azure.kusto.tools"
[string]$KustoConnectionString = "$AdxEngineUrl;Fed=True"
[string]$NuGetIndex = "https://api.nuget.org/v3/index.json"
[string]$NuGetDownloadUrl = "https://dist.nuget.org/win-x86-commandline/latest/nuget.exe"
[string]$nugetPackageLocation = "$($env:USERPROFILE)\.nuget\packages"

# Cloud Endpoints (copy from 'az cloud show')
[string]$endpointsJson = @"
{
    "activeDirectory": "https://login.microsoftonline.us",
    "activeDirectoryDataLakeResourceId": null,
    "activeDirectoryGraphResourceId": "https://graph.windows.net/",
    "activeDirectoryResourceId": "https://management.core.usgovcloudapi.net/",
    "appInsightsResourceId": "https://api.applicationinsights.us",
    "appInsightsTelemetryChannelResourceId": "https://dc.applicationinsights.us/v2/track",
    "attestationResourceId": null,
    "azmirrorStorageAccountResourceId": null,
    "batchResourceId": "https://batch.core.usgovcloudapi.net/",
    "gallery": "https://gallery.usgovcloudapi.net/",
    "logAnalyticsResourceId": "https://api.loganalytics.us",
    "management": "https://management.core.usgovcloudapi.net/",
    "mediaResourceId": "https://rest.media.usgovcloudapi.net",
    "microsoftGraphResourceId": "https://graph.microsoft.us/",
    "ossrdbmsResourceId": "https://ossrdbms-aad.database.usgovcloudapi.net",
    "portal": "https://portal.azure.us",
    "resourceManager": "https://management.usgovcloudapi.net/",
    "sqlManagement": "https://management.core.usgovcloudapi.net:8443/",
    "synapseAnalyticsResourceId": "https://dev.azuresynapse.usgovcloudapi.net",
    "vmImageAliasDoc": "https://raw.githubusercontent.com/Azure/azure-rest-api-specs/main/arm-compute/quickstart-templates/aliases.json"
}
"@
$az = $endpointsJson | ConvertFrom-Json

#endregion StaticValues

#region HelperFunctions

function Write-Log {
    <#
    .DESCRIPTION 
    Write-Log is used to write information to a log file and to the console.
    
    .PARAMETER Severity
    parameter specifies the severity of the log message. Values can be: Information, Warning, or Error. 
    #>

    [CmdletBinding()]
    param(
        [parameter()]
        [ValidateNotNullOrEmpty()]
        [string]$Message,
        [string]$LogFileName,
 
        [parameter()]
        [ValidateNotNullOrEmpty()]
        [ValidateSet('Information', 'Warning', 'Error')]
        [string]$Severity = 'Information'
    )
    # Write the message out to the correct channel											  
    switch ($Severity) {
        "Information" { Write-Host $Message -ForegroundColor Green }
        "Warning" { Write-Host $Message -ForegroundColor Yellow }
        "Error" { Write-Host $Message -ForegroundColor Red }
    } 											  
    try {
        [PSCustomObject]@{
            Time     = (Get-Date -f g)
            Message  = $Message
            Severity = $Severity
        } | Export-Csv -Path "$PSScriptRoot\$LogFileName" -Append -NoTypeInformation -Force
    }
    catch {
        Write-Error "An error occurred in Write-Log() method" -ErrorAction SilentlyContinue		
    }    
}

function Get-RandomSuffix {
    <#
    .SYNOPSIS
    Generates a random alphanumeric string.

    .DESCRIPTION
    This function returns a random string composed of uppercase letters, lowercase letters, and digits.
    The length of the string is specified by the user.

    .PARAMETER Length
    The number of characters in the generated string. Must be a positive integer.

    .EXAMPLE
    Get-RandomSuffix -Length 5
    Returns a random 5-character alphanumeric string, e.g., "aZ3kL"

    #>

    [CmdletBinding()]
    param (
        [Parameter(Mandatory = $true)]
        [ValidateRange(1, [int]::MaxValue)]
        [int]$Length
    )

    $chars = (65..90) + (97..122) + (48..57)
    -join ($chars | Get-Random -Count $Length | ForEach-Object { [char]$_ })
}

function Get-RequiredModules {
    <#
    .DESCRIPTION 
    Get-Required is used to install and then import a specified PowerShell module.
    
    .PARAMETER Module
    parameter specifices the PowerShell module to install. 
    #>

    [CmdletBinding()]
    param (        
        [parameter(Mandatory = $true)] $Module        
    )
    
    try {
        $installedModule = Get-InstalledModule -Name $Module -ErrorAction SilentlyContinue
        if ($null -eq $installedModule) {
            Write-Log -Message "The $Module PowerShell module was not found" -LogFileName $LogFileName -Severity Warning
            #check for Admin Privleges
            $currentPrincipal = New-Object Security.Principal.WindowsPrincipal([Security.Principal.WindowsIdentity]::GetCurrent())

            if (-not ($currentPrincipal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator))) {
                #Not an Admin, install to current user            
                Write-Log -Message "Can not install the $Module module. You are not running as Administrator" -LogFileName $LogFileName -Severity Warning
                Write-Log -Message "Installing $Module module to current user Scope" -LogFileName $LogFileName -Severity Warning
                
                Install-Module -Name $Module -Scope CurrentUser -Repository PSGallery -Force -AllowClobber
                Import-Module -Name $Module -Force
            }
            else {
                #Admin, install to all users																		   
                Write-Log -Message "Installing the $Module module to all users" -LogFileName $LogFileName -Severity Warning
                Install-Module -Name $Module -Repository PSGallery -Force -AllowClobber
                Import-Module -Name $Module -Force
            }
        }
        # Install-Module will obtain the module from the gallery and install it on your local machine, making it available for use.
        # Import-Module will bring the module and its functions into your current powershell session, if the module is installed.  
    }
    catch {
        Write-Log -Message "An error occurred in Get-RequiredModules() method" -LogFileName $LogFileName -Severity Error																			
        exit
    }
}

function Split-ArrayBySize {
    [CmdletBinding()]
    param (        
        [parameter(Mandatory = $true)] $AdxTabsArray,
        [parameter(Mandatory = $true)] $ArraySize
    )    
    try {        
        $slicedArraysResult = Split-Array -Item $AdxTabsArray -Size $ArraySize | ForEach-Object { '{0}' -f ($_ -join '","') }    
        return $slicedArraysResult
    }
    catch {        
        Write-Error "An error occurred in Split-ArrayBySize() method" -ErrorAction stop
        exit
    }
}

function Split-Array {
    [CmdletBinding()]
    param (        
        [parameter(Mandatory = $true)] [String[]]$Item,
        [parameter(Mandatory = $true)] [int]$Size
    )
    
    begin { $Items = @() }
    process {
        foreach ($i in $Item ) { $Items += $i }
    }
    end {
        0..[math]::Floor($Items.count / $Size) | ForEach-Object { 
            $x, $Items = $Items[0..($Size - 1)], $Items[$Size..$Items.Length]; , $x
        } 
    }  
}

function Start-SleepMessage {
    <#
    .DESCRIPTION 
    Start-SleepMessage is used to display a progress bar.
    
    .PARAMETER Seconds
    Specifices the path the the file that includes the commands to execute

    .PARAMETER WaitMessage
    Specifies the message to display with the progress bar.

    #>

    [CmdletBinding()]
    param(
        $Seconds, 
        $WaitMessage
    )

    $DoneDT = (Get-Date).AddSeconds($seconds)
    
    while ($DoneDT -gt (Get-Date)) {
        $SecondsLeft = $doneDT.Subtract((Get-Date)).TotalSeconds
        $Percent = ($Seconds - $SecondsLeft) / $seconds * 100
        Write-Progress -Activity $WaitMessage -Status "Please wait..." -SecondsRemaining $SecondsLeft -PercentComplete $Percent
        [System.Threading.Thread]::Sleep(500)
    }
    
    Write-Progress -Activity $waitMessage -Status "Please wait..." -SecondsRemaining 0 -Completed
}

#endregion

#region MainFunctions

function Invoke-KustoCLI {
    <#
    .DESCRIPTION 
    Invoke-KustoCLI is used to execute the KustoCLI with the specified AdxCommandsFile.
    
    .PARAMETER AdxCommandsFile
    parameter specifices the path the the file that includes the commands to execute 
    #>

    [CmdletBinding()]
    param (        
        [parameter(Mandatory = $true)] $AdxCommandsFile        
    )

    try {
        $KustoToolsDir = "$env:USERPROFILE\.nuget\packages\$KustoToolsPackage\"
        $CurrentDir = Get-Location
        Set-Location $ScriptDir
        

        if (!(Test-Path $KustoToolsDir)) {				 
            if (!(Test-Path nuget)) {
                Write-Log -Message "The NuGet module is not found" -LogFileName $LogFileName -Severity Warning                
                Write-Log -Message "Downloading NuGet package" -LogFileName $LogFileName -Severity Information
                (New-Object net.webclient).downloadFile($NuGetDownloadUrl, "$pwd\nuget.exe")
            }
            
            Write-Log -Message "Installing Kusto Tools Package" -LogFileName $LogFileName -Severity Information
            &.\nuget.exe install $kustoToolsPackage -Source $nugetIndex -OutputDirectory $nugetPackageLocation
        }

        $KustoExe = $KustoToolsDir + @(Get-ChildItem -Recurse -Path $KustoToolsDir -Name kusto.cli.exe)[-1]
        
        if (!(Test-Path $KustoExe)) {		 
            Write-Log -Message "Unable to find Kusto client tool $KustoExe. exiting" -LogFileName $LogFileName -Severity Warning
            Write-Warning "Unable to find Kusto client tool $KustoExe. exiting"
            return
        }    
        
        Write-Log -Message "Executing queries on Azure Data Explorer (ADX)" -LogFileName $LogFileName -Severity Information
        Invoke-Expression "$kustoExe `"$kustoConnectionString`" -script:$adxCommandsFile"
        Set-Location $CurrentDir								
    }
    catch {
        Write-Log -Message "An error occurred in Invoke-KustoCLI() method" -LogFileName $LogFileName -Severity Error																	  
        exit
    }
}

function New-AdxRawMappingTables {   
    <#
    .DESCRIPTION 
    New-AdxRawMappingTables is used to create raw mapping tables
    
    .PARAMETER LaTables
    Parameter specifices the Log Analytics tables to create 
    #>
    [CmdletBinding()]
    param (        
        [parameter(Mandatory = $true)] $LaTables,
        [parameter(Mandatory = $true)] $LaMappingDecision        
    )

    if (!(Test-Path "$PSScriptRoot\KustoQueries" -PathType Container)) { 
        New-Item -Path $PSScriptRoot -Name "KustoQueries" -ItemType "directory"
    }
        
    if (Test-Path "$PSScriptRoot\ADXSupportedTables.json") {
        $supportedTables = Get-Content "$PSScriptRoot\ADXSupportedTables.json" | ConvertFrom-Json
    }
    else {
        Write-Log " Unable to load $($PSScriptRoot)\ADXSupportedTables.json" -Severity Error
        exit
    }
  
    foreach ($table in $LaTables) {
        if ($LaMappingDecision -eq 0) { # if yes
            $TableName = $table.'$table'
        }
        else {
            $TableName = $table
        }

        $TableName = $TableName.ToString().Trim() # table name clean up
                
        if ($TableName -match '_CL$') {                
            Write-Log -Message "Custom log table : $TableName not supported" -LogFileName $LogFileName -Severity Information
        }
        elseif ($supportedTables.SupportedTables -ccontains $TableName) {        
            Write-Log -Message "Retrieving schema and mappings for $TableName" -LogFileName $LogFileName -Severity Information
            $query = $TableName + ' | getschema | project ColumnName, DataType, ColumnType'  # gets table schema      
            $AdxTablesArray.Add($TableName)
            
            Write-Verbose "Executing: (Invoke-AzOperationalInsightsQuery -WorkspaceId $LogAnalyticsWorkspaceId -Query $query).Results"																														  
            $output = (Invoke-AzOperationalInsightsQuery -WorkspaceId $LogAnalyticsWorkspaceId -Query $query).Results

            $TableExpandFunction = $TableName + 'Expand' #table expand function
            $TableRaw = $TableName + 'Raw' #raw table definition
            $RawMapping = $TableRaw + 'Mapping' # table mapping

            $FirstCommand = @()
            $ThirdCommand = @()

            foreach ($record in $output) {
                $typeKey = $record.ColumnType.ToLower()

                switch ($typeKey) {
                    'datetime' {
                        $adxType   = 'datetime' 
                        $converter = 'todatetime'
                    }
                    'string' {
                        $adxType   = 'string'
                        $converter = 'tostring'
                    }
                    'dynamic' {
                        $adxType   = 'dynamic'
                        $converter = 'todynamic'
                    }
                    'bool' {
                        $adxType   = 'bool'
                        $converter = 'tobool'
                    }
                    'guid' {
                        $adxType   = 'guid'
                        $converter = 'toguid'
                    }
                    'timespan' {
                        $adxType   = 'timespan'
                        $converter = 'totimespan'
                    }
                    'int' {
                        $adxType   = 'int'
                        $converter = 'toint'
                    }
                    'long' {
                        $adxType   = 'long'
                        $converter = 'tolong'
                    }
                    'real' {
                        $adxType   = 'real'
                        $converter = 'todouble'
                    }
                    'decimal' {
                        $adxType   = 'decimal'
                        $converter = 'todecimal'
                    }
                    default {
                        $adxType   = 'string'
                        $converter = 'tostring'
                    }
                }
                $FirstCommand += $record.ColumnName + ":" + "$adxType" + ","
                $ThirdCommand  += "$($record.ColumnName) = $converter(events.$($record.ColumnName)),"
            }

            $schema = ($FirstCommand -join '') -replace ',$'
            $function = ($ThirdCommand -join '') -replace ',$'

            $CreateRawTable = '.create table {0} (Records:dynamic)' -f $TableRaw

            $CreateRawMapping = @'
.create table {0} ingestion json mapping '{1}' '[{{"column":"Records","Properties":{{"path":"$.records"}}}}]'
'@ -f $TableRaw, $RawMapping

            $CreateRetention = ".alter table {0} policy retention '{{""SoftDeletePeriod"" : ""7.00:00:00"", ""Recoverability"" : ""Enabled""}}'" -f $TableRaw
 #retention changed per architecture requirements
            $CreateRawCaching = '.alter table {0} policy caching hot = 1d' -f $TableRaw #caching changed per architecture requirements

            $CreateTable = '.create table {0} ({1})' -f $TableName, $schema
            $CreateTableCaching = '.alter table {0} policy caching hot = 365d' -f $TableName #caching changed per architecture requirements $followup with 33rd 90? 0?

            
            $CreateFunction = @'
.create-or-alter function {0} {{{1} | mv-expand events = Records | project {2} }}
'@ -f $TableExpandFunction, $TableRaw, $function

            $CreatePolicyUpdate = @'
.alter table {0} policy update '[{{"Source": "{1}", "Query": "{2}()", "IsEnabled": true, "IsTransactional": false}}]' 
'@ -f $TableName, $TableRaw, $TableExpandFunction 

            $scriptDir = "$PSScriptRoot\KustoQueries"
            New-Item "$scriptDir\adxCommands.txt"
            Add-Content "$scriptDir\adxCommands.txt" "`n$CreateRawTable"
            Add-Content "$scriptDir\adxCommands.txt" "`n$CreateRawMapping"
            Add-Content "$scriptDir\adxCommands.txt" "`n$CreateRetention"
            Add-Content "$scriptDir\adxCommands.txt" "`n$CreateRawCaching"
            Add-Content "$scriptDir\adxCommands.txt" "`n$CreateTable"
            Add-Content "$scriptDir\adxCommands.txt" "`n$CreateTableCaching"
            Add-Content "$scriptDir\adxCommands.txt" "`n$CreateFunction"
            Add-Content "$scriptDir\adxCommands.txt" "`n$CreatePolicyUpdate"
            
            try {         
                Invoke-KustoCLI -AdxCommandsFile "$scriptDir\adxCommands.txt"
                Remove-Item $ScriptDir\adxCommands.txt -Force -ErrorAction Ignore        
            }
            catch {        
                Write-Log -Message "An error occurred in New-AdxRawMappingTables() method" -LogFileName $LogFileName -Severity Error		
                exit
            }
            Write-Log -Message "Successfully created Raw and Mapping tables for: $TableName in ADX cluster database." -LogFileName $LogFileName -Severity Information
        }
        else {
            Write-Log -Message "$TableName table is not supported by data export." -LogFileName $LogFileName -Severity Error
        }
    }
} 

function New-EventHubNamespace {
    <#
    .DESCRIPTION 
    New-EventHubNamespace is used to create an Event Hub namespace.
    
    .PARAMETER ArraysObject
    Parameter specifices the Event Hub namespace.
    #> 
    [CmdletBinding()]
    param (        
        [parameter(Mandatory = $true)] $ArraysObject        
    )
    try {
        $EventHubsArray = @()        
        foreach ($slicedArray in $ArraysObject) {
            if ($slicedArray.Length -gt 0) {
                #Create Event Hub NameSpace
                #$randomNumber = Get-Random #find different naming schema
                #$EventHubNamespaceName = "$($LogAnalyticsWorkspaceName)-$($randomNumber)"
                $tableName = $slicedArray
                $EventHubNamespaceName = "$($AdxDBName)-$($tableName)" # Only works for 1:1 architecture
                $EventHubsArray += $EventHubNamespaceName
                
                Write-Verbose "Executing: New-AzEventHubNamespace -ResourceGroupName $AdxResourceGroup -Name $EventHubNamespaceName `
                -Location $LogAnalyticsLocation -SkuName Premium -SkuCapacity 4"
                
                try {
                    Write-Log -Message "Create a new Event Hub Namespace:$EventHubNamespaceName in resource group:$AdxResourceGroup" -LogFileName $LogFileName -Severity Information
                    #Set-Item Env:\SuppressAzurePowerShellBreakingChangeWarnings "true"
                    #adjusted for architecture requirements
                    #TODO address logic for standard and premium for table ingestion
                    $ResultEventHubNS = New-AzEventHubNamespace -ResourceGroupName $AdxResourceGroup `
                        -Name $EventHubNamespaceName `
                        -Location $LogAnalyticsLocation `
                        -SkuName "Premium" `
                        -SkuCapacity 4
                    
                    # $ResultEventHubNS = New-AzEventHubNamespace -ResourceGroupName $LogAnalyticsResourceGroup `
                    #     -NamespaceName $EventHubNamespaceName `
                    #     -Location $LogAnalyticsLocation `
                    #     -SkuName "Standard" `
                    #     -SkuCapacity 12 `
                    #     -EnableAutoInflate `
                    #     -MaximumThroughputUnits 20

                    if ($ResultEventHubNS.ProvisioningState.Trim().ToLower() -eq "succeeded") {                        
                        Write-Log -Message "$EventHubNamespaceName created successfully" -LogFileName $LogFileName -Severity Information
                    }                
                }
                catch {                    
                    Write-Log -Message "$($_.ErrorDetails.Message)" -LogFileName $LogFileName -Severity Error                  
                    Write-Log -Message "$($_.InvocationInfo.Line)" -LogFileName $LogFileName -Severity Error
                }
            }
        } 
        return $EventHubsArray
    }
    catch {       
        Write-Log -Message "An error occurred in Create-EventHubNamespace() method : $($_.ErrorDetails.Message)" -LogFileName $LogFileName -Severity Error                  
        Write-Log -Message "An error occurred in Create-EventHubNamespace() method : $($_.InvocationInfo.Line)" -LogFileName $LogFileName -Severity Error         
        exit
    }
}

function New-LaDataExportRule {
    <#
    .DESCRIPTION 
    New-LaDataExportRule is used to create the Log Analytics export rule,
    
    .PARAMETER AdxEventHubs
    Parameter specifices Azure Data Explorer Event Hub to create export rule.

    .PARAMETER TablesArrayCollection
    Parameter specifies the table names used to create the export rule.
    #> 

    [CmdletBinding()]
    param (        
        [Parameter(Mandatory = $true, Position = 0)] $AdxEventHubs,
        [Parameter(Mandatory = $true, Position = 1)] $TablesArrayCollection     
    )

    Write-Log -Message "Creating Log Analytics data export rules" -LogFileName $LogFileName -Severity Information

    try {
        foreach ($AdxEventHub in $AdxEventHubs) {
            $tableName = $($AdxEventHub -split "-")[-1] # Only works in a 1:1 architecture
            Write-Verbose "Executing: Get-AzEventHubNamespace -ResourceGroupName $AdxResourceGroup -NamespaceName $AdxEventHub"
            $EventHubNameSpace = Get-AzEventHubNamespace -ResourceGroupName $AdxResourceGroup -NamespaceName $AdxEventHub

            if ($EventHubNameSpace.ProvisioningState -eq "Succeeded") {
                # $RandomSuffix = Get-RandomSuffix
                # $LaDataExportRuleName = "$($LogAnalyticsWorkspaceName)-$($RandomSuffix)"
                $LaDataExportRuleName = "ilz-sentinel-adx-exp-$tableName"
                $DataExportAPI = "$($az.resourceManager)subscriptions/$SubscriptionId/resourcegroups/$LogAnalyticsResourceGroup/providers/Microsoft.operationalInsights/workspaces/$LogAnalyticsWorkspaceName/dataexports/$laDataExportRuleName" + "?api-version=2020-08-01"
            
                $AzureAccessTokenSecure = (Get-AzAccessToken -ResourceUrl $az.resourceManager).Token
                $AzureAccessTokenBstr = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($AzureAccessTokenSecure)
                $AzureAccessToken = [System.Runtime.InteropServices.Marshal]::PtrToStringBSTR($AzureAccessTokenBstr)
                $LaAPIHeaders = @{
                    "Content-Type" = "application/json"
                    Authorization = "Bearer $AzureAccessToken"
                }
                # $LaAPIHeaders = New-Object "System.Collections.Generic.Dictionary[[String],[String]]"
                # $LaAPIHeaders.Add("Content-Type", "application/json")
                # $LaAPIHeaders.Add("Authorization", "Bearer $AzureAccessToken")
                
                $DataExportBody = @"
{
    "properties": {
        "destination": {
        "resourceId": "$($EventHubNameSpace.Id)"
        },
        "tablenames": ["$tableName"],
        "enable": true
    }
}
"@
                
                Write-Verbose "Executing: Invoke-RestMethod -Uri $DataExportAPI -Method 'PUT' -Headers $LaAPIHeaders -Body $DataExportBody"
                
                try {        
                    $CreateDataExportRule = Invoke-RestMethod -Uri $DataExportAPI -Method "PUT" -Headers $LaAPIHeaders -Body $DataExportBody                    
                    Write-Log -Message $CreateDataExportRule -LogFileName $LogFileName -Severity Information
                } 
                catch {                    
                    Write-Log -Message "$($_.ErrorDetails.Message)" -LogFileName $LogFileName -Severity Error                  
                    Write-Log -Message "$($_.InvocationInfo.Line)" -LogFileName $LogFileName -Severity Error                
                }
                $Count++
            }
            else {
                Start-SleepMessage 300
            }
        }
    }
    catch {        
        Write-Error "An error occurred in Create-LaDataExportRule() method" -ErrorAction stop
        exit
    }
}

function New-ADXDataConnectionRules {    
    [CmdletBinding()]
    param (        
        [Parameter(Mandatory = $true, Position = 0)] $AdxEventHubs      
    )
    
    try {   
        Register-AzResourceProvider -ProviderNamespace Microsoft.Kusto        
        Write-Log -Message "Creating Azure Data Explorer data connection" -LogFileName $LogFileName -Severity Information
        $ADXClusterName = $ADXClusterURL.split('.')[0].replace("https://", "").Trim()
        foreach ($AdxEH in $AdxEventHubs) {            
            Write-Verbose "Executing: Get-AzEventHub -ResourceGroup $AdxResourceGroup -NamespaceName $AdxEH"            
            try {
                $EventHubTopics = Get-AzEventHub -ResourceGroup $AdxResourceGroup -NamespaceName $AdxEH 
                                                        
                if ($null -ne $EventHubTopics) {
                    foreach ($EventHubTopic in $EventHubTopics) {
                        Write-Log -Message "Setting Event Hub Partition Count" -LogFileName $LogFileName -Severity Information
                        Write-Verbose "Executing: Set-AzEventHub -ResourceGroupName $AdxResourceGroup -NamespaceName $AdxEH -Name $EventHubTopic.Name -PartitionCount 8"
                        try {
                            Set-AzEventHub -ResourceGroupName $AdxResourceGroup -NamespaceName $AdxEH -Name $EventHubTopic.Name -PartitionCount 8 
                        } catch {
                            Write-Log -Message "An error occurred in setting partition count for $($eventHubTopic.Name)" -LogFileName $LogFileName -Severity Error
                        }

                        $TableEventHubTopic = $EventHubTopic.Name.split('-')[1]
                        # The above statement will return Table name in lower case
                        # Azure Kusto Data connection is expecting the table name in title case (Case Sensitive)
                        # In order to get exact same case table name, getting it from Source array                                               
                        $AdxTables = $AdxTablesArray.ToArray()                        
                        $ArrIndex = $AdxTables.ForEach{ $_.ToLower() }.IndexOf($tableEventHubTopic)                        
                        $EventHubResourceId = $EventHubTopic.Id
                        $AdxTableRealName = $AdxTables[$ArrIndex].Trim().ToString()
                        $AdxTableRaw = "$($AdxTableRealName)Raw"
                        $AdxTableRawMapping = "$($AdxTableRealName)RawMapping"
                        $DataConnName = "dc-$($TableEventHubTopic)"

                        $DataConnAPI = "$($az.resourceManager)subscriptions/$SubscriptionId/resourceGroups/$ADXResourceGroup/providers/Microsoft.Kusto/clusters/$ADXClusterName/databases/$ADXDBName/dataConnections/$dataConnName" + "?api-version=2021-01-01"
            
                        $AzureAccessTokenSecure = (Get-AzAccessToken -ResourceUrl $az.resourceManager).Token
                        $AzureAccessTokenBstr = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($AzureAccessTokenSecure)
                        $AzureAccessToken = [System.Runtime.InteropServices.Marshal]::PtrToStringBSTR($AzureAccessTokenBstr)
                        $DataConnAPIHeaders = @{
                            "Content-Type" = "application/json"
                            Authorization = "Bearer $AzureAccessToken"
                        }
                        # $DataConnAPIHeaders = New-Object "System.Collections.Generic.Dictionary[[String],[String]]"
                        # $DataConnAPIHeaders.Add("Content-Type", "application/json")
                        # $DataConnAPIHeaders.Add("Authorization", "Bearer $AzureAccessToken")
                                                                        
                        $DataConnBody = @"
{
    "location": "$LogAnalyticsLocation",
    "kind": "EventHub",
    "properties": {
        "eventHubResourceId": "$EventHubResourceId",
        "consumerGroup": "$('$Default')",
        "dataFormat":"JSON",
        "tableName":"$AdxTableRaw",
        "mappingRuleName":"$AdxTableRawMapping",
        "compression":"None"
    }
}
"@
                        Write-Verbose "Executing: Invoke-RestMethod -Uri $DataConnAPI -Method 'PUT' -Headers $DataConnAPIHeaders -Body $DataConnBody"
                        
                        try {        
                            $CreateDataConnRule = Invoke-RestMethod -Uri $DataConnAPI -Method "PUT" -Headers $DataConnAPIHeaders -Body $DataConnBody
                            Write-Log -Message $CreateDataConnRule -LogFileName $LogFileName -Severity Information
                        } 
                        catch {
                            
                            Write-Log -Message "An error occurred in creating data connection for $($eventHubTopic.Name)" -LogFileName $LogFileName -Severity Error            
                            Write-Log -Message $($_.Exception.Response.StatusCode.value__) -LogFileName $LogFileName -Severity Error                            
                            Write-Log -Message $($_.Exception.Response.StatusDescription) -LogFileName $LogFileName -Severity Error
                        }
                        
                    }
                }
                else {                                        
                    Write-Log -Message "Event Hub topics not available in $AdxEH" -LogFileName $LogFileName -Severity Error        
                }
                
            } 
            catch {
                Write-Log -Message "An error occurred in retrieving Event Hub topics - $($_.ErrorDetails.Message) $($_.InvocationInfo.Line)" -LogFileName $LogFileName -Severity Error                                  
            }
        }
    }
    catch {
        Write-Log -Message "An error occurred in Create-ADXDataConnectionRules() method - $($_.ErrorDetails.Message) $($_.InvocationInfo.Line)" -LogFileName $LogFileName -Severity Error		
        exit
    }
}

#endregion

#region DriverProgram

Get-RequiredModules("Az.Resources")
Get-RequiredModules("Az.OperationalInsights")
Get-RequiredModules("Az.EventHub")

# Check Powershell version, needs to be 5 or higher
if ($host.Version.Major -lt 5) {
    Write-Log "Supported PowerShell version for this script is 5 or above" -LogFileName $LogFileName -Severity Error    
    exit
}

$TimeStamp = Get-Date -Format yyyyMMdd_HHmmss 
$LogFileName = '{0}_{1}.csv' -f "ADXMigration", $TimeStamp

Write-Output "`n`n Starting Migrate-LA-to-Adx.ps1 at: $(Get-Date)"
Write-Output " Creating log $LogFileName"

Write-Host "`n`n`r`If not already authenticated, you will be prompted to sign in to Azure.`nMake sure that your credentials have:" -BackgroundColor Blue
Write-Host "`n > Azure Log Analytics workspace 'Read' permissions on: $($LogAnalyticsWorkspaceName)`n > Azure Data Explorer Database 'User' permissions on: $($AdxDBName). `n`nThese permissions are required for the script to read the Log Analytics workspace tables and to create tables in Azure Data Explorer.`r`n" -BackgroundColor Blue

Read-Host -Prompt "Press enter to continue or CTRL+C to exit the script."

$Context = Get-AzContext

if (!$Context) {
    Connect-AzAccount
    $Context = Get-AzContext
}

$SubscriptionId = $Context.Subscription.Id

Write-Verbose "Executing: Get-AzOperationalInsightsWorkspace -Name $LogAnalyticsWorkspaceName -ResourceGroupName $LogAnalyticsResourceGroup -DefaultProfile $context"

try {
    $WorkspaceObject = Get-AzOperationalInsightsWorkspace -Name $LogAnalyticsWorkspaceName -ResourceGroupName $LogAnalyticsResourceGroup -DefaultProfile $Context 
    $LogAnalyticsLocation = $WorkspaceObject.Location
    $LogAnalyticsWorkspaceId = $WorkspaceObject.CustomerId
    if ($null -ne $LogAnalyticsWorkspaceId) {
        Write-Log -Message "Workspace named $LogAnalyticsWorkspaceName in region $LogAnalyticsLocation exists." -LogFileName $LogFileName -Severity Information
    }
    else {            
        Write-Log -Message "$LogAnalyticsWorkspaceName not found" -LogFileName $LogFileName -Severity Error       
    } 
}
catch {    
    Write-Log -Message "Error occurred in retreiving Log Analytics workspace: $LogAnalyticsWorkspaceName" -LogFileName $LogFileName -Severity Error
}

#region ADXTableCreation
$LaTablesQuestion = "Do you want to create/update ADX Raw and Mapping tables for all tables in Log Analytics workspace: $($LogAnalyticsWorkspaceName)"
$LaTablesQuestionChoices = New-Object Collections.ObjectModel.Collection[Management.Automation.Host.ChoiceDescription]
$LaTablesQuestionChoices.Add((New-Object Management.Automation.Host.ChoiceDescription -ArgumentList '&Yes'))
$LaTablesQuestionChoices.Add((New-Object Management.Automation.Host.ChoiceDescription -ArgumentList '&No'))

$LaTablesQuestionDecision = $Host.UI.PromptForChoice($title, $LaTablesQuestion, $LaTablesQuestionChoices, 1)

if ($LaTablesQuestionDecision -eq 0) {    
    Write-Verbose "Executing: Invoke-AzOperationalInsightsQuery -WorkspaceId $LogAnalyticsWorkspaceId -Query $QueryAllTables" 
    
    try {       
        Write-Log -Message "Retrieving tables from $LogAnalyticsWorkspaceName" -LogFileName $LogFileName -Severity Information
        $QueryAllTables = 'search *| distinct $table| sort by $table asc nulls last'
        $ResultsAllTables = (Invoke-AzOperationalInsightsQuery -WorkspaceId $LogAnalyticsWorkspaceId -Query $QueryAllTables).Results
    }
    catch {            
        Write-Log -Message "An error occurred in querying table names from $LogAnalyticsWorkspaceName" -LogFileName $LogFileName -Severity Error         
        exit
    }
} 
else {
    try {
        Write-Host "`nEnter selected Log Analytics workspace table names separated by comma (,) (Case-Sensitive)" -ForegroundColor Blue
        $UserInputTables = Read-Host 
        $ResultsAllTables = $UserInputTables.Split(',')
    }
    catch {
        Write-Log -Message "Incorrect user input! Table names must be separated by comma (,)" -LogFileName $LogFileName -Severity Error       
        exit
    }    
}

$AdxTablesArray = New-Object System.Collections.Generic.List[System.Object]    
New-AdxRawMappingTables -LaTables $ResultsAllTables -LaMappingDecision $LaTablesQuestionDecision
#endregion

$CreateOrUpdateQuestion = "Are you updating existing table schemas in Azure Data Explorer(ADX)? If you are running this Script for the first time to integrate ADX, Select No" #select no on first run, future runs yes
$CreateOrUpdateQuestionChoices = New-Object Collections.ObjectModel.Collection[Management.Automation.Host.ChoiceDescription]
$CreateOrUpdateQuestionChoices.Add((New-Object Management.Automation.Host.ChoiceDescription -ArgumentList '&Yes'))
$CreateOrUpdateQuestionChoices.Add((New-Object Management.Automation.Host.ChoiceDescription -ArgumentList '&No'))

$CreateOrUpdateQuestionDecision = $Host.UI.PromptForChoice($title, $CreateOrUpdateQuestion, $CreateOrUpdateQuestionChoices, 1)

if ($CreateOrUpdateQuestionDecision -eq 1) {
    #region EventHubsCreation
    Write-Verbose " There are $($AdxTablesArray.ToArray().Count) supported tables to map."

    if ($AdxTablesArray.ToArray().Count -gt 0) {      

        $AdxMappedTables = Split-ArrayBySize -AdxTabsArray $AdxTablesArray.ToArray() -ArraySize 1 # TODO clean up the logic for 1:1 architecture
        
        Write-Verbose "Executing: New-EventHubNamespace -ArraysObject $AdxMappedTables" 
        $EventHubsForADX = New-EventHubNamespace -ArraysObject $AdxMappedTables
    }
    else {
        Write-Log "There are $($AdxTablesArray.ToArray().Count) supported tables to map in $($LogAnalyticsWorkspaceName), you must choose a workspace with at least one supported table." -LogFileName $LogFileName -Severity Error
        exit
    }

    #endregion

    #region LogAnalyticsDataExportRule
    New-LaDataExportRule -AdxEventHubs $EventHubsForADX -TablesArrayCollection $AdxMappedTables
    #endregion

    #region ADXDataConnectionRule
    $DataConnectionQuestion = "Do you want to create data connection rules in $AdxDBName for each table with corresponding Event Hub topic, TableRaw and TableRawMappings? `
                            If Yes, the script will wait for 30 minutes, If No, you must create the data connection rules manually."
    $DataConnectionQuestionChoices = New-Object Collections.ObjectModel.Collection[Management.Automation.Host.ChoiceDescription]
    $DataConnectionQuestionChoices.Add((New-Object Management.Automation.Host.ChoiceDescription -ArgumentList '&Yes'))
    $DataConnectionQuestionChoices.Add((New-Object Management.Automation.Host.ChoiceDescription -ArgumentList '&No'))

    $DataConnectionQuestionDecision = $Host.UI.PromptForChoice($title, $DataConnectionQuestion, $DataConnectionQuestionChoices, 0)
    if ($DataConnectionQuestionDecision -eq 0) {
        Start-SleepMessage -Seconds 1800 -waitMessage "Provisioning Event Hub topics for Log Analytics tables"                    
        New-ADXDataConnectionRules -AdxEventHubs $EventHubsForADX
    }
    else {            
        Write-Log -Message "Please manually create data connection rules for $AdxDBName in $AdxEngineUrl" -LogFileName $LogFileName -Severity Warning    
    }
    #endregion
}
else {
    Write-Log "Table schemas has been updated" -LogFileName $LogFileName -Severity Information
    exit
}