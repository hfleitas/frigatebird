# Define a list of Azure environments to choose from (only public and US Government clouds)
$environments = @(
    [PSCustomObject]@{Name = "AzureCloud"; DisplayName = "Azure Public Cloud"},
    [PSCustomObject]@{Name = "AzureUSGovernment"; DisplayName = "Azure US Government Cloud"}
)

# Prompt the user to select an Azure environment
$environments | ForEach-Object { Write-Host "$($_.Name): $($_.DisplayName)" }
$selectedEnvironmentName = Read-Host "Enter the name of the Azure environment (e.g., AzureCloud or AzureUSGovernment)"

# Check if the entered environment is valid
$selectedEnvironment = $environments | Where-Object { $_.Name -eq $selectedEnvironmentName }

if ($null -eq $selectedEnvironment) {
    Write-Host "Invalid environment selected. Exiting script."
    exit
}

# Connect to the selected Azure environment using device authentication
Write-Host "Connecting to $($selectedEnvironment.DisplayName)..."
Connect-AzAccount -Environment $selectedEnvironment.Name -UseDeviceAuthentication

# Set the current subscription (use the default subscription from Connect-AzAccount)
$context = Get-AzContext
if ($null -eq $context) {
    Write-Host "No active Azure context found. Exiting script."
    exit
}
Write-Host "Using default subscription: $($context.Subscription.Name)"

# Prompt user for folder locations and Azure Data Factory details
$linkedServiceFolder = Read-Host "Enter the path to the LinkedServices folder"
$datasetFolder = Read-Host "Enter the path to the Datasets folder"
$pipelineFolder = Read-Host "Enter the path to the Pipelines folder"
$triggerFolder = Read-Host "Enter the path to the Triggers folder"
$resourceGroupName = Read-Host "Enter the Resource Group Name"
$dataFactoryName = Read-Host "Enter the Data Factory Name"

# Loop through LinkedService files and set them
Get-ChildItem -Path $linkedServiceFolder -Filter "*.json" | ForEach-Object {
    $linkedServiceFile = $_.FullName
    $linkedServiceName = $_.BaseName
    Write-Host "Setting Linked Service: $linkedServiceName"
    
    Set-AzDataFactoryV2LinkedService -Force -ResourceGroupName $resourceGroupName -DataFactoryName $dataFactoryName -Name $linkedServiceName -File $linkedServiceFile | Format-List
}

# Loop through Dataset files and set them
Get-ChildItem -Path $datasetFolder -Filter "*.json" | ForEach-Object {
    $datasetFile = $_.FullName
    $datasetName = $_.BaseName
    Write-Host "Setting Dataset: $datasetName"
    
    Set-AzDataFactoryV2Dataset -Force -ResourceGroupName $resourceGroupName -DataFactoryName $dataFactoryName -Name $datasetName -DefinitionFile $datasetFile
}

# Loop through Pipeline files and set them
Get-ChildItem -Path $pipelineFolder -Filter "*.json" | ForEach-Object {
    $pipelineFile = $_.FullName
    $pipelineName = $_.BaseName
    Write-Host "Setting Pipeline: $pipelineName"
    
    Set-AzDataFactoryV2Pipeline -Force -ResourceGroupName $resourceGroupName -Name $pipelineName -DataFactoryName $dataFactoryName -File $pipelineFile
}

# Loop through Trigger files and set them
Get-ChildItem -Path $triggerFolder -Filter "*.json" | ForEach-Object {
    $triggerFile = $_.FullName
    $triggerName = $_.BaseName
    Write-Host "Setting Trigger: $triggerName"
    
    Set-AzDataFactoryV2Trigger -Force -ResourceGroupName $resourceGroupName -DataFactoryName $dataFactoryName -Name $triggerName -File $triggerFile
}
