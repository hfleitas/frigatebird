# Run once to install
Install-Module -Name AWS.Tools.Common
Install-Module -Name AWS.Tools.S3

#################
## S3 Download ##
#################

# Start
Import-Module AWS.Tools.Common
Import-Module AWS.Tools.S3

# Define your AWS credentials and region
$AccessKey = "..." 
$SecretKey = "..."
$Region = "us-east-1"  # Replace with your S3 bucket's region

# https://hiram.s3.us-east-1.amazonaws.com/myfolder/mysubfolder/
# Define the S3 bucket and folder path

$BucketName = "hiram"
$FolderPath = "myfolder/mysubfolder/"  # Include trailing slash for folder

# Set AWS credentials
Set-AWSCredential -AccessKey $AccessKey -SecretKey $SecretKey -StoreAs "MyProfile"

# Retrieve the list of files in the specified folder
$Files = Get-S3Object -BucketName $BucketName -KeyPrefix $FolderPath -Region $Region -ProfileName "MyProfile"

# Display the list of files
$Files | Where-Object { $_.Key -ne $FolderPath } | Select-Object Key, LastModified, Size

# loop through files
foreach ($File in $Files | Where-Object { $_.Key -ne $FolderPath }) {
    # Download a file
    Read-S3Object -BucketName $BucketName -Key $File.Key -File "C:\Downloads\$($File.Key)" -Region $Region -ProfileName "MyProfile"
}

# Clean up the stored profile
Remove-AWSCredentialProfile -ProfileName "MyProfile" -Force



##################
## Kusto Ingest ##
##################

#  dependencies
$pkgroot = "C:\Microsoft.Azure.Kusto.Tools\tools\net5.0"
$null = [System.Reflection.Assembly]::LoadFrom("$pkgroot\Kusto.Data.dll")
$null = [System.Reflection.Assembly]::LoadFrom("$pkgroot\Kusto.Ingest.dll")

#  destination
$uri = "https://ingest-kvc43f0ee6600e24ef2b0e.southcentralus.kusto.windows.net;Fed=True"
$db = "MyDatabase"
$t = "s3"

# ingestion client
$s = [Kusto.Data.KustoConnectionStringBuilder]::new($uri, $db)
$c = [Kusto.Ingest.KustoIngestFactory]::CreateQueuedIngestClient($s)
$p = [Kusto.Ingest.KustoQueuedIngestionProperties]::new($db, $t)
$p.Format = [Kusto.Data.Common.DataSourceFormat]::raw
$p.IgnoreFirstRecord = $false

## loop send files
foreach ($File in $Files | Where-Object { $_.Key -ne $FolderPath }) {
    $fp = "C:\Downloads\$($File.Key)"
    $r=$c.IngestFromStorageAsync($fp,$p)
    $r.Result.GetIngestionStatusCollection()
}
