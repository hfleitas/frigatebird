Install-Module -Name AWS.Tools.Common
Install-Module -Name AWS.Tools.S3

Import-Module AWS.Tools.Common
Import-Module AWS.Tools.S3

# Define your AWS credentials and region
$AccessKey = "myAccessKey"  # Replace with your actual access key
$SecretKey = "mySecretKey"  # Replace with your actual secret key
# Define the AWS region where your S3 bucket is located
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