param(
    [string]$FabricWorkspaceName = "FUJIV_Fabric_Test",
    [string]$WorkspaceId,
    [string]$NotebookName = "40_monitor_bronze_ingestion",
    [string]$NotebookFolderName = "DICOM Tag Extension",
    [string]$SilverLakehouseName = "healthcare1_msft_silver",
    [string]$AdminLakehouseName = "healthcare1_msft_admin",
    [hashtable]$NotebookParameters = @{},
    [switch]$RunAfterDeploy,
    [int]$JobTimeoutSeconds = 14400
)

$ErrorActionPreference = "Stop"
$deployer = Join-Path $PSScriptRoot "Deploy-DicomTagExtensionNotebook.ps1"
$params = @{
    FabricWorkspaceName = $FabricWorkspaceName
    NotebookName = $NotebookName
    SourceFile = "monitor_bronze_ingestion.py"
    NotebookFolderName = $NotebookFolderName
    SilverLakehouseName = $SilverLakehouseName
    AdminLakehouseName = $AdminLakehouseName
    NotebookParameters = $NotebookParameters
}
if ($WorkspaceId) { $params.WorkspaceId = $WorkspaceId }
if ($RunAfterDeploy) { $params.RunAfterDeploy = $true }
$params.JobTimeoutSeconds = $JobTimeoutSeconds

& $deployer @params
if ($LASTEXITCODE -ne 0) {
    throw "Bronze ingestion monitor notebook deployment failed with exit code $LASTEXITCODE."
}
