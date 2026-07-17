param(
    [string]$FabricWorkspaceName = "FUJIV_Fabric_Test",
    [string]$WorkspaceId,
    [string]$NotebookName = "30_full_backfill_imaging_metastore_extension",
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
    SourceFile = "full_backfill_imaging_metastore_extension.py"
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
    throw "DICOM full-backfill notebook deployment failed with exit code $LASTEXITCODE."
}
