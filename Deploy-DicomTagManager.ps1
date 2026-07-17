param(
    [string]$FabricWorkspaceName = "FUJIV_Fabric_Test",
    [string]$WorkspaceId,
    [string]$NotebookName = "00_add_or_update_dicom_tag",
    [string]$NotebookFolderName = "DICOM Tag Extension",
    [string]$SilverLakehouseName = "healthcare1_msft_silver",
    [string]$AdminLakehouseName = "healthcare1_msft_admin",
    [hashtable]$NotebookParameters = @{},
    [switch]$RunAfterDeploy
)

$ErrorActionPreference = "Stop"
$deployer = Join-Path $PSScriptRoot "Deploy-DicomTagExtensionNotebook.ps1"
$params = @{
    FabricWorkspaceName = $FabricWorkspaceName
    NotebookName = $NotebookName
    SourceFile = "add_or_update_dicom_tag.py"
    NotebookFolderName = $NotebookFolderName
    SilverLakehouseName = $SilverLakehouseName
    AdminLakehouseName = $AdminLakehouseName
    NotebookParameters = $NotebookParameters
}
if ($WorkspaceId) { $params.WorkspaceId = $WorkspaceId }
if ($RunAfterDeploy) { $params.RunAfterDeploy = $true }

& $deployer @params
if ($LASTEXITCODE -ne 0) {
    throw "DICOM tag manager deployment failed with exit code $LASTEXITCODE."
}
