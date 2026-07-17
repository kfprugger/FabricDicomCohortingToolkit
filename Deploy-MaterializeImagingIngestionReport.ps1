param(
    [string]$FabricWorkspaceName = "FUJIV_Fabric_Test",
    [string]$WorkspaceId,
    [string]$NotebookName = "05_materialize_imaging_ingestion_report",
    [string]$NotebookFolderName = "DICOM Tag Extension",
    [switch]$RunAfterDeploy
)
$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$script = Join-Path $root 'Deploy-DicomTagExtensionNotebook.ps1'
& $script -FabricWorkspaceName $FabricWorkspaceName -WorkspaceId $WorkspaceId -NotebookName $NotebookName -SourceFile 'materialize_imaging_ingestion_report.py' -NotebookFolderName $NotebookFolderName -RunAfterDeploy:$RunAfterDeploy
if ($LASTEXITCODE -ne 0) { throw "Materialization notebook deployment failed." }
