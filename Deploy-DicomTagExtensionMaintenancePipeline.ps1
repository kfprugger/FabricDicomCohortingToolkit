<#
.SYNOPSIS
    Creates or updates the DICOM Tag Extension maintenance DataPipeline.

.DESCRIPTION
    The pipeline is intentionally separate from active extraction. Its safe default
    runs 20_maintain_imaging_metastore_extension in dry-run mode. To run OPTIMIZE
    or VACUUM, edit the pipeline activity parameters and pass the notebook's
    explicit confirmation string.
#>
param(
    [string]$FabricWorkspaceName = "FUJIV_Fabric_Test",
    [string]$WorkspaceId,
    [string]$PipelineName = "20_dicom_tag_extension_maintenance_pipeline",
    [string]$NotebookFolderName = "DICOM Tag Extension",
    [string]$MaintenanceNotebookName = "20_maintain_imaging_metastore_extension"
)

$ErrorActionPreference = "Stop"
$FabricApiBase = "https://api.fabric.microsoft.com/v1"

function Get-FabricToken {
    $token = az account get-access-token --resource "https://api.fabric.microsoft.com" --query accessToken -o tsv
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($token)) { throw "Failed to get Fabric access token. Run az login first." }
    return $token
}

function Invoke-FabricApi {
    param([string]$Method = "GET", [string]$Endpoint, [object]$Body = $null)
    $headers = @{ Authorization = "Bearer $(Get-FabricToken)"; "Content-Type" = "application/json" }
    $params = @{ Method = $Method; Uri = "$FabricApiBase$Endpoint"; Headers = $headers }
    if ($null -ne $Body -and $Method -ne "GET") { $params.Body = ($Body | ConvertTo-Json -Depth 50) }
    Invoke-RestMethod @params
}

function Invoke-FabricWebRequest {
    param([string]$Method, [string]$Endpoint, [object]$Body)
    $headers = @{ Authorization = "Bearer $(Get-FabricToken)"; "Content-Type" = "application/json" }
    Invoke-WebRequest -Method $Method -Uri "$FabricApiBase$Endpoint" -Headers $headers -Body ($Body | ConvertTo-Json -Depth 50) -UseBasicParsing
}

function Wait-FabricOperation {
    param([string]$OperationId, [int]$TimeoutSeconds = 300)
    if ([string]::IsNullOrWhiteSpace($OperationId)) { return }
    $elapsed = 0
    while ($elapsed -lt $TimeoutSeconds) {
        Start-Sleep -Seconds 5
        $elapsed += 5
        $op = Invoke-FabricApi -Endpoint "/operations/$OperationId"
        Write-Host "  Status: $($op.status)"
        if ($op.status -eq "Succeeded") { return }
        if ($op.status -in @("Failed", "Cancelled", "Canceled")) { throw "Fabric operation failed: $($op | ConvertTo-Json -Depth 10 -Compress)" }
    }
    throw "Fabric operation $OperationId timed out after $TimeoutSeconds seconds."
}

function To-B64([string]$Text) {
    [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($Text))
}

if (-not $WorkspaceId) {
    $workspace = (Invoke-FabricApi -Endpoint "/workspaces").value | Where-Object { $_.displayName -eq $FabricWorkspaceName } | Select-Object -First 1
    if (-not $workspace) { throw "Workspace '$FabricWorkspaceName' not found." }
    $WorkspaceId = $workspace.id
}
Write-Host "Workspace: $FabricWorkspaceName ($WorkspaceId)" -ForegroundColor Green

$items = (Invoke-FabricApi -Endpoint "/workspaces/$WorkspaceId/items").value
$maintenanceNotebook = $items | Where-Object { $_.displayName -eq $MaintenanceNotebookName -and $_.type -eq "Notebook" } | Select-Object -First 1
if (-not $maintenanceNotebook) { throw "Maintenance notebook '$MaintenanceNotebookName' not found in workspace '$WorkspaceId'." }
Write-Host "Maintenance notebook: $($maintenanceNotebook.displayName) ($($maintenanceNotebook.id))" -ForegroundColor Green

$folders = (Invoke-FabricApi -Endpoint "/workspaces/$WorkspaceId/folders").value
$folder = $folders | Where-Object { $_.displayName -eq $NotebookFolderName } | Select-Object -First 1
if (-not $folder) {
    $folder = Invoke-FabricApi -Method POST -Endpoint "/workspaces/$WorkspaceId/folders" -Body @{ displayName = $NotebookFolderName }
    Write-Host "Created folder '$NotebookFolderName'" -ForegroundColor Green
}

$pipelineContent = @{
    properties = @{
        description = "Separate guarded maintenance pipeline for DICOM Tag Extension. Safe default runs maintenance in dry-run mode; enable OPTIMIZE/VACUUM only by editing parameters and supplying CONFIRM_MAINTENANCE."
        activities = @(
            @{
                name = "Run_20_Maintain_Imaging_Metastore_Extension"
                type = "TridentNotebook"
                dependsOn = @()
                policy = @{
                    timeout = "0.12:00:00"
                    retry = 0
                    retryIntervalInSeconds = 30
                    secureOutput = $false
                    secureInput = $false
                }
                typeProperties = @{
                    notebookId = $maintenanceNotebook.id
                    workspaceId = $WorkspaceId
                    parameters = @{
                        DRY_RUN_ONLY = @{ value = "true" }
                        RUN_OPTIMIZE = @{ value = "false" }
                        RUN_VACUUM = @{ value = "false" }
                        CONFIRM_MAINTENANCE = @{ value = "" }
                        VACUUM_RETAIN_HOURS = @{ value = "168" }
                    }
                }
            }
        )
        annotations = @("DICOM Tag Extension", "Maintenance", "Safe default dry run")
    }
} | ConvertTo-Json -Depth 50

$parts = @(
    @{ path = "pipeline-content.json"; payload = (To-B64 $pipelineContent); payloadType = "InlineBase64" }
)
$definition = @{ parts = $parts }

$pipeline = $items | Where-Object { $_.displayName -eq $PipelineName -and $_.type -eq "DataPipeline" } | Select-Object -First 1
if ($pipeline) {
    Write-Host "Updating DataPipeline: $($pipeline.id)" -ForegroundColor Green
    $resp = Invoke-FabricWebRequest -Method POST -Endpoint "/workspaces/$WorkspaceId/items/$($pipeline.id)/updateDefinition" -Body @{ definition = $definition }
    $pipelineId = $pipeline.id
} else {
    Write-Host "Creating DataPipeline '$PipelineName'..." -ForegroundColor Green
    $resp = Invoke-FabricWebRequest -Method POST -Endpoint "/workspaces/$WorkspaceId/items" -Body @{ displayName = $PipelineName; type = "DataPipeline"; definition = $definition }
    if ($resp.StatusCode -eq 201) {
        $pipelineId = ($resp.Content | ConvertFrom-Json).id
    } elseif ($resp.StatusCode -eq 202) {
        $opId = $resp.Headers["x-ms-operation-id"]; if ($opId -is [array]) { $opId = $opId[0] }
        Wait-FabricOperation -OperationId $opId
        $pipelineId = ((Invoke-FabricApi -Endpoint "/workspaces/$WorkspaceId/items?type=DataPipeline").value | Where-Object { $_.displayName -eq $PipelineName } | Select-Object -First 1).id
    } else {
        throw "DataPipeline create returned HTTP $($resp.StatusCode)"
    }
}
if ([string]::IsNullOrWhiteSpace($pipelineId)) { throw "Pipeline ID could not be resolved." }

Invoke-FabricWebRequest -Method POST -Endpoint "/workspaces/$WorkspaceId/items/$pipelineId/move" -Body @{ targetFolderId = $folder.id } | Out-Null
Write-Host "Pipeline moved to folder '$NotebookFolderName'" -ForegroundColor Green
Write-Host "Maintenance pipeline ready: $PipelineName ($pipelineId)" -ForegroundColor Cyan
