<#
.SYNOPSIS
    Creates or updates the DICOM Tag Extension extraction DataPipeline.

.DESCRIPTION
    The pipeline runs the normal production extraction path:
      01_preflight -> 02_extract -> 03_canary -> 04_alerts

    Reconciliation and maintenance are intentionally separate operational flows.
#>
param(
    [string]$FabricWorkspaceName = "FUJIV_Fabric_Test",
    [string]$WorkspaceId,
    [string]$PipelineName = "01_dicom_tag_extension_extraction_pipeline",
    [string]$NotebookFolderName = "DICOM Tag Extension",
    [string]$PreflightNotebookName = "01_preflight_imaging_metastore_extension",
    [string]$ExtractNotebookName = "02_extract_imaging_metastore_extension",
    [string]$CanaryNotebookName = "03_validate_imaging_metastore_extension_canaries",
    [string]$AlertsNotebookName = "04_evaluate_imaging_metastore_extension_alerts",
    [string]$DefaultFilterStudyInstanceUids = "",
    [string]$DefaultSampleStudyCount = "",
    [string]$DefaultDryRunOnly = "false",
    [string]$DefaultMaxSourceRowsPerBatch = "50000000",
    [string]$DefaultWatermarkOverlapHours = "24",
    [string]$DefaultHashBucketCount = "128",
    [string]$DefaultMaxEnabledTags = "500",
    [string]$DefaultLeaseTimeoutMinutes = "240"
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
    if ($null -ne $Body -and $Method -ne "GET") { $params.Body = ($Body | ConvertTo-Json -Depth 60) }
    Invoke-RestMethod @params
}

function Invoke-FabricWebRequest {
    param([string]$Method, [string]$Endpoint, [object]$Body)
    $headers = @{ Authorization = "Bearer $(Get-FabricToken)"; "Content-Type" = "application/json" }
    Invoke-WebRequest -Method $Method -Uri "$FabricApiBase$Endpoint" -Headers $headers -Body ($Body | ConvertTo-Json -Depth 60) -UseBasicParsing
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

function Get-NotebookItem {
    param([array]$Items, [string]$DisplayName)
    $item = $Items | Where-Object { $_.displayName -eq $DisplayName -and $_.type -eq "Notebook" } | Select-Object -First 1
    if (-not $item) { throw "Notebook '$DisplayName' not found." }
    return $item
}

function New-NotebookActivity {
    param(
        [string]$Name,
        [string]$NotebookId,
        [hashtable]$Parameters,
        [array]$DependsOn = @(),
        [string]$Timeout = "0.12:00:00"
    )
    $paramMap = @{}
    foreach ($key in $Parameters.Keys) { $paramMap[$key] = @{ value = [string]$Parameters[$key]; type = "string" } }
    return @{
        name = $Name
        type = "TridentNotebook"
        dependsOn = $DependsOn
        policy = @{
            timeout = $Timeout
            retry = 0
            retryIntervalInSeconds = 30
            secureOutput = $false
            secureInput = $false
        }
        typeProperties = @{
            notebookId = $NotebookId
            workspaceId = $WorkspaceId
            parameters = $paramMap
        }
    }
}

function New-Dependency([string]$ActivityName) {
    return @(@{ activity = $ActivityName; dependencyConditions = @("Succeeded") })
}

if (-not $WorkspaceId) {
    $workspace = (Invoke-FabricApi -Endpoint "/workspaces").value | Where-Object { $_.displayName -eq $FabricWorkspaceName } | Select-Object -First 1
    if (-not $workspace) { throw "Workspace '$FabricWorkspaceName' not found." }
    $WorkspaceId = $workspace.id
}
Write-Host "Workspace: $FabricWorkspaceName ($WorkspaceId)" -ForegroundColor Green

$items = (Invoke-FabricApi -Endpoint "/workspaces/$WorkspaceId/items").value
$preflight = Get-NotebookItem -Items $items -DisplayName $PreflightNotebookName
$extract = Get-NotebookItem -Items $items -DisplayName $ExtractNotebookName
$canary = Get-NotebookItem -Items $items -DisplayName $CanaryNotebookName
$alerts = Get-NotebookItem -Items $items -DisplayName $AlertsNotebookName
Write-Host "Preflight notebook: $($preflight.id)" -ForegroundColor Green
Write-Host "Extract notebook:   $($extract.id)" -ForegroundColor Green
Write-Host "Canary notebook:    $($canary.id)" -ForegroundColor Green
Write-Host "Alerts notebook:    $($alerts.id)" -ForegroundColor Green

$folders = (Invoke-FabricApi -Endpoint "/workspaces/$WorkspaceId/folders").value
$folder = $folders | Where-Object { $_.displayName -eq $NotebookFolderName } | Select-Object -First 1
if (-not $folder) {
    $folder = Invoke-FabricApi -Method POST -Endpoint "/workspaces/$WorkspaceId/folders" -Body @{ displayName = $NotebookFolderName }
    Write-Host "Created folder '$NotebookFolderName'" -ForegroundColor Green
}

$activities = @(
    (New-NotebookActivity -Name "01_Preflight" -NotebookId $preflight.id -Timeout "0.02:00:00" -Parameters @{ FAIL_ON_ERROR = "true" }),
    (New-NotebookActivity -Name "02_Extract_Incremental" -NotebookId $extract.id -DependsOn (New-Dependency "01_Preflight") -Parameters @{
        DRY_RUN_ONLY = "@pipeline().parameters.DryRunOnly"
        FILTER_STUDY_INSTANCE_UIDS = "@pipeline().parameters.FilterStudyInstanceUids"
        SAMPLE_STUDY_COUNT = "@pipeline().parameters.SampleStudyCount"
        MAX_SOURCE_ROWS_PER_BATCH = "@pipeline().parameters.MaxSourceRowsPerBatch"
        WATERMARK_OVERLAP_HOURS = "@pipeline().parameters.WatermarkOverlapHours"
        HASH_BUCKET_COUNT = "@pipeline().parameters.HashBucketCount"
        MAX_ENABLED_TAGS = "@pipeline().parameters.MaxEnabledTags"
        LEASE_TIMEOUT_MINUTES = "@pipeline().parameters.LeaseTimeoutMinutes"
    }),
    (New-NotebookActivity -Name "03_Canary_Validation" -NotebookId $canary.id -Timeout "0.02:00:00" -DependsOn (New-Dependency "02_Extract_Incremental") -Parameters @{
        FAIL_ON_ERROR = "true"
        SEED_FROM_TARGET_IF_EMPTY = "true"
        MAX_CANARY_STUDIES = "5"
    }),
    (New-NotebookActivity -Name "04_Evaluate_Alerts" -NotebookId $alerts.id -Timeout "0.02:00:00" -DependsOn (New-Dependency "03_Canary_Validation") -Parameters @{ FAIL_ON_RED = "true" })
)

$pipelineContent = @{
    properties = @{
        parameters = @{
            FilterStudyInstanceUids = @{ type = "String"; defaultValue = $DefaultFilterStudyInstanceUids }
            SampleStudyCount = @{ type = "String"; defaultValue = $DefaultSampleStudyCount }
            DryRunOnly = @{ type = "String"; defaultValue = $DefaultDryRunOnly }
            MaxSourceRowsPerBatch = @{ type = "String"; defaultValue = $DefaultMaxSourceRowsPerBatch }
            WatermarkOverlapHours = @{ type = "String"; defaultValue = $DefaultWatermarkOverlapHours }
            HashBucketCount = @{ type = "String"; defaultValue = $DefaultHashBucketCount }
            MaxEnabledTags = @{ type = "String"; defaultValue = $DefaultMaxEnabledTags }
            LeaseTimeoutMinutes = @{ type = "String"; defaultValue = $DefaultLeaseTimeoutMinutes }
        }
        description = "Production DICOM Tag Extension extraction pipeline: preflight gate, incremental extraction, canary validation, and alert evaluation. Maintenance and reconciliation remain separate pipelines/processes."
        activities = $activities
        annotations = @("DICOM Tag Extension", "Extraction", "Preflight gated", "Canary gated")
    }
} | ConvertTo-Json -Depth 60

$definition = @{ parts = @(@{ path = "pipeline-content.json"; payload = (To-B64 $pipelineContent); payloadType = "InlineBase64" }) }
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
Write-Host "Extraction pipeline ready: $PipelineName ($pipelineId)" -ForegroundColor Cyan
