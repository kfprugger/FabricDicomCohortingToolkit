param(
    [string]$FabricWorkspaceName = "FUJIV_Fabric_Test",
    [string]$WorkspaceId,
    [string]$NotebookName = "99_reset_imaging_metastore_extension",
    [string]$NotebookFolderName = "DICOM Tag Extension",
    [string]$SilverLakehouseName = "healthcare1_msft_silver",
    [string]$AdminLakehouseName = "healthcare1_msft_admin",
    [hashtable]$NotebookParameters = @{},
    [switch]$RunAfterDeploy
)

$ErrorActionPreference = 'Stop'

function Get-ResponseHeaderValue {
    param(
        [Parameter(Mandatory)][object]$Headers,
        [Parameter(Mandatory)][string]$Name
    )
    $key = $Headers.Keys | Where-Object { $_ -ieq $Name } | Select-Object -First 1
    if (-not $key) { return $null }
    $value = $Headers[$key]
    if ($null -eq $value) { return $null }
    if ($value -is [System.Collections.IEnumerable] -and $value -isnot [string]) { return [string]($value | Select-Object -First 1) }
    return [string]$value
}

function Get-ErrorMessage {
    param([Parameter(Mandatory)][object]$ErrorRecord)
    if ($ErrorRecord.ErrorDetails -and $ErrorRecord.ErrorDetails.Message) { return $ErrorRecord.ErrorDetails.Message }
    if ($ErrorRecord.Exception -and $ErrorRecord.Exception.Message) { return $ErrorRecord.Exception.Message }
    return [string]$ErrorRecord
}

function Test-TransientFabricError {
    param(
        [int]$StatusCode,
        [string]$Body
    )
    if ($StatusCode -eq 403 -and $Body -match 'RequestDeniedByInboundPolicy|Forbidden') { return $true }
    return $StatusCode -in @(409, 429, 500, 502, 503, 504)
}

function Invoke-FabricRestWithRetry {
    param(
        [Parameter(Mandatory)][string]$Uri,
        [Parameter(Mandatory)][hashtable]$Headers,
        [Parameter(Mandatory)][string]$Method,
        [string]$Body,
        [int]$MaxAttempts = 5,
        [string]$OperationName = "Fabric REST call"
    )

    for ($attempt = 1; $attempt -le $MaxAttempts; $attempt++) {
        try {
            if ($PSBoundParameters.ContainsKey('Body')) {
                return Invoke-WebRequest -Uri $Uri -Headers $Headers -Method $Method -Body $Body
            }
            return Invoke-WebRequest -Uri $Uri -Headers $Headers -Method $Method
        } catch {
            $errCode = $null
            try { $errCode = [int]$_.Exception.Response.StatusCode } catch {}
            $errBody = Get-ErrorMessage $_
            if ($attempt -lt $MaxAttempts -and (Test-TransientFabricError -StatusCode $errCode -Body $errBody)) {
                $sleepSec = if ($errCode -eq 409) { 10 } elseif ($errCode -eq 429) { 15 * $attempt } else { 15 }
                Write-Host "  $OperationName transient HTTP ${errCode} — retrying in ${sleepSec}s... ($attempt/$MaxAttempts)" -ForegroundColor Yellow
                if ($errBody) { Write-Host "    $errBody" -ForegroundColor DarkYellow }
                Start-Sleep -Seconds $sleepSec
                continue
            }
            throw
        }
    }
    throw "$OperationName failed after $MaxAttempts attempts."
}

function Wait-FabricOperation {
    param(
        [Parameter(Mandatory)][string]$OperationId,
        [Parameter(Mandatory)][hashtable]$Headers,
        [int]$TimeoutSeconds = 300,
        [int]$PollSeconds = 5
    )
    if ([string]::IsNullOrWhiteSpace($OperationId)) {
        throw "Fabric operation did not return an x-ms-operation-id header."
    }

    $elapsed = 0
    while ($elapsed -lt $TimeoutSeconds) {
        Start-Sleep -Seconds $PollSeconds
        $elapsed += $PollSeconds
        try {
            $operation = Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/operations/$OperationId" -Headers $Headers -Method Get
        } catch {
            $errCode = $null
            try { $errCode = [int]$_.Exception.Response.StatusCode } catch {}
            $errBody = Get-ErrorMessage $_
            if (Test-TransientFabricError -StatusCode $errCode -Body $errBody) {
                Write-Host "  Operation poll transient HTTP $errCode ($elapsed s): $errBody" -ForegroundColor Yellow
                continue
            }
            throw
        }
        Write-Host "  Status: $($operation.status)"

        if ($operation.status -eq "Succeeded") {
            try {
                return Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/operations/$OperationId/result" -Headers $Headers -Method Get
            } catch {
                return $operation
            }
        }
        if ($operation.status -in @("Failed", "Cancelled", "Canceled")) {
            throw "Fabric operation $OperationId ended with status '$($operation.status)': $($operation | ConvertTo-Json -Depth 8 -Compress)"
        }
    }

    throw "Fabric operation $OperationId timed out after $TimeoutSeconds seconds."
}

function Wait-FabricNotebookJob {
    param(
        [Parameter(Mandatory)][string]$WorkspaceId,
        [Parameter(Mandatory)][string]$NotebookId,
        [Parameter(Mandatory)][string]$JobId,
        [Parameter(Mandatory)][hashtable]$Headers,
        [int]$TimeoutSeconds = 1800,
        [int]$PollSeconds = 15
    )

    $jobUri = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/items/$NotebookId/jobs/instances/$JobId"
    $elapsed = 0
    while ($elapsed -lt $TimeoutSeconds) {
        Start-Sleep -Seconds $PollSeconds
        $elapsed += $PollSeconds
        try {
            $job = Invoke-RestMethod -Uri $jobUri -Headers $Headers -Method Get
        } catch {
            $errCode = $null
            $errBody = Get-ErrorMessage $_
            try { $errCode = [int]$_.Exception.Response.StatusCode } catch {}
            if (Test-TransientFabricError -StatusCode $errCode -Body $errBody) {
                Write-Host "  Job status poll transient HTTP $errCode ($elapsed s): $errBody" -ForegroundColor Yellow
                continue
            }
            throw
        }
        Write-Host "  Job status: $($job.status) ($elapsed s)"

        if ($job.status -in @("Completed", "Succeeded")) { return $job }
        if ($job.status -in @("Failed", "Cancelled", "Canceled")) {
            throw "Notebook job $JobId ended with status '$($job.status)': $($job | ConvertTo-Json -Depth 8 -Compress)"
        }
    }

    throw "Notebook job $JobId timed out after $TimeoutSeconds seconds."
}

function Move-NotebookToFolder {
    param(
        [Parameter(Mandatory)][string]$WorkspaceId,
        [Parameter(Mandatory)][string]$NotebookId,
        [string]$FolderName = "Notebooks"
    )
    try {
        $folders = (Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/folders" -Headers $script:FabricHeaders -Method Get).value
        $folder = $folders | Where-Object { $_.displayName -eq $FolderName } | Select-Object -First 1
        if (-not $folder) {
            $folderBody = @{ displayName = $FolderName } | ConvertTo-Json -Depth 3
            $folder = Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/folders" -Headers $script:FabricHeaders -Method Post -Body $folderBody
            Write-Host "  ✓ Created folder '$FolderName'"
        }
        $moveBody = @{ targetFolderId = $folder.id } | ConvertTo-Json -Depth 3
        Invoke-FabricRestWithRetry -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/items/$NotebookId/move" -Headers $script:FabricHeaders -Method Post -Body $moveBody -MaxAttempts 4 -OperationName "Notebook move" | Out-Null
        Write-Host "  ✓ Notebook moved to folder '$FolderName'"
    } catch {
        Write-Host "  ⚠ Could not move notebook to folder '$FolderName': $($_.Exception.Message)" -ForegroundColor Yellow
    }
}

function ConvertTo-NotebookSourceLines {
    param([Parameter(Mandatory)][string]$Source)
    $normalized = $Source -replace "`r`n", "`n"
    $lines = $normalized -split "`n", 0, "SimpleMatch"
    if ($lines.Count -gt 0 -and $lines[$lines.Count - 1] -eq "") {
        $lines = $lines[0..($lines.Count - 2)]
    }
    $sourceLines = @()
    for ($i = 0; $i -lt $lines.Count; $i++) {
        if ($i -lt ($lines.Count - 1)) { $sourceLines += "$($lines[$i])`n" } else { $sourceLines += $lines[$i] }
    }
    return $sourceLines
}

$tok = az account get-access-token --resource "https://api.fabric.microsoft.com" --query accessToken -o tsv
if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($tok)) {
    throw "Failed to get Fabric access token. Run 'az login' first."
}
$script:FabricHeaders = @{ "Authorization" = "Bearer $tok"; "Content-Type" = "application/json" }

if (-not $WorkspaceId) {
    $workspaces = (Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces" -Headers $script:FabricHeaders -Method Get).value
    $workspace = $workspaces | Where-Object { $_.displayName -eq $FabricWorkspaceName } | Select-Object -First 1
    if (-not $workspace) { throw "Workspace '$FabricWorkspaceName' not found. Workspace creation is intentionally not attempted." }
    $WorkspaceId = $workspace.id
}
Write-Host "Workspace: $FabricWorkspaceName ($WorkspaceId)"

$lakehouses = (Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/lakehouses" -Headers $script:FabricHeaders -Method Get).value
$silverLakehouse = $lakehouses | Where-Object { $_.displayName -eq $SilverLakehouseName } | Select-Object -First 1
if (-not $silverLakehouse) { throw "Silver lakehouse '$SilverLakehouseName' not found in workspace '$WorkspaceId'." }
$adminLakehouse = $lakehouses | Where-Object { $_.displayName -eq $AdminLakehouseName } | Select-Object -First 1
if (-not $adminLakehouse) { throw "Admin lakehouse '$AdminLakehouseName' not found in workspace '$WorkspaceId'." }
Write-Host "Silver LH: $($silverLakehouse.id) ($SilverLakehouseName)"
Write-Host "Admin LH:  $($adminLakehouse.id) ($AdminLakehouseName)"

$sourcePath = Join-Path $PSScriptRoot "reset_imaging_metastore_extension.py"
if (-not (Test-Path $sourcePath)) { throw "Notebook source not found: $sourcePath" }
$pyContent = Get-Content $sourcePath -Raw
$normalizedPyContent = $pyContent -replace "`r`n", "`n"
$allSourceLines = $normalizedPyContent -split "`n", 0, "SimpleMatch"
$parameterLines = @()
$mainLines = @()
$inParameterBlock = $false
foreach ($line in $allSourceLines) {
    if ($line -eq "# Fabric parameters. Deployment tags the notebook code cell as a parameter cell so") {
        $inParameterBlock = $true
    }
    if ($inParameterBlock) {
        $parameterLines += $line
        if ($line -like "RESET_CONTROL_WATERMARK = *") {
            $inParameterBlock = $false
        }
        continue
    }
    $mainLines += $line
}
if ($parameterLines.Count -eq 0) { throw "Notebook source parameter block was not found." }
$parameterContent = ($parameterLines -join "`n").TrimEnd()
$mainContent = ($mainLines -join "`n").TrimStart("`n").TrimEnd()
$parameterPyLines = ConvertTo-NotebookSourceLines -Source $parameterContent
$mainPyLines = ConvertTo-NotebookSourceLines -Source $mainContent

$overviewMarkdownLines = ConvertTo-NotebookSourceLines -Source @'
# Reset Imaging Metastore Extension

This guarded operations notebook clears the Silver `ImagingMetastoreExtension` target rows and/or the Admin `ImagingMetastoreExtensionControl` watermark row.

Use it before an intentional full rebuild. It does not run extraction; run `02_extract_imaging_metastore_extension` afterward with no study or time-window filters.
'@
$parametersMarkdownLines = ConvertTo-NotebookSourceLines -Source @'
## Parameters

The next cell is a Fabric parameter cell.

- `CONFIRM_RESET` must equal `RESET_IMAGING_METASTORE_EXTENSION` or the notebook refuses to run.
- `RESET_TARGET_TABLE=true` clears all rows in Silver `ImagingMetastoreExtension`.
- `RESET_CONTROL_WATERMARK=true` clears the Admin control row for `extract_imaging_metastore_extension` / `ImagingMetastoreExtension`.
'@
$executionMarkdownLines = ConvertTo-NotebookSourceLines -Source @'
## Execution steps

1. Resolve workspace, Silver lakehouse, and Admin lakehouse IDs dynamically.
2. Validate the explicit confirmation token.
3. Clear the Silver target Delta table when requested.
4. Clear the Admin control watermark row when requested.
5. Print row counts before and after each operation.
'@


$ipynb = @{
    nbformat = 4
    nbformat_minor = 5
    metadata = @{
        kernel_info = @{ name = "synapse_pyspark" }
        kernelspec = @{ name = "synapse_pyspark"; display_name = "Synapse PySpark" }
        language_info = @{ name = "python" }
        dependencies = @{
            lakehouse = @{
                default_lakehouse = $silverLakehouse.id
                default_lakehouse_workspace_id = $WorkspaceId
                default_lakehouse_name = $SilverLakehouseName
            }
        }
    }
    cells = @(
        @{
            cell_type = "markdown"
            source = $overviewMarkdownLines
            metadata = @{}
        },
        @{
            cell_type = "markdown"
            source = $parametersMarkdownLines
            metadata = @{}
        },
        @{
            cell_type = "code"
            source = $parameterPyLines
            metadata = @{ tags = @("parameters") }
            outputs = @()
            execution_count = $null
        },
        @{
            cell_type = "markdown"
            source = $executionMarkdownLines
            metadata = @{}
        },
        @{
            cell_type = "code"
            source = $mainPyLines
            metadata = @{}
            outputs = @()
            execution_count = $null
        }
    )
}

$ipynbJson = $ipynb | ConvertTo-Json -Depth 20 -Compress
$ipynbBase64 = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($ipynbJson))
$definition = @{
    format = "ipynb"
    parts = @(
        @{
            path = "notebook-content.ipynb"
            payloadType = "InlineBase64"
            payload = $ipynbBase64
        }
    )
}

Write-Host "Checking for existing notebook '$NotebookName'..."
$items = (Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/items" -Headers $script:FabricHeaders -Method Get).value
$notebook = $items | Where-Object { $_.displayName -eq $NotebookName -and $_.type -eq "Notebook" } | Select-Object -First 1

if ($notebook) {
    Write-Host "Updating notebook: $($notebook.id)"
    $updateBody = @{ definition = $definition } | ConvertTo-Json -Depth 20 -Compress
    $resp = Invoke-FabricRestWithRetry -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/items/$($notebook.id)/updateDefinition" -Headers $script:FabricHeaders -Method Post -Body $updateBody -OperationName "Notebook update"
    if ($resp.StatusCode -eq 202) {
        $opId = Get-ResponseHeaderValue -Headers $resp.Headers -Name "x-ms-operation-id"
        Write-Host "  LRO: $opId"
        Wait-FabricOperation -OperationId $opId -Headers $script:FabricHeaders -TimeoutSeconds 300 -PollSeconds 5 | Out-Null
    } elseif ($resp.StatusCode -notin @(200, 201)) {
        throw "Notebook update returned unexpected status code $($resp.StatusCode)."
    }
    $notebookId = $notebook.id
    Write-Host "  ✓ Notebook updated: $notebookId"
} else {
    Write-Host "Creating notebook '$NotebookName'..."
    $createBody = @{
        displayName = $NotebookName
        type = "Notebook"
        definition = $definition
    } | ConvertTo-Json -Depth 20 -Compress
    $resp = Invoke-FabricRestWithRetry -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/items" -Headers $script:FabricHeaders -Method Post -Body $createBody -OperationName "Notebook create"
    if ($resp.StatusCode -eq 202) {
        $opId = Get-ResponseHeaderValue -Headers $resp.Headers -Name "x-ms-operation-id"
        Write-Host "  LRO: $opId"
        $lroResult = Wait-FabricOperation -OperationId $opId -Headers $script:FabricHeaders -TimeoutSeconds 300 -PollSeconds 5
        $notebookId = $lroResult.id
    } elseif ($resp.StatusCode -eq 201) {
        $created = $resp.Content | ConvertFrom-Json
        $notebookId = $created.id
    } else {
        throw "Notebook create returned unexpected status code $($resp.StatusCode)."
    }
    if ([string]::IsNullOrWhiteSpace($notebookId)) {
        $itemsAfterCreate = (Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/items" -Headers $script:FabricHeaders -Method Get).value
        $notebookId = ($itemsAfterCreate | Where-Object { $_.displayName -eq $NotebookName -and $_.type -eq "Notebook" } | Select-Object -First 1).id
    }
    if ([string]::IsNullOrWhiteSpace($notebookId)) { throw "Notebook '$NotebookName' was created but its item ID could not be resolved." }
    Write-Host "  ✓ Notebook created: $notebookId"
}

Move-NotebookToFolder -WorkspaceId $WorkspaceId -NotebookId $notebookId -FolderName $NotebookFolderName

function ConvertTo-FabricNotebookRunParameters {
    param([hashtable]$Parameters)
    $typed = @{}
    foreach ($key in $Parameters.Keys) {
        $typed[$key] = @{
            value = [string]$Parameters[$key]
            type = "string"
        }
    }
    return $typed
}


if ($RunAfterDeploy) {
    Write-Host "Starting notebook execution..."
    $runParameters = ConvertTo-FabricNotebookRunParameters -Parameters $NotebookParameters
    $runBody = @{
        executionData = @{
            parameters = $runParameters
        }
    } | ConvertTo-Json -Depth 8
    $runResp = Invoke-FabricRestWithRetry -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/items/$notebookId/jobs/instances?jobType=RunNotebook" -Headers $script:FabricHeaders -Method Post -Body $runBody -OperationName "RunNotebook start"
    Write-Host "  Run start status: $($runResp.StatusCode)"
    $jobLocation = Get-ResponseHeaderValue -Headers $runResp.Headers -Name "Location"
    if ([string]::IsNullOrWhiteSpace($jobLocation)) {
        throw "RunNotebook start response did not include a Location header; cannot poll notebook job to completion."
    }
    Write-Host "  Job Location: $jobLocation"
    if ($jobLocation -notmatch '/instances/([^/?]+)') {
        throw "RunNotebook Location header did not include a notebook job instance ID: $jobLocation"
    }
    $jobId = $Matches[1]
    Wait-FabricNotebookJob -WorkspaceId $WorkspaceId -NotebookId $notebookId -JobId $jobId -Headers $script:FabricHeaders | Out-Null
    Write-Host "  Notebook job completed successfully: $jobId"
} else {
    Write-Host "RunAfterDeploy not supplied; notebook execution skipped."
}

Write-Host "Notebook deployment complete: $NotebookName ($notebookId)"
