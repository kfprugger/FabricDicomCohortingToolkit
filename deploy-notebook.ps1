param(
    [Parameter(Mandatory)][string]$FabricWorkspaceName,
    [string]$WorkspaceId,
    [string]$ReportingLhId,
    [string]$ReportingLhName = "healthcare1_reporting_gold",
    [string]$OhifViewerBaseUrl,
    [string]$DicomViewerResourceGroup = "rg-hds-dicom-viewer",
    [string]$NotebookFolderName = "Notebooks"
)

$ErrorActionPreference = 'Stop'

$tok = az account get-access-token --resource "https://api.fabric.microsoft.com" --query accessToken -o tsv
if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($tok)) {
    throw "Failed to get Fabric access token. Run 'az login' first."
}
$h = @{ "Authorization" = "Bearer $tok"; "Content-Type" = "application/json" }

function Move-NotebookToFolder {
    param(
        [Parameter(Mandatory)][string]$WorkspaceId,
        [Parameter(Mandatory)][string]$NotebookId,
        [string]$FolderName = "Notebooks"
    )
    try {
        $folders = (Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/folders" -Headers $h -Method Get).value
        $folder = $folders | Where-Object { $_.displayName -eq $FolderName } | Select-Object -First 1
        if (-not $folder) {
            $folderBody = @{ displayName = $FolderName } | ConvertTo-Json -Depth 3
            $folder = Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/folders" -Headers $h -Method Post -Body $folderBody
            Write-Host "  ✓ Created folder '$FolderName'"
        }
        $moveBody = @{ targetFolderId = $folder.id } | ConvertTo-Json -Depth 3
        for ($attempt = 1; $attempt -le 4; $attempt++) {
            try {
                Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/items/$NotebookId/move" -Headers $h -Method Post -Body $moveBody | Out-Null
                Write-Host "  ✓ Notebook moved to folder '$FolderName'"
                break
            } catch {
                $errCode = $null
                try { $errCode = [int]$_.Exception.Response.StatusCode } catch {}
                if ($errCode -eq 429 -and $attempt -lt 4) {
                    $sleepSec = 10 * $attempt
                    Write-Host "  Throttled moving notebook — retrying in ${sleepSec}s..."
                    Start-Sleep -Seconds $sleepSec
                } else { throw }
            }
        }
    } catch {
        Write-Host "  ⚠ Could not move notebook to folder '$FolderName': $($_.Exception.Message)"
    }
}
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

function Test-PlaceholderOhifViewerUrl {
    param([string]$Url)
    if ([string]::IsNullOrWhiteSpace($Url)) { return $true }
    return $Url -match 'example\.azurestaticapps\.net|placeholder|__|<|>|^https?://(null|none)(/|$)'
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
        $operation = Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/operations/$OperationId" -Headers $Headers -Method Get
        Write-Host "  Status: $($operation.status)"

        if ($operation.status -eq "Succeeded") {
            try {
                return Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/operations/$OperationId/result" -Headers $Headers -Method Get
            } catch {
                return $operation
            }
        }
        if ($operation.status -in @("Failed", "Cancelled", "Canceled")) {
            throw "Fabric operation $OperationId ended with status '$($operation.status)': $($operation | ConvertTo-Json -Depth 5 -Compress)"
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
            if (($errCode -eq 403 -and $errBody -match 'RequestDeniedByInboundPolicy|Forbidden') -or $errCode -in @(429, 500, 502, 503, 504)) {
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


# Resolve workspace ID from name if not provided
if (-not $WorkspaceId) {
    $ws = (Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces" -Headers $h -Method Get).value |
        Where-Object { $_.displayName -eq $FabricWorkspaceName }
    if (-not $ws) { throw "Workspace '$FabricWorkspaceName' not found" }
    $WorkspaceId = $ws.id
    Write-Host "Workspace: $FabricWorkspaceName ($WorkspaceId)"
}

# Resolve reporting lakehouse ID if not provided
if (-not $ReportingLhId) {
    $lakehouses = (Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/lakehouses" -Headers $h -Method Get).value
    $rptLh = $lakehouses | Where-Object { $_.displayName -eq $ReportingLhName }
    if (-not $rptLh) { throw "Lakehouse '$ReportingLhName' not found in workspace" }
    $ReportingLhId = $rptLh.id
    Write-Host "Reporting LH: $ReportingLhId"
}

# Auto-discover OHIF Viewer URL if not provided
if (-not $OhifViewerBaseUrl) {
    # Try deployment state file first
    $stateFile = Join-Path $PSScriptRoot "dicom-viewer/state-tracking/.deployment-state.json"
    if (Test-Path $stateFile) {
        try {
            $state = Get-Content $stateFile -Raw | ConvertFrom-Json
            if ($state.swaHostname) {
                $swaHost = [string]$state.swaHostname
                if ($swaHost -notmatch '^https?://') {
                    $swaHost = "https://$swaHost"
                }
                $OhifViewerBaseUrl = "$swaHost/viewer?StudyInstanceUIDs="
                Write-Host "OHIF Viewer (from state): $OhifViewerBaseUrl"
            }
        } catch {
            Write-Host "Could not read OHIF deployment state: $(Get-ErrorMessage $_)" -ForegroundColor Yellow
        }
    }

    # Try Azure SWA lookup
    if (-not $OhifViewerBaseUrl) {
        try {
            $swaUrl = az staticwebapp list --resource-group $DicomViewerResourceGroup --query "[0].defaultHostname" -o tsv 2>$null
            if ($LASTEXITCODE -eq 0 -and -not [string]::IsNullOrWhiteSpace($swaUrl) -and $swaUrl -notin @("null", "None")) {
                $OhifViewerBaseUrl = "https://$swaUrl/viewer?StudyInstanceUIDs="
                Write-Host "OHIF Viewer (from Azure): $OhifViewerBaseUrl"
            }
        } catch {
            Write-Host "Could not auto-discover OHIF Viewer from Azure: $(Get-ErrorMessage $_)" -ForegroundColor Yellow
        }
    }
}

if (Test-PlaceholderOhifViewerUrl -Url $OhifViewerBaseUrl) {
    throw "OHIF Viewer URL could not be discovered or is a placeholder. Deploy the DICOM viewer first or pass -OhifViewerBaseUrl with the real viewer URL."
}

# Clean any existing materialize notebook
Write-Host "Checking for existing notebooks..."
$items = (Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/items" -Headers $h -Method Get).value
$existing = $items | Where-Object { $_.displayName -like "materialize_reporting*" -and $_.type -eq "Notebook" }
foreach ($e in $existing) {
    Write-Host "  Deleting existing: $($e.displayName)..."
    Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/items/$($e.id)" -Headers $h -Method Delete
    Start-Sleep 5
}

# Read Python code and patch OHIF viewer URL
$pyContent = Get-Content (Join-Path $PSScriptRoot "materialize_reporting.py") -Raw
$pyContent = $pyContent -replace 'OHIF_VIEWER_BASE_URL = "[^"]*"',
    "OHIF_VIEWER_BASE_URL = `"$OhifViewerBaseUrl`""
Write-Host "Patched OHIF URL: $OhifViewerBaseUrl"

$pyLines = $pyContent -split "`n" | ForEach-Object { "$_`n" }

$ipynb = @{
    nbformat = 4
    nbformat_minor = 5
    metadata = @{
        kernel_info = @{ name = "synapse_pyspark" }
        kernelspec = @{ name = "synapse_pyspark"; display_name = "Synapse PySpark" }
        language_info = @{ name = "python" }
    }
    cells = @(
        @{
            cell_type = "code"
            source = $pyLines
            metadata = @{}
            outputs = @()
        }
    )
}

$ipynbJson = $ipynb | ConvertTo-Json -Depth 10 -Compress
$ipynbBase64 = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($ipynbJson))

$nbBody = @{
    displayName = "materialize_reporting_tables"
    type = "Notebook"
    definition = @{
        format = "ipynb"
        parts = @(
            @{
                path = "notebook-content.py"
                payload = $ipynbBase64
                payloadType = "InlineBase64"
            }
        )
    }
} | ConvertTo-Json -Depth 5

Write-Host "Creating notebook..."
$nbCreated = $false
for ($attempt = 1; $attempt -le 5; $attempt++) {
    try {
        $resp = Invoke-WebRequest -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/items" -Headers $h -Method Post -Body $nbBody
        Write-Host "  Status: $($resp.StatusCode)"
        $nbCreated = $true
        break
    } catch {
        $errCode = $null
        try { $errCode = [int]$_.Exception.Response.StatusCode } catch {}
        $errBody = Get-ErrorMessage $_
        if (($errCode -eq 409 -and $attempt -lt 5) -or (($errCode -eq 403 -and $errBody -match 'RequestDeniedByInboundPolicy|Forbidden') -and $attempt -lt 5) -or ($errCode -in @(429, 500, 502, 503, 504) -and $attempt -lt 5)) {
            $sleepSec = if ($errCode -eq 409) { 10 } else { 15 }
            Write-Host "  Notebook create transient HTTP ${errCode} — retrying in ${sleepSec}s... ($attempt/5)" -ForegroundColor Yellow
            if ($errBody) { Write-Host "    $errBody" -ForegroundColor DarkYellow }
            Start-Sleep $sleepSec
        } else {
            throw
        }
    }
}
if (-not $nbCreated) { throw "Failed to create notebook after 5 attempts" }

if ($resp.StatusCode -eq 202) {
    $opId = Get-ResponseHeaderValue -Headers $resp.Headers -Name "x-ms-operation-id"
    Write-Host "  LRO: $opId"
    $lroResult = Wait-FabricOperation -OperationId $opId -Headers $h -TimeoutSeconds 300 -PollSeconds 5
    if ($lroResult.id) {
        Write-Host "  Notebook ID: $($lroResult.id)"
    }
} elseif ($resp.StatusCode -eq 201) {
    $nb = $resp.Content | ConvertFrom-Json
    Write-Host "  Notebook ID: $($nb.id)"
} else {
    throw "Notebook create returned unexpected status code $($resp.StatusCode)."
}

# Now run the notebook
Write-Host ""

# Find notebook ID
$tok2 = az account get-access-token --resource "https://api.fabric.microsoft.com" --query accessToken -o tsv
if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($tok2)) {
    throw "Failed to refresh Fabric access token before running notebook."
}
$h2 = @{ "Authorization" = "Bearer $tok2"; "Content-Type" = "application/json" }
$items2 = (Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/items" -Headers $h2 -Method Get).value
$nbItem = $items2 | Where-Object { $_.displayName -eq "materialize_reporting_tables" -and $_.type -eq "Notebook" } | Select-Object -First 1
if (-not $nbItem) {
    throw "Notebook 'materialize_reporting_tables' was not found after create; cannot start RunNotebook."
}

Write-Host "  Notebook found: $($nbItem.id)"
Move-NotebookToFolder -WorkspaceId $WorkspaceId -NotebookId $nbItem.id -FolderName $NotebookFolderName

# Run via Spark job API
Write-Host "  Starting notebook execution..."
$runBody = @{
    executionData = @{
        parameters = @{}
    }
} | ConvertTo-Json -Depth 3

try {
    $runResp = Invoke-WebRequest -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/items/$($nbItem.id)/jobs/instances?jobType=RunNotebook" -Headers $h2 -Method Post -Body $runBody
} catch {
    throw "Failed to start RunNotebook for notebook '$($nbItem.id)': $(Get-ErrorMessage $_). Attach the lakehouse in Fabric portal if required, then rerun deployment."
}

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
Wait-FabricNotebookJob -WorkspaceId $WorkspaceId -NotebookId $nbItem.id -JobId $jobId -Headers $h2 | Out-Null
Write-Host "  Notebook job completed successfully: $jobId"
