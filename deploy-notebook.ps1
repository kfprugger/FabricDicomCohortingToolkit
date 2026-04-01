param(
    [Parameter(Mandatory)][string]$FabricWorkspaceName,
    [string]$WorkspaceId,
    [string]$ReportingLhId,
    [string]$ReportingLhName = "healthcare1_reporting_gold",
    [string]$OhifViewerBaseUrl,
    [string]$DicomViewerResourceGroup = "rg-hds-dicom-viewer"
)

$tok = az account get-access-token --resource "https://api.fabric.microsoft.com" --query accessToken -o tsv
$h = @{ "Authorization" = "Bearer $tok"; "Content-Type" = "application/json" }

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
    $stateFile = Join-Path $PSScriptRoot "dicom-viewer\.deployment-state.json"
    if (Test-Path $stateFile) {
        $state = Get-Content $stateFile -Raw | ConvertFrom-Json
        if ($state.swaHostname) {
            $OhifViewerBaseUrl = "$($state.swaHostname)/viewer?StudyInstanceUIDs="
            Write-Host "OHIF Viewer (from state): $OhifViewerBaseUrl"
        }
    }
    # Try Azure SWA lookup
    if (-not $OhifViewerBaseUrl) {
        try {
            $swaUrl = az staticwebapp list --resource-group $DicomViewerResourceGroup --query "[0].defaultHostname" -o tsv 2>$null
            if ($swaUrl) {
                $OhifViewerBaseUrl = "https://$swaUrl/viewer?StudyInstanceUIDs="
                Write-Host "OHIF Viewer (from Azure): $OhifViewerBaseUrl"
            }
        } catch {}
    }
    if (-not $OhifViewerBaseUrl) {
        $OhifViewerBaseUrl = "https://example.azurestaticapps.net/viewer?StudyInstanceUIDs="
        Write-Host "OHIF Viewer: using placeholder (deploy DICOM viewer first for real URLs)"
    }
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
        if ($errCode -eq 409 -and $attempt -lt 5) {
            Write-Host "  409 Conflict (previous deletion still propagating) — retrying in 10s... ($attempt/5)" -ForegroundColor Yellow
            Start-Sleep 10
        } else {
            throw
        }
    }
}
if (-not $nbCreated) { throw "Failed to create notebook after 5 attempts" }

if ($resp.StatusCode -eq 202) {
    $opId = ($resp.Headers["x-ms-operation-id"])[0]
    Write-Host "  LRO: $opId"
    
    # Poll LRO
    for ($i = 0; $i -lt 12; $i++) {
        Start-Sleep 5
        $lro = Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/operations/$opId" -Headers $h
        Write-Host "  Status: $($lro.status)"
        if ($lro.status -eq "Succeeded") {
            # Get result
            $lroResult = Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/operations/$opId/result" -Headers $h
            Write-Host "  Notebook ID: $($lroResult.id)"
            break
        }
        if ($lro.status -eq "Failed") {
            Write-Host "  ERROR: $($lro.error.message)"
            break
        }
    }
} elseif ($resp.StatusCode -eq 201) {
    $nb = $resp.Content | ConvertFrom-Json
    Write-Host "  Notebook ID: $($nb.id)"
}

# Now run the notebook
Write-Host ""
Write-Host "To run the notebook, open it in Fabric portal and:"
Write-Host "  1. Attach it to the 'healthcare1_reporting_gold' lakehouse"
Write-Host "  2. Click Run All"
Write-Host ""
Write-Host "Or run via API (requires lakehouse attachment):"

# Find notebook ID
$tok2 = az account get-access-token --resource "https://api.fabric.microsoft.com" --query accessToken -o tsv
$h2 = @{ "Authorization" = "Bearer $tok2"; "Content-Type" = "application/json" }
$items2 = (Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/items" -Headers $h2 -Method Get).value
$nbItem = $items2 | Where-Object { $_.displayName -eq "materialize_reporting_tables" -and $_.type -eq "Notebook" }
if ($nbItem) {
    Write-Host "  Notebook found: $($nbItem.id)"
    
    # Run via Spark job API
    Write-Host "  Starting notebook execution..."
    $runBody = @{
        executionData = @{
            parameters = @{}
        }
    } | ConvertTo-Json -Depth 3
    
    try {
        $runResp = Invoke-WebRequest -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/items/$($nbItem.id)/jobs/instances?jobType=RunNotebook" -Headers $h2 -Method Post -Body $runBody
        Write-Host "  Run started! Status: $($runResp.StatusCode)"
        if ($runResp.Headers["Location"]) {
            Write-Host "  Job Location: $($runResp.Headers['Location'])"
        }
    } catch {
        Write-Host "  Run error: $($_.ErrorDetails.Message)"
        Write-Host "  You may need to attach the lakehouse in Fabric portal first."
    }
}
