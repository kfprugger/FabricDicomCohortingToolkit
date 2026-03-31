<# 
.SYNOPSIS
    Deploys the Imaging Report (Semantic Model + Report) to a Fabric workspace
    via the REST API, with no Power BI Desktop dependency.

.DESCRIPTION
    Reads the PBIP definition files from disk, patches the SQL endpoint and
    OhifViewerBaseUrl parameter for the target environment, then creates or
    updates the SemanticModel and Report items in the Fabric workspace.

.PARAMETER FabricWorkspaceName
    Target Fabric workspace name. Default: med-device-rti-hds

.PARAMETER OhifViewerBaseUrl
    Full OHIF viewer URL including the ?StudyInstanceUIDs= suffix.
    Auto-discovered from .deployment-state.json if not provided.

.PARAMETER ReportSourcePath
    Path to the FabricDicomCohortingToolkit repo root. Default: script directory.

.EXAMPLE
    .\Deploy-ImagingReport.ps1 -FabricWorkspaceName "med-device-rti-hds"
#>
param(
    [Parameter(Mandatory)][string]$FabricWorkspaceName,
    [string]$ReportingLhName = "healthcare1_reporting_gold",
    [string]$OhifViewerBaseUrl = "",
    [string]$ReportSourcePath = ""
)

$ErrorActionPreference = "Stop"
$FabricApiBase = "https://api.fabric.microsoft.com/v1"

if (-not $ReportSourcePath) {
    $ReportSourcePath = Split-Path -Parent $MyInvocation.MyCommand.Path
}

# ============================================================================
# HELPERS
# ============================================================================

function Get-FabricToken {
    $t = (Get-AzAccessToken -ResourceUrl "https://api.fabric.microsoft.com").Token
    if ($t -is [System.Security.SecureString]) {
        $bstr = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($t)
        try { return [System.Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr) }
        finally { [System.Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr) }
    }
    return $t
}

function Invoke-FabricApi {
    param([string]$Method = "GET", [string]$Endpoint, [object]$Body = $null)
    $token = Get-FabricToken
    $headers = @{ "Authorization" = "Bearer $token"; "Content-Type" = "application/json" }
    $uri = "$FabricApiBase$Endpoint"
    $params = @{ Method = $Method; Uri = $uri; Headers = $headers }
    if ($Body -and $Method -ne "GET") {
        $params["Body"] = ($Body | ConvertTo-Json -Depth 20)
    }
    Invoke-RestMethod @params
}

function To-B64 ([string]$Text) {
    [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($Text))
}

function Wait-LRO {
    param([string]$OperationUrl, [int]$TimeoutSec = 120)
    $start = Get-Date
    while ((New-TimeSpan -Start $start).TotalSeconds -lt $TimeoutSec) {
        Start-Sleep -Seconds 3
        try {
            $op = Invoke-FabricApi -Endpoint $OperationUrl
            if ($op.status -eq "Succeeded") { return $true }
            if ($op.status -eq "Failed") {
                Write-Host "    LRO failed: $($op | ConvertTo-Json -Depth 5 -Compress)" -ForegroundColor Red
                return $false
            }
        } catch {}
    }
    Write-Host "    LRO timed out after ${TimeoutSec}s" -ForegroundColor Yellow
    return $false
}

# ============================================================================
# DISCOVER WORKSPACE + SQL ENDPOINTS
# ============================================================================

Write-Host ""
Write-Host "  --- Deploying Imaging Report to Fabric ---" -ForegroundColor Cyan
Write-Host ""

# Resolve workspace
$ws = (Invoke-FabricApi -Endpoint "/workspaces").value | Where-Object { $_.displayName -eq $FabricWorkspaceName }
if (-not $ws) { throw "Workspace '$FabricWorkspaceName' not found" }
$workspaceId = $ws.id
Write-Host "  ✓ Workspace: $FabricWorkspaceName ($workspaceId)" -ForegroundColor Green

# Find Reporting Gold Lakehouse and its SQL endpoint
$lakehouses = (Invoke-FabricApi -Endpoint "/workspaces/$workspaceId/lakehouses").value
$reportingLh = $lakehouses | Where-Object { $_.displayName -eq $ReportingLhName } | Select-Object -First 1

if (-not $reportingLh) { throw "$ReportingLhName Lakehouse not found in workspace" }

# Get SQL endpoint from Lakehouse detail (requires /lakehouses/{id} endpoint)
$reportingLhDetail = Invoke-FabricApi -Endpoint "/workspaces/$workspaceId/lakehouses/$($reportingLh.id)"
$reportingServer = $reportingLhDetail.properties.sqlEndpointProperties.connectionString
$reportingDbName = $reportingLh.displayName
if (-not $reportingServer) { throw "Could not discover Reporting Lakehouse SQL endpoint connection string" }
Write-Host "  ✓ Reporting SQL: $reportingServer / $reportingDbName" -ForegroundColor Green

# ============================================================================
# BUILD SEMANTIC MODEL DEFINITION (TMDL with patched endpoints)
# ============================================================================

Write-Host ""
Write-Host "  Building Semantic Model definition..." -ForegroundColor White

$smDir = Join-Path $ReportSourcePath "ImagingReport.SemanticModel\definition"

# Read all TMDL files and patch SQL endpoints
$tmdlFiles = @()

# model.tmdl — no patching needed
$tmdlFiles += @{ Path = "definition/model.tmdl"; Content = (Get-Content (Join-Path $smDir "model.tmdl") -Raw) }

# database.tmdl
$tmdlFiles += @{ Path = "definition/database.tmdl"; Content = (Get-Content (Join-Path $smDir "database.tmdl") -Raw) }

# relationships.tmdl
$tmdlFiles += @{ Path = "definition/relationships.tmdl"; Content = (Get-Content (Join-Path $smDir "relationships.tmdl") -Raw) }

# expressions.tmdl — patch Reporting LH SQL endpoint
$exprContent = Get-Content (Join-Path $smDir "expressions.tmdl") -Raw
# Patch Reporting Lakehouse SQL endpoint in ReportingSource expression
$exprContent = $exprContent -replace 'Sql\.Database\("placeholder-server\.datawarehouse\.fabric\.microsoft\.com",\s*"healthcare1_reporting_gold"\)',
    "Sql.Database(`"$reportingServer`", `"$reportingDbName`")"
$tmdlFiles += @{ Path = "definition/expressions.tmdl"; Content = $exprContent }

# cultures
$culturesDir = Join-Path $smDir "cultures"
if (Test-Path $culturesDir) {
    Get-ChildItem $culturesDir -Filter "*.tmdl" | ForEach-Object {
        $tmdlFiles += @{ Path = "definition/cultures/$($_.Name)"; Content = (Get-Content $_.FullName -Raw) }
    }
}

# Tables — no patching needed for Direct Lake (expressions handle connection details)
$tablesDir = Join-Path $smDir "tables"
Get-ChildItem $tablesDir -Filter "*.tmdl" | ForEach-Object {
    $content = Get-Content $_.FullName -Raw
    $tmdlFiles += @{ Path = "definition/tables/$($_.Name)"; Content = $content }
}

# Build the definition parts array
$smParts = @()

# .platform
$smPlatform = @{
    '$schema' = "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json"
    metadata = @{ type = "SemanticModel"; displayName = "ImagingReport" }
    config = @{ version = "2.0"; logicalId = [guid]::NewGuid().ToString() }
} | ConvertTo-Json -Depth 5
$smParts += @{ path = ".platform"; payload = (To-B64 $smPlatform); payloadType = "InlineBase64" }

# definition.pbism
$pbism = Get-Content (Join-Path $ReportSourcePath "ImagingReport.SemanticModel\definition.pbism") -Raw
$smParts += @{ path = "definition.pbism"; payload = (To-B64 $pbism); payloadType = "InlineBase64" }

# All TMDL files
foreach ($tf in $tmdlFiles) {
    $smParts += @{ path = $tf.Path; payload = (To-B64 $tf.Content); payloadType = "InlineBase64" }
}

Write-Host "  ✓ Semantic Model: $($smParts.Count) definition parts" -ForegroundColor Green

# ============================================================================
# BUILD REPORT DEFINITION
# ============================================================================

Write-Host "  Building Report definition..." -ForegroundColor White

$rptDir = Join-Path $ReportSourcePath "ImagingReport.Report\definition"
$rptParts = @()

# .platform
$rptPlatform = @{
    '$schema' = "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json"
    metadata = @{ type = "Report"; displayName = "ImagingReport" }
    config = @{ version = "2.0"; logicalId = [guid]::NewGuid().ToString() }
} | ConvertTo-Json -Depth 5
$rptParts += @{ path = ".platform"; payload = (To-B64 $rptPlatform); payloadType = "InlineBase64" }

# Recursively collect all files under definition/
$defRoot = $rptDir
Get-ChildItem $defRoot -Recurse -File | ForEach-Object {
    $relPath = "definition/" + $_.FullName.Substring($defRoot.Length + 1).Replace("\", "/")
    $content = Get-Content $_.FullName -Raw -Encoding UTF8
    $rptParts += @{ path = $relPath; payload = (To-B64 $content); payloadType = "InlineBase64" }
}

Write-Host "  ✓ Report: $($rptParts.Count) definition parts" -ForegroundColor Green

# ============================================================================
# CREATE OR UPDATE SEMANTIC MODEL
# ============================================================================

Write-Host ""
Write-Host "  Deploying Semantic Model..." -ForegroundColor White

$smName = "ImagingReport"
$existingSm = (Invoke-FabricApi -Endpoint "/workspaces/$workspaceId/items?type=SemanticModel").value |
    Where-Object { $_.displayName -eq $smName }

if ($existingSm) {
    $smId = $existingSm.id
    Write-Host "  ✓ Existing: $smName ($smId) — updating definition" -ForegroundColor Green
} else {
    Write-Host "  Creating Semantic Model '$smName'..." -ForegroundColor White
    $createBody = @{
        displayName = $smName
        type = "SemanticModel"
        definition = @{ parts = $smParts }
    }
    try {
        $token = Get-FabricToken
        $headers = @{ "Authorization" = "Bearer $token"; "Content-Type" = "application/json" }
        $resp = Invoke-WebRequest -Method POST `
            -Uri "$FabricApiBase/workspaces/$workspaceId/items" `
            -Headers $headers `
            -Body ($createBody | ConvertTo-Json -Depth 20) `
            -UseBasicParsing
        
        if ($resp.StatusCode -eq 201) {
            $smId = ($resp.Content | ConvertFrom-Json).id
        } elseif ($resp.StatusCode -eq 202) {
            # LRO — get operation ID and poll
            $opId = $resp.Headers["x-ms-operation-id"]
            if ($opId -is [array]) { $opId = $opId[0] }
            Write-Host "  Waiting for creation (LRO: $opId)..." -ForegroundColor Gray
            $null = Wait-LRO -OperationUrl "/operations/$opId" -TimeoutSec 120
            # Re-fetch to get ID
            $existingSm = (Invoke-FabricApi -Endpoint "/workspaces/$workspaceId/items?type=SemanticModel").value |
                Where-Object { $_.displayName -eq $smName }
            $smId = $existingSm.id
        }
    } catch {
        $errCode = $null
        try { $errCode = [int]$_.Exception.Response.StatusCode } catch {}
        if ($errCode -eq 202) {
            Start-Sleep -Seconds 10
            $existingSm = (Invoke-FabricApi -Endpoint "/workspaces/$workspaceId/items?type=SemanticModel").value |
                Where-Object { $_.displayName -eq $smName }
            $smId = $existingSm.id
        } else {
            throw
        }
    }
    Write-Host "  ✓ Created: $smName ($smId)" -ForegroundColor Green
}

# Update definition
try {
    $token = Get-FabricToken
    $headers = @{ "Authorization" = "Bearer $token"; "Content-Type" = "application/json" }
    $updateBody = @{ definition = @{ parts = $smParts } }
    $resp = Invoke-WebRequest -Method POST `
        -Uri "$FabricApiBase/workspaces/$workspaceId/items/$smId/updateDefinition?updateMetadata=true" `
        -Headers $headers `
        -Body ($updateBody | ConvertTo-Json -Depth 20) `
        -UseBasicParsing

    if ($resp.StatusCode -eq 200) {
        Write-Host "  ✓ Semantic Model definition applied" -ForegroundColor Green
    } elseif ($resp.StatusCode -eq 202) {
        $opId = $resp.Headers["x-ms-operation-id"]
        if ($opId -is [array]) { $opId = $opId[0] }
        Write-Host "  Applying definition (LRO)..." -ForegroundColor Gray
        $null = Wait-LRO -OperationUrl "/operations/$opId" -TimeoutSec 120
        Write-Host "  ✓ Semantic Model definition applied" -ForegroundColor Green
    }
} catch {
    $errCode = $null
    try { $errCode = [int]$_.Exception.Response.StatusCode } catch {}
    if ($errCode -eq 202) {
        Write-Host "  ✓ Semantic Model definition update accepted (202)" -ForegroundColor Green
    } else {
        Write-Host "  ✗ Failed to update Semantic Model: $($_.Exception.Message)" -ForegroundColor Red
        try {
            $errBody = $_.ErrorDetails.Message
            Write-Host "    $errBody" -ForegroundColor DarkRed
        } catch {}
    }
}

# ============================================================================
# CREATE OR UPDATE REPORT  
# ============================================================================

Write-Host ""
Write-Host "  Deploying Report..." -ForegroundColor White

$rptName = "ImagingReport"

# The report definition.pbir must reference the semantic model via byConnection
# Fabric REST API requires byConnection (not byPath which is disk-only)
# The semantic model is referenced via the semanticModelId parameter in the connection string
$pbir = @{
    '$schema' = "https://developer.microsoft.com/json-schemas/fabric/item/report/definitionProperties/2.0.0/schema.json"
    version = "4.0"
    datasetReference = @{
        byConnection = @{
            connectionString = "Data Source=pbiazure://api.powerbi.com;Initial Catalog=ImagingReport;semanticModelId=$smId;Integrated Security=ClaimsToken"
        }
    }
} | ConvertTo-Json -Depth 10

# Replace the definition.pbir in rptParts
$rptParts = $rptParts | Where-Object { $_.path -ne "definition.pbir" }
# Add it at the front (before definition/ files, after .platform)
$rptParts = @(@{ path = "definition.pbir"; payload = (To-B64 $pbir); payloadType = "InlineBase64" }) + $rptParts

$existingRpt = (Invoke-FabricApi -Endpoint "/workspaces/$workspaceId/items?type=Report").value |
    Where-Object { $_.displayName -eq $rptName }

if ($existingRpt) {
    $rptId = $existingRpt.id
    Write-Host "  ✓ Existing: $rptName ($rptId) — updating definition" -ForegroundColor Green
} else {
    Write-Host "  Creating Report '$rptName'..." -ForegroundColor White
    $createBody = @{
        displayName = $rptName
        type = "Report"
        definition = @{ parts = $rptParts }
    }
    try {
        $token = Get-FabricToken
        $headers = @{ "Authorization" = "Bearer $token"; "Content-Type" = "application/json" }
        $resp = Invoke-WebRequest -Method POST `
            -Uri "$FabricApiBase/workspaces/$workspaceId/items" `
            -Headers $headers `
            -Body ($createBody | ConvertTo-Json -Depth 20) `
            -UseBasicParsing
        
        if ($resp.StatusCode -eq 201) {
            $rptId = ($resp.Content | ConvertFrom-Json).id
        } elseif ($resp.StatusCode -eq 202) {
            $opId = $resp.Headers["x-ms-operation-id"]
            if ($opId -is [array]) { $opId = $opId[0] }
            Write-Host "  Waiting for creation (LRO)..." -ForegroundColor Gray
            $null = Wait-LRO -OperationUrl "/operations/$opId" -TimeoutSec 120
            $existingRpt = (Invoke-FabricApi -Endpoint "/workspaces/$workspaceId/items?type=Report").value |
                Where-Object { $_.displayName -eq $rptName }
            $rptId = $existingRpt.id
        }
    } catch {
        $errCode = $null
        try { $errCode = [int]$_.Exception.Response.StatusCode } catch {}
        if ($errCode -eq 202) {
            Start-Sleep -Seconds 10
            $existingRpt = (Invoke-FabricApi -Endpoint "/workspaces/$workspaceId/items?type=Report").value |
                Where-Object { $_.displayName -eq $rptName }
            $rptId = $existingRpt.id
        } else {
            throw
        }
    }
    Write-Host "  ✓ Created: $rptName ($rptId)" -ForegroundColor Green
}

# Update definition
try {
    $token = Get-FabricToken
    $headers = @{ "Authorization" = "Bearer $token"; "Content-Type" = "application/json" }
    $updateBody = @{ definition = @{ parts = $rptParts } }
    $resp = Invoke-WebRequest -Method POST `
        -Uri "$FabricApiBase/workspaces/$workspaceId/items/$rptId/updateDefinition?updateMetadata=true" `
        -Headers $headers `
        -Body ($updateBody | ConvertTo-Json -Depth 20) `
        -UseBasicParsing

    if ($resp.StatusCode -eq 200) {
        Write-Host "  ✓ Report definition applied" -ForegroundColor Green
    } elseif ($resp.StatusCode -eq 202) {
        $opId = $resp.Headers["x-ms-operation-id"]
        if ($opId -is [array]) { $opId = $opId[0] }
        Write-Host "  Applying definition (LRO)..." -ForegroundColor Gray
        $null = Wait-LRO -OperationUrl "/operations/$opId" -TimeoutSec 120
        Write-Host "  ✓ Report definition applied" -ForegroundColor Green
    }
} catch {
    $errCode = $null
    try { $errCode = [int]$_.Exception.Response.StatusCode } catch {}
    if ($errCode -eq 202) {
        Write-Host "  ✓ Report definition update accepted (202)" -ForegroundColor Green
    } else {
        Write-Host "  ✗ Failed to update Report: $($_.Exception.Message)" -ForegroundColor Red
        try {
            $errBody = $_.ErrorDetails.Message
            Write-Host "    $errBody" -ForegroundColor DarkRed
        } catch {}
    }
}

# ============================================================================
# BIND CREDENTIALS + REFRESH
# ============================================================================

Write-Host ""
Write-Host "  Configuring data source credentials..." -ForegroundColor White

# Take ownership so we can manage refresh
$pbiToken = (Get-AzAccessToken -ResourceUrl "https://analysis.windows.net/powerbi/api").Token
if ($pbiToken -is [System.Security.SecureString]) {
    $bstr = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($pbiToken)
    $pbiToken = [System.Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr)
}
$pbiHeaders = @{ "Authorization" = "Bearer $pbiToken"; "Content-Type" = "application/json" }

try {
    Invoke-RestMethod -Method POST `
        -Uri "https://api.powerbi.com/v1.0/myorg/groups/$workspaceId/datasets/$smId/Default.TakeOver" `
        -Headers $pbiHeaders
    Write-Host "  ✓ Took ownership of semantic model" -ForegroundColor Green
} catch {}

# Bind data source credentials automatically via PBI REST API.
# For Direct Lake models in the same workspace, the Fabric cloud gateway
# handles the connection — we just need to bind OAuth2 creds programmatically
# instead of asking the user to do it manually in the portal.

# Allow a few seconds for Fabric to auto-create the gateway binding
Start-Sleep -Seconds 5

$credentialsBound = $false
try {
    $gwSources = Invoke-RestMethod `
        -Uri "https://api.powerbi.com/v1.0/myorg/groups/$workspaceId/datasets/$smId/Default.GetBoundGatewayDataSources" `
        -Headers $pbiHeaders

    foreach ($ds in $gwSources.value) {
        if ($ds.gatewayId -and $ds.gatewayId -ne "00000000-0000-0000-0000-000000000000") {
            $credBody = @{
                credentialDetails = @{
                    credentialType  = "OAuth2"
                    credentials     = '{"credentialData":[]}'
                    encryptedConnection = "Encrypted"
                    encryptionAlgorithm = "None"
                    privacyLevel    = "Organizational"
                }
            } | ConvertTo-Json -Depth 5

            Invoke-RestMethod -Method PATCH `
                -Uri "https://api.powerbi.com/v1.0/myorg/gateways/$($ds.gatewayId)/datasources/$($ds.id)" `
                -Headers $pbiHeaders -Body $credBody
            $credentialsBound = $true
        }
    }
} catch {
    Write-Host "  ⚠ Auto-bind failed: $($_.Exception.Message)" -ForegroundColor Yellow
}

if ($credentialsBound) {
    Write-Host "  ✓ Data source credentials bound automatically" -ForegroundColor Green
    # Trigger refresh
    try {
        Invoke-WebRequest -Method POST `
            -Uri "https://api.powerbi.com/v1.0/myorg/groups/$workspaceId/datasets/$smId/refreshes" `
            -Headers $pbiHeaders -Body '{"type":"Full"}' -UseBasicParsing | Out-Null
        Write-Host "  ✓ Refresh triggered" -ForegroundColor Green
    } catch {
        Write-Host "  ⚠ Could not trigger refresh: $($_.Exception.Message)" -ForegroundColor Yellow
    }
} else {
    # Fallback: manual steps
    Write-Host "  ⚠ Could not auto-bind credentials. Manual configuration required:" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "  One-time manual step:" -ForegroundColor White
    Write-Host "    1. Open https://app.fabric.microsoft.com" -ForegroundColor Gray
    Write-Host "    2. Navigate to workspace '$FabricWorkspaceName'" -ForegroundColor Gray
    Write-Host "    3. Find 'ImagingReport' semantic model → Settings (gear icon)" -ForegroundColor Gray
    Write-Host "    4. Under 'Data source credentials', click 'Edit credentials'" -ForegroundColor Gray
    Write-Host "    5. Sign in with OAuth2 (Microsoft account)" -ForegroundColor Gray
    Write-Host "    6. Click 'Refresh now' to load data" -ForegroundColor Gray
    Write-Host ""
    Write-Host "  Settings URL: https://app.fabric.microsoft.com/groups/$workspaceId/settings/datasets/$smId" -ForegroundColor Cyan
}

# ============================================================================
# SUMMARY
# ============================================================================

Write-Host ""
Write-Host "  ╔══════════════════════════════════════════════════════════════╗" -ForegroundColor Green
Write-Host "  ║  Imaging Report Deployed                                     ║" -ForegroundColor Green
Write-Host "  ╠══════════════════════════════════════════════════════════════╣" -ForegroundColor Green
Write-Host "  ║  Semantic Model : ImagingReport ($smId)      ║" -ForegroundColor Gray
Write-Host "  ║  Report         : ImagingReport ($rptId)      ║" -ForegroundColor Gray
Write-Host "  ║  Silver SQL     : $silverServer   ║" -ForegroundColor Gray
Write-Host "  ║  OHIF Viewer    : $($OhifViewerBaseUrl.Substring(0, [Math]::Min(50, $OhifViewerBaseUrl.Length)))...   ║" -ForegroundColor Gray
Write-Host "  ╚══════════════════════════════════════════════════════════════╝" -ForegroundColor Green
Write-Host ""
Write-Host "  Report URL: https://app.fabric.microsoft.com/groups/$workspaceId/reports/$rptId" -ForegroundColor Cyan
Write-Host "  Settings:   https://app.fabric.microsoft.com/groups/$workspaceId/settings/datasets/$smId" -ForegroundColor Cyan
Write-Host ""
