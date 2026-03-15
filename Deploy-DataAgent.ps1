<#
.SYNOPSIS
    Deploy a fully configured Fabric Data Agent via the REST API.

.DESCRIPTION
    Creates (or updates) a Fabric Data Agent with instructions, data sources,
    and few-shot examples using the Fabric Items REST API.

    Requires:
    - Azure CLI authenticated (az login)
    - Contributor role on the target Fabric workspace
    - The workspace must contain the silver and gold lakehouses/warehouses

.PARAMETER FabricWorkspaceName
    The display name of the Fabric workspace containing the HDS lakehouses.

.PARAMETER AgentName
    Display name for the Data Agent. Defaults to "HDS Multi-Layer Imaging Cohort Agent".

.PARAMETER SilverLakehouseName
    Display name of the silver lakehouse. Defaults to "healthcare1_msft_silver".

.PARAMETER GoldLakehouseName
    Display name of the gold lakehouse. Defaults to "healthcare1_msft_gold_omop".

.PARAMETER Force
    Force re-creation even if a Data Agent with the same name already exists (deletes and recreates).

.EXAMPLE
    .\Deploy-DataAgent.ps1 -FabricWorkspaceName "my-hds-workspace"
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [string]$FabricWorkspaceName,

    [string]$AgentName = "HDS Multi-Layer Imaging Cohort Agent",

    [string]$SilverLakehouseName = "healthcare1_msft_silver",

    [string]$GoldLakehouseName = "healthcare1_msft_gold_omop",

    [switch]$Force
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$fabricApiBase = "https://api.fabric.microsoft.com/v1"

# ── Helpers ──────────────────────────────────────────────────────────────

function Get-FabricToken {
    $tokenJson = az account get-access-token --resource https://api.fabric.microsoft.com 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to get Fabric access token. Run 'az login' first.`n$tokenJson"
    }
    return ($tokenJson | ConvertFrom-Json).accessToken
}

function ConvertTo-Base64 ([string]$Text) {
    [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($Text))
}

function Invoke-FabricApi {
    param(
        [string]$Method,
        [string]$Uri,
        [string]$Token,
        [object]$Body
    )
    $headers = @{
        Authorization  = "Bearer $Token"
        'Content-Type' = 'application/json'
    }
    $params = @{
        Method                = $Method
        Uri                   = $Uri
        Headers               = $headers
        ResponseHeadersVariable = 'respHeaders'
        StatusCodeVariable      = 'statusCode'
    }
    if ($Body) {
        $params.Body = ($Body | ConvertTo-Json -Depth 20)
    }
    try {
        $response = Invoke-RestMethod @params
    }
    catch {
        $status = $_.Exception.Response.StatusCode.value__
        $detail = $_.ErrorDetails.Message
        throw "Fabric API $Method $Uri returned $status : $detail"
    }

    # Handle Long Running Operations (202 Accepted)
    if ($statusCode -eq 202 -and $respHeaders.'x-ms-operation-id') {
        $operationId = $respHeaders.'x-ms-operation-id'[0]
        $retryAfter  = if ($respHeaders.'Retry-After') { [int]$respHeaders.'Retry-After'[0] } else { 5 }
        Write-Host "  Waiting for operation $operationId ..." -ForegroundColor Yellow
        $maxWait = 120
        $elapsed = 0
        while ($elapsed -lt $maxWait) {
            Start-Sleep -Seconds $retryAfter
            $elapsed += $retryAfter
            $opResult = Invoke-RestMethod -Method GET `
                -Uri "$fabricApiBase/operations/$operationId" `
                -Headers @{ Authorization = "Bearer $Token" }
            if ($opResult.status -eq 'Succeeded') {
                Write-Host "  Operation completed." -ForegroundColor Green
                # Try to get the result from the operation
                try {
                    $opResultDetail = Invoke-RestMethod -Method GET `
                        -Uri "$fabricApiBase/operations/$operationId/result" `
                        -Headers @{ Authorization = "Bearer $Token" }
                    return $opResultDetail
                }
                catch {
                    # Some LROs don't have /result — return the Location header content
                    return $opResult
                }
            }
            elseif ($opResult.status -eq 'Failed') {
                throw "Operation $operationId failed: $($opResult | ConvertTo-Json -Depth 5)"
            }
            Write-Host "  Still running ($elapsed s) ..." -ForegroundColor Yellow
        }
        throw "Operation $operationId timed out after $maxWait seconds."
    }

    return $response
}

# ── Resolve workspace name → ID ──────────────────────────────────────

Write-Host "Authenticating to Fabric API ..." -ForegroundColor Cyan
$token = Get-FabricToken
Write-Host "  Authenticated." -ForegroundColor Green

Write-Host "Resolving workspace '$FabricWorkspaceName' ..." -ForegroundColor Cyan
$workspacesUri = "$fabricApiBase/workspaces"
$workspaces = Invoke-FabricApi -Method GET -Uri $workspacesUri -Token $token
$workspace = $workspaces.value | Where-Object { $_.displayName -eq $FabricWorkspaceName } | Select-Object -First 1
if (-not $workspace) {
    throw "Workspace '$FabricWorkspaceName' not found. Check the name and your permissions."
}
$WorkspaceId = $workspace.id
Write-Host "  Workspace ID: $WorkspaceId" -ForegroundColor Green

# ── Resolve lakehouse names → artifact IDs ───────────────────────────

Write-Host "Looking up lakehouses in workspace ..." -ForegroundColor Cyan
$itemsUri = "$fabricApiBase/workspaces/$WorkspaceId/lakehouses"
$lakehouses = Invoke-FabricApi -Method GET -Uri $itemsUri -Token $token

$silverLakehouse = $lakehouses.value | Where-Object { $_.displayName -eq $SilverLakehouseName } | Select-Object -First 1
if (-not $silverLakehouse) {
    throw "Silver lakehouse '$SilverLakehouseName' not found in workspace '$FabricWorkspaceName'."
}
$SilverArtifactId = $silverLakehouse.id
Write-Host "  Silver: $SilverLakehouseName → $SilverArtifactId" -ForegroundColor Green

$goldLakehouse = $lakehouses.value | Where-Object { $_.displayName -eq $GoldLakehouseName } | Select-Object -First 1
if (-not $goldLakehouse) {
    throw "Gold lakehouse '$GoldLakehouseName' not found in workspace '$FabricWorkspaceName'."
}
$GoldArtifactId = $goldLakehouse.id
Write-Host "  Gold:   $GoldLakehouseName → $GoldArtifactId" -ForegroundColor Green

# ── Extract instructions from data-agent-instructions.md ─────────────

Write-Host "Reading data-agent-instructions.md ..." -ForegroundColor Cyan
$mdPath = Join-Path $scriptDir "data-agent-instructions.md"
if (-not (Test-Path $mdPath)) {
    throw "data-agent-instructions.md not found at $mdPath"
}
$mdContent = Get-Content $mdPath -Raw
# Extract the text between the first ``` and the next ```
if ($mdContent -match '(?s)```\r?\n(.*?)\r?\n```') {
    $aiInstructions = $Matches[1]
}
else {
    throw "Could not extract instruction block from data-agent-instructions.md (expected content between triple backticks)."
}
Write-Host "  Extracted $($aiInstructions.Length) characters of instructions." -ForegroundColor Green

# ── Load few-shot files ──────────────────────────────────────────────

Write-Host "Loading few-shot examples ..." -ForegroundColor Cyan
$silverFewshotsPath = Join-Path $scriptDir "fewshots-silver-fhir.json"
$goldFewshotsPath   = Join-Path $scriptDir "fewshots-gold-omop.json"

if (-not (Test-Path $silverFewshotsPath)) { throw "fewshots-silver-fhir.json not found." }
if (-not (Test-Path $goldFewshotsPath))   { throw "fewshots-gold-omop.json not found." }

$silverFewshotsJson = Get-Content $silverFewshotsPath -Raw
$goldFewshotsJson   = Get-Content $goldFewshotsPath -Raw

$silverCount = ($silverFewshotsJson | ConvertFrom-Json).fewShots.Count
$goldCount   = ($goldFewshotsJson   | ConvertFrom-Json).fewShots.Count
Write-Host "  Silver: $silverCount examples, Gold: $goldCount examples" -ForegroundColor Green

# ── Build definition parts ───────────────────────────────────────────

Write-Host "Building Data Agent definition ..." -ForegroundColor Cyan

# 1. Top-level data_agent.json
$dataAgentConfig = @{ '$schema' = "2.1.0" } | ConvertTo-Json

# 2. Stage config (instructions)
$stageConfig = @{
    '$schema'      = "1.0.0"
    aiInstructions = $aiInstructions
} | ConvertTo-Json -Depth 5

# 3. Silver data source — selected tables from FHIR R4
$silverTables = @(
    'AllergyIntolerance', 'Condition', 'DiagnosticReport', 'Encounter',
    'ImagingMetastore', 'ImagingStudy', 'Location', 'MedicationRequest',
    'Observation', 'Organization', 'Patient', 'Practitioner', 'Procedure'
)
$silverElements = @(
    @{
        display_name = 'dbo'
        type         = 'lakehouse_tables.schema'
        is_selected  = $true
        children     = @($silverTables | ForEach-Object {
            @{
                display_name = $_
                type         = 'lakehouse_tables.table'
                is_selected  = $true
            }
        })
    }
)
$silverDatasource = @{
    '$schema'    = "1.0.0"
    artifactId   = $SilverArtifactId
    workspaceId  = $WorkspaceId
    displayName  = $SilverLakehouseName
    type         = "lakehouse_tables"
    userDescription = "FHIR R4 silver layer — patient identity, conditions, imaging, medications, encounters, procedures, allergies, observations, reports"
    dataSourceInstructions = "Use this source for any query that requires patient names or individual clinical data. Contains Patient, Condition, ImagingStudy, MedicationRequest, AllergyIntolerance, Encounter, Procedure, Observation, DiagnosticReport."
    elements     = $silverElements
} | ConvertTo-Json -Depth 10

# 4. Gold data source — selected tables from OMOP CDM v5.4
$goldTables = @(
    'care_site', 'concept', 'concept_ancestor', 'concept_relationship',
    'condition_era', 'condition_occurrence', 'death',
    'drug_era', 'drug_exposure',
    'image_occurrence', 'location', 'measurement',
    'observation', 'person', 'procedure_occurrence',
    'provider', 'relationship',
    'visit_detail', 'visit_occurrence'
)
$goldElements = @(
    @{
        display_name = 'dbo'
        type         = 'lakehouse_tables.schema'
        is_selected  = $true
        children     = @($goldTables | ForEach-Object {
            @{
                display_name = $_
                type         = 'lakehouse_tables.table'
                is_selected  = $true
            }
        })
    }
)
$goldDatasource = @{
    '$schema'    = "1.0.0"
    artifactId   = $GoldArtifactId
    workspaceId  = $WorkspaceId
    displayName  = $GoldLakehouseName
    type         = "lakehouse_tables"
    userDescription = "OMOP CDM v5.4 gold layer — aggregate analytics, demographics (race/ethnicity), conditions, drugs, imaging, visits, measurements. No patient names."
    dataSourceInstructions = "Use this source for aggregate counts, modality breakdowns, demographic distributions, condition co-occurrences, and mortality analysis. Never for patient names."
    elements     = $goldElements
} | ConvertTo-Json -Depth 10

# Determine folder paths using dataSourceType-displayName convention
$silverFolder = "lakehouse_tables-$SilverLakehouseName"
$goldFolder   = "lakehouse_tables-$GoldLakehouseName"

$definition = @{
    parts = @(
        @{
            path        = "Files/Config/data_agent.json"
            payload     = (ConvertTo-Base64 $dataAgentConfig)
            payloadType = "InlineBase64"
        },
        @{
            path        = "Files/Config/draft/stage_config.json"
            payload     = (ConvertTo-Base64 $stageConfig)
            payloadType = "InlineBase64"
        },
        @{
            path        = "Files/Config/draft/$silverFolder/datasource.json"
            payload     = (ConvertTo-Base64 $silverDatasource)
            payloadType = "InlineBase64"
        },
        @{
            path        = "Files/Config/draft/$silverFolder/fewshots.json"
            payload     = (ConvertTo-Base64 $silverFewshotsJson)
            payloadType = "InlineBase64"
        },
        @{
            path        = "Files/Config/draft/$goldFolder/datasource.json"
            payload     = (ConvertTo-Base64 $goldDatasource)
            payloadType = "InlineBase64"
        },
        @{
            path        = "Files/Config/draft/$goldFolder/fewshots.json"
            payload     = (ConvertTo-Base64 $goldFewshotsJson)
            payloadType = "InlineBase64"
        }
    )
}

# ── Check for existing Data Agent ────────────────────────────────────

Write-Host "Checking for existing Data Agent '$AgentName' ..." -ForegroundColor Cyan
$listUri = "$fabricApiBase/workspaces/$WorkspaceId/DataAgents"
$existing = $null
try {
    $agents = Invoke-FabricApi -Method GET -Uri $listUri -Token $token
    $existing = $agents.value | Where-Object { $_.displayName -eq $AgentName } | Select-Object -First 1
}
catch {
    Write-Host "  Could not list existing agents (may be empty workspace): $_" -ForegroundColor Yellow
}

if ($existing) {
    if ($Force) {
        Write-Host "  Found existing agent $($existing.id) — deleting (Force mode) ..." -ForegroundColor Yellow
        Invoke-FabricApi -Method DELETE -Uri "$listUri/$($existing.id)" -Token $token
        Write-Host "  Deleted. Waiting for name to become available ..." -ForegroundColor Yellow
        # Fabric needs time to release the display name after deletion
        $nameReady = $false
        for ($wait = 0; $wait -lt 60; $wait += 10) {
            Start-Sleep -Seconds 10
            try {
                $agents = Invoke-FabricApi -Method GET -Uri $listUri -Token $token
                $still = $agents.value | Where-Object { $_.displayName -eq $AgentName }
                if (-not $still) { $nameReady = $true; break }
            }
            catch { }
            Write-Host "  Still waiting ($($wait + 10)s) ..." -ForegroundColor Yellow
        }
        if (-not $nameReady) {
            Write-Host "  Name may still be reserved. Proceeding anyway ..." -ForegroundColor Yellow
        }
        $existing = $null
    }
    else {
        Write-Host "  Found existing agent $($existing.id) — updating definition ..." -ForegroundColor Yellow
        $updateUri = "$listUri/$($existing.id)/updateDefinition"
        Invoke-FabricApi -Method POST -Uri $updateUri -Token $token -Body @{ definition = $definition }
        Write-Host ""
        Write-Host "Data Agent updated successfully!" -ForegroundColor Green
        Write-Host "  Agent ID:    $($existing.id)" -ForegroundColor White
        Write-Host "  Workspace:   $FabricWorkspaceName ($WorkspaceId)" -ForegroundColor White
        Write-Host ""
        Write-Host "Next steps:" -ForegroundColor Cyan
        Write-Host "  1. Open the Data Agent in the Fabric portal to verify configuration" -ForegroundColor White
        Write-Host "  2. Test with sample questions from data-agent-instructions.md" -ForegroundColor White
        Write-Host "  3. Publish the agent when ready" -ForegroundColor White
        return
    }
}

# ── Create new Data Agent ────────────────────────────────────────────

Write-Host "Creating Data Agent '$AgentName' ..." -ForegroundColor Cyan
$createBody = @{
    displayName = $AgentName
    description = "Clinical cohort builder using FHIR silver layer (patient names + clinical data) and OMOP gold layer (analytics only, no names)."
    definition  = $definition
}

# Retry create in case the display name isn't released yet after delete
$result = $null
$maxRetries = 6
for ($attempt = 1; $attempt -le $maxRetries; $attempt++) {
    try {
        $result = Invoke-FabricApi -Method POST -Uri $listUri -Token $token -Body $createBody
        break
    }
    catch {
        if ($_ -match 'ItemDisplayNameNotAvailableYet' -and $attempt -lt $maxRetries) {
            Write-Host "  Name not available yet, retrying in 15s (attempt $attempt/$maxRetries) ..." -ForegroundColor Yellow
            Start-Sleep -Seconds 15
        }
        else {
            throw
        }
    }
}

# The result may come from LRO polling or direct 201 response
$agentId = $result.id
if (-not $agentId) {
    # LRO completed but result didn't include ID — look up by name
    Write-Host "  Retrieving agent ID ..." -ForegroundColor Yellow
    $agents = Invoke-FabricApi -Method GET -Uri $listUri -Token $token
    $created = $agents.value | Where-Object { $_.displayName -eq $AgentName } | Select-Object -First 1
    $agentId = $created.id
}
if (-not $agentId) {
    Write-Host "Data Agent was created but could not retrieve the agent ID. Check the Fabric portal." -ForegroundColor Yellow
    return
}
Write-Host ""
Write-Host "Data Agent created successfully!" -ForegroundColor Green
Write-Host "  Agent ID:    $agentId" -ForegroundColor White
Write-Host "  Workspace:   $FabricWorkspaceName ($WorkspaceId)" -ForegroundColor White
Write-Host "  Name:        $AgentName" -ForegroundColor White
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Cyan
Write-Host "  1. Open the Data Agent in the Fabric portal to verify configuration" -ForegroundColor White
Write-Host "  2. Select which tables to expose in each data source" -ForegroundColor White
Write-Host "  3. Test with sample questions from data-agent-instructions.md" -ForegroundColor White
Write-Host "  4. Publish the agent when ready" -ForegroundColor White
