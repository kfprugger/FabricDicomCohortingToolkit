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

    [string]$WorkspaceId = "",

    [string]$AgentName = "HDS Multi-Layer Imaging Cohort Agent",

    [string]$SilverLakehouseName = "healthcare1_msft_silver",

    [string]$GoldLakehouseName = "healthcare1_msft_gold_omop",

    [string]$TenantId = "",

    [switch]$Force
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$fabricApiBase = "https://api.fabric.microsoft.com/v1"
. (Join-Path $scriptDir 'data-agent-selection.ps1')

# ── Helpers ──────────────────────────────────────────────────────────────

function Get-FabricToken {
    if ($TenantId -and (Get-Command Get-AzAccessToken -ErrorAction SilentlyContinue)) {
        $nativeToken = Get-AzAccessToken -TenantId $TenantId -ResourceUrl 'https://api.fabric.microsoft.com'
        if ([string]$nativeToken.TenantId -ne $TenantId) { throw 'Fabric token tenant does not match -TenantId.' }
        if ($nativeToken.Token -is [Security.SecureString]) { return [Net.NetworkCredential]::new('', $nativeToken.Token).Password }
        return [string]$nativeToken.Token
    }
    $arguments = @('account', 'get-access-token', '--resource', 'https://api.fabric.microsoft.com', '--output', 'json')
    if ($TenantId) { $arguments += @('--tenant', $TenantId) }
    $tokenJson = & az @arguments
    if ($LASTEXITCODE -ne 0) { throw "Failed to get Fabric access token. Sign in to the intended tenant first." }
    $credential = ($tokenJson -join "`n") | ConvertFrom-Json
    if ($TenantId -and [string]$credential.tenant -ne $TenantId) { throw 'Fabric token tenant does not match -TenantId.' }
    return $credential.accessToken
}

function ConvertTo-Base64 ([string]$Text) {
    [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($Text))
}

function Invoke-FabricApi {
    param(
        [string]$Method,
        [string]$Uri,
        [string]$Token,
        [object]$Body,
        [ValidateRange(1,3600)][int]$MaxWaitSeconds = 300
    )
    $headers = @{
        Authorization = "Bearer $Token"
        'Content-Type' = 'application/json'
        'x-ms-fabric-skill' = 'e2e-medallion-architecture'
    }
    $params = @{
        Method = $Method
        Uri = $Uri
        Headers = $headers
        ResponseHeadersVariable = 'respHeaders'
        StatusCodeVariable = 'statusCode'
        TimeoutSec = [Math]::Min(100, $MaxWaitSeconds)
    }
    if ($Body) { $params.Body = ($Body | ConvertTo-Json -Depth 20) }
    # A lost mutation response is ambiguous: never reissue the original request.
    $response = Invoke-RestMethod @params
    if ($statusCode -ne 202) { return $response }
    if (-not $respHeaders.'x-ms-operation-id') {
        throw "Fabric API $Method $Uri returned HTTP 202 without x-ms-operation-id; operation cannot be verified."
    }
    $operationId = $respHeaders.'x-ms-operation-id'[0]
    $retryAfter = if ($respHeaders.'Retry-After') { [Math]::Max(1, [int]$respHeaders.'Retry-After'[0]) } else { 5 }
    $watch = [System.Diagnostics.Stopwatch]::StartNew()
    $succeeded = $false
    while ($watch.Elapsed.TotalSeconds -lt $MaxWaitSeconds) {
        $remaining = $MaxWaitSeconds - $watch.Elapsed.TotalSeconds
        Start-Sleep -Milliseconds ([int]([Math]::Min($retryAfter, $remaining) * 1000))
        $remaining = $MaxWaitSeconds - $watch.Elapsed.TotalSeconds
        if ($remaining -lt 1) { break }
        $operationPath = "$fabricApiBase/operations/$operationId"
        if ($succeeded) { $operationPath += '/result' }
        try {
            $opResult = Invoke-RestMethod -Method GET -Uri $operationPath -Headers $headers `
                -TimeoutSec ([int][Math]::Floor([Math]::Min(30, $remaining))) -ErrorAction Stop
        } catch {
            $errCode = $null
            try { $errCode = [int]$_.Exception.Response.StatusCode } catch {}
            if ($succeeded -and $errCode -eq 404) { return $completedOperation }
            $networkException = $_.Exception
            $transport = $false
            $tlsFailure = $false
            while ($networkException) {
                if ($networkException -is [System.Security.Authentication.AuthenticationException]) { $tlsFailure = $true }
                if ($networkException -is [System.IO.IOException] -or $networkException -is [System.Net.Sockets.SocketException] -or $networkException -is [System.TimeoutException] -or $networkException -is [System.OperationCanceledException]) { $transport = $true }
                $networkException = $networkException.InnerException
            }
            if ($errCode -in @(408,429,500,502,503,504) -or (-not $errCode -and $transport -and -not $tlsFailure)) {
                Write-Host "  Read-only operation poll transient error; retrying within deadline." -ForegroundColor Yellow
                continue
            }
            # Authentication, TLS validation and inbound-policy denials are not softened.
            throw
        }
        if ($watch.Elapsed.TotalSeconds -ge $MaxWaitSeconds) { break }
        if ($succeeded) { return $opResult }
        if ($opResult.status -eq 'Succeeded') {
            $completedOperation = $opResult
            $succeeded = $true
        } elseif ($opResult.status -in @('Failed','Cancelled','Canceled')) {
            throw "Operation $operationId ended with status '$($opResult.status)': $($opResult | ConvertTo-Json -Depth 5)"
        }
    }
    throw "Operation $operationId timed out after $MaxWaitSeconds seconds; success was not verified."
}

function Complete-DataAgentConfiguration([string]$DataAgentId) {
    $nativeApi = { param($method, $endpoint, $body) Invoke-FabricApi -Method $method -Uri "$fabricApiBase$endpoint" -Token $token -Body $body }
    $contracts = @(
        @{ WorkspaceId = $WorkspaceId; DataAgentId = $DataAgentId; DatasourceId = $SilverArtifactId; Tables = $silverTables; InvokeApi = $nativeApi },
        @{ WorkspaceId = $WorkspaceId; DataAgentId = $DataAgentId; DatasourceId = $GoldArtifactId; Tables = $goldTables; InvokeApi = $nativeApi }
    )
    if ($kqlDb) {
        $contracts += @{ WorkspaceId = $WorkspaceId; DataAgentId = $DataAgentId; DatasourceId = $kqlDb.id; Tables = @('agent_imaging_summary'); Functions = @('agent_ImagingModalityCounts', 'agent_ImagingStatusCounts', 'agent_ImagingTotals'); InvokeApi = $nativeApi }
    }
    foreach ($contract in $contracts) { Set-DataAgentNativeSchemaSelection @contract }
    $null = Invoke-FabricApi -Method POST -Uri "$fabricApiBase/workspaces/$WorkspaceId/dataAgents/$DataAgentId/staging/publish" -Token $token -Body @{ publishedDescription = "$AgentName - validated native schema selections" }
    foreach ($contract in $contracts) {
        Set-DataAgentNativeSchemaSelection @contract -VerifyOnly
        Set-DataAgentNativeSchemaSelection @contract -VerifyOnly -Published
    }
}

# ── Resolve workspace name → ID ──────────────────────────────────────

Write-Host "Authenticating to Fabric API ..." -ForegroundColor Cyan
$token = Get-FabricToken
Write-Host "  Authenticated." -ForegroundColor Green

if ([string]::IsNullOrWhiteSpace($WorkspaceId)) {
    Write-Host "Resolving workspace '$FabricWorkspaceName' ..." -ForegroundColor Cyan
    $workspacesUri = "$fabricApiBase/workspaces"
    $workspaces = Invoke-FabricApi -Method GET -Uri $workspacesUri -Token $token
    $workspace = $workspaces.value | Where-Object { $_.displayName -eq $FabricWorkspaceName } | Select-Object -First 1
    if (-not $workspace) {
        throw "Workspace '$FabricWorkspaceName' not found. Check the name and your permissions."
    }
    $WorkspaceId = $workspace.id
} else {
    Write-Host "Using workspace '$FabricWorkspaceName' ($WorkspaceId) ..." -ForegroundColor Cyan
}
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

# Optional deterministic KQL aggregate source supplied by hls-data-accelerator Phase 7.
$kqlDb = $null
try {
    $kqlDatabases = Invoke-FabricApi -Method GET -Uri "$fabricApiBase/workspaces/$WorkspaceId/kqlDatabases" -Token $token
    $kqlDb = $kqlDatabases.value | Where-Object { $_.displayName -eq 'MasimoEventhouse' } | Select-Object -First 1
    if ($kqlDb) { Write-Host "  KQL:    $($kqlDb.displayName) → $($kqlDb.id)" -ForegroundColor Green }
} catch {
    Write-Host "  ⚠ MasimoEventhouse was not available; deploying Lakehouse-only imaging grounding." -ForegroundColor Yellow
}

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
if ($kqlDb) {
    $aiInstructions += @"

DETERMINISTIC IMAGING AGGREGATES (MANDATORY):
- Use agent_ImagingModalityCounts() for modality counts, agent_ImagingStatusCounts() for status totals, and agent_ImagingTotals() for overall study and represented-patient totals.
- Never sum total_studies or total_patients across rows; those values repeat on every modality row.
- Never use modality_string, nested JSON, or Gold imaging tables for these aggregate questions.
- Identify the source as agent_imaging_summary derived from Silver ImagingStudy.
"@
}

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
$dataAgentConfig = @{ '$schema' = "https://developer.microsoft.com/json-schemas/fabric/item/dataAgent/definition/dataAgent/2.1.0/schema.json" } | ConvertTo-Json

# 2. Stage config (instructions)
$stageConfig = @{
    '$schema'      = "https://developer.microsoft.com/json-schemas/fabric/item/dataAgent/definition/stageConfiguration/1.0.0/schema.json"
    aiInstructions = $aiInstructions
} | ConvertTo-Json -Depth 5

# 3. Silver data source — selected tables from FHIR R4
$silverTables = @(
    'AllergyIntolerance', 'Condition', 'DiagnosticReport', 'Encounter',
    'ImagingMetastore', 'ImagingStudy', 'Location', 'MedicationRequest',
    'Observation', 'Organization', 'Patient', 'Practitioner', 'Procedure'
)
$silverDatasource = @{
    '$schema'    = "https://developer.microsoft.com/json-schemas/fabric/item/dataAgent/definition/dataSource/1.0.0/schema.json"
    artifactId   = $SilverArtifactId
    workspaceId  = $WorkspaceId
    displayName  = $SilverLakehouseName
    type         = "lakehouse_tables"
    userDescription = "FHIR R4 silver layer — patient identity, conditions, imaging, medications, encounters, procedures, allergies, observations, reports"
    dataSourceInstructions = "Use this source for any query that requires patient names or individual clinical data. Contains Patient, Condition, ImagingStudy, MedicationRequest, AllergyIntolerance, Encounter, Procedure, Observation, DiagnosticReport."
    elements     = @()
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
$goldDatasource = @{
    '$schema'    = "https://developer.microsoft.com/json-schemas/fabric/item/dataAgent/definition/dataSource/1.0.0/schema.json"
    artifactId   = $GoldArtifactId
    workspaceId  = $WorkspaceId
    displayName  = $GoldLakehouseName
    type         = "lakehouse_tables"
    userDescription = "OMOP CDM v5.4 gold layer — aggregate analytics, demographics (race/ethnicity), conditions, drugs, imaging, visits, measurements. No patient names."
    dataSourceInstructions = "Use this source for aggregate counts, modality breakdowns, demographic distributions, condition co-occurrences, and mortality analysis. Never for patient names."
    elements     = @()
} | ConvertTo-Json -Depth 10

$kqlDatasource = $null
$kqlFewshotsJson = $null
if ($kqlDb) {
    $kqlDatasource = @{
        '$schema' = 'https://developer.microsoft.com/json-schemas/fabric/item/dataAgent/definition/dataSource/1.0.0/schema.json'
        artifactId = $kqlDb.id
        workspaceId = $WorkspaceId
        displayName = $kqlDb.displayName
        type = 'kusto'
        userDescription = 'Deterministic imaging aggregates derived from Silver ImagingStudy'
        dataSourceInstructions = 'Use agent_ImagingModalityCounts() for modality counts, agent_ImagingStatusCounts() for status counts, and agent_ImagingTotals() for overall study and patient totals. Never sum repeated total columns.'
        elements = @()
    } | ConvertTo-Json -Depth 20
    $kqlFewshotsJson = @{
        '$schema' = 'https://developer.microsoft.com/json-schemas/fabric/item/dataAgent/definition/fewShots/1.0.0/schema.json'
        fewShots = @(
            @{ id = [guid]::NewGuid().ToString(); question = 'Count imaging studies by modality from the connected imaging data. Include each count and the data source.'; query = 'agent_ImagingModalityCounts()' },
            @{ id = [guid]::NewGuid().ToString(); question = 'How many imaging studies are available, and how many distinct patients do they represent? Include the data source.'; query = 'agent_ImagingTotals()' },
            @{ id = [guid]::NewGuid().ToString(); question = 'Summarize imaging studies by status and modality, returning only aggregate counts.'; query = 'agent_ImagingModalityCounts() | project modality_code, study_count' },
            @{ id = [guid]::NewGuid().ToString(); question = 'How many CT imaging studies are present? Include the source.'; query = 'agent_ImagingModalityCounts() | where modality_code == "CT" | project modality_code, study_count, data_source="agent_imaging_summary derived from Silver ImagingStudy"' },
            @{ id = [guid]::NewGuid().ToString(); question = 'Return CR and DX study counts only.'; query = 'agent_ImagingModalityCounts() | where modality_code in ("CR", "DX") | project modality_code, study_count | order by modality_code asc' },
            @{ id = [guid]::NewGuid().ToString(); question = 'What status values exist for imaging studies and how many studies are in each?'; query = 'agent_ImagingStatusCounts()' },
            @{ id = [guid]::NewGuid().ToString(); question = 'Return the total imaging studies and total represented patients in one line.'; query = 'agent_ImagingTotals() | project total_studies, total_patients' },
            @{ id = [guid]::NewGuid().ToString(); question = 'State the imaging aggregate source and its refresh timestamp.'; query = 'agent_ImagingTotals() | project data_source, refreshed_at, scenario_source' }
        )
    } | ConvertTo-Json -Depth 20
}

# Determine folder paths using dataSourceType-displayName convention
$silverFolder = "lakehouse-tables-$SilverLakehouseName"
$goldFolder   = "lakehouse-tables-$GoldLakehouseName"

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
if ($kqlDb) {
    $kqlFolder = "kusto-$($kqlDb.displayName)"
    $definition.parts += @(
        @{ path = "Files/Config/draft/$kqlFolder/datasource.json"; payload = (ConvertTo-Base64 $kqlDatasource); payloadType = 'InlineBase64' },
        @{ path = "Files/Config/draft/$kqlFolder/fewshots.json"; payload = (ConvertTo-Base64 $kqlFewshotsJson); payloadType = 'InlineBase64' }
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
        $updatedAgentId = $existing.id
        if (-not $updatedAgentId) {
            throw "Data Agent '$AgentName' update completed but the agent ID could not be resolved."
        }
        Write-Host "  Found existing agent $updatedAgentId — updating definition ..." -ForegroundColor Yellow
        $updateUri = "$listUri/$updatedAgentId/updateDefinition"
        $current = Invoke-FabricApi -Method POST -Uri "$listUri/$updatedAgentId/getDefinition" -Token $token -Body @{}
        $replacedArtifacts = @($SilverArtifactId, $GoldArtifactId)
        if ($kqlDb) { $replacedArtifacts += $kqlDb.id }
        foreach ($part in @($current.definition.parts | Where-Object { $_.path.StartsWith('Files/Config/draft/') -and $_.path.EndsWith('/datasource.json') })) {
            $source = [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($part.payload)) | ConvertFrom-Json -Depth 100
            if ($source.artifactId -in $replacedArtifacts) { continue }
            $folder = $part.path.Substring(0, $part.path.LastIndexOf('/') + 1)
            $definition.parts += @($current.definition.parts | Where-Object { $_.path.StartsWith($folder) })
        }
        Invoke-FabricApi -Method POST -Uri $updateUri -Token $token -Body @{ definition = $definition }
        $updatedAgents = Invoke-FabricApi -Method GET -Uri $listUri -Token $token
        $updatedAgent = $updatedAgents.value | Where-Object { $_.displayName -eq $AgentName } | Select-Object -First 1
        $updatedAgentId = if ($updatedAgent) { $updatedAgent.id } else { $null }
        if (-not $updatedAgentId) {
            throw "Data Agent '$AgentName' update completed but the agent ID could not be resolved."
        }
        Complete-DataAgentConfiguration -DataAgentId $updatedAgentId
        Write-Host ""
        Write-Host "Data Agent updated successfully!" -ForegroundColor Green
        Write-Host "  Agent ID:    $updatedAgentId" -ForegroundColor White
        Write-Host "  Workspace:   $FabricWorkspaceName ($WorkspaceId)" -ForegroundColor White
        Write-Host ""
        return
    }
}

# ── Create new Data Agent ────────────────────────────────────────────

Write-Host "Creating Data Agent '$AgentName' ..." -ForegroundColor Cyan
$createBody = @{
    displayName = $AgentName
    description = "Imaging cohort agent using FHIR Silver patient/clinical/imaging context and OMOP Gold analytics for DICOM cohort discovery, clinical criteria, and reporting workflows."
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
    $agentId = if ($created) { $created.id } else { $null }
}
if (-not $agentId) {
    throw "Data Agent '$AgentName' create completed but the agent ID could not be resolved."
}
Complete-DataAgentConfiguration -DataAgentId $agentId
Write-Host ""
Write-Host "Data Agent created successfully!" -ForegroundColor Green
Write-Host "  Agent ID:    $agentId" -ForegroundColor White
Write-Host "  Workspace:   $FabricWorkspaceName ($WorkspaceId)" -ForegroundColor White
Write-Host "  Name:        $AgentName" -ForegroundColor White
Write-Host ""
