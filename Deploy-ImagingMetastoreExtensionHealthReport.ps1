<#
.SYNOPSIS
    Deploys the Imaging Metastore Extension Health semantic model and report.
#>
param(
    [string]$FabricWorkspaceName = "FUJIV_Fabric_Test",
    [string]$WorkspaceId,
    [string]$AdminLakehouseName = "healthcare1_msft_admin",
    [string]$ReportSourcePath = ""
)

$ErrorActionPreference = "Stop"
$FabricApiBase = "https://api.fabric.microsoft.com/v1"
$SemanticModelName = "Imaging Metastore Extension Health"
$ReportName = "Imaging Metastore Extension Health"

if (-not $ReportSourcePath) { $ReportSourcePath = Split-Path -Parent $MyInvocation.MyCommand.Path }

function Get-FabricToken {
    $token = az account get-access-token --resource "https://api.fabric.microsoft.com" --query accessToken -o tsv
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($token)) { throw "Failed to get Fabric access token. Run az login first." }
    return $token
}

function Invoke-FabricApi {
    param([string]$Method = "GET", [string]$Endpoint, [object]$Body = $null)
    $headers = @{ Authorization = "Bearer $(Get-FabricToken)"; "Content-Type" = "application/json" }
    $params = @{ Method = $Method; Uri = "$FabricApiBase$Endpoint"; Headers = $headers }
    if ($null -ne $Body -and $Method -ne "GET") { $params.Body = ($Body | ConvertTo-Json -Depth 30) }
    Invoke-RestMethod @params
}

function Invoke-FabricWebRequest {
    param([string]$Method, [string]$Endpoint, [object]$Body)
    $headers = @{ Authorization = "Bearer $(Get-FabricToken)"; "Content-Type" = "application/json" }
    Invoke-WebRequest -Method $Method -Uri "$FabricApiBase$Endpoint" -Headers $headers -Body ($Body | ConvertTo-Json -Depth 30) -UseBasicParsing
}

function Wait-FabricOperation {
    param([string]$OperationId, [int]$TimeoutSeconds = 300)
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

function To-B64([string]$Text) { [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($Text)) }

function Add-Part {
    param([System.Collections.ArrayList]$Parts, [string]$Path, [string]$Content)
    [void]$Parts.Add(@{ path = $Path; payload = (To-B64 $Content); payloadType = "InlineBase64" })
}

function Apply-Definition {
    param([string]$ItemId, [array]$Parts)
    $body = @{ definition = @{ parts = $Parts } }
    $resp = Invoke-FabricWebRequest -Method POST -Endpoint "/workspaces/$WorkspaceId/items/$ItemId/updateDefinition?updateMetadata=true" -Body $body
    if ($resp.StatusCode -eq 202) {
        $opId = $resp.Headers["x-ms-operation-id"]
        if ($opId -is [array]) { $opId = $opId[0] }
        Wait-FabricOperation -OperationId $opId
    } elseif ($resp.StatusCode -notin @(200, 201)) {
        throw "updateDefinition returned HTTP $($resp.StatusCode)"
    }
}

Write-Host "Deploying $ReportName" -ForegroundColor Cyan

if (-not $WorkspaceId) {
    $workspace = (Invoke-FabricApi -Endpoint "/workspaces").value | Where-Object { $_.displayName -eq $FabricWorkspaceName } | Select-Object -First 1
    if (-not $workspace) { throw "Workspace '$FabricWorkspaceName' not found." }
    $WorkspaceId = $workspace.id
}
Write-Host "  Workspace: $FabricWorkspaceName ($WorkspaceId)" -ForegroundColor Green

$lakehouses = (Invoke-FabricApi -Endpoint "/workspaces/$WorkspaceId/lakehouses").value
$adminLakehouse = $lakehouses | Where-Object { $_.displayName -eq $AdminLakehouseName } | Select-Object -First 1
if (-not $adminLakehouse) { throw "Admin lakehouse '$AdminLakehouseName' not found." }
$adminDetail = Invoke-FabricApi -Endpoint "/workspaces/$WorkspaceId/lakehouses/$($adminLakehouse.id)"
$adminServer = $adminDetail.properties.sqlEndpointProperties.connectionString
$adminDbName = $adminLakehouse.displayName
if (-not $adminServer) { throw "Could not resolve Admin lakehouse SQL endpoint." }
Write-Host "  Admin SQL: $adminServer / $adminDbName" -ForegroundColor Green

$smDir = Join-Path $ReportSourcePath "ImagingMetastoreExtensionHealth.SemanticModel/definition"
$rptDir = Join-Path $ReportSourcePath "ImagingMetastoreExtensionHealth.Report/definition"
if (-not (Test-Path $smDir)) { throw "Semantic model source not found: $smDir" }
if (-not (Test-Path $rptDir)) { throw "Report source not found: $rptDir" }

$smParts = [System.Collections.ArrayList]::new()
$smPlatform = @{ '$schema' = "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json"; metadata = @{ type = "SemanticModel"; displayName = $SemanticModelName }; config = @{ version = "2.0"; logicalId = [guid]::NewGuid().ToString() } } | ConvertTo-Json -Depth 10
Add-Part -Parts $smParts -Path ".platform" -Content $smPlatform
Add-Part -Parts $smParts -Path "definition.pbism" -Content (Get-Content (Join-Path $ReportSourcePath "ImagingMetastoreExtensionHealth.SemanticModel/definition.pbism") -Raw)
Get-ChildItem $smDir -Recurse -File | ForEach-Object {
    $rel = "definition/" + $_.FullName.Substring($smDir.Length + 1).Replace("\", "/")
    $content = Get-Content $_.FullName -Raw -Encoding UTF8
    if ($rel -eq "definition/expressions.tmdl") {
        $content = $content -replace 'Sql\.Database\("placeholder-server\.datawarehouse\.fabric\.microsoft\.com",\s*"healthcare1_msft_admin"\)', "Sql.Database(`"$adminServer`", `"$adminDbName`")"
    }
    Add-Part -Parts $smParts -Path $rel -Content $content
}

$existingSm = (Invoke-FabricApi -Endpoint "/workspaces/$WorkspaceId/items?type=SemanticModel").value | Where-Object { $_.displayName -eq $SemanticModelName } | Select-Object -First 1
if ($existingSm) {
    $smId = $existingSm.id
    Write-Host "  Updating semantic model: $smId" -ForegroundColor Green
} else {
    Write-Host "  Creating semantic model..."
    $resp = Invoke-FabricWebRequest -Method POST -Endpoint "/workspaces/$WorkspaceId/items" -Body @{ displayName = $SemanticModelName; type = "SemanticModel"; definition = @{ parts = $smParts } }
    if ($resp.StatusCode -eq 201) { $smId = ($resp.Content | ConvertFrom-Json).id }
    elseif ($resp.StatusCode -eq 202) {
        $opId = $resp.Headers["x-ms-operation-id"]; if ($opId -is [array]) { $opId = $opId[0] }
        Wait-FabricOperation -OperationId $opId
        $smId = ((Invoke-FabricApi -Endpoint "/workspaces/$WorkspaceId/items?type=SemanticModel").value | Where-Object { $_.displayName -eq $SemanticModelName } | Select-Object -First 1).id
    } else { throw "Semantic model create returned HTTP $($resp.StatusCode)" }
}
Apply-Definition -ItemId $smId -Parts $smParts

$rptParts = [System.Collections.ArrayList]::new()
$rptPlatform = @{ '$schema' = "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json"; metadata = @{ type = "Report"; displayName = $ReportName }; config = @{ version = "2.0"; logicalId = [guid]::NewGuid().ToString() } } | ConvertTo-Json -Depth 10
Add-Part -Parts $rptParts -Path ".platform" -Content $rptPlatform
$pbir = @{ '$schema' = "https://developer.microsoft.com/json-schemas/fabric/item/report/definitionProperties/2.0.0/schema.json"; version = "4.0"; datasetReference = @{ byConnection = @{ connectionString = "Data Source=pbiazure://api.powerbi.com;Initial Catalog=$SemanticModelName;semanticModelId=$smId;Integrated Security=ClaimsToken" } } } | ConvertTo-Json -Depth 10
Add-Part -Parts $rptParts -Path "definition.pbir" -Content $pbir
Get-ChildItem $rptDir -Recurse -File | ForEach-Object {
    $rel = "definition/" + $_.FullName.Substring($rptDir.Length + 1).Replace("\", "/")
    Add-Part -Parts $rptParts -Path $rel -Content (Get-Content $_.FullName -Raw -Encoding UTF8)
}

$existingRpt = (Invoke-FabricApi -Endpoint "/workspaces/$WorkspaceId/items?type=Report").value | Where-Object { $_.displayName -eq $ReportName } | Select-Object -First 1
if ($existingRpt) {
    $rptId = $existingRpt.id
    Write-Host "  Updating report: $rptId" -ForegroundColor Green
} else {
    Write-Host "  Creating report..."
    $resp = Invoke-FabricWebRequest -Method POST -Endpoint "/workspaces/$WorkspaceId/items" -Body @{ displayName = $ReportName; type = "Report"; definition = @{ parts = $rptParts } }
    if ($resp.StatusCode -eq 201) { $rptId = ($resp.Content | ConvertFrom-Json).id }
    elseif ($resp.StatusCode -eq 202) {
        $opId = $resp.Headers["x-ms-operation-id"]; if ($opId -is [array]) { $opId = $opId[0] }
        Wait-FabricOperation -OperationId $opId
        $rptId = ((Invoke-FabricApi -Endpoint "/workspaces/$WorkspaceId/items?type=Report").value | Where-Object { $_.displayName -eq $ReportName } | Select-Object -First 1).id
    } else { throw "Report create returned HTTP $($resp.StatusCode)" }
}
Apply-Definition -ItemId $rptId -Parts $rptParts

Write-Host "" -ForegroundColor Green
Write-Host "Deployed $ReportName" -ForegroundColor Green
Write-Host "  Semantic model: $smId" -ForegroundColor Gray
Write-Host "  Report:         $rptId" -ForegroundColor Gray
Write-Host "  Report URL:     https://app.fabric.microsoft.com/groups/$WorkspaceId/reports/$rptId" -ForegroundColor Cyan
