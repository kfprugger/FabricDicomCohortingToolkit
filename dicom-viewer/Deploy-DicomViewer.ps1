<#
.SYNOPSIS
    Deploy OHIF Viewer + DICOMweb Proxy (Container App) to Azure, connected to a Fabric workspace.

.DESCRIPTION
    1. Discovers the Silver Lakehouse SQL endpoint from the specified Fabric workspace
    2. Checks deployment state — skips if workspace/server/database haven't changed
    3. Rebuilds the DICOM index from ImagingMetastore (via SQL endpoint)
    4. Deploys DICOMweb proxy (Container App) + OHIF (Static Web App) via Bicep
    5. Builds OHIF Viewer with proxy URL baked into config
    6. Deploys OHIF static files to Azure Static Web App

    DICOM files stay in OneLake — no pre-loading. The proxy fetches on-demand.
    Idempotent: re-run with the same workspace and it skips. Change workspace and it redeploys.

.PARAMETER ResourceGroup
    Azure resource group name (created if it doesn't exist)

.PARAMETER FabricWorkspaceName
    Name of the Fabric workspace containing the Silver Lakehouse with ImagingMetastore

.PARAMETER Location
    Azure region (default: westus3)

.PARAMETER Force
    Force redeploy even if workspace hasn't changed

.EXAMPLE
    .\Deploy-DicomViewer.ps1 -ResourceGroup rg-hds-dicom -FabricWorkspaceName "my-hds-workspace"

.EXAMPLE
    # Switch to a different workspace
    .\Deploy-DicomViewer.ps1 -ResourceGroup rg-hds-dicom -FabricWorkspaceName "other-workspace"
#>

param(
    [Parameter(Mandatory)]
    [string]$ResourceGroup,

    [Parameter(Mandatory)]
    [string]$FabricWorkspaceName,

    [string]$Location = "eastus",
    [string]$SwaLocation = "westus2",
    [string]$BaseName = "hds-dicom",
    [switch]$SkipOhifBuild,
    [switch]$Force
)

$ErrorActionPreference = "Stop"
$scriptDir = $PSScriptRoot
$stateFile = Join-Path $scriptDir ".deployment-state.json"

Write-Host "`n=== DICOM Viewer Deployment (JIT from OneLake) ===" -ForegroundColor Cyan
Write-Host "Resource Group   : $ResourceGroup"
Write-Host "Fabric Workspace : $FabricWorkspaceName"
Write-Host "Location         : $Location"
Write-Host "SWA Location     : $SwaLocation"
Write-Host "Base Name        : $BaseName`n"

# ── 0. Discover Fabric workspace SQL endpoint + Silver Lakehouse ──
Write-Host "[0/6] Discovering Fabric workspace..." -ForegroundColor Yellow

function Get-FabricAccessToken {
    $tokenObj = Get-AzAccessToken -ResourceUrl "https://api.fabric.microsoft.com"
    $rawToken = $tokenObj.Token
    if ($rawToken -is [System.Security.SecureString]) {
        $bstr = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($rawToken)
        try { return [System.Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr) }
        finally { [System.Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr) }
    }
    elseif ($rawToken -is [string]) { return $rawToken }
    else { return $rawToken | ConvertFrom-SecureString -AsPlainText }
}

$fabricToken = Get-FabricAccessToken
$fabricHeaders = @{ "Authorization" = "Bearer $fabricToken" }
$fabricApi = "https://api.fabric.microsoft.com/v1"

# Find workspace
$workspaces = (Invoke-RestMethod -Uri "$fabricApi/workspaces" -Headers $fabricHeaders).value
$ws = $workspaces | Where-Object { $_.displayName -eq $FabricWorkspaceName }
if (-not $ws) {
    Write-Error "Fabric workspace '$FabricWorkspaceName' not found. Check the name and your access."
    exit 1
}
$fabricWorkspaceId = $ws.id
Write-Host "  \u2713 Workspace: $FabricWorkspaceName ($fabricWorkspaceId)" -ForegroundColor Green

# Find Silver Lakehouse
$lakehouses = (Invoke-RestMethod -Uri "$fabricApi/workspaces/$fabricWorkspaceId/lakehouses" -Headers $fabricHeaders).value
$silverLh = $lakehouses | Where-Object { $_.displayName -match '[Ss]ilver' }
if (-not $silverLh) {
    Write-Error "No Silver Lakehouse found in workspace '$FabricWorkspaceName'."
    exit 1
}
if ($silverLh -is [array]) { $silverLh = $silverLh[0] }
$silverLhName = $silverLh.displayName
Write-Host "  \u2713 Silver Lakehouse: $silverLhName ($($silverLh.id))" -ForegroundColor Green

# Get SQL analytics endpoint
$lhDetail = Invoke-RestMethod -Uri "$fabricApi/workspaces/$fabricWorkspaceId/lakehouses/$($silverLh.id)" -Headers $fabricHeaders
$sqlEndpoint = $null
if ($lhDetail.properties -and $lhDetail.properties.sqlEndpointProperties) {
    $sqlEndpoint = $lhDetail.properties.sqlEndpointProperties.connectionString
}
if (-not $sqlEndpoint) {
    # Fallback: try oneLakeTablesPath or construct from workspace
    try { $sqlEndpoint = $lhDetail.properties.sqlEndpointProperties.provisioningStatus } catch {}
}
if (-not $sqlEndpoint) {
    # Use the SQL analytics endpoint items API
    $sqlItems = (Invoke-RestMethod -Uri "$fabricApi/workspaces/$fabricWorkspaceId/sqlEndpoints" -Headers $fabricHeaders -ErrorAction SilentlyContinue).value
    $sqlItem = $sqlItems | Where-Object { $_.displayName -eq $silverLhName }
    if ($sqlItem) {
        try {
            $sqlDetail = Invoke-RestMethod -Uri "$fabricApi/workspaces/$fabricWorkspaceId/sqlEndpoints/$($sqlItem.id)" -Headers $fabricHeaders
            $sqlEndpoint = $sqlDetail.properties.connectionString
        } catch {}
    }
}
if (-not $sqlEndpoint) {
    Write-Host "  \u26a0 Could not auto-detect SQL endpoint. Falling back to manual entry." -ForegroundColor Yellow
    Write-Host "    Find it in: Fabric portal \u2192 Silver Lakehouse \u2192 SQL analytics endpoint \u2192 Copy connection string" -ForegroundColor Gray
    $sqlEndpoint = Read-Host "  Enter SQL endpoint server (e.g., xxxxx.datawarehouse.fabric.microsoft.com)"
}

# Clean up the SQL endpoint — extract just the server hostname
$fabricServer = $sqlEndpoint -replace '^.*Server=', '' -replace ';.*$', '' -replace ',$', ''
if ($fabricServer -notmatch 'datawarehouse\.fabric\.microsoft\.com') {
    $fabricServer = $sqlEndpoint  # Use as-is if it's already a hostname
}
Write-Host "  \u2713 SQL Endpoint: $fabricServer" -ForegroundColor Green
Write-Host "  \u2713 Database: $silverLhName" -ForegroundColor Green

# ── Idempotent check: compare with previous deployment state ──
$currentState = @{
    fabricWorkspace = $FabricWorkspaceName
    fabricServer    = $fabricServer
    fabricDatabase  = $silverLhName
    resourceGroup   = $ResourceGroup
}

$needsRedeploy = $true
if ((Test-Path $stateFile) -and -not $Force) {
    $previousState = Get-Content $stateFile | ConvertFrom-Json
    if ($previousState.fabricServer -eq $fabricServer -and
        $previousState.fabricDatabase -eq $silverLhName -and
        $previousState.resourceGroup -eq $ResourceGroup) {
        Write-Host "`n  \u2713 Deployment state unchanged — workspace, server, and database match." -ForegroundColor Green
        Write-Host "    Skipping redeploy. Use -Force to redeploy anyway." -ForegroundColor Gray
        $needsRedeploy = $false
    } else {
        Write-Host "`n  \u26a0 Workspace changed:" -ForegroundColor Yellow
        if ($previousState.fabricServer -ne $fabricServer)     { Write-Host "    Server:   $($previousState.fabricServer) \u2192 $fabricServer" -ForegroundColor White }
        if ($previousState.fabricDatabase -ne $silverLhName)   { Write-Host "    Database: $($previousState.fabricDatabase) \u2192 $silverLhName" -ForegroundColor White }
        if ($previousState.resourceGroup -ne $ResourceGroup)   { Write-Host "    RG:       $($previousState.resourceGroup) \u2192 $ResourceGroup" -ForegroundColor White }
        Write-Host "    Proceeding with full redeploy..." -ForegroundColor White
    }
}

if (-not $needsRedeploy) { exit 0 }

# ── Rebuild DICOM index from new workspace ──
Write-Host "`n  Rebuilding DICOM index from $silverLhName..." -ForegroundColor White
$env:FABRIC_SERVER = $fabricServer
$env:FABRIC_DB = $silverLhName

$indexOutput = Join-Path $scriptDir "proxy" "dicom_index.json"
try {
    python (Join-Path $scriptDir "build_index.py") --output $indexOutput --server $fabricServer --database $silverLhName
    Write-Host "  \u2713 DICOM index rebuilt" -ForegroundColor Green
} catch {
    Write-Error "Failed to build DICOM index: $_"
    exit 1
}

# ── 1. Create RG if needed ──
Write-Host "[1/6] Ensuring resource group exists..." -ForegroundColor Yellow
az group create --name $ResourceGroup --location $Location --output none 2>$null

# ── 2. Build & push proxy container image to ACR ──
Write-Host "`n[2/6] Building proxy container image..." -ForegroundColor Yellow

$proxyDir = "$scriptDir\proxy"
if (-not (Test-Path "$proxyDir\dicom_index.json")) {
    Write-Error "dicom_index.json not found in proxy/. The index rebuild in step 0 may have failed."
    exit 1
}

$acrNameParam = $BaseName.Replace('-', '') + 'acr'
# Create ACR if it doesn't exist
$acrExists = az acr show --name $acrNameParam --resource-group $ResourceGroup --query name -o tsv 2>$null
if (-not $acrExists) {
    Write-Host "  Creating ACR: $acrNameParam"
    az acr create --name $acrNameParam --resource-group $ResourceGroup --location $Location --sku Basic --admin-enabled true --output none 2>&1
}
$acrLogin = az acr show --name $acrNameParam --query loginServer -o tsv

Write-Host "  Building image via ACR Tasks (no local Docker needed)..."
az acr build --registry $acrNameParam --image "${BaseName}-proxy:latest" $proxyDir 2>&1 | ForEach-Object { if ($_ -match "Step|Successfully|Run ID|Elapsed|latest:") { Write-Host "  $_" } }
Write-Host "  Image built: ${acrLogin}/${BaseName}-proxy:latest" -ForegroundColor Green

# ── 3. Deploy Bicep (infra + Container App referencing the image) ──
Write-Host "`n[3/6] Deploying infrastructure..." -ForegroundColor Yellow
$deployment = az deployment group create `
    --resource-group $ResourceGroup `
    --template-file "$scriptDir\infra\main.bicep" `
    --parameters baseName=$BaseName location=$Location swaLocation=$SwaLocation `
    --query "properties.outputs" `
    --output json | ConvertFrom-Json

if (-not $deployment.proxyUrl.value) {
    Write-Error "Bicep deployment failed — check the Azure CLI output above."
    exit 1
}

$proxyUrl = $deployment.proxyUrl.value
$proxyName = $deployment.proxyName.value
$swaName = $deployment.ohifSwaName.value
$swaHostname = $deployment.ohifSwaDefaultHostname.value

Write-Host "  Proxy URL    : $proxyUrl" -ForegroundColor Green
Write-Host "  SWA Hostname : https://$swaHostname" -ForegroundColor Green

# ── 4. Build OHIF Viewer ──
if ($SkipOhifBuild) {
    Write-Host "`n[4/6] Skipping OHIF build (-SkipOhifBuild)" -ForegroundColor Yellow
    # Still update the config in dist with the current proxy URL
    $distConfig = "$scriptDir\ohif-build\platform\app\dist\app-config.js"
    if (Test-Path $distConfig) {
        Write-Host "  Updating proxy URL in existing dist..."
        $configContent = Get-Content "$scriptDir\ohif\app-config.js" -Raw
        $configContent = $configContent.Replace("__PROXY_URL__", $proxyUrl)
        Set-Content $distConfig $configContent
    }
} else {
    Write-Host "`n[4/6] Building OHIF Viewer..." -ForegroundColor Yellow

    $ohifBuildDir = "$scriptDir\ohif-build"
    if (-not (Test-Path "$ohifBuildDir\platform\app\node_modules")) {
        if (Test-Path $ohifBuildDir) { Remove-Item -Recurse -Force $ohifBuildDir }
        Write-Host "  Cloning OHIF Viewer v3..."
        git clone --depth 1 --branch master https://github.com/OHIF/Viewers.git $ohifBuildDir 2>&1 | Out-Null
    } else {
        Write-Host "  Using existing OHIF source (delete ohif-build/ to force fresh clone)"
    }

    # Write config with proxy URL
    Write-Host "  Applying proxy configuration..."
    $configContent = Get-Content "$scriptDir\ohif\app-config.js" -Raw
    $configContent = $configContent.Replace("__PROXY_URL__", $proxyUrl)
    Set-Content -Path "$ohifBuildDir\platform\app\public\config\default.js" -Value $configContent

    Copy-Item "$scriptDir\ohif\staticwebapp.config.json" "$ohifBuildDir\platform\app\staticwebapp.config.json" -Force

    # Install dependencies if needed
    if (-not (Test-Path "$ohifBuildDir\node_modules")) {
        Write-Host "  Ensuring yarn is available..."
        if (-not (Get-Command yarn -ErrorAction SilentlyContinue)) {
            npm install -g yarn 2>&1 | Out-Null
        }
        Push-Location $ohifBuildDir
        Write-Host "  Installing dependencies (this takes a few minutes)..."
        yarn install 2>&1 | Out-Null
        Pop-Location
    } else {
        Write-Host "  Dependencies already installed"
    }

    # Build
    Write-Host "  Building OHIF (webpack, ~1-2 minutes)..."
    Push-Location "$ohifBuildDir\platform\app"
    $env:NODE_ENV = "production"
    node --max_old_space_size=8096 ./../../node_modules/webpack/bin/webpack.js --config .webpack/webpack.pwa.js 2>&1 | Out-Null
    Pop-Location

    $distDir = "$ohifBuildDir\platform\app\dist"
    if (-not (Test-Path $distDir)) {
        Write-Error "OHIF build failed — dist directory not found"
        exit 1
    }
    Copy-Item "$ohifBuildDir\platform\app\staticwebapp.config.json" "$distDir\staticwebapp.config.json" -Force
    Write-Host "  OHIF build complete" -ForegroundColor Green
}

# ── 5. Deploy OHIF to SWA ──
Write-Host "`n[5/6] Deploying OHIF to Static Web App..." -ForegroundColor Yellow

$distDir = "$scriptDir\ohif-build\platform\app\dist"
if (-not (Test-Path $distDir)) {
    Write-Error "No dist directory found. Run without -SkipOhifBuild first."
    exit 1
}

$deployToken = az staticwebapp secrets list `
    --name $swaName `
    --resource-group $ResourceGroup `
    --query "properties.apiKey" `
    --output tsv

npx --yes @azure/static-web-apps-cli deploy $distDir `
    --deployment-token $deployToken `
    --env production 2>&1 | ForEach-Object { Write-Host "  $_" }

Write-Host "  OHIF deployed" -ForegroundColor Green

# ── 6. Summary ──
Write-Host "`n[6/6] Deployment complete!" -ForegroundColor Yellow

# Save deployment state for idempotent checks
$currentState | ConvertTo-Json | Set-Content $stateFile
Write-Host "  Deployment state saved to .deployment-state.json" -ForegroundColor DarkGray

Write-Host "`n=== Deployment Complete ===" -ForegroundColor Cyan
Write-Host ""
Write-Host "Fabric Workspace : $FabricWorkspaceName" -ForegroundColor Green
Write-Host "SQL Endpoint     : $fabricServer" -ForegroundColor Green
Write-Host "Database         : $silverLhName" -ForegroundColor Green
Write-Host "OHIF Viewer      : https://$swaHostname" -ForegroundColor Green
Write-Host "DICOMweb Proxy   : $proxyUrl" -ForegroundColor Green
Write-Host ""
Write-Host "To switch workspaces, re-run with a different -FabricWorkspaceName:" -ForegroundColor Yellow
Write-Host "  .\Deploy-DicomViewer.ps1 -ResourceGroup $ResourceGroup -FabricWorkspaceName `"<new-workspace>`""
Write-Host ""
Write-Host "Open viewer for a specific study:" -ForegroundColor Yellow
Write-Host "  https://$swaHostname/viewer?StudyInstanceUIDs=<study-uid>"
Write-Host ""
