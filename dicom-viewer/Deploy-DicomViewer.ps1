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
    [string]$SwaLocation = "eastus2",
    [string]$BaseName = "hds-dicom",
    [string]$SwaName = "",
    [switch]$SkipOhifBuild,
    [string]$FabricSqlEndpoint = "",
    [switch]$AllowManualSqlEndpointPrompt,
    [switch]$Force
)

$effectiveSwaName = if ([string]::IsNullOrWhiteSpace($SwaName)) { "$BaseName-ohif-v2" } else { $SwaName }

$ErrorActionPreference = "Stop"
$scriptDir = $PSScriptRoot
$stateDir = Join-Path $scriptDir "state-tracking"
if (-not (Test-Path $stateDir)) { New-Item -ItemType Directory -Path $stateDir -Force | Out-Null }
$stateFile = Join-Path $stateDir ".deployment-state.json"


Write-Host "`n=== DICOM Viewer Deployment (JIT from OneLake) ===" -ForegroundColor Cyan
Write-Host "Resource Group   : $ResourceGroup"
Write-Host "Fabric Workspace : $FabricWorkspaceName"
Write-Host "Location         : $Location"
Write-Host "SWA Location     : $SwaLocation"
Write-Host "SWA Name         : $effectiveSwaName"
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

function Invoke-FabricApi {
    param(
        [Parameter(Mandatory)][string]$Uri,
        [string]$Method = "GET",
        [int]$MaxRetries = 8
    )

    for ($attempt = 1; $attempt -le $MaxRetries; $attempt++) {
        try {
            return Invoke-RestMethod -Uri $Uri -Headers $fabricHeaders -Method $Method
        } catch {
            $statusCode = $null
            try { $statusCode = [int]$_.Exception.Response.StatusCode } catch {}
            $errBody = if ($_.ErrorDetails -and $_.ErrorDetails.Message) { $_.ErrorDetails.Message } else { $_.Exception.Message }
            if (($statusCode -eq 403 -and $errBody -match 'RequestDeniedByInboundPolicy|Forbidden') -or $statusCode -in @(429, 500, 502, 503, 504)) {
                if ($attempt -lt $MaxRetries) {
                    $delay = [Math]::Min(120, 10 * [Math]::Pow(2, $attempt - 1))
                    Write-Host "  Fabric API transient HTTP ${statusCode}; retrying in ${delay}s... ($attempt/$MaxRetries)" -ForegroundColor Yellow
                    Start-Sleep -Seconds $delay
                    continue
                }
            }
            throw $_
        }
    }
}

function Assert-LastExitCode {
    param([Parameter(Mandatory)][string]$Operation)
    if ($LASTEXITCODE -ne 0) {
        throw "$Operation failed with exit code $LASTEXITCODE"
    }
}

function Test-OhifHttpEndpoint {
    param(
        [Parameter(Mandatory)][string]$ViewerUrl,
        [Parameter(Mandatory)][string]$ExpectedProxyUrl
    )
    $siteUrl = $ViewerUrl.TrimEnd('/')
    $expectedDicomWebRoot = "$($ExpectedProxyUrl.TrimEnd('/'))/dicom-web"
    for ($attempt = 1; $attempt -le 4; $attempt++) {
        try {
            $indexResponse = Invoke-WebRequest -Uri "$siteUrl/" -TimeoutSec 30 -UseBasicParsing
            if ($indexResponse.StatusCode -ne 200) { throw "index returned HTTP $($indexResponse.StatusCode)" }
            $bundleMatch = [regex]::Match([string]$indexResponse.Content, '<script[^>]+src=["'']([^"'']+\.js)["'']', 'IgnoreCase')
            if (-not $bundleMatch.Success) { throw "index.html has no JavaScript entry bundle" }
            $bundlePath = $bundleMatch.Groups[1].Value
            $bundleUrl = if ($bundlePath -match '^https?://') { $bundlePath } else { "$siteUrl/$($bundlePath.TrimStart('/'))" }
            $bundleResponse = Invoke-WebRequest -Uri $bundleUrl -TimeoutSec 30 -UseBasicParsing
            if ($bundleResponse.StatusCode -ne 200 -or $bundleResponse.RawContentLength -lt 1024) { throw "entry bundle is unavailable or unexpectedly small" }

            $configNonce = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
            $configResponse = Invoke-WebRequest -Uri "$siteUrl/app-config.js?deployment-check=$configNonce" -Headers @{ "Cache-Control" = "no-cache" } -TimeoutSec 30 -UseBasicParsing
            if ($configResponse.StatusCode -ne 200) { throw "app-config.js returned HTTP $($configResponse.StatusCode)" }
            $configuredRootCount = [regex]::Matches([string]$configResponse.Content, [regex]::Escape($expectedDicomWebRoot)).Count
            if ($configuredRootCount -lt 3) {
                throw "app-config.js does not consistently target the deployed proxy '$expectedDicomWebRoot'"
            }

            Write-Host "    OHIF HTTP health passed at $siteUrl ($($bundleResponse.RawContentLength) byte entry bundle, current proxy config)." -ForegroundColor Green
            return $true
        } catch {
            Write-Host "    OHIF HTTP check failed (attempt $attempt/4): $($_.Exception.Message)" -ForegroundColor Yellow
            if ($attempt -lt 4) { Start-Sleep -Seconds 15 }
        }
    }
    return $false
}

function Test-DicomViewerDeploymentHealth {
    param(
        [Parameter(Mandatory)][string]$ResourceGroup,
        [string]$ProxyName,
        [string]$SwaName,
        [string]$SwaHostname,
        [string]$ViewerUrl
    )
    if ([string]::IsNullOrWhiteSpace($ProxyName) -or ([string]::IsNullOrWhiteSpace($ViewerUrl) -and ([string]::IsNullOrWhiteSpace($SwaName) -or [string]::IsNullOrWhiteSpace($SwaHostname)))) {
        Write-Host "    Previous state lacks proxy/viewer resource names; redeploy required." -ForegroundColor Yellow
        return $false
    }

    $proxyFqdn = az containerapp show -g $ResourceGroup -n $ProxyName --query "properties.configuration.ingress.fqdn" -o tsv 2>$null
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($proxyFqdn)) {
        Write-Host "    Proxy Container App '$ProxyName' is not reachable/discoverable." -ForegroundColor Yellow
        return $false
    }
    $expectedProxyUrl = "https://$proxyFqdn"
    $health = $null
    for ($attempt = 1; $attempt -le 6; $attempt++) {
        try {
            $health = Invoke-RestMethod -Uri "https://$proxyFqdn/health" -TimeoutSec 60
            if ($health.status -eq "ok" -and $null -ne $health.studies -and [int]$health.studies -gt 0) {
                break
            }
            Write-Host "    Proxy health not ready (attempt $attempt/6): status=$($health.status), studies=$($health.studies)" -ForegroundColor Yellow
        } catch {
            Write-Host "    Proxy health check failed (attempt $attempt/6): $($_.Exception.Message)" -ForegroundColor Yellow
        }
        if ($attempt -lt 6) { Start-Sleep -Seconds 15 }
    }
    if (-not $health -or $health.status -ne "ok") {
        Write-Host "    Proxy health did not return status ok." -ForegroundColor Yellow
        return $false
    }
    if ($null -eq $health.studies -or [int]$health.studies -le 0) {
        Write-Host "    Proxy health has no indexed studies." -ForegroundColor Yellow
        return $false
    }

    if (-not [string]::IsNullOrWhiteSpace($ViewerUrl)) {
        if (-not (Test-OhifHttpEndpoint -ViewerUrl $ViewerUrl -ExpectedProxyUrl $expectedProxyUrl)) { return $false }
    } else {
        $actualSwaHost = az staticwebapp show --name $SwaName --resource-group $ResourceGroup --query "defaultHostname" -o tsv 2>$null
        if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($actualSwaHost)) {
            Write-Host "    Static Web App '$SwaName' is not reachable/discoverable." -ForegroundColor Yellow
            return $false
        }

        $expectedHost = $SwaHostname -replace '^https?://', ''
        if ($actualSwaHost -ne $expectedHost) {
            Write-Host "    Static Web App hostname changed from '$expectedHost' to '$actualSwaHost'." -ForegroundColor Yellow
            return $false
        }

        $swaEnv = az staticwebapp environment list --name $SwaName --resource-group $ResourceGroup --query "[?name=='default'] | [0].{status:status,hostname:hostname}" -o json 2>$null | ConvertFrom-Json
        if ($LASTEXITCODE -ne 0 -or -not $swaEnv -or $swaEnv.status -ne "Ready") {
            Write-Host "    Static Web App default environment is not Ready." -ForegroundColor Yellow
            return $false
        }
        if (-not (Test-OhifHttpEndpoint -ViewerUrl "https://$actualSwaHost" -ExpectedProxyUrl $expectedProxyUrl)) {
            Write-Host "    Static Web App control plane is Ready, but the deployed site is not reachable." -ForegroundColor Yellow
            return $false
        }
    }

    $indexPath = Join-Path $scriptDir "proxy/dicom_index.json"
    if (-not (Test-Path $indexPath)) {
        Write-Host "    Local proxy index file is missing." -ForegroundColor Yellow
        return $false
    }
    try {
        $indexContent = Get-Content $indexPath -Raw
        if ([string]::IsNullOrWhiteSpace($indexContent)) { throw "index file is empty" }
        $null = $indexContent | ConvertFrom-Json
    } catch {
        Write-Host "    Local proxy index file is invalid: $($_.Exception.Message)" -ForegroundColor Yellow
        return $false
    }

    return $true
}


# Find workspace
$workspaces = (Invoke-FabricApi -Uri "$fabricApi/workspaces").value
$ws = $workspaces | Where-Object { $_.displayName -eq $FabricWorkspaceName }
if (-not $ws) {
    Write-Error "Fabric workspace '$FabricWorkspaceName' not found. Check the name and your access."
    exit 1
}
$fabricWorkspaceId = $ws.id
Write-Host "  \u2713 Workspace: $FabricWorkspaceName ($fabricWorkspaceId)" -ForegroundColor Green

# Find Silver Lakehouse
$lakehouses = (Invoke-FabricApi -Uri "$fabricApi/workspaces/$fabricWorkspaceId/lakehouses").value
$silverLh = $lakehouses | Where-Object { $_.displayName -match '[Ss]ilver' }
if (-not $silverLh) {
    Write-Error "No Silver Lakehouse found in workspace '$FabricWorkspaceName'."
    exit 1
}
if ($silverLh -is [array]) { $silverLh = $silverLh[0] }
$silverLhName = $silverLh.displayName
Write-Host "  \u2713 Silver Lakehouse: $silverLhName ($($silverLh.id))" -ForegroundColor Green

# Get SQL analytics endpoint
$lhDetail = Invoke-FabricApi -Uri "$fabricApi/workspaces/$fabricWorkspaceId/lakehouses/$($silverLh.id)"
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
    $sqlItems = (Invoke-FabricApi -Uri "$fabricApi/workspaces/$fabricWorkspaceId/sqlEndpoints").value
    $sqlItem = $sqlItems | Where-Object { $_.displayName -eq $silverLhName }
    if ($sqlItem) {
        try {
            $sqlDetail = Invoke-FabricApi -Uri "$fabricApi/workspaces/$fabricWorkspaceId/sqlEndpoints/$($sqlItem.id)"
            $sqlEndpoint = $sqlDetail.properties.connectionString
        } catch {}
    }
}
if (-not $sqlEndpoint -and $FabricSqlEndpoint) {
    $sqlEndpoint = $FabricSqlEndpoint
}
if (-not $sqlEndpoint -and $AllowManualSqlEndpointPrompt) {
    Write-Host "  ⚠ Could not auto-detect SQL endpoint. Falling back to manual entry because -AllowManualSqlEndpointPrompt was specified." -ForegroundColor Yellow
    Write-Host "    Find it in: Fabric portal → Silver Lakehouse → SQL analytics endpoint → Copy connection string" -ForegroundColor Gray
    $sqlEndpoint = Read-Host "  Enter SQL endpoint server (e.g., xxxxx.datawarehouse.fabric.microsoft.com)"
}
if (-not $sqlEndpoint) {
    throw "Could not auto-detect SQL endpoint for '$silverLhName'. Pass -FabricSqlEndpoint or use -AllowManualSqlEndpointPrompt for an explicit interactive run."
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
        Write-Host "`n  ✓ Deployment state unchanged — verifying live proxy/SWA/index health." -ForegroundColor Green
        $previousHealthy = Test-DicomViewerDeploymentHealth -ResourceGroup $ResourceGroup -ProxyName $previousState.proxyName -SwaName $previousState.swaName -SwaHostname $previousState.swaHostname -ViewerUrl $previousState.viewerUrl
        if ($previousHealthy) {
            Write-Host "    Existing deployment is healthy. Use -Force to redeploy anyway." -ForegroundColor Gray
            $needsRedeploy = $false
        } else {
            Write-Host "    Existing deployment is not healthy; proceeding with redeploy." -ForegroundColor Yellow
        }
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
$sqlTokenObj = Get-AzAccessToken -ResourceUrl "https://database.windows.net" -ErrorAction Stop
$sqlToken = $sqlTokenObj.Token
if ($sqlToken -is [System.Security.SecureString]) {
    $bstr = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($sqlToken)
    try { $sqlToken = [System.Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr) }
    finally { [System.Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr) }
}
if ([string]::IsNullOrWhiteSpace($sqlToken)) {
    throw "Failed to acquire Fabric SQL access token from Az PowerShell. Run 'Connect-AzAccount' and align the active subscription."
}
$env:FABRIC_SQL_ACCESS_TOKEN = $sqlToken


$indexOutput = Join-Path $scriptDir "proxy" "dicom_index.json"
try {
    python (Join-Path $scriptDir "build_index.py") --output $indexOutput --server $fabricServer --database $silverLhName
    if ($LASTEXITCODE -ne 0) { throw "build_index.py exited with code $LASTEXITCODE" }
    Write-Host "  ✓ DICOM index rebuilt from Fabric ImagingMetastore" -ForegroundColor Green
} catch {
    Write-Error "Failed to build DICOM index: $_"
    exit 1
}

# ── 1. Create RG if needed ──
Write-Host "[1/6] Ensuring resource group exists..." -ForegroundColor Yellow
az group create --name $ResourceGroup --location $Location --output none 2>$null
Assert-LastExitCode "Resource group create"

# ── 2. Build & push proxy container image to ACR ──
Write-Host "`n[2/6] Building proxy container image..." -ForegroundColor Yellow

$proxyDir = "$scriptDir/proxy"
$proxyOhifDir = Join-Path $proxyDir "ohif-dist"
if (Test-Path $proxyOhifDir) { Remove-Item $proxyOhifDir -Recurse -Force }
$existingOhifDist = Join-Path $scriptDir "ohif-build/platform/app/dist"
if (Test-Path $existingOhifDist) {
    Copy-Item $existingOhifDist $proxyOhifDir -Recurse -Force
} else {
    New-Item -ItemType Directory -Path $proxyOhifDir -Force | Out-Null
    Set-Content -Path (Join-Path $proxyOhifDir "index.html") -Value "<!doctype html><title>OHIF build pending</title>"
}
if (-not (Test-Path "$proxyDir/dicom_index.json")) {
    Write-Error "dicom_index.json not found in proxy/. The index rebuild in step 0 may have failed."
    exit 1
}

# Derive a unique ACR name matching the Bicep uniqueString(resourceGroup().id) pattern.
# First check if an ACR already exists in the RG (idempotent re-runs).
$existingAcr = az acr list --resource-group $ResourceGroup --query "[0].name" -o tsv 2>$null
Assert-LastExitCode "ACR list"
if ($existingAcr) {
    $acrNameParam = $existingAcr
    Write-Host "  Using existing ACR: $acrNameParam" -ForegroundColor Green
} else {
    # Generate a short hash from the RG name to ensure global uniqueness (mirrors Bicep uniqueString)
    $rgHash = [System.BitConverter]::ToString(
        [System.Security.Cryptography.SHA256]::Create().ComputeHash(
            [System.Text.Encoding]::UTF8.GetBytes($ResourceGroup)
        )
    ).Replace('-','').Substring(0,13).ToLower()
    $acrNameParam = $BaseName.Replace('-', '') + $rgHash + 'acr'
    # ACR names max 50 chars, alphanumeric only
    if ($acrNameParam.Length -gt 50) { $acrNameParam = $acrNameParam.Substring(0, 50) }
}
# Create ACR if it doesn't exist
$acrExists = az acr show --name $acrNameParam --resource-group $ResourceGroup --query name -o tsv 2>$null
if (-not $acrExists) {
    Write-Host "  Creating ACR: $acrNameParam"
    az acr create --name $acrNameParam --resource-group $ResourceGroup --location $Location --sku Basic --admin-enabled true --output none 2>&1
    Assert-LastExitCode "ACR create"
}
$acrLogin = az acr show --name $acrNameParam --query loginServer -o tsv
Assert-LastExitCode "ACR lookup"
if ([string]::IsNullOrWhiteSpace($acrLogin)) { throw "ACR login server not found for $acrNameParam" }

$proxyImageTag = "deploy-$(Get-Date -AsUTC -Format 'yyyyMMddHHmmss')"
Write-Host "  Building image via ACR Tasks (no local Docker needed): ${BaseName}-proxy:$proxyImageTag..."
$acrBuildOutput = az acr build --registry $acrNameParam --image "${BaseName}-proxy:$proxyImageTag" $proxyDir 2>&1
$acrBuildExit = $LASTEXITCODE
$acrBuildOutput | ForEach-Object { if ($_ -match "Step|Successfully|Run ID|Elapsed|digest") { Write-Host "  $_" } }
if ($acrBuildExit -ne 0) { throw "ACR build failed with exit code $acrBuildExit" }
$proxyImageDigest = az acr manifest show-metadata --registry $acrNameParam --name "${BaseName}-proxy:$proxyImageTag" --query digest -o tsv 2>$null
if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($proxyImageDigest)) { throw "Could not resolve immutable DICOM proxy image digest" }
$proxyImageReference = "${acrLogin}/${BaseName}-proxy@$proxyImageDigest"
Write-Host "  Image built: $proxyImageReference" -ForegroundColor Green

# ── 3. Deploy Bicep (infra + Container App referencing the image) ──
Write-Host "`n[3/6] Deploying infrastructure..." -ForegroundColor Yellow
$deploymentRaw = az deployment group create `
    --resource-group $ResourceGroup `
    --template-file "$scriptDir/infra/main.bicep" `
    --parameters baseName=$BaseName swaName=$effectiveSwaName location=$Location swaLocation=$SwaLocation fabricSqlServer=$fabricServer fabricSqlDatabase=$silverLhName acrName=$acrNameParam proxyImage=$proxyImageReference `
    --query "properties.outputs" `
    --output json
Assert-LastExitCode "DICOM viewer infrastructure deployment"
$deployment = $deploymentRaw | ConvertFrom-Json

if (-not $deployment.proxyUrl.value) {
    Write-Error "Bicep deployment failed — check the Azure CLI output above."
    exit 1
}

$proxyUrl = $deployment.proxyUrl.value
$proxyName = $deployment.proxyName.value
$swaName = $deployment.ohifSwaName.value
$swaHostname = $deployment.ohifSwaDefaultHostname.value
$currentState.proxyUrl = $proxyUrl
$currentState.proxyName = $proxyName
$currentState.swaName = $swaName
$currentState.swaHostname = $swaHostname

Write-Host "  Proxy URL    : $proxyUrl" -ForegroundColor Green
Write-Host "  SWA Hostname : https://$swaHostname" -ForegroundColor Green

# ── 4. Build OHIF Viewer ──
if ($SkipOhifBuild) {
    Write-Host "`n[4/6] Skipping OHIF build (-SkipOhifBuild)" -ForegroundColor Yellow
    # Still update the config in dist with the current proxy URL
    $distConfig = "$scriptDir/ohif-build/platform/app/dist/app-config.js"
    if (Test-Path $distConfig) {
        Write-Host "  Updating proxy URL in existing dist..."
        $configContent = Get-Content "$scriptDir/ohif/app-config.js" -Raw
        $configContent = $configContent.Replace("__PROXY_URL__", $proxyUrl)
        Set-Content $distConfig $configContent
    }
} else {
    Write-Host "`n[4/6] Building OHIF Viewer..." -ForegroundColor Yellow

    $ohifBuildDir = "$scriptDir/ohif-build"
    if (-not (Test-Path "$ohifBuildDir/platform/app/node_modules")) {
        if (Test-Path $ohifBuildDir) { Remove-Item -Recurse -Force $ohifBuildDir }
        Write-Host "  Cloning OHIF Viewer v3..."
        git clone --depth 1 --branch master https://github.com/OHIF/Viewers.git $ohifBuildDir 2>&1 | Out-Null
        Assert-LastExitCode "OHIF source clone"
    } else {
        Write-Host "  Using existing OHIF source (delete ohif-build/ to force fresh clone)"
    }

    # Write config with proxy URL
    Write-Host "  Applying proxy configuration..."
    $configContent = Get-Content "$scriptDir/ohif/app-config.js" -Raw
    $configContent = $configContent.Replace("__PROXY_URL__", $proxyUrl)
    Set-Content -Path "$ohifBuildDir/platform/app/public/config/default.js" -Value $configContent

    Copy-Item "$scriptDir/ohif/staticwebapp.config.json" "$ohifBuildDir/platform/app/staticwebapp.config.json" -Force

    # Install dependencies if needed
    if (-not (Test-Path "$ohifBuildDir/node_modules")) {
        Write-Host "  Ensuring yarn is available..."
        if (-not (Get-Command yarn -ErrorAction SilentlyContinue)) {
            npm install -g yarn 2>&1 | Out-Null
            Assert-LastExitCode "Install yarn"
        }
        Push-Location $ohifBuildDir
        try {
            Write-Host "  Installing dependencies (this takes a few minutes)..."
            yarn install 2>&1 | Out-Null
            Assert-LastExitCode "OHIF dependency install"
        } finally {
            Pop-Location
        }
    } else {
        Write-Host "  Dependencies already installed"
    }

    # Build
    Write-Host "  Building OHIF (webpack, ~1-2 minutes)..."
    Push-Location "$ohifBuildDir/platform/app"
    try {
        $env:NODE_ENV = "production"
        node --max_old_space_size=8096 ./../../node_modules/webpack/bin/webpack.js --config .webpack/webpack.pwa.js 2>&1 | Out-Null
        Assert-LastExitCode "OHIF webpack build"
    } finally {
        Pop-Location
    }

    $distDir = "$ohifBuildDir/platform/app/dist"
    if (-not (Test-Path $distDir)) {
        Write-Error "OHIF build failed — dist directory not found"
        exit 1
    }
    Copy-Item "$ohifBuildDir/platform/app/staticwebapp.config.json" "$distDir/staticwebapp.config.json" -Force
    Write-Host "  OHIF build complete" -ForegroundColor Green
}

# ── 5. Deploy OHIF to SWA ──
Write-Host "`n[5/6] Deploying OHIF to Static Web App..." -ForegroundColor Yellow

$distDir = "$scriptDir/ohif-build/platform/app/dist"
if (-not (Test-Path $distDir)) {
    Write-Error "No dist directory found. Run without -SkipOhifBuild first."
    exit 1
}

$deployToken = az staticwebapp secrets list `
    --name $swaName `
    --resource-group $ResourceGroup `
    --query "properties.apiKey" `
    --output tsv
Assert-LastExitCode "SWA deployment token lookup"
if ([string]::IsNullOrWhiteSpace($deployToken)) { throw "Static Web App deployment token not found for $swaName" }

$swaDeployOutput = npx --yes @azure/static-web-apps-cli deploy $distDir `
    --swa-config-location $distDir `
    --deployment-token $deployToken `
    --env production 2>&1
$swaDeployExit = $LASTEXITCODE
$swaDeployOutput | ForEach-Object { Write-Host "  $_" }
if ($swaDeployExit -ne 0) { throw "Static Web App deploy failed with exit code $swaDeployExit" }
Write-Host "  OHIF deployed" -ForegroundColor Green

$viewerUrl = "https://$swaHostname"
if (Test-DicomViewerDeploymentHealth -ResourceGroup $ResourceGroup -ProxyName $proxyName -SwaName $swaName -SwaHostname $swaHostname) {
    $currentState.viewerMode = "static-web-app"
    $currentState.viewerUrl = $viewerUrl
} else {
    Write-Host "  Static Web App is not reachable; using the DICOM proxy Container App as the viewer host." -ForegroundColor Yellow
    $viewerUrl = $proxyUrl
    $currentState.viewerMode = "container-app"
    $currentState.viewerUrl = $viewerUrl
    if (-not (Test-DicomViewerDeploymentHealth -ResourceGroup $ResourceGroup -ProxyName $proxyName -ViewerUrl $viewerUrl)) {
        Write-Host "  The initial proxy image did not contain a usable OHIF build; rebuilding it from the completed dist directory." -ForegroundColor Yellow
        if (Test-Path $proxyOhifDir) { Remove-Item $proxyOhifDir -Recurse -Force }
        Copy-Item $distDir $proxyOhifDir -Recurse -Force
        $fallbackTag = "ohif-$(Get-Date -AsUTC -Format 'yyyyMMddHHmmss')"
        $fallbackBuild = az acr build --registry $acrNameParam --image "${BaseName}-proxy:$fallbackTag" $proxyDir 2>&1
        $fallbackExit = $LASTEXITCODE
        $fallbackBuild | ForEach-Object { if ($_ -match "Step|Successfully|Run ID|Elapsed|digest") { Write-Host "  $_" } }
        if ($fallbackExit -ne 0) { throw "Proxy-hosted OHIF image build failed with exit code $fallbackExit" }
        $fallbackDigest = az acr manifest show-metadata --registry $acrNameParam --name "${BaseName}-proxy:$fallbackTag" --query digest -o tsv 2>$null
        if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($fallbackDigest)) { throw "Could not resolve proxy-hosted OHIF image digest" }
        $fallbackImage = "${acrLogin}/${BaseName}-proxy@$fallbackDigest"
        az containerapp update --name $proxyName --resource-group $ResourceGroup --image $fallbackImage --set-env-vars "OHIF_DEPLOYMENT_ID=$fallbackTag" --output none
        Assert-LastExitCode "Proxy-hosted OHIF Container App update"
        if (-not (Test-DicomViewerDeploymentHealth -ResourceGroup $ResourceGroup -ProxyName $proxyName -ViewerUrl $viewerUrl)) {
            throw "DICOM viewer fallback did not pass live proxy/OHIF/index health checks."
        }
    }
}

if ([string]::IsNullOrWhiteSpace($currentState.viewerUrl)) {
    throw "DICOM viewer deployment completed without a healthy viewer URL."
}


# ── 6. Summary ──
Write-Host "`n[6/6] Deployment complete!" -ForegroundColor Yellow

# Save deployment state for idempotent checks
$currentState | ConvertTo-Json | Set-Content $stateFile
Write-Host "  Deployment state saved to state-tracking/.deployment-state.json" -ForegroundColor DarkGray

Write-Host "`n=== Deployment Complete ===" -ForegroundColor Cyan
Write-Host ""
Write-Host "Fabric Workspace : $FabricWorkspaceName" -ForegroundColor Green
Write-Host "SQL Endpoint     : $fabricServer" -ForegroundColor Green
Write-Host "Database         : $silverLhName" -ForegroundColor Green
Write-Host "OHIF Viewer      : $viewerUrl" -ForegroundColor Green
Write-Host "DICOMweb Proxy   : $proxyUrl" -ForegroundColor Green
Write-Host ""
Write-Host "To switch workspaces, re-run with a different -FabricWorkspaceName:" -ForegroundColor Yellow
Write-Host "  .\Deploy-DicomViewer.ps1 -ResourceGroup $ResourceGroup -FabricWorkspaceName `"<new-workspace>`""
Write-Host ""
Write-Host "Open viewer for a specific study:" -ForegroundColor Yellow
Write-Host "  $($viewerUrl.TrimEnd('/'))/viewer?StudyInstanceUIDs=<study-uid>"
Write-Host ""
