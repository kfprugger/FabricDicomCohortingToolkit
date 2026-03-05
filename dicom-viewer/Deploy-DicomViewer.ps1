<#
.SYNOPSIS
    Deploy OHIF Viewer + DICOMweb Proxy (Function App) to Azure.

.DESCRIPTION
    1. Deploys DICOMweb proxy (Function App) + OHIF (Static Web App) via Bicep
    2. Deploys proxy Function App code (reads DICOM from OneLake JIT)
    3. Builds OHIF Viewer with proxy URL baked into config
    4. Deploys OHIF static files to Azure Static Web App

    DICOM files stay in OneLake — no pre-loading. The proxy fetches on-demand.

.PARAMETER ResourceGroup
    Azure resource group name (created if it doesn't exist)

.PARAMETER Location
    Azure region (default: westus3)

.PARAMETER BaseName
    Base name prefix for resources (default: hds-dicom)

.EXAMPLE
    .\Deploy-DicomViewer.ps1 -ResourceGroup rg-hds-dicom -Location westus3
#>

param(
    [Parameter(Mandatory)]
    [string]$ResourceGroup,

    [string]$Location = "westus3",
    [string]$SwaLocation = "westus2",
    [string]$BaseName = "hds-dicom",
    [switch]$SkipOhifBuild
)

$ErrorActionPreference = "Stop"
$scriptDir = $PSScriptRoot

Write-Host "`n=== DICOM Viewer Deployment (JIT from OneLake) ===" -ForegroundColor Cyan
Write-Host "Resource Group : $ResourceGroup"
Write-Host "Location       : $Location"
Write-Host "SWA Location   : $SwaLocation"
Write-Host "Base Name      : $BaseName`n"

# ── 1. Create RG if needed ──
Write-Host "[1/6] Ensuring resource group exists..." -ForegroundColor Yellow
az group create --name $ResourceGroup --location $Location --output none 2>$null

# ── 2. Build & push proxy container image to ACR ──
Write-Host "`n[2/6] Building proxy container image..." -ForegroundColor Yellow

$proxyDir = "$scriptDir\proxy"
if (-not (Test-Path "$proxyDir\dicom_index.json")) {
    Write-Error "dicom_index.json not found in proxy/. Run: python build_index.py --output proxy/dicom_index.json"
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
Write-Host "`n=== Deployment Complete ===" -ForegroundColor Cyan
Write-Host ""
Write-Host "OHIF Viewer    : https://$swaHostname" -ForegroundColor Green
Write-Host "DICOMweb Proxy : $proxyUrl" -ForegroundColor Green
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Yellow
Write-Host "  1. Build the DICOM index from Fabric ImagingMetastore:"
Write-Host "     pip install pyodbc azure-identity"
Write-Host "     python build_index.py --output proxy/dicom_index.json"
Write-Host ""
Write-Host "  2. Redeploy the proxy with the index:"
Write-Host "     .\Deploy-DicomViewer.ps1 -ResourceGroup $ResourceGroup -SkipOhifBuild"
Write-Host ""
Write-Host "  3. Open viewer for a specific study:"
Write-Host "     https://$swaHostname/viewer?StudyInstanceUIDs=<study-uid>"
Write-Host ""
