$ErrorActionPreference = 'Stop'
$scriptDir = "C:\git\FabricDicomCohortingToolkit"
$parts = Get-Content "$scriptDir\_deployed_definition.json" -Raw | ConvertFrom-Json

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Data Agent Definition Validation" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

# --- 1. data_agent.json ---
Write-Host "`n--- 1. data_agent.json ---" -ForegroundColor Yellow
$p = $parts | Where-Object { $_.path -eq "Files/Config/data_agent.json" }
$decoded = [System.Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($p.payload))
Write-Host $decoded

# --- 2. Instructions (stage_config.json) ---
Write-Host "`n--- 2. Instructions (stage_config.json) ---" -ForegroundColor Yellow
$p = $parts | Where-Object { $_.path -eq "Files/Config/draft/stage_config.json" }
$stageJson = [System.Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($p.payload))
$stage = $stageJson | ConvertFrom-Json
Write-Host "  Deployed instructions length: $($stage.aiInstructions.Length) chars"

$mdContent = Get-Content "$scriptDir\data-agent-instructions.md" -Raw
if ($mdContent -match '(?s)```\r?\n(.*?)\r?\n```') {
    $repoInstructions = $Matches[1]
    Write-Host "  Repo instructions length:     $($repoInstructions.Length) chars"
    if ($stage.aiInstructions.Trim() -eq $repoInstructions.Trim()) {
        Write-Host "  RESULT: MATCH" -ForegroundColor Green
    } else {
        Write-Host "  RESULT: MISMATCH" -ForegroundColor Red
        $a = $stage.aiInstructions.Trim(); $b = $repoInstructions.Trim()
        if ($a.Length -ne $b.Length) { Write-Host "  Length diff: deployed=$($a.Length) repo=$($b.Length)" -ForegroundColor Red }
    }
}

# --- 3. Silver datasource ---
Write-Host "`n--- 3. Silver datasource ---" -ForegroundColor Yellow
$p = $parts | Where-Object { $_.path -like "*silver/datasource.json" }
$silverDs = [System.Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($p.payload)) | ConvertFrom-Json
Write-Host "  displayName: $($silverDs.displayName)"
Write-Host "  type:        $($silverDs.type)"
Write-Host "  artifactId:  $($silverDs.artifactId)"
Write-Host "  workspaceId: $($silverDs.workspaceId)"
$silverSelectedTables = @()
if ($silverDs.elements) {
    foreach ($schema in $silverDs.elements) {
        if ($schema.children) {
            $silverSelectedTables = $schema.children | Where-Object { $_.is_selected -eq $true } | ForEach-Object { $_.display_name }
        }
    }
}
Write-Host "  Selected tables ($($silverSelectedTables.Count)): $($silverSelectedTables -join ', ')"

# Compare against script expected tables
$expectedSilver = @('AllergyIntolerance','Condition','DiagnosticReport','Encounter','ImagingMetastore','ImagingStudy','Location','MedicationRequest','Observation','Organization','Patient','Practitioner','Procedure')
$missingS = $expectedSilver | Where-Object { $_ -notin $silverSelectedTables }
$extraS = $silverSelectedTables | Where-Object { $_ -notin $expectedSilver }
if ($missingS) { Write-Host "  MISSING tables: $($missingS -join ', ')" -ForegroundColor Red } 
if ($extraS) { Write-Host "  EXTRA tables: $($extraS -join ', ')" -ForegroundColor Yellow }
if (-not $missingS -and -not $extraS) { Write-Host "  RESULT: MATCH" -ForegroundColor Green }

# --- 4. Gold datasource ---
Write-Host "`n--- 4. Gold datasource ---" -ForegroundColor Yellow
$p = $parts | Where-Object { $_.path -like "*gold_omop/datasource.json" }
$goldDs = [System.Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($p.payload)) | ConvertFrom-Json
Write-Host "  displayName: $($goldDs.displayName)"
Write-Host "  type:        $($goldDs.type)"
Write-Host "  artifactId:  $($goldDs.artifactId)"
Write-Host "  workspaceId: $($goldDs.workspaceId)"
$goldSelectedTables = @()
if ($goldDs.elements) {
    foreach ($schema in $goldDs.elements) {
        if ($schema.children) {
            $goldSelectedTables = $schema.children | Where-Object { $_.is_selected -eq $true } | ForEach-Object { $_.display_name }
        }
    }
}
Write-Host "  Selected tables ($($goldSelectedTables.Count)): $($goldSelectedTables -join ', ')"

$expectedGold = @('care_site','concept','concept_ancestor','concept_relationship','condition_era','condition_occurrence','death','drug_era','drug_exposure','image_occurrence','location','measurement','observation','person','procedure_occurrence','provider','relationship','visit_detail','visit_occurrence')
$missingG = $expectedGold | Where-Object { $_ -notin $goldSelectedTables }
$extraG = $goldSelectedTables | Where-Object { $_ -notin $expectedGold }
if ($missingG) { Write-Host "  MISSING tables: $($missingG -join ', ')" -ForegroundColor Red }
if ($extraG) { Write-Host "  EXTRA tables: $($extraG -join ', ')" -ForegroundColor Yellow }
if (-not $missingG -and -not $extraG) { Write-Host "  RESULT: MATCH" -ForegroundColor Green }

# --- 5. Silver fewshots ---
Write-Host "`n--- 5. Silver fewshots ---" -ForegroundColor Yellow
$p = $parts | Where-Object { $_.path -like "*silver/fewshots.json" }
$silverFsDeployed = [System.Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($p.payload)) | ConvertFrom-Json
$silverFsRepo = Get-Content "$scriptDir\fewshots-silver-fhir.json" -Raw | ConvertFrom-Json
Write-Host "  Deployed: $($silverFsDeployed.fewShots.Count) examples"
Write-Host "  Repo:     $($silverFsRepo.fewShots.Count) examples"
# Compare questions
$deployedQs = $silverFsDeployed.fewShots | ForEach-Object { $_.question } | Sort-Object
$repoQs = $silverFsRepo.fewShots | ForEach-Object { $_.question } | Sort-Object
$diff = Compare-Object $deployedQs $repoQs
if ($diff) { Write-Host "  RESULT: MISMATCH ($($diff.Count) differences)" -ForegroundColor Red; $diff | ForEach-Object { Write-Host "    $($_.SideIndicator) $($_.InputObject)" } }
else { Write-Host "  RESULT: MATCH" -ForegroundColor Green }

# --- 6. Gold fewshots ---
Write-Host "`n--- 6. Gold fewshots ---" -ForegroundColor Yellow
$p = $parts | Where-Object { $_.path -like "*gold_omop/fewshots.json" }
$goldFsDeployed = [System.Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($p.payload)) | ConvertFrom-Json
$goldFsRepo = Get-Content "$scriptDir\fewshots-gold-omop.json" -Raw | ConvertFrom-Json
Write-Host "  Deployed: $($goldFsDeployed.fewShots.Count) examples"
Write-Host "  Repo:     $($goldFsRepo.fewShots.Count) examples"
$deployedQs = $goldFsDeployed.fewShots | ForEach-Object { $_.question } | Sort-Object
$repoQs = $goldFsRepo.fewShots | ForEach-Object { $_.question } | Sort-Object
$diff = Compare-Object $deployedQs $repoQs
if ($diff) { Write-Host "  RESULT: MISMATCH ($($diff.Count) differences)" -ForegroundColor Red; $diff | ForEach-Object { Write-Host "    $($_.SideIndicator) $($_.InputObject)" } }
else { Write-Host "  RESULT: MATCH" -ForegroundColor Green }

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "  Validation Complete" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
