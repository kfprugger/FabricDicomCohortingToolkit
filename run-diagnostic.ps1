param(
    [string]$WorkspaceId = "422cd073-b3fa-4af3-b098-c2142939bd2d",
    [string]$NotebookId = "c4fa2da5-0702-470d-8beb-ea5a32b76199",
    [string]$ReportingLhId = "472ce5de-86fc-40e8-aae0-089124f2cf4a"
)

$tok = az account get-access-token --resource "https://api.fabric.microsoft.com" --query accessToken -o tsv
$h = @{ "Authorization" = "Bearer $tok"; "Content-Type" = "application/json" }

$pyCode = @'
import json
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

WS = "422cd073-b3fa-4af3-b098-c2142939bd2d"
SILVER = "75b2bfee-c605-43a4-925f-215d2406db8c"
REPORTING = "472ce5de-86fc-40e8-aae0-089124f2cf4a"

def abfss(lh, tbl):
    return f"abfss://{WS}@onelake.dfs.fabric.microsoft.com/{lh}/Tables/{tbl}"

results = []
pr = spark.read.format("delta").load(abfss(REPORTING, "PatientReporting"))
pr_sample = pr.select("PatientId", "PatientUUID", "FullName").limit(5).collect()
results.append("=== PatientReporting (first 5) ===")
for r in pr_sample:
    results.append(f"  PatientId={r.PatientId}, UUID={r.PatientUUID}, Name={r.FullName}")
results.append(f"  Total: {pr.count()}, NonNull UUID: {pr.filter('PatientUUID is not null').count()}")

ir = spark.read.format("delta").load(abfss(REPORTING, "ImagingStudyReporting"))
ir_sample = ir.select("StudyId", "PatientUUID", "StudyInstanceUid", "Modality").limit(5).collect()
results.append("=== ImagingStudyReporting (first 5) ===")
for r in ir_sample:
    results.append(f"  StudyId={r.StudyId}, UUID={r.PatientUUID}, SIUid={r.StudyInstanceUid}")
results.append(f"  Total: {ir.count()}, NonNull UUID: {ir.filter('PatientUUID is not null').count()}")

try:
    si = spark.read.format("delta").load(abfss(SILVER, "ImagingStudy"))
    results.append(f"=== Silver ImagingStudy columns: {si.columns}")
    for r in si.select("id", "subject_string", "identifier_string").limit(2).collect():
        results.append(f"  id={r.id}")
        results.append(f"  subject_string={r.subject_string}")
        results.append(f"  identifier_string={r.identifier_string}")
except Exception as e:
    results.append(f"=== Silver ImagingStudy ERROR: {e}")

try:
    sp = spark.read.format("delta").load(abfss(SILVER, "Patient"))
    results.append(f"=== Silver Patient columns: {sp.columns}")
    for r in sp.select("id", "idOrig").limit(3).collect():
        results.append(f"  id={r.id}, idOrig={r.idOrig}")
except Exception as e:
    results.append(f"=== Silver Patient ERROR: {e}")

matched = ir.join(pr, ir["PatientUUID"] == pr["PatientUUID"], "inner").count()
results.append(f"=== Join ImagingStudy->Patient: {matched} / {ir.count()}")

dr = spark.read.format("delta").load(abfss(REPORTING, "DicomFileReporting"))
dr_m = dr.join(ir, dr["StudyInstanceUid"] == ir["StudyInstanceUid"], "inner").count()
results.append(f"=== Join DicomFile->ImagingStudy: {dr_m} / {dr.count()}")

output = "\n".join(results)
print(output)

out_path = f"abfss://{WS}@onelake.dfs.fabric.microsoft.com/{REPORTING}/Files/diag.txt"
dbutils.fs.put(out_path, output, True)
'@

$pyLines = $pyCode -split "`n" | ForEach-Object { "$_`n" }
$ipynb = @{
    nbformat = 4; nbformat_minor = 5
    metadata = @{ kernel_info = @{ name = "synapse_pyspark" }; kernelspec = @{ name = "synapse_pyspark"; display_name = "Synapse PySpark" }; language_info = @{ name = "python" } }
    cells = @(@{ cell_type = "code"; source = $pyLines; metadata = @{}; outputs = @() })
}
$ipynbJson = $ipynb | ConvertTo-Json -Depth 10 -Compress
$ipynbBase64 = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($ipynbJson))

Write-Host "Updating notebook..."
$updateBody = @{ definition = @{ format = "ipynb"; parts = @(@{ path = "notebook-content.py"; payload = $ipynbBase64; payloadType = "InlineBase64" }) } } | ConvertTo-Json -Depth 5
$resp = Invoke-WebRequest -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/items/$NotebookId/updateDefinition" -Headers $h -Method Post -Body $updateBody
if ($resp.StatusCode -eq 202) {
    $opId = ($resp.Headers["x-ms-operation-id"])[0]
    for ($i = 0; $i -lt 12; $i++) { Start-Sleep 5; $lro = Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/operations/$opId" -Headers $h; if ($lro.status -ne "Running") { break } }
    Write-Host "  Update: $($lro.status)"
}

Write-Host "Running..."
$runResp = Invoke-WebRequest -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/items/$NotebookId/jobs/instances?jobType=RunNotebook" -Headers $h -Method Post -Body '{}'
$jobId = (($runResp.Headers["Location"])[0]) -replace '.*/instances/', ''

for ($i = 0; $i -lt 30; $i++) {
    Start-Sleep 15
    $tok2 = az account get-access-token --resource "https://api.fabric.microsoft.com" --query accessToken -o tsv
    $h2 = @{ "Authorization" = "Bearer $tok2"; "Content-Type" = "application/json" }
    $job = Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/items/$NotebookId/jobs/instances/$jobId" -Headers $h2
    Write-Host "  [$i] $($job.status)"
    if ($job.status -in @("Completed","Failed","Cancelled")) { break }
}

if ($job.status -eq "Completed") {
    Write-Host "`nReading diagnostic output from OneLake..."
    Start-Sleep 5
    $tok3 = az account get-access-token --resource "https://storage.azure.com" --query accessToken -o tsv
    $hF = @{ "Authorization" = "Bearer $tok3" }
    try {
        $output = Invoke-RestMethod -Uri "https://onelake.dfs.fabric.microsoft.com/$WorkspaceId/$ReportingLhId/Files/diag.txt" -Headers $hF -Method Get
        Write-Host $output
    } catch {
        Write-Host "Read error: $($_.ErrorDetails.Message)"
    }
}
