function Set-DataAgentNativeSchemaSelection {
    param(
        [Parameter(Mandatory)][string]$WorkspaceId,
        [Parameter(Mandatory)][string]$DataAgentId,
        [Parameter(Mandatory)][string]$DatasourceId,
        [Parameter(Mandatory)][AllowEmptyCollection()][string[]]$Tables,
        [string[]]$Functions = @(),
        [string]$Schema = 'dbo',
        [switch]$VerifyOnly,
        [switch]$Published,
        # A just-attached datasource syncs its element tree asynchronously.
        [int]$SyncWaitSeconds = 300,
        [Parameter(Mandatory)][scriptblock]$InvokeApi
    )
    if ($Published -and -not $VerifyOnly) { throw 'Published selections are read-only; use -VerifyOnly.' }
    $stagePrefix = if ($Published) { '' } else { 'staging/' }
    $endpoint = "/workspaces/$WorkspaceId/dataAgents/$DataAgentId/${stagePrefix}datasources/$DatasourceId/elements"
    function Read-NativeObjects([string]$RootId = '', [string]$CurrentSchema = '') {
        $continuation = ''
        do {
            $query = @()
            if ($RootId) { $query += "rootId=$([uri]::EscapeDataString($RootId))" }
            if ($continuation) { $query += "continuationToken=$([uri]::EscapeDataString($continuation))" }
            $uri = $endpoint + $(if ($query.Count) { '?' + ($query -join '&') } else { '' })
            $page = & $InvokeApi 'GET' $uri $null
            foreach ($element in @($page.value)) {
                $nextSchema = if ($element.type -eq 'Schema') { [string]$element.displayName } else { $CurrentSchema }
                if ($element.type -in @('Table', 'Function')) {
                    [pscustomobject]@{ Element = $element; Schema = $nextSchema }
                } elseif ($element.type -in @('Schemas', 'Schema', 'Tables', 'Functions') -and $element.hasSubElements) {
                    Read-NativeObjects -RootId ([string]$element.id) -CurrentSchema $nextSchema
                }
            }
            $continuation = if ($page.PSObject.Properties['continuationToken']) { [string]$page.continuationToken } else { '' }
        } while ($continuation)
    }
    # Existing tables and functions can be absent from the first reads of a datasource attached
    # moments ago. Re-read within a bounded window before calling a target missing; nothing is
    # selected until every target resolves.
    $targets = @($Tables | ForEach-Object { @{ Name = $_; Type = 'Table' } }) + @($Functions | ForEach-Object { @{ Name = $_; Type = 'Function' } })
    $attempts = [Math]::Max(1, [int][Math]::Ceiling($SyncWaitSeconds / 15) + 1)
    for ($attempt = 1; ; $attempt++) {
        $objects = @(Read-NativeObjects)
        $problem = ''
        foreach ($target in $targets) {
            $match = @($objects | Where-Object {
                $_.Element.type -eq $target.Type -and $_.Element.displayName -eq $target.Name -and
                ($target.Type -ne 'Table' -or -not $_.Schema -or $_.Schema -eq $Schema)
            })
            if ($match.Count -ne 1 -or -not $match[0].Element.id -or $match[0].Element.state -ne 'Available') {
                $problem = "Missing, ambiguous, or unavailable native $($target.Type) '$($target.Name)' in datasource '$DatasourceId'."
                break
            }
        }
        if (-not $problem) { break }
        if ($attempt -ge $attempts) { throw "$problem No selection updates were made." }
        Start-Sleep -Seconds 15
    }
    if (-not $VerifyOnly) {
    foreach ($object in $objects) {
        $element = $object.Element
        $selected = if ($element.type -eq 'Function') { $element.displayName -in $Functions } else {
            $element.displayName -in $Tables -and (-not $object.Schema -or $object.Schema -eq $Schema)
        }
        if ([bool]$element.isSelected -eq $selected) { continue }
        $null = & $InvokeApi 'PATCH' "${endpoint}?id=$([uri]::EscapeDataString([string]$element.id))" @{ isSelected = $selected }
    }
    }
    $actual = if ($VerifyOnly) { $objects } else { @(Read-NativeObjects) }
    $actualTables = @($actual | Where-Object { $_.Element.type -eq 'Table' -and $_.Element.isSelected } | ForEach-Object { $_.Element.displayName } | Sort-Object -Unique)
    $actualFunctions = @($actual | Where-Object { $_.Element.type -eq 'Function' -and $_.Element.isSelected } | ForEach-Object { $_.Element.displayName } | Sort-Object -Unique)
    if (($actualTables -join '|') -ne (($Tables | Sort-Object -Unique) -join '|') -or
        ($actualFunctions -join '|') -ne (($Functions | Sort-Object -Unique) -join '|')) {
        throw "Native datasource selections did not match the requested table/function contract: $DatasourceId"
    }
}

