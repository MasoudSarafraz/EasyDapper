param(
    [Parameter(Mandatory=$true)][string]$EvidenceDirectory,
    [Parameter(Mandatory=$true)][string]$BeforeLibrary,
    [Parameter(Mandatory=$true)][string]$OptimizedLibrary,
    [string]$OriginalLibrary,
    [ValidateSet('query','sql','diagnostics','sustained','read-check')][string]$Phase = 'query',
    [int]$Repetitions = 3,
    [double]$Seconds = 5,
    [double]$WarmupSeconds = 2
)
$ErrorActionPreference = 'Stop'
$consumer = Join-Path $PSScriptRoot 'bin/Release/net8.0/EasyDapper.LoadTests.dll'
$consumerHash = (Get-FileHash -LiteralPath $consumer).Hash
$libraryDestination = Join-Path $PSScriptRoot 'bin/Release/net8.0/EasyDapper.dll'
New-Item -ItemType Directory -Path $EvidenceDirectory -Force | Out-Null
$libraries = [ordered]@{ before = (Resolve-Path -LiteralPath $BeforeLibrary).Path; optimized = (Resolve-Path -LiteralPath $OptimizedLibrary).Path }
if ($OriginalLibrary) { $libraries.original = (Resolve-Path -LiteralPath $OriginalLibrary).Path }
$manifest = [System.Collections.Generic.List[object]]::new()
$phaseManifest = Join-Path $EvidenceDirectory "manifest-$Phase.json"
if (Test-Path -LiteralPath $phaseManifest) { throw "Existing phase evidence must be preserved; use a new EvidenceDirectory: $phaseManifest" }

function Run-Case([string]$Label, [string]$Scenario, [int]$Workers, [int]$Clauses, [int]$Repeat, [string]$Backend, [bool]$Diagnostics) {
    $libraryPath = if ($Label -eq 'dapper') { $libraries.optimized } else { $libraries[$Label] }
    Copy-Item -LiteralPath $libraryPath -Destination $libraryDestination -Force
    $libraryHash = (Get-FileHash -LiteralPath $libraryDestination).Hash
    if ($libraryHash -ne (Get-FileHash -LiteralPath $libraryPath).Hash) { throw 'Library copy mismatch.' }
    if ((Get-FileHash -LiteralPath $consumer).Hash -ne $consumerHash) { throw 'Consumer changed or was rebuilt.' }
    $caseName = "$Phase-$Scenario-c$Clauses-w$Workers-$Label-r$Repeat"
    $jsonPath = Join-Path $EvidenceDirectory "$caseName.json"
    $logPath = Join-Path $EvidenceDirectory "$caseName.log"
    if ((Test-Path -LiteralPath $jsonPath) -or (Test-Path -LiteralPath $logPath)) {
        throw "Existing case evidence must be preserved; use a new EvidenceDirectory: $caseName"
    }
    if ($Phase -eq 'query') {
        $arguments = @($consumer, 'query-build', '--workers', "$Workers", '--clauses', "$Clauses", '--seconds', "$Seconds", '--label', $Label, '--output', $jsonPath)
    } else {
        $arguments = @($consumer, 'sql', '--backend', $Backend, '--scenario', $Scenario, '--workers', "$Workers", '--clauses', "$Clauses", '--seconds', "$Seconds", '--warmup', "$WarmupSeconds", '--label', $Label, '--provider-timings', $Diagnostics.ToString().ToLowerInvariant(), '--output', $jsonPath)
    }
    & dotnet @arguments > $logPath 2>&1
    $exitCode = $LASTEXITCODE
    if (-not (Test-Path -LiteralPath $jsonPath)) { throw "No report for $caseName; inspect $logPath" }
    $result = Get-Content -LiteralPath $jsonPath -Raw | ConvertFrom-Json
    if ($Phase -eq 'query') {
        $passed = $exitCode -eq 0 -and $result.Errors -eq 0 -and $result.Success -gt 0
        $ops = $result.OperationsPerSecond
    } else {
        $passed = $exitCode -eq 0 -and $result.passed -eq $true
        $ops = $result.benchmark.measured.operations.successfulOperationsPerSecond
        if ($result.fixture.dropped -ne $true -or $result.fixture.cleanupError) { throw "Fixture cleanup failed for $caseName" }
    }
    $manifest.Add([pscustomobject]@{ Case = $caseName; Label = $Label; Scenario = $Scenario; Workers = $Workers; Clauses = $Clauses; Repeat = $Repeat; Backend = $Backend; Diagnostics = $Diagnostics; ExitCode = $exitCode; Passed = $passed; LibraryHash = $libraryHash; ConsumerHash = $consumerHash; Report = $jsonPath; Log = $logPath })
    ConvertTo-Json -InputObject @($manifest.ToArray()) -Depth 6 | Set-Content -LiteralPath $phaseManifest -Encoding utf8
    Write-Output "$caseName passed=$passed ops/s=$([math]::Round($ops,2))"
    # Original sharing failures are evidence of provider-unsafe concurrency, not
    # valid throughput. Current versions and the matched reference must succeed.
    if (-not $passed -and $Label -ne 'original') { throw "Validated version failed: $caseName" }
}

if ($Phase -eq 'query') {
    foreach ($clauses in @(1,64,512)) { foreach ($workers in @(1,8)) {
        for ($repeat = 1; $repeat -le $Repetitions; $repeat++) {
            $labels = @($libraries.Keys)
            if ($repeat % 2 -eq 0) { [array]::Reverse($labels) }
            foreach ($label in $labels) { Run-Case $label 'query-build' $workers $clauses $repeat 'easy' $false }
        }
    } }
} elseif ($Phase -eq 'sql') {
    foreach ($scenario in @('shared-read','local-read','local-mixed')) { foreach ($workers in @(1,16,64,128,256)) {
        for ($repeat = 1; $repeat -le $Repetitions; $repeat++) {
            $labels = @($libraries.Keys) + @('dapper')
            if ($repeat % 2 -eq 0) { [array]::Reverse($labels) }
            foreach ($label in $labels) {
                if ($label -eq 'original' -and ($repeat -ne 1 -or $workers -in @(16,128))) { continue }
                Run-Case $label $scenario $workers 1 $repeat $(if ($label -eq 'dapper') { 'dapper' } else { 'easy' }) $false
            }
        }
    } }
    foreach ($clauses in @(1,64,128)) { foreach ($workers in @(1,64,256)) {
        for ($repeat = 1; $repeat -le $Repetitions; $repeat++) {
            $labels = @($libraries.Keys) + @('dapper')
            if ($repeat % 2 -eq 0) { [array]::Reverse($labels) }
            foreach ($label in $labels) {
                if ($label -eq 'original' -and $repeat -ne 1) { continue }
                Run-Case $label 'local-query' $workers $clauses $repeat $(if ($label -eq 'dapper') { 'dapper' } else { 'easy' }) $false
            }
        }
    } }
} elseif ($Phase -eq 'read-check') {
    # Resolve the short-window basic-read differences without mixing these
    # longer observations into the original three-repeat SQL matrix.
    foreach ($workers in @(16,64)) {
        for ($repeat = 1; $repeat -le $Repetitions; $repeat++) {
            $labels = @('before','optimized')
            if ($repeat % 2 -eq 0) { [array]::Reverse($labels) }
            foreach ($label in $labels) { Run-Case $label 'local-read' $workers 1 $repeat 'easy' $false }
        }
    }
} elseif ($Phase -eq 'sustained') {
    # Longer correctness/drain checks of the frozen optimized library. These
    # single runs are separate from the repeated throughput comparison matrix.
    foreach ($scenario in @('shared-read','local-mixed','local-query')) {
        Run-Case 'optimized' $scenario 256 $(if ($scenario -eq 'local-query') { 64 } else { 1 }) 1 'easy' $false
    }
} else {
    foreach ($scenario in @('shared-read','local-read','local-query')) {
        foreach ($label in @('before','optimized','dapper')) {
            Run-Case $label $scenario 128 $(if ($scenario -eq 'local-query') { 64 } else { 1 }) 1 $(if ($label -eq 'dapper') { 'dapper' } else { 'easy' }) $true
        }
    }
}
Copy-Item -LiteralPath $libraries.optimized -Destination $libraryDestination -Force
Write-Output "Completed $($manifest.Count) cases; consumer remained $consumerHash"
