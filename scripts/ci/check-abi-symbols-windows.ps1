param(
    [Parameter(Mandatory = $true)]
    [string]$BuildRoot
)

$ErrorActionPreference = "Stop"

function Write-AdvisoryWarning {
    param([string]$Message)
    Write-Error "check-abi-symbols-windows: WARNING: $Message" -ErrorAction Continue
    Write-Output "::warning file=abi/libwirelog-1.0.windows.symbols,title=Windows ABI advisory::$Message"
}

if (-not [System.Runtime.InteropServices.RuntimeInformation]::IsOSPlatform(
        [System.Runtime.InteropServices.OSPlatform]::Windows)) {
    Write-Error "check-abi-symbols-windows: SKIP: host is not Windows" -ErrorAction Continue
    exit 0
}

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = (Resolve-Path (Join-Path $ScriptDir "../..")).Path
$Allowlist = Join-Path $RepoRoot "abi/libwirelog-1.0.windows.symbols"

$Dll = $null
$Candidates = @(
    (Join-Path $BuildRoot "wirelog-1.dll"),
    (Join-Path $BuildRoot "wirelog.dll"),
    (Join-Path $BuildRoot "libwirelog.dll")
)

foreach ($Candidate in $Candidates) {
    if (Test-Path $Candidate) {
        $Dll = $Candidate
        break
    }
}

if (-not $Dll) {
    $Glob = Get-ChildItem -Path $BuildRoot -Filter "*wirelog*.dll" -File -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($Glob) {
        $Dll = $Glob.FullName
    }
}

if (-not $Dll) {
    Write-Error "check-abi-symbols-windows: SKIP: wirelog DLL not found in $BuildRoot" -ErrorAction Continue
    exit 0
}

if (-not (Test-Path $Allowlist)) {
    Write-AdvisoryWarning "allowlist missing: $Allowlist"
    exit 0
}

$Dumpbin = Get-Command dumpbin.exe -ErrorAction SilentlyContinue
if (-not $Dumpbin) {
    Write-Error "check-abi-symbols-windows: SKIP: dumpbin.exe not found on PATH" -ErrorAction Continue
    exit 0
}

$Actual = @(& $Dumpbin.Source /EXPORTS $Dll |
    ForEach-Object {
        if ($_ -match '^\s*\d+\s+[0-9A-Fa-f]+\s+[0-9A-Fa-f]+\s+([A-Za-z_][A-Za-z0-9_]*)\b') {
            $Matches[1]
        }
    } |
    Where-Object { $_ -like 'wirelog_*' } |
    Sort-Object -Unique)

$Expected = @(Get-Content $Allowlist | Where-Object { $_ -ne "" } | Sort-Object -Unique)

$ActualText = ($Actual -join "`n")
$ExpectedText = ($Expected -join "`n")

if ($ActualText -eq $ExpectedText) {
    Write-Output "check-abi-symbols-windows: OK; $($Actual.Count) exported symbols match allowlist"
    exit 0
}

Write-AdvisoryWarning "exported symbols differ from abi/libwirelog-1.0.windows.symbols"
Compare-Object -ReferenceObject $Expected -DifferenceObject $Actual | Out-String | Write-Error -ErrorAction Continue
Write-Error "Regenerate after a deliberate public ABI change:" -ErrorAction Continue
Write-Error "  dumpbin /EXPORTS $Dll | <extract wirelog_* names> | sort > abi/libwirelog-1.0.windows.symbols" -ErrorAction Continue
exit 0
