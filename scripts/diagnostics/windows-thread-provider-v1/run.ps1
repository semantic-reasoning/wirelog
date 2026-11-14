# Standalone collection only. No engine target, provider permission, or PE parser.
[CmdletBinding()]
param([Parameter(Mandatory = $true)][string]$OutputDirectory)
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$baseCommit = 'a8dfa5b35618a874b8f34fd25efbca0ada99f009'
$wheelUrl = 'https://files.pythonhosted.org/packages/07/68/b0117422eb0a46d9d8d9e328f0c5b5c835179bfc058688bca35c90c89eba/meson-1.12.0-py3-none-any.whl'
$wheelHash = '71f133147fa0fcfe8f4df49fa1045771064947834538409e5d97b3613aac8b4e'
$root = [IO.Path]::GetFullPath($env:GITHUB_WORKSPACE)
$expectedOutput = Join-Path $env:RUNNER_TEMP "wirelog-thread-provider-$env:GITHUB_RUN_ID-$env:GITHUB_RUN_ATTEMPT"
if ([IO.Path]::GetFullPath($OutputDirectory) -ne [IO.Path]::GetFullPath($expectedOutput)) {
    throw 'Unexpected diagnostic output directory'
}
if (Test-Path -LiteralPath $OutputDirectory) { throw 'Output directory already exists' }
$publish = Join-Path $OutputDirectory 'publish'
$work = Join-Path $OutputDirectory 'work'
$commands = Join-Path $publish 'commands'
New-Item -ItemType Directory -Path $commands, $work | Out-Null
$manifest = [ordered]@{
    schema = 1; base_commit = $baseCommit; commit = $env:GITHUB_SHA
    run_id = $env:GITHUB_RUN_ID; run_attempt = $env:GITHUB_RUN_ATTEMPT
    collection_status = 'INCOMPLETE'; qualification = 'OFF'
    provider_binding = 'UNKNOWN'; retained_engine_lifetime = 'NOT_TESTED'
    selections = @(); profiles = @(); errors = @()
    both_profiles_pass = $false
}
$counter = 0
$failed = $false

function Save-Text([string]$Path, [string]$Text) {
    if ([Text.Encoding]::UTF8.GetByteCount($Text) -gt 1048576) {
        throw 'Text evidence exceeds individual byte limit'
    }
    [IO.File]::WriteAllText($Path, $Text, [Text.UTF8Encoding]::new($false))
}
function Save-Json([string]$Path, $Value) {
    Save-Text $Path ($Value | ConvertTo-Json -Depth 16)
}
function File-Record([string]$Path) {
    $item = Get-Item -LiteralPath $Path
    return [ordered]@{ path = $item.FullName; size = $item.Length
        sha256 = (Get-FileHash -LiteralPath $item.FullName -Algorithm SHA256).Hash.ToLowerInvariant()
        version = $item.VersionInfo.FileVersion }
}

# Drain bounded chunks from both streams. No ReadToEnd/line accumulator, shell
# evaluation, or unbounded compiler diagnostic printing to workflow commands.
function Invoke-Bounded([string]$Executable, [string[]]$Arguments,
    [string]$Directory, [int]$Seconds = 90) {
    $start = [Diagnostics.ProcessStartInfo]::new()
    $start.FileName = $Executable
    $start.WorkingDirectory = $Directory
    $start.UseShellExecute = $false
    $start.RedirectStandardOutput = $true
    $start.RedirectStandardError = $true
    foreach ($argument in $Arguments) { $start.ArgumentList.Add($argument) }
    $process = [Diagnostics.Process]::new()
    $process.StartInfo = $start
    $out = [Text.StringBuilder]::new()
    $err = [Text.StringBuilder]::new()
    $outBuffer = [char[]]::new(4096)
    $errBuffer = [char[]]::new(4096)
    $outDone = $false; $errDone = $false; $limited = $false; $timedOut = $false
    if (-not $process.Start()) { throw 'Cannot start diagnostic tool' }
    $clock = [Diagnostics.Stopwatch]::StartNew()
    $outTask = $process.StandardOutput.ReadAsync($outBuffer, 0, $outBuffer.Length)
    $errTask = $process.StandardError.ReadAsync($errBuffer, 0, $errBuffer.Length)
    try {
        while (-not ($outDone -and $errDone)) {
            if (-not $outDone -and $outTask.IsCompleted) {
                $n = $outTask.GetAwaiter().GetResult()
                if ($n -eq 0) { $outDone = $true } else {
                    if ($out.Length + $n -le 262144) { [void]$out.Append($outBuffer, 0, $n) } else { $limited = $true }
                    $outTask = $process.StandardOutput.ReadAsync($outBuffer, 0, $outBuffer.Length)
                }
            }
            if (-not $errDone -and $errTask.IsCompleted) {
                $n = $errTask.GetAwaiter().GetResult()
                if ($n -eq 0) { $errDone = $true } else {
                    if ($err.Length + $n -le 262144) { [void]$err.Append($errBuffer, 0, $n) } else { $limited = $true }
                    $errTask = $process.StandardError.ReadAsync($errBuffer, 0, $errBuffer.Length)
                }
            }
            if ($clock.Elapsed.TotalSeconds -gt $Seconds) { $timedOut = $true }
            if (($limited -or $timedOut) -and -not $process.HasExited) {
                $process.Kill($true) # Whole child process tree, never one worker thread.
            }
            if ($clock.Elapsed.TotalSeconds -gt $Seconds + 5) {
                throw 'Cannot drain terminated diagnostic process safely'
            }
            Start-Sleep -Milliseconds 10
        }
        if (-not $process.WaitForExit(3000)) {
            $process.Kill($true)
            throw 'Diagnostic child did not exit'
        }
        return [ordered]@{ executable = $Executable; arguments = $Arguments
            exit_code = $process.ExitCode; stdout = $out.ToString(); stderr = $err.ToString()
            output_limited = $limited; timed_out = $timedOut }
    } finally {
        if (-not $process.HasExited) { $process.Kill($true); [void]$process.WaitForExit(3000) }
        $process.Dispose()
    }
}
function Invoke-Recorded([string]$Label, [string]$Executable,
    [string[]]$Arguments, [string]$Directory, [int]$Seconds = 90) {
    if ($Label -notmatch '^[a-zA-Z0-9_-]+$') { throw 'Invalid command label' }
    $script:counter += 1
    $prefix = '{0:D3}-{1}' -f $script:counter, $Label
    $r = Invoke-Bounded $Executable $Arguments $Directory $Seconds
    Save-Text (Join-Path $commands "$prefix.stdout.txt") $r.stdout
    Save-Text (Join-Path $commands "$prefix.stderr.txt") $r.stderr
    Save-Json (Join-Path $commands "$prefix.command.json") ([ordered]@{
        executable = $r.executable; arguments = $r.arguments; directory = $Directory
        exit_code = $r.exit_code; output_limited = $r.output_limited; timed_out = $r.timed_out
    })
    return $r
}
function Command-Succeeded($Result) {
    return $Result.exit_code -eq 0 -and -not $Result.output_limited -and -not $Result.timed_out
}
function Require-Command($Result) {
    if (-not (Command-Succeeded $Result)) { throw 'Diagnostic command failed; see bounded command evidence' }
}
function Copy-Evidence([string]$Source, [string]$Destination, [long]$Limit = 4194304) {
    $item = Get-Item -LiteralPath $Source
    if ($item.Length -gt $Limit) { throw 'Individual generated evidence exceeds limit' }
    Copy-Item -LiteralPath $Source -Destination $Destination
}

try {
    Save-Json (Join-Path $publish 'manifest.json') $manifest
    $git = (Get-Command git.exe -CommandType Application).Source
    $source = Invoke-Recorded 'frozen-selection-source' $git @('show', "${baseCommit}:wirelog/meson.build") $root
    Require-Command $source
    $selectionDir = Join-Path $PSScriptRoot 'selection'
    $sourceMatch = [regex]::Match($source.stdout.Replace("`r`n", "`n"), "(?s)c11_threads_test = '''(.*?)'''")
    $localSelection = [IO.File]::ReadAllText((Join-Path $selectionDir 'meson.build')).Replace("`r`n", "`n")
    $localMatch = [regex]::Match($localSelection, "(?s)c11_threads_test = '''(.*?)'''")
    if (-not $sourceMatch.Success -or -not $localMatch.Success -or
        $sourceMatch.Groups[1].Value -cne $localMatch.Groups[1].Value) {
        throw 'Detection literal differs from exact frozen base'
    }
    Save-Text (Join-Path $publish 'frozen-detection.c') $sourceMatch.Groups[1].Value
    Save-Json (Join-Path $publish 'inputs.json') @(
        (File-Record (Join-Path $PSScriptRoot 'probe.c')),
        (File-Record (Join-Path $PSScriptRoot 'transport.c')),
        (File-Record (Join-Path $PSScriptRoot 'run.ps1')),
        (File-Record (Join-Path $selectionDir 'meson.build')),
        (File-Record (Join-Path $root '.github/workflows/diagnostic-windows-thread-provider-v1.yml'))
    )
    $vswhere = Join-Path ${env:ProgramFiles(x86)} 'Microsoft Visual Studio/Installer/vswhere.exe'
    $found = Invoke-Recorded 'vswhere' $vswhere @('-latest', '-products', '*', '-requires',
        'Microsoft.VisualStudio.Component.VC.Tools.x86.x64', '-property', 'installationPath') $root
    Require-Command $found
    $vs = $found.stdout.Trim()
    if (-not $vs -or $vs -match '["\r\n]') { throw 'Invalid Visual Studio selection' }
    $vcvars = Join-Path $vs 'VC/Auxiliary/Build/vcvars64.bat'
    if (-not (Test-Path -LiteralPath $vcvars -PathType Leaf)) { throw 'Missing selected vcvars64' }
    # Internal bounded env transfer only. Never persist or print `set` output.
    $vcCommand = 'call "' + $vcvars + '" >nul && set'
    $envResult = Invoke-Bounded (Join-Path $env:SystemRoot 'System32/cmd.exe') @('/d', '/s', '/c', $vcCommand) $root
    Require-Command $envResult
    $envNames = @('PATH', 'INCLUDE', 'LIB', 'LIBPATH', 'WindowsSdkDir', 'WindowsSDKVersion',
        'UCRTVersion', 'UniversalCRTSdkDir', 'VCToolsInstallDir', 'VCToolsVersion',
        'VCINSTALLDIR', 'VSINSTALLDIR', 'VSCMD_ARG_TGT_ARCH', 'VSCMD_ARG_HOST_ARCH')
    foreach ($line in ($envResult.stdout -split "`r?`n")) {
        $equals = $line.IndexOf('=')
        if ($equals -gt 0) {
            $name = $line.Substring(0, $equals)
            if ($envNames -contains $name) {
                [Environment]::SetEnvironmentVariable($name, $line.Substring($equals + 1), 'Process')
            }
        }
    }
    $envResult = $null
    # Do not silently inherit extra compiler arguments from a runner account.
    foreach ($flagVariable in @('CL', '_CL_', 'CFLAGS', 'CPPFLAGS', 'LDFLAGS')) {
        if ([Environment]::GetEnvironmentVariable($flagVariable, 'Process')) {
            throw "Unsupported pre-existing compiler-argument environment: $flagVariable"
        }
    }
    $env:CC = 'cl'; $env:CXX = 'cl'; $env:VSLANG = '1033'
    $cl = (Get-Command cl.exe -CommandType Application).Source
    $link = (Get-Command link.exe -CommandType Application).Source
    $dumpbin = (Get-Command dumpbin.exe -CommandType Application).Source
    $uv = (Get-Command uv.exe -CommandType Application).Source
    $python = (Get-Command python.exe -CommandType Application).Source
    if (-not $cl.StartsWith($vs + '\', [StringComparison]::OrdinalIgnoreCase) -or
        -not $link.StartsWith($vs + '\', [StringComparison]::OrdinalIgnoreCase) -or
        -not $dumpbin.StartsWith($vs + '\', [StringComparison]::OrdinalIgnoreCase)) {
        throw 'Tools are not from the selected Visual Studio installation'
    }
    Save-Json (Join-Path $publish 'toolchain.json') ([ordered]@{
        tools = @((File-Record $cl), (File-Record $link), (File-Record $dumpbin),
            (File-Record $vswhere), (File-Record $vcvars), (File-Record $uv), (File-Record $python))
        toolset = $env:VCToolsVersion; sdk = $env:WindowsSDKVersion; ucrt = $env:UCRTVersion
        host_arch = $env:VSCMD_ARG_HOST_ARCH; target_arch = $env:VSCMD_ARG_TGT_ARCH
        os = [Environment]::OSVersion.VersionString; architecture = [Runtime.InteropServices.RuntimeInformation]::OSArchitecture.ToString()
        runner_os = $env:RUNNER_OS; image_os = $env:ImageOS; image_version = $env:ImageVersion
        default_engine_lto = $true; probe_lto = $false; qualification = 'OFF'
    })
    # One immutable wheel, no dependency resolver/install of Ninja or extras.
    $wheel = Join-Path $work 'meson-1.12.0-py3-none-any.whl'
    $handler = [Net.Http.HttpClientHandler]::new(); $handler.AllowAutoRedirect = $false
    $client = [Net.Http.HttpClient]::new($handler); $client.Timeout = [TimeSpan]::FromSeconds(30)
    try {
        $response = $client.GetAsync($wheelUrl, [Net.Http.HttpCompletionOption]::ResponseHeadersRead).GetAwaiter().GetResult()
        try {
            $response.EnsureSuccessStatusCode() | Out-Null
            $stream = $response.Content.ReadAsStreamAsync().GetAwaiter().GetResult()
            $file = [IO.File]::Create($wheel)
            try {
                $buffer = [byte[]]::new(32768); $total = 0
                $downloadClock = [Diagnostics.Stopwatch]::StartNew()
                while ($true) {
                    $read = $stream.ReadAsync($buffer, 0, $buffer.Length)
                    if (-not $read.Wait(30000) -or $downloadClock.Elapsed.TotalSeconds -gt 60) { throw 'Meson download timed out' }
                    $n = $read.GetAwaiter().GetResult()
                    if (-not $n) { break }
                    $total += $n
                    if ($total -gt 8388608) { throw 'Meson wheel exceeds declared bound' }
                    $file.Write($buffer, 0, $n)
                }
            } finally { $file.Dispose(); $stream.Dispose() }
        } finally { $response.Dispose() }
    } finally { $client.Dispose(); $handler.Dispose() }
    if ((Get-FileHash -LiteralPath $wheel -Algorithm SHA256).Hash.ToLowerInvariant() -cne $wheelHash) {
        throw 'Meson wheel hash mismatch'
    }
    Save-Json (Join-Path $publish 'package.json') ([ordered]@{
        name = 'meson'; version = '1.12.0'; url = $wheelUrl; sha256 = $wheelHash
        dependencies = @(); extras = @(); installed_on_host = $false
    })
    $venv = Join-Path $work 'tools'
    Require-Command (Invoke-Recorded 'uv-venv' $uv @('venv', '--no-managed-python', '--python', $python, $venv) $work)
    $venvPython = Join-Path $venv 'Scripts/python.exe'
    Require-Command (Invoke-Recorded 'uv-meson' $uv @('pip', 'install', '--offline', '--no-deps', '--python', $venvPython, $wheel) $work)
    $meson = Join-Path $venv 'Scripts/meson.exe'
    Require-Command (Invoke-Recorded 'meson-version' $meson @('--version') $work)
    foreach ($configuration in @('DEFAULT', 'EXPLICIT_C11')) {
        $dir = Join-Path $work $configuration
        $selectionArguments = @('setup', $dir, $selectionDir, '--backend=vs', '--buildtype=release', '-Doptimization=s', '-Db_lto=true')
        if ($configuration -eq 'EXPLICIT_C11') { $selectionArguments += '-Dc_std=c11' }
        $setup = Invoke-Recorded "meson-$configuration" $meson $selectionArguments $work
        $row = [ordered]@{ configuration = $configuration; status = 'SETUP_FAILED'
            check_success = $false; selected_backend = 'UNKNOWN'; setup_exit = $setup.exit_code }
        $phaseDir = Join-Path $publish $configuration
        New-Item -ItemType Directory -Path $phaseDir | Out-Null
        $log = Join-Path $dir 'meson-logs/meson-log.txt'
        if (Test-Path -LiteralPath $log) { Copy-Evidence $log (Join-Path $phaseDir 'meson-log.txt') 1048576 }
        if (Command-Succeeded $setup) {
            $resultPath = Join-Path $dir 'selection-result.h'
            $text = [IO.File]::ReadAllText($resultPath)
            $match = [regex]::Match($text, '(?m)^#define WL_DIAGNOSTIC_C11_SUPPORTED ([01])\s*$')
            if (-not $match.Success) { throw 'Missing actual detection result' }
            $row.status = 'CHECK_COMPLETED'
            $row.check_success = $match.Groups[1].Value -eq '1'
            $row.selected_backend = if ($row.check_success) { 'C11' } else { 'Windows-native' }
            Copy-Evidence $resultPath (Join-Path $phaseDir 'selection-result.h')
            foreach ($kind in @('compilers', 'buildoptions')) {
                $inspection = Invoke-Recorded "meson-$configuration-$kind" $meson @('introspect', "--$kind", $dir) $work
                Require-Command $inspection
                Save-Text (Join-Path $phaseDir "$kind.json") $inspection.stdout
            }
        }
        $manifest.selections += $row
        if ($row.status -ne 'CHECK_COMPLETED' -or -not $row.check_success) {
            $manifest.profiles += [ordered]@{ name = "c11-$configuration"
                compile = 'NOT_RUN'; link = 'NOT_RUN'; runtime = 'NOT_RUN'
                abi = 'NOT_COLLECTED'; provider_binding = 'UNKNOWN'; errors = @()
                reason = if ($row.status -eq 'CHECK_COMPLETED') { 'DETECTION_NO' } else { 'SETUP_FAILED' } }
        }
    }
    # Native is always collected; no failed C11 selection skips this row.
    $profileSpecs = @([ordered]@{ name = 'native-DEFAULT'; c11 = $false; explicit = $false })
    foreach ($row in $manifest.selections) {
        if ($row.status -eq 'CHECK_COMPLETED' -and $row.check_success) {
            $profileSpecs += [ordered]@{ name = "c11-$($row.configuration)"; c11 = $true; explicit = ($row.configuration -eq 'EXPLICIT_C11') }
        }
    }
    foreach ($spec in $profileSpecs) {
        $profileDir = Join-Path $publish $spec.name
        New-Item -ItemType Directory -Path $profileDir | Out-Null
        $row = [ordered]@{ name = $spec.name; compile = 'NOT_RUN'; link = 'NOT_RUN'; runtime = 'NOT_RUN'
            abi = 'UNQUALIFIED'; provider_binding = 'UNKNOWN'; errors = @() }
        try {
            $objects = @(); $includeFiles = @{}
            foreach ($unit in @('probe', 'transport')) {
                $src = Join-Path $PSScriptRoot "$unit.c"
                $obj = Join-Path $profileDir "$unit.obj"
                $listing = Join-Path $profileDir "$unit.asm"
                $flags = @('/nologo', '/TC', '/c', '/MD', '/O1', '/Ob0', '/GL-', '/W4', '/WX',
                    '/D__STDC_NO_ATOMICS__', '/showIncludes', '/FAcs', "/Fa$listing", "/Fo$obj")
                if ($spec.explicit) { $flags += '/std:c11' }
                if ($spec.c11) { $flags += '/DWL_DIAG_C11' }
                $compile = Invoke-Recorded "$($spec.name)-$unit-compile" $cl ($flags + $src) $profileDir
                foreach ($line in (($compile.stdout + "`n" + $compile.stderr) -split "`r?`n")) {
                    $inc = [regex]::Match($line, '^Note: including file:\s+(.+?)\s*$')
                    if ($inc.Success) {
                        $header = [IO.Path]::GetFullPath($inc.Groups[1].Value)
                        if (-not ($header.StartsWith($vs + '\', [StringComparison]::OrdinalIgnoreCase) -or
                            $header.StartsWith($env:WindowsSdkDir, [StringComparison]::OrdinalIgnoreCase))) {
                            throw 'Unexpected header outside selected toolchain/SDK'
                        }
                        $includeFiles[$header] = $true
                    }
                }
                Require-Command $compile
                # Capture actual primary declarations as generated TU text, not copied fake types.
                $preprocessed = Join-Path $profileDir "$unit.i"
                $preFlags = @('/nologo', '/TC', '/MD', '/P', "/Fi$preprocessed", '/D__STDC_NO_ATOMICS__')
                if ($spec.explicit) { $preFlags += '/std:c11' }
                if ($spec.c11) { $preFlags += '/DWL_DIAG_C11' }
                Require-Command (Invoke-Recorded "$($spec.name)-$unit-preprocess" $cl ($preFlags + $src) $profileDir)
                if ((Get-Item -LiteralPath $preprocessed).Length -gt 4194304) { throw 'Preprocessed TU exceeds evidence bound' }
                $objects += $obj
            }
            $row.compile = 'PASS'
            Save-Json (Join-Path $profileDir 'headers.json') @($includeFiles.Keys | Sort-Object | ForEach-Object { File-Record $_ })
            if (-not $includeFiles.Count) { throw 'No actual include dependencies captured' }
            $exe = Join-Path $profileDir 'probe.exe'
            $linkArgs = @('/NOLOGO', '/DEBUG', '/VERBOSE:LIB', "/OUT:$exe", "/PDB:$(Join-Path $profileDir 'probe.pdb')", "/MAP:$(Join-Path $profileDir 'probe.map')") + $objects + 'kernel32.lib'
            $linked = Invoke-Recorded "$($spec.name)-link" $link $linkArgs $profileDir
            Require-Command $linked
            $row.link = 'PASS'
            $libraries = @{}
            foreach ($libraryLine in (($linked.stdout + "`n" + $linked.stderr) -split "`r?`n")) {
                $libraryMatch = [regex]::Match($libraryLine, '^\s*Searching\s+(.+?\.lib):?\s*$')
                if ($libraryMatch.Success) {
                    $library = [IO.Path]::GetFullPath($libraryMatch.Groups[1].Value)
                    if (-not ($library.StartsWith($vs + '\', [StringComparison]::OrdinalIgnoreCase) -or
                        $library.StartsWith($env:WindowsSdkDir, [StringComparison]::OrdinalIgnoreCase))) { throw 'Unexpected linked library location' }
                    $libraries[$library] = $true
                }
            }
            Save-Json (Join-Path $profileDir 'link-libraries.json') @($libraries.Keys | Sort-Object | ForEach-Object { File-Record $_ })
            if (-not $libraries.Count) { throw 'No actual linker library identities captured' }
            foreach ($kind in @('headers', 'imports', 'disasm')) {
                $dump = Invoke-Recorded "$($spec.name)-dumpbin-$kind" $dumpbin @('/NOLOGO', "/$kind", $exe) $profileDir
                Require-Command $dump
                Save-Text (Join-Path $profileDir "dumpbin-$kind.txt") $dump.stdout
            }
            # The helper's timeout kills the whole probe process, not one thread.
            $ran = Invoke-Recorded "$($spec.name)-run" $exe @() $profileDir 60
            Save-Text (Join-Path $profileDir 'observations.jsonl') $ran.stdout
            $records = @($ran.stdout -split "`r?`n" | Where-Object { $_.Length } | ForEach-Object { $_ | ConvertFrom-Json })
            $layout = @($records | Where-Object { $_.kind -eq 'layout' })
            $checks = @($records | Where-Object { $_.kind -eq 'live_checks' })
            $join = @($records | Where-Object { $_.kind -eq 'joined' })
            if ($layout.Count -ne 1 -or $checks.Count -ne 1 -or $join.Count -ne 1) { throw 'Incomplete live observation records' }
            Require-Command $ran
            if (-not ($checks[0].same -and $checks[0].different_lr -and $checks[0].different_rl -and
                $checks[0].foreign_self -and $checks[0].worker_self) -or $join[0].worker_result -ne 0) {
                throw 'Live identity semantic checks failed'
            }
            $moduleRecords = @()
            foreach ($record in $records) {
                if ($record.kind -eq 'module') {
                    $modulePath = [IO.Path]::GetFullPath($record.module_path)
                    if (-not ($modulePath.StartsWith($env:SystemRoot + '\', [StringComparison]::OrdinalIgnoreCase) -or
                        $modulePath.StartsWith($vs + '\', [StringComparison]::OrdinalIgnoreCase) -or
                        $modulePath.StartsWith($profileDir + '\', [StringComparison]::OrdinalIgnoreCase))) {
                        throw 'Unexpected observed module outside allowed Windows/VS/probe roots'
                    }
                    $moduleRecords += File-Record $modulePath
                }
            }
            Save-Json (Join-Path $profileDir 'modules.json') $moduleRecords
            $row.runtime = 'PASS_LIVE_CASES_ONLY'
            $row.abi = 'COLLECTED_NOT_ENGINE_QUALIFIED'
        } catch {
            $row.errors += $_.Exception.Message
        }
        $manifest.profiles += $row
    }
    $nativePass = @($manifest.profiles | Where-Object { $_.name -eq 'native-DEFAULT' -and $_.runtime -eq 'PASS_LIVE_CASES_ONLY' }).Count -eq 1
    $c11Pass = @($manifest.profiles | Where-Object { $_.name.StartsWith('c11-') -and $_.runtime -eq 'PASS_LIVE_CASES_ONLY' }).Count -ge 1
    $manifest.both_profiles_pass = $nativePass -and $c11Pass
    $manifest.collection_status = if (@($manifest.selections | Where-Object { $_.status -eq 'SETUP_FAILED' }).Count) { 'PARTIAL_SETUP_FAILURE' } else { 'COMPLETED_WITH_EXPLICIT_PROFILE_STATUSES' }
    if (-not $manifest.both_profiles_pass -or @($manifest.profiles | Where-Object { $_.errors.Count }).Count -or @($manifest.selections | Where-Object { $_.status -eq 'SETUP_FAILED' }).Count) { $failed = $true }
} catch {
    $failed = $true
    $manifest.errors += $_.Exception.Message
} finally {
    Save-Json (Join-Path $publish 'manifest.json') $manifest
    # Only explicitly generated metadata and our own small compiled probe files.
    $allowed = @('.json', '.jsonl', '.txt', '.c', '.h', '.i', '.obj', '.asm', '.exe', '.pdb', '.map', '.ilk')
    $files = @(Get-ChildItem -LiteralPath $publish -Recurse -File)
    $bytes = 0L
    foreach ($file in $files) {
        if ($file.Attributes -band [IO.FileAttributes]::ReparsePoint) { throw 'Reparse point in evidence' }
        if ($allowed -notcontains $file.Extension -or $file.Length -gt 4194304) {
            Remove-Item -LiteralPath $file.FullName -Force
            $manifest.errors += 'Generated evidence rejected by file allowlist/size bound'
            $failed = $true
        } else { $bytes += $file.Length }
    }
    if ($bytes -gt 25MB) {
        # No oversized partial archive: remove generated large/binary evidence,
        # retain only manifest describing the honest collection failure.
        foreach ($file in $files) { if ($file.FullName -ne (Join-Path $publish 'manifest.json') -and (Test-Path -LiteralPath $file.FullName)) { Remove-Item -LiteralPath $file.FullName -Force } }
        $manifest.errors += 'Aggregate evidence exceeded25MiB; only failure manifest retained'
        $failed = $true
    }
    Save-Json (Join-Path $publish 'manifest.json') $manifest
    $inventory = @(Get-ChildItem -LiteralPath $publish -Recurse -File | Sort-Object FullName | ForEach-Object { File-Record $_.FullName })
    Save-Json (Join-Path $publish 'generated-files.json') $inventory
    $finalBytes = (Get-ChildItem -LiteralPath $publish -Recurse -File | Measure-Object -Property Length -Sum).Sum
    if ($finalBytes -gt 25MB) {
        Get-ChildItem -LiteralPath $publish -Recurse -File | Where-Object { $_.FullName -ne (Join-Path $publish 'manifest.json') } | Remove-Item -Force
        $manifest.errors += 'Final inventory exceeded25MiB; only failure manifest retained'
        Save-Json (Join-Path $publish 'manifest.json') $manifest
        $failed = $true
    }
}
# No untrusted diagnostic text is emitted to the Actions command interpreter.
if ($failed) { Write-Output 'Diagnostic collected bounded failure/unknown evidence; qualification remains OFF.'; exit 1 }
Write-Output 'Diagnostic live cases collected; provider/engine qualification remains OFF.'
