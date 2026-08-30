Set-StrictMode -Version Latest

if ($args.Count -lt 3) {
    throw 'usage: run-downstream-phase-windows.ps1 LogPath TimeoutSeconds Command [CommandArguments...]'
}
$LogPath = [string]$args[0]
$TimeoutSeconds = [int]$args[1]
$Command = [string]$args[2]
$CommandArguments = if ($args.Count -gt 3) { [string[]]$args[3..($args.Count - 1)] } else { [string[]]@() }

function Resolve-GitBash {
    $candidates = [System.Collections.Generic.List[string]]::new()
    $installRoot = [Environment]::GetEnvironmentVariable('GIT_INSTALL_ROOT')
    if ($installRoot) { $candidates.Add((Join-Path $installRoot 'bin\bash.exe')) }
    $roots = @(
        $env:ProgramFiles,
        $env:ProgramW6432,
        ${env:ProgramFiles(x86)}
    )
    if ($env:LOCALAPPDATA) { $roots += (Join-Path $env:LOCALAPPDATA 'Programs') }
    foreach ($root in $roots) {
        if ($root) { $candidates.Add((Join-Path $root 'Git\bin\bash.exe')) }
    }
    foreach ($candidate in $candidates) {
        if (Test-Path -LiteralPath $candidate -PathType Leaf) {
            return (Get-Item -LiteralPath $candidate).FullName
        }
    }
    foreach ($command in @(Get-Command bash.exe -All -CommandType Application -ErrorAction SilentlyContinue)) {
        if ($command.Source -match '[\\/]Git[\\/]bin[\\/]bash\.exe$') {
            return $command.Source
        }
    }
    throw 'Git for Windows bash.exe was not found'
}

# A Windows Job Object owns the complete descendant tree, including children
# which detach or outlive the phase process. This is the Windows equivalent of
# the process group used by run-downstream-phase.sh on Unix.
if (-not ('Wirelog.JobObject' -as [type])) {
    Add-Type @'
using System;
using System.ComponentModel;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text;

namespace Wirelog {
    public sealed class JobObject : IDisposable {
        private const uint JobObjectExtendedLimitInformation = 9;
        private const uint JobObjectLimitKillOnJobClose = 0x2000;
        private readonly IntPtr handle;

        [StructLayout(LayoutKind.Sequential)]
        private struct IoCounters {
            public ulong ReadOperationCount, WriteOperationCount, OtherOperationCount;
            public ulong ReadTransferCount, WriteTransferCount, OtherTransferCount;
        }
        [StructLayout(LayoutKind.Sequential)]
        private struct BasicLimitInformation {
            public long PerProcessUserTimeLimit, PerJobUserTimeLimit;
            public uint LimitFlags;
            public UIntPtr MinimumWorkingSetSize, MaximumWorkingSetSize;
            public uint ActiveProcessLimit;
            public UIntPtr Affinity;
            public uint PriorityClass, SchedulingClass;
        }
        [StructLayout(LayoutKind.Sequential)]
        private struct ExtendedLimitInformation {
            public BasicLimitInformation BasicLimitInformation;
            public IoCounters IoInfo;
            public UIntPtr ProcessMemoryLimit, JobMemoryLimit, PeakProcessMemoryUsed, PeakJobMemoryUsed;
        }

        [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
        private static extern IntPtr CreateJobObject(IntPtr attributes, string name);
        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern bool SetInformationJobObject(IntPtr job, uint infoClass, ref ExtendedLimitInformation info, uint length);
        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern bool AssignProcessToJobObject(IntPtr job, IntPtr process);
        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern bool TerminateJobObject(IntPtr job, uint exitCode);
        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern bool CloseHandle(IntPtr handle);

        public JobObject() {
            handle = CreateJobObject(IntPtr.Zero, null);
            if (handle == IntPtr.Zero) throw new Win32Exception(Marshal.GetLastWin32Error());
            var limits = new ExtendedLimitInformation();
            limits.BasicLimitInformation.LimitFlags = JobObjectLimitKillOnJobClose;
            if (!SetInformationJobObject(handle, JobObjectExtendedLimitInformation, ref limits,
                    (uint)Marshal.SizeOf<ExtendedLimitInformation>())) {
                Dispose();
                throw new Win32Exception(Marshal.GetLastWin32Error());
            }
        }
        public void Assign(Process process) {
            if (!AssignProcessToJobObject(handle, process.Handle))
                throw new Win32Exception(Marshal.GetLastWin32Error());
        }
        public void AssignHandle(IntPtr processHandle) {
            if (!AssignProcessToJobObject(handle, processHandle))
                throw new Win32Exception(Marshal.GetLastWin32Error());
        }
        public void Terminate(uint exitCode) {
            if (!TerminateJobObject(handle, exitCode))
                throw new Win32Exception(Marshal.GetLastWin32Error());
        }
        public void Dispose() {
            if (handle != IntPtr.Zero) CloseHandle(handle);
        }
    }

    public sealed class SuspendedProcess : IDisposable {
        private const uint CreateSuspended = 0x00000004;
        private readonly IntPtr processHandle;
        private readonly IntPtr threadHandle;
        public readonly int Id;

        [StructLayout(LayoutKind.Sequential)]
        private struct StartupInfo { public uint cb; public IntPtr reserved, desktop, title; public uint x, y, xSize, ySize, xCountChars, yCountChars, fill; public uint flags; public ushort show; public ushort reserved2; public IntPtr reserved3, stdin, stdout, stderr; }
        [StructLayout(LayoutKind.Sequential)]
        private struct ProcessInformation { public IntPtr process, thread; public uint processId, threadId; }
        private const uint StartfUseStdHandles = 0x00000100;
        private const int StdInputHandle = -10, StdOutputHandle = -11, StdErrorHandle = -12;
        [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
        private static extern bool CreateProcess(string applicationName, StringBuilder commandLine, IntPtr processAttributes, IntPtr threadAttributes, bool inheritHandles, uint flags, IntPtr environment, string currentDirectory, ref StartupInfo startupInfo, out ProcessInformation processInformation);
        [DllImport("kernel32.dll", SetLastError = true)] private static extern IntPtr GetStdHandle(int handle);
        [DllImport("kernel32.dll", SetLastError = true)] private static extern uint ResumeThread(IntPtr thread);
        [DllImport("kernel32.dll", SetLastError = true)] private static extern bool TerminateProcess(IntPtr process, uint code);
        [DllImport("kernel32.dll", SetLastError = true)] private static extern bool CloseHandle(IntPtr handle);
        [DllImport("kernel32.dll", SetLastError = true)] private static extern uint WaitForSingleObject(IntPtr handle, uint milliseconds);
        [DllImport("kernel32.dll", SetLastError = true)] private static extern bool GetExitCodeProcess(IntPtr process, out uint exitCode);

        private static string Quote(string value) {
            var result = new StringBuilder("\"");
            var slashes = 0;
            foreach (var ch in value) {
                if (ch == '\\') { slashes++; continue; }
                if (ch == '"') { result.Append('\\', slashes * 2 + 1).Append('"'); slashes = 0; continue; }
                result.Append('\\', slashes).Append(ch); slashes = 0;
            }
            result.Append('\\', slashes * 2).Append('"');
            return result.ToString();
        }

        public SuspendedProcess(string applicationName, string[] arguments) {
            var commandLine = new StringBuilder(Quote(applicationName));
            foreach (var argument in arguments) commandLine.Append(' ').Append(Quote(argument));
            var startup = new StartupInfo {
                cb = (uint)Marshal.SizeOf<StartupInfo>(),
                flags = StartfUseStdHandles,
                stdin = GetStdHandle(StdInputHandle),
                stdout = GetStdHandle(StdOutputHandle),
                stderr = GetStdHandle(StdErrorHandle)
            };
            ProcessInformation info;
            if (!CreateProcess(applicationName, commandLine, IntPtr.Zero, IntPtr.Zero, true, CreateSuspended,
                    IntPtr.Zero, null, ref startup, out info))
                throw new Win32Exception(Marshal.GetLastWin32Error());
            processHandle = info.process;
            threadHandle = info.thread;
            Id = (int)info.processId;
        }
        public IntPtr Handle { get { return processHandle; } }
        public void Resume() {
            if (ResumeThread(threadHandle) == uint.MaxValue) throw new Win32Exception(Marshal.GetLastWin32Error());
        }
        public bool WaitForExit(uint milliseconds) {
            return WaitForSingleObject(processHandle, milliseconds) == 0;
        }
        public int GetExitCode() {
            uint code;
            if (!GetExitCodeProcess(processHandle, out code))
                throw new Win32Exception(Marshal.GetLastWin32Error());
            return (int)code;
        }
        public void Terminate(uint code) { TerminateProcess(processHandle, code); }
        public void Dispose() { CloseHandle(threadHandle); CloseHandle(processHandle); }
    }
}
'@
}

$job = $null
$suspended = $null
$process = $null
$exitCode = 1
$timedOut = $false
$deadline = [Diagnostics.Stopwatch]::GetTimestamp() +
    ([Diagnostics.Stopwatch]::Frequency * $TimeoutSeconds)
try {
    try {
        $gitDir = Split-Path -Parent (Split-Path -Parent (Resolve-GitBash))
        $gitUsrBin = Join-Path $gitDir 'usr\bin'
        if (Test-Path -LiteralPath $gitUsrBin -PathType Container) {
            $env:PATH = "$gitUsrBin;$env:PATH"
        }
    } catch { }

    $nativeCommand = $Command
    $nativeArguments = @($CommandArguments)
    if ($Command -match '\.sh$') {
        $nativeCommand = Resolve-GitBash
        $nativeArguments = @('--noprofile', '--norc', $Command) + $nativeArguments
    } elseif ($Command -in @('bash', 'bash.exe')) {
        # A bare `bash` or `bash.exe` can resolve to the WSL launcher in
        # System32. Use the
        # same Git-for-Windows bash executable as the shell-script path above.
        $nativeCommand = Resolve-GitBash
        $nativeArguments = @('--noprofile', '--norc') + $nativeArguments
    } elseif ($Command -notmatch '[\\/]') {
        # CreateProcess does not perform PowerShell/POSIX-style PATH lookup
        # when applicationName is supplied. Resolve bare fixture commands
        # before passing them to the suspended native process.
        $resolvedCommand = Get-Command $Command -CommandType Application -ErrorAction Stop
        $nativeCommand = $resolvedCommand.Source
    }
    if ($nativeCommand -match '[\\/]tar(\.exe)?$') {
        $nativeArguments = @($nativeArguments | ForEach-Object { $_.Replace('\', '/') })
        if ($nativeArguments -notcontains '--force-local') {
            $nativeArguments = @('--force-local') + $nativeArguments
        }
    }
    # Create suspended, assign to the job, then resume. This closes the race
    # where a fast command could spawn a child before ownership is established.
    $suspended = [Wirelog.SuspendedProcess]::new($nativeCommand, $nativeArguments)
    $job = [Wirelog.JobObject]::new()
    $job.AssignHandle($suspended.Handle)
    $suspended.Resume()

    while (-not $suspended.WaitForExit(100) -and [Diagnostics.Stopwatch]::GetTimestamp() -lt $deadline) {
    }
    if (-not $suspended.WaitForExit(0)) {
        $timedOut = $true
        $job.Terminate(124)
    }
    [void]$suspended.WaitForExit([uint32]::MaxValue)
    if ($timedOut) { $exitCode = 124 } else { $exitCode = $suspended.GetExitCode() }
}
catch {
    [Console]::Error.WriteLine("Windows phase launcher failed: $($_.Exception.Message)")
    if ($job -ne $null) {
        try { $job.Terminate(1) } catch { }
    }
    if ($suspended -ne $null) {
        try { $suspended.Terminate(1) } catch { }
    }
    $exitCode = 1
}
finally {
    if ($job -ne $null) { $job.Dispose() }
    if ($suspended -ne $null) { $suspended.Dispose() }
}

exit $exitCode
