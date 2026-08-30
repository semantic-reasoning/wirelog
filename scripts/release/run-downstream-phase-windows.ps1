param(
    [Parameter(Mandatory = $true, Position = 0)] [string]$LogPath,
    [Parameter(Mandatory = $true, Position = 1)] [int]$TimeoutSeconds,
    [Parameter(Mandatory = $true, Position = 2)] [string]$Command,
    [Parameter(Position = 3, ValueFromRemainingArguments = $true)] [string[]]$CommandArguments
)

Set-StrictMode -Version Latest

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
    $nativeCommand = $Command
    $nativeArguments = @($CommandArguments)
    if ($Command -match '\.sh$') {
        $nativeCommand = (Get-Command bash.exe -ErrorAction Stop).Source
        $nativeArguments = @('--noprofile', '--norc', $Command) + $nativeArguments
    } elseif ($Command -notmatch '[\\/]') {
        # CreateProcess does not perform PowerShell/POSIX-style PATH lookup
        # when applicationName is supplied. Resolve bare fixture commands
        # before passing them to the suspended native process.
        $resolvedCommand = Get-Command $Command -CommandType Application -ErrorAction Stop
        $nativeCommand = $resolvedCommand.Source
    }
    # Create suspended, assign to the job, then resume. This closes the race
    # where a fast command could spawn a child before ownership is established.
    $suspended = [Wirelog.SuspendedProcess]::new($nativeCommand, $nativeArguments)
    $job = [Wirelog.JobObject]::new()
    $job.AssignHandle($suspended.Handle)
    $process = [Diagnostics.Process]::GetProcessById($suspended.Id)
    $suspended.Resume()

    while (-not $process.HasExited -and [Diagnostics.Stopwatch]::GetTimestamp() -lt $deadline) {
        Start-Sleep -Milliseconds 100
    }
    if (-not $process.HasExited) {
        $timedOut = $true
        $job.Terminate(124)
    }
    $process.WaitForExit()
    if ($timedOut) { $exitCode = 124 } else { $exitCode = $process.ExitCode }
}
catch {
    [Console]::Error.WriteLine("Windows phase launcher failed: $($_.Exception.Message)")
    if ($job -ne $null) {
        try { $job.Terminate(1) } catch { }
    }
    if ($suspended -ne $null) {
        try { $suspended.Terminate(1) } catch { }
    }
    if ($process -ne $null) {
        try {
            if (-not $process.HasExited) { $process.Kill($true) }
            $process.WaitForExit()
        } catch { }
    }
    $exitCode = 1
}
finally {
    if ($job -ne $null) { $job.Dispose() }
    if ($suspended -ne $null) { $suspended.Dispose() }
    if ($process -ne $null) { $process.Dispose() }
}

exit $exitCode
