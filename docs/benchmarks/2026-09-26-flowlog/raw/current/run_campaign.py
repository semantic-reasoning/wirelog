import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import time
from datetime import datetime, timezone

HERE = Path(__file__).resolve().parent
DATA = Path('/home/joykim/tmp/wirelog-1361-data')
RUNNER = Path('/dev/shm/wirelog-1361/scripts/perf/run-flowlog-portfolio.py')
PYTHON = '/dev/shm/wirelog-1361/.venv/bin/python'
WORKLOADS = ['tc', 'reach', 'cc', 'sssp', 'sg', 'bipartite', 'andersen',
             'dyck', 'cspa-fast', 'cspa', 'csda', 'galen', 'polonius', 'ddisasm', 'crdt']

def now():
    return datetime.now(timezone.utc).isoformat()

def sha(path):
    digest = hashlib.sha256()
    with path.open('rb') as f:
        for block in iter(lambda: f.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()

def command(args, cwd=None):
    return subprocess.check_output(args, cwd=cwd, text=True, encoding='utf-8').strip()

def memory():
    info = {}
    for line in Path('/proc/meminfo').read_text().splitlines():
        key, value = line.split(':', 1)
        if key in ['MemTotal', 'MemAvailable', 'SwapTotal', 'SwapFree']:
            info[key] = int(value.split()[0])
    return info

def sample():
    vm = dict(line.split() for line in Path('/proc/vmstat').read_text().splitlines())
    return {'utc': now(), 'load': os.getloadavg(), 'memory_kb': memory(),
            'pswpin': int(vm['pswpin']), 'pswpout': int(vm['pswpout']),
            'memory_psi': Path('/proc/pressure/memory').read_text()}

def write(path, obj):
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + '\n', encoding='utf-8')

def data_identity():
    return {str(p.relative_to(DATA)): {'bytes': p.stat().st_size, 'sha256': sha(p)}
            for base, dirs, files in os.walk(DATA, followlinks=True)
            for p in sorted(Path(base) / name for name in files)
            if p.suffix in ['.csv', '.facts']}

def wait_quiet(label):
    previous = sample()
    with (HERE / (label + '-resource-wait.jsonl')).open('w', encoding='utf-8') as log:
        while True:
            time.sleep(30)
            current = sample()
            log.write(json.dumps(current, sort_keys=True) + '\n'); log.flush()
            psi = float(current['memory_psi'].split('full avg10=')[1].split()[0])
            if (current['memory_kb']['MemAvailable'] >= 50 * 1024 * 1024
                    and current['load'][0] <= 4 and psi <= 0.1
                    and current['pswpin'] == previous['pswpin']
                    and current['pswpout'] == previous['pswpout']):
                return
            previous = current


def execute(label, root, workloads, repeat, context):
    out = HERE / label
    assert not out.exists(), 'Never overwrite prior evidence'
    bench = root / 'build-portfolio/bench/bench_flowlog'
    assert command(['git', 'rev-parse', 'HEAD'], root) == context['source_sha']
    assert command(['git', 'rev-parse', 'HEAD^{tree}'], root) == context['source_tree']
    assert command(['git', 'status', '--porcelain'], root) == ''
    assert sha(bench) == context['binary_sha256']
    assert sha(RUNNER) == context['runner_sha256']
    assert data_identity() == context['data_files']
    if workloads == ['doop']:
        wait_quiet(label)
        before = sample()
        write(HERE / (label + '-preflight.json'), before)
        # A 41 GiB process needs headroom; never run under active memory pressure.
        if before['memory_kb']['MemAvailable'] < 50 * 1024 * 1024:
            raise RuntimeError('DOOP needs at least 50 GiB available memory')
        if before['load'][0] > 4:
            raise RuntimeError('DOOP preflight ambient load is too high')
    args = ['taskset', '-c', '0-15', PYTHON, str(RUNNER), '--repo-root', str(root),
            '--bench', str(bench), '--data-root', str(DATA), '--out-dir', str(out),
            '--workers', '1,8,16', '--repeat', str(repeat)]
    for workload in workloads:
        args += ['--workload', workload]
    write(HERE / (label + '-launch.json'), {'utc': now(), 'command': args, 'context': context})
    with (HERE / (label + '-driver.log')).open('w', encoding='utf-8') as log:
        assert sha(RUNNER) == context['runner_sha256']
        proc = subprocess.Popen(args, stdout=log, stderr=subprocess.STDOUT)
        with (HERE / (label + '-host-samples.jsonl')).open('w', encoding='utf-8') as samples:
            while proc.poll() is None:
                samples.write(json.dumps(sample(), sort_keys=True) + '\n')
                samples.flush()
                time.sleep(30)
    result = {'utc': now(), 'exit_code': proc.returncode, 'host': sample()}
    write(HERE / (label + '-exit.json'), result)
    if proc.returncode:
        raise RuntimeError(f'{label} exited {proc.returncode}; retain failed evidence')
    manifest = json.loads((out / 'manifest.json').read_text())
    assert manifest['run_complete'] and manifest['record_count'] == len(workloads) * 3
    assert manifest['status_counts'] == {'ok': len(workloads) * 3}
    assert sha(RUNNER) == context['runner_sha256']
    assert command(['git', 'rev-parse', 'HEAD'], root) == context['source_sha']
    assert command(['git', 'rev-parse', 'HEAD^{tree}'], root) == context['source_tree']
    assert command(['git', 'status', '--porcelain'], root) == ''
    assert sha(bench) == context['binary_sha256'] and data_identity() == context['data_files']

def main():
    os.sched_setaffinity(0, range(16))
    inputs = data_identity()
    write(HERE / 'data-files.json', inputs)
    for label, directory in [('main', 'wirelog-1972-current-afcdd8a3'),
                             ('reference', 'wirelog-1972-reference-7e498e7')]:
        root = Path('/home/joykim/tmp') / directory
        bench = root / 'build-portfolio/bench/bench_flowlog'
        context = {'captured_at': now(), 'source_sha': command(['git', 'rev-parse', 'HEAD'], root),
                   'source_tree': command(['git', 'rev-parse', 'HEAD^{tree}'], root),
                   'git_status': command(['git', 'status', '--porcelain'], root),
                   'binary_sha256': sha(bench), 'binary_bytes': bench.stat().st_size,
                   'runner_sha256': sha(RUNNER), 'data_files': inputs,
                   'compiler': command(['cc', '--version']), 'linker': command(['ld', '--version']),
                   'meson_options': json.loads(command(['meson', 'introspect', str(root / 'build-portfolio'), '--buildoptions'])),
                   'uname': list(platform.uname()), 'cpu_topology': command(['lscpu', '-e']),
                   'affinity': sorted(os.sched_getaffinity(0)), 'host_start': sample(),
                   'governors': sorted({p.read_text().strip() for p in Path('/sys/devices/system/cpu').glob('cpu*/cpufreq/scaling_governor')}),
                   'kernel_cmdline': Path('/proc/cmdline').read_text(),
                   'microcode': sorted({line.split(':', 1)[1].strip() for line in Path('/proc/cpuinfo').read_text().splitlines() if line.startswith('microcode')}),
                   'cgroup': Path('/proc/self/cgroup').read_text(),
                   'effective_cpuset': Path('/sys/fs/cgroup/cpuset.cpus.effective').read_text(),
                   'smt_active': Path('/sys/devices/system/cpu/smt/active').read_text(),
                   'turbo_disabled': Path('/sys/devices/system/cpu/intel_pstate/no_turbo').read_text() if Path('/sys/devices/system/cpu/intel_pstate/no_turbo').exists() else None}
        write(HERE / (label + '-context-start.json'), context)
        execute(label + '-portfolio', root, WORKLOADS, 5, context)
        execute(label + '-doop', root, ['doop'], 1, context)
        write(HERE / (label + '-context-end.json'), sample())
    write(HERE / 'current-campaign-complete.json', {'completed_at': now(), 'required_records_per_source': 48})

if __name__ == '__main__':
    main()
