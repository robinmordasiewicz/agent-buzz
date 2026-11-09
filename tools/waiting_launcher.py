"""Supervise reviewed waiting runtime using an explicitly paired Herdr lease.

The service manager owns restart policy. This launcher never starts a Codex
writer and never substitutes another pane or thread when pairing is unavailable.
"""
import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import time

BASE_ENV = ('HOME', 'PATH', 'USER', 'LOGNAME', 'LANG', 'XDG_RUNTIME_DIR',
            'DBUS_SESSION_BUS_ADDRESS', 'XDG_CONFIG_HOME', 'CODEX_HOME',
            'CODEX_FOLDER_BASE_HOME', 'CODEX_SQLITE_HOME')
HERDR_ENV = {'HERDR_BIN_PATH', 'HERDR_ENV', 'HERDR_PANE_ID',
             'HERDR_SOCKET_PATH', 'HERDR_TAB_ID', 'HERDR_WORKSPACE_ID'}


def private_json(path):
    info = path.lstat()
    if path.is_symlink() or info.st_uid != os.getuid() or info.st_mode & 0o077:
        raise RuntimeError('Pairing/config file must be private, owned and regular')
    import stat
    if not stat.S_ISREG(info.st_mode):
        raise RuntimeError('Pairing/config file must be regular')
    return json.loads(path.read_text())


def resolve(root, run=subprocess.run):
    pairing = private_json(root / 'pairing.json')
    lease = private_json(root / 'herdr-lease.json')['lease']
    # Opaque secret travels only on stdin. Never include captured output in errors.
    result = run(['herdr', 'context', 'resolve', '--endpoint', pairing['endpoint'],
                  '--consumer-id', pairing['consumer_id']], input=lease,
                 text=True, capture_output=True, timeout=10)
    if result.returncode:
        raise RuntimeError('Herdr pairing unavailable; retaining events without wake')
    resolved = json.loads(result.stdout)
    env = resolved['environment']
    if set(env) - HERDR_ENV or env.get('HERDR_ENV') != '1':
        raise RuntimeError('Unexpected resolved environment')
    child_env = {key: os.environ[key] for key in BASE_ENV if key in os.environ}
    child_env.update(env)
    config = private_json(root / 'config.json')
    result = run(['herdr', 'pane', 'current', '--current'], env=child_env,
                 text=True, capture_output=True, timeout=10)
    if result.returncode:
        raise RuntimeError('Paired pane unavailable')
    pane = json.loads(result.stdout)['result']['pane']
    if pane.get('agent_session', {}).get('value') != config['thread']:
        raise RuntimeError('Paired pane no longer contains the authorized thread')
    return child_env


def goal_endpoint(env, run=subprocess.run):
    result = run(['codex', 'app-server', 'daemon', 'version'], env=env,
                 text=True, capture_output=True, timeout=10)
    if result.returncode:
        raise RuntimeError('Owning daemon metadata unavailable')
    info = json.loads(result.stdout)
    if info.get('status') != 'running' or not info.get('socketPath'):
        raise RuntimeError('Owning daemon is unavailable')
    path = Path(info['socketPath'])
    import stat
    status = path.stat()
    if status.st_uid != os.getuid() or not stat.S_ISSOCK(status.st_mode):
        raise RuntimeError('Owning daemon endpoint is not an owned socket')
    return str(path)


def stop_child(child):
    code = child.poll()
    if code is not None:
        return code  # Preserve an exit observed before we initiate retirement.
    child.terminate()
    try:
        child.wait(timeout=15)
    except subprocess.TimeoutExpired:
        child.kill()
        child.wait(timeout=5)


def main():
    try:
        from .loop_control import atomic
    except ImportError:
        from loop_control import atomic
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--state-dir', type=Path, required=True)
    parser.add_argument('--python', required=True)
    parser.add_argument('--service-script', type=Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    root = args.state_dir
    if root.is_symlink() or root.stat().st_uid != os.getuid() or root.stat().st_mode & 0o077:
        raise SystemExit('State directory must be private and owned')
    stopping = [False]
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stopping.__setitem__(0, True))
    child = None
    active_context = None
    backoff = 5
    retry_at = 0
    try:
        while not stopping[0]:
            # Terminal worker state precedes every fallible revalidation and
            # backoff. Never erase native pause/completion because IPC is down.
            if child is not None:
                code = child.poll()
                if code is not None:
                    return code
            if time.monotonic() < retry_at:
                time.sleep(.2)
                continue
            try:
                env = resolve(root)
                endpoint = goal_endpoint(env)
                config = private_json(root / 'config.json')
                changed = config.get('goal_socket') != endpoint
                context = {key: env.get(key) for key in HERDR_ENV}
                if child is not None and (changed or context != active_context):
                    code = stop_child(child)
                    if code is not None:
                        return code
                    child = None
                    active_context = None
                if changed:
                    config['goal_socket'] = endpoint
                    atomic(root / 'config.json', config)
                if child is None:
                    child = subprocess.Popen([args.python, str(args.service_script),
                                              '--state-dir', str(root)], env=env,
                                             start_new_session=True)
                    active_context = context
                code = child.poll()
                if code is not None:
                    # A clean goal stop must not be turned into an endless restart.
                    return code
                atomic(root / 'launcher.json', {'mode': 'paired', 'child_pid': child.pid,
                                               'last_success_epoch': time.time()})
                backoff = 5
                retry_at = time.monotonic() + 10
            except Exception:
                if child is not None:
                    code = stop_child(child)
                    if code is not None:
                        return code
                    child = None
                    active_context = None
                atomic(root / 'launcher.json', {'mode': 'awaiting_runtime',
                    'detail': 'Pairing or owning runtime unavailable; no control attempted',
                    'last_failure_epoch': time.time()})
                retry_at = time.monotonic() + backoff
                backoff = min(backoff * 2, 60)
    finally:
        if child is not None:
            stop_child(child)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
