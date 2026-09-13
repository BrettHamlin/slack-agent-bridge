"""Own the thread lock and bound Codex cleanup after its terminal turn event.

The lock descriptor never reaches Codex/MCP descendants. A terminal turn event
means the model has stopped; the dispatcher separately verifies Slack delivery.
Only this wrapper's own Codex process group is stopped during cleanup.
"""
import json
import os
import signal
import subprocess
import sys


def signal_group(child, signum):
    """Signal only this invocation; group EPERM may concern a helper process."""
    try:
        os.killpg(child.pid, signum)
    except ProcessLookupError:
        pass
    except PermissionError:
        if child.poll() is None:
            try:
                child.send_signal(signum)
            except (PermissionError, ProcessLookupError):
                pass


def run(lock_fd, command):
    os.set_inheritable(lock_fd, False)
    child = subprocess.Popen(command, stdin=sys.stdin, stdout=subprocess.PIPE,
        stderr=sys.stderr, text=True, bufsize=1, close_fds=True, process_group=0)
    terminal = None

    def terminate(*_):
        signal_group(child, signal.SIGTERM)
        raise InterruptedError('Worker interrupted')

    signal.signal(signal.SIGTERM, terminate)
    signal.signal(signal.SIGINT, terminate)
    try:
        for line in child.stdout:
            sys.stdout.write(line)
            sys.stdout.flush()
            try:
                event = json.loads(line)
            except ValueError:
                continue
            if event.get('type') in ('turn.completed', 'turn.failed'):
                terminal = event['type']
                break
    except InterruptedError:
        terminal = 'interrupted'
    finally:
        # Do not let a second supervisor signal interrupt cleanup/lock release.
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal_group(child, signal.SIGTERM)
        try:
            child.wait(timeout=1)
        except subprocess.TimeoutExpired:
            pass
        # Helpers may outlive their CLI parent. The dedicated group still
        # belongs to this invocation, and no other service shares it.
        signal_group(child, signal.SIGKILL)
        try:
            child.wait(timeout=1)
        except subprocess.TimeoutExpired:
            # Persist an explicit fence before releasing the OS lock. The
            # dispatcher must not retry this root while an orphan may act.
            print(json.dumps({'type': 'worker.cleanup_failed'}), flush=True)
            terminal = 'cleanup_failed'
        finally:
            child.stdout.close()
            os.close(lock_fd)
    return 0 if terminal == 'turn.completed' else 1


if __name__ == '__main__':
    raise SystemExit(run(int(sys.argv[1]), sys.argv[2:]))
