"""Private stdin/JSON boundary for the pinned LiteLLM classifier."""
import asyncio
import contextlib
import json
import os
import sys
from dataclasses import asdict



@contextlib.contextmanager
def _auth_lock(local_only):
    """Serialize native OAuth refresh; parent bounds both waiting and inference."""
    if local_only:
        yield
        return
    import fcntl
    from pathlib import Path
    import stat
    import time
    directory = Path(os.environ.get('CHATGPT_TOKEN_DIR', '~/.config/litellm/chatgpt')).expanduser()
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    fd = os.open(directory / '.director-selection.lock',
                 os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        metadata = os.fstat(fd)
        if metadata.st_uid != os.getuid() or not stat.S_ISREG(metadata.st_mode):
            raise RuntimeError('selection_lock_invalid')
        os.fchmod(fd, 0o600)
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                time.sleep(0.05)
        yield
    finally:
        os.close(fd)


def main():
    # Credentials remain owned by LiteLLM. Permit refresh of its stored login,
    # but never start an interactive device-login flow inside the daemon.
    os.umask(0o077)
    os.environ['LITELLM_LOCAL_MODEL_COST_MAP'] = 'True'
    raw = sys.stdin.buffer.read(1_000_001)
    if len(raw) > 1_000_000:
        return 1
    try:
        request = json.loads(raw)
        if (not isinstance(request, dict)
                or set(request) != {'prompt', 'backend', 'local_only'}
                or not isinstance(request['local_only'], bool)):
            return 1
        with open(os.devnull, 'w') as sink, contextlib.redirect_stdout(sink), contextlib.redirect_stderr(sink):
            from litellm.llms.chatgpt.authenticator import Authenticator
            from director.model_selection import ModelSelector

            def no_device_login(self):
                raise RuntimeError('classifier_login_required')

            Authenticator._login_device_code = no_device_login
            # Constructor errors (including unavailable OAuth) are included in
            # fallback scope, rather than leaving the receiver stuck in setup.
            try:
                with _auth_lock(request['local_only']):
                    selector = ModelSelector(request['backend'], local_only=request['local_only'])
                    decision = asyncio.run(selector.select(request['prompt']))
            except Exception:
                if request['local_only']:
                    raise
                selector = ModelSelector(request['backend'], local_only=True)
                decision = asyncio.run(selector.select(request['prompt']))
            result = asdict(decision)
            result['cause'] = 'llm_classifier' if decision.cause == 'llm_classifier' else 'local_heuristic'
        sys.stdout.write(json.dumps(result))
        return 0
    except Exception:
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
