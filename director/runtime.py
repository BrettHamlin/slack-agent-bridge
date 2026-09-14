"""Credential-safe runtime helpers and process lifetime management."""
from contextlib import contextmanager
import fcntl
import logging
import os
import json
import stat
import tempfile
import signal
from pathlib import Path
from urllib.parse import urlparse

from .inbox import InboxStore
from .slack_transport import SlackAllowlist


CREDENTIAL_TIMEOUT_SECONDS = 30
CREDENTIAL_NAMES = ("SLACK_BOT_TOKEN", "SLACK_APP_TOKEN")
NEEDS_YOU_OAUTH_NAMES = ("SLACK_OPENID_CLIENT_ID", "SLACK_OPENID_CLIENT_SECRET")
RUNTIME_ENV_FILE = ".env.runtime"


class CredentialUnavailable(RuntimeError):
    """The selected credential source did not supply the runtime variables."""


def database_path(config, project):
    os.umask(0o077)
    path = (project / config['database_path']).resolve()
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    return path


def quiet_sdk():
    logger = logging.getLogger('slack_sdk')
    logger.handlers = [logging.NullHandler()]
    logger.propagate = False


@contextmanager
def singleton(path):
    with open(path, 'a') as handle:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def mounted_credentials(project):
    from dotenv import dotenv_values
    # Explicit one-time provisioning only. Normal startup never opens this FIFO.
    def expired(*_):
        raise CredentialUnavailable('Slack runtime environment timed out')
    previous_handler = signal.signal(signal.SIGALRM, expired)
    previous_timer = signal.setitimer(signal.ITIMER_REAL, CREDENTIAL_TIMEOUT_SECONDS)
    try:
        values = dotenv_values(project / '.env', interpolate=False)
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_handler)
        signal.setitimer(signal.ITIMER_REAL, *previous_timer)
    if not values.get('SLACK_BOT_TOKEN') or not values.get('SLACK_APP_TOKEN'):
        raise CredentialUnavailable('Slack runtime environment unavailable')
    return values


def _credential_pair(values):
    if any(not isinstance(values.get(name), str) or not values[name].strip() for name in CREDENTIAL_NAMES):
        raise CredentialUnavailable('Both Slack runtime variables are required')
    return {name: values[name] for name in CREDENTIAL_NAMES}


def credentials(project):
    """Use supplied variables or the private service env file, never 1Password.

    A partial process environment fails closed rather than mixing accounts
    across sources. File validation happens on the opened descriptor, with
    nonblocking/no-follow flags so a FIFO or symlink cannot become a fallback.
    """
    from dotenv import dotenv_values
    if any(name in os.environ for name in CREDENTIAL_NAMES):
        return _credential_pair(os.environ)
    try:
        fd = os.open(Path(project) / RUNTIME_ENV_FILE, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except OSError:
        raise CredentialUnavailable('Private runtime environment unavailable') from None
    try:
        metadata = os.fstat(fd)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.getuid() or stat.S_IMODE(metadata.st_mode) != 0o600:
            raise CredentialUnavailable('Runtime environment must be an owner-only regular file')
        with os.fdopen(fd, 'r') as stream:
            fd = None
            return _credential_pair(dotenv_values(stream=stream, interpolate=False))
    finally:
        if fd is not None:
            os.close(fd)


def needs_you_oauth_credentials(project):
    """Read the optional owner-only Sign in with Slack client pair."""
    from dotenv import dotenv_values
    if any(name in os.environ for name in NEEDS_YOU_OAUTH_NAMES):
        values = os.environ
    else:
        try:
            fd = os.open(Path(project) / RUNTIME_ENV_FILE, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        except OSError:
            raise CredentialUnavailable('Needs-you Sign in with Slack credentials unavailable') from None
        try:
            metadata = os.fstat(fd)
            if not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.getuid() or stat.S_IMODE(metadata.st_mode) != 0o600:
                raise CredentialUnavailable('Runtime environment must be an owner-only regular file')
            with os.fdopen(fd, 'r') as stream:
                fd = None
                values = dotenv_values(stream=stream, interpolate=False)
        finally:
            if fd is not None:
                os.close(fd)
    if any(not isinstance(values.get(name), str) or not values[name].strip() for name in NEEDS_YOU_OAUTH_NAMES):
        raise CredentialUnavailable('Needs-you Sign in with Slack credentials unavailable')
    return {name: values[name] for name in NEEDS_YOU_OAUTH_NAMES}


def provision_runtime_environment(project, config):
    """Transfer only the verified Slack pair from the existing mounted source.

    Secret values never return to the caller or enter command arguments. Do
    not replace an existing runtime file; rotation is a separate explicit step.
    """
    from slack_sdk import WebClient
    target = Path(project) / RUNTIME_ENV_FILE
    if target.exists() or target.is_symlink():
        raise FileExistsError('Runtime environment already exists')
    values = _credential_pair(mounted_credentials(project))
    quiet_sdk()
    verify_identity(WebClient(token=values['SLACK_BOT_TOKEN'], timeout=30), config)
    descriptor, temporary = tempfile.mkstemp(prefix='.env.runtime-', dir=project)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, 'w') as stream:
            descriptor = None
            for name in CREDENTIAL_NAMES:
                stream.write(name + '=' + json.dumps(values[name], ensure_ascii=False) + '\n')
            stream.flush()
            os.fsync(stream.fileno())
        # Atomic no-clobber installation; a concurrently created target wins.
        os.link(temporary, target)
    finally:
        if descriptor is not None:
            os.close(descriptor)
        os.unlink(temporary)
    return {'provisioned': True, 'variable_names': list(CREDENTIAL_NAMES),
            'file_mode': '0600', 'identity_verified': True}


def verify_identity(web, config):
    identity = web.auth_test()
    if identity.get('team_id') != config['team_id'] or identity.get('user_id') != config['bot_user_id']:
        raise RuntimeError('Slack identity mismatch')
    if urlparse(str(identity.get('url', ''))).netloc != config['workspace_domain']:
        raise RuntimeError('Slack workspace domain mismatch')
    channel = web.conversations_info(channel=config['channel_id']).get('channel', {})
    if channel.get('id') != config['channel_id'] or not channel.get('is_private'):
        raise RuntimeError('Slack private destination mismatch')
    members, cursor, seen = set(), None, set()
    while True:
        page = web.conversations_members(channel=config['channel_id'], limit=200, cursor=cursor)
        members.update(page.get('members', []))
        cursor = page.get('response_metadata', {}).get('next_cursor')
        if not cursor:
            break
        if cursor in seen:
            raise RuntimeError('Slack membership pagination repeated')
        seen.add(cursor)
    if not {config['owner_user_id'], config['bot_user_id']}.issubset(members):
        raise RuntimeError('Slack membership mismatch')


def verify_conversation_feed_identity(web, config):
    """Verify the optional feed is a distinct private channel for this bot/user."""
    from .conversation_feed import feed_target
    target = feed_target(config)
    if target is None:
        return
    channel = web.conversations_info(channel=target.channel_id).get('channel', {})
    if channel.get('id') != target.channel_id or not channel.get('is_private'):
        raise RuntimeError('Slack private conversation-feed destination mismatch')
    members, cursor, seen = set(), None, set()
    while True:
        page = web.conversations_members(channel=target.channel_id, limit=200, cursor=cursor)
        members.update(page.get('members', []))
        cursor = page.get('response_metadata', {}).get('next_cursor')
        if not cursor:
            break
        if cursor in seen:
            raise RuntimeError('Slack conversation-feed membership pagination repeated')
        seen.add(cursor)
    if not {target.owner_user_id, target.bot_user_id}.issubset(members):
        raise RuntimeError('Slack conversation-feed membership mismatch')


@contextmanager
def slack_session(config, project):
    from slack_sdk import WebClient
    from .slack_service import SlackService
    quiet_sdk()
    values = credentials(project)
    web = WebClient(token=values['SLACK_BOT_TOKEN'], timeout=30)
    verify_identity(web, config)
    verify_conversation_feed_identity(web, config)
    path = database_path(config, project)
    with InboxStore(path) as store:
        store.set_checkpoint('runtime.credentials', 'available')
        allowlist = SlackAllowlist(
            config['team_id'], config['channel_id'], config['owner_user_id'], config['workspace_domain']
        )
        with SlackService(web, store, allowlist, path) as service:
            yield store, service
