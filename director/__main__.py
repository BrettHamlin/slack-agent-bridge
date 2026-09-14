"""Director's local receiver and manager operations. Secret values never print."""
from __future__ import annotations
import argparse
from dataclasses import asdict
import json
import os
import time
from pathlib import Path

from .inbox import InboxStore, RelayLease
from .poll import collect_poll
from .responsibilities import ResponsibilityStore
from .slack_transport import SlackAllowlist
from .runtime import CredentialUnavailable, database_path, singleton, provision_runtime_environment


def load_config(path):
    config = json.loads(path.read_text())
    SlackAllowlist(config['team_id'], config['channel_id'], config['owner_user_id'], config.get('workspace_domain'))
    if type(config.get('enabled')) is not bool:
        raise ValueError('enabled must be a boolean')
    if not config.get('database_path'):
        raise ValueError('database_path is required')
    from .conversation_feed import feed_target
    feed_target(config)
    from .needs_you import needs_you_target
    needs_you_target(config)
    config['_config_path'] = str(path.resolve())
    return config


def listen(config, project):
    from .receiver import listen_channels
    return listen_channels(config, project, load_config, execute_slack_command)


def execute_slack_command(args, config, project, path, store, service):
    result = None
    if args.command == 'canvas-sync':
        from .canvas import SlackCanvasSync
        if args.payload_text is None:
            raise ValueError('canvas content required')
        with singleton(path.parent / 'canvas.lock'):
            with SlackCanvasSync(
                service._web_client,
                path,
                config['channel_id'],
                notes_header=config.get('canvas_notes_header', 'Owner notes'),
            ) as canvas:
                result = asdict(canvas.sync(args.payload_text))
                if result['status'] != 'blocked':
                    store.set_checkpoint('canvas.sync', str(time.time()))
    elif args.command == 'deliver-reminders':
        from .reminders import ReminderStore
        result = []
        with ReminderStore(path) as reminders:
            for item in reminders.claim_due('slack-delivery', 300, limit=20):
                delivery = service.send_outgoing(item.text, idempotency_key=item.service_idempotency_key, thread_ts=item.thread_ts)
                if delivery.state == 'sent':
                    reminders.mark_sent(item)
                result.append({'id': item.id, 'state': delivery.state})
    elif args.command == 'recover':
        with singleton(path.parent / 'recovery.lock'):
            count = service.recover_owner_messages()
            store.set_checkpoint('luna.scan', str(time.time()))
            result = {'recovered': count}
    elif args.command == 'source':
        fetched = service.fetch_owner_source_evidence(store.get_message(args.message_id))
        evidence = fetched.evidence
        result = {'pointer': asdict(fetched.message), 'source_updated': fetched.source_updated, 'message': dict(evidence.message), 'thread': [dict(item) for item in evidence.thread]}
    elif args.command == 'read':
        if args.revision is None:
            raise ValueError('observed revision required')
        result = {'read': service.mark_read_after_manager_command(args.message_id, expected_revision=args.revision)}
    elif args.command == 'send':
        if not args.file or not args.key:
            raise ValueError('text file and stable idempotency key required')
        result = asdict(service.send_outgoing(args.payload_text, idempotency_key=args.key, thread_ts=args.thread_ts))
    elif args.command == 'inbox-card':
        if not args.file or not args.key or not args.title or not args.conversation_url:
            raise ValueError('title, body file, conversation URL, and stable idempotency key required')
        result = asdict(
            service.create_inbox_card(
                args.title,
                args.payload_text,
                args.conversation_url,
                idempotency_key=args.key,
            )
        )
    elif args.command == 'responsibility-post-card':
        if not args.responsibility_id or not args.file or not args.key:
            raise ValueError('responsibility id, body file, and stable idempotency key required')
        with ResponsibilityStore(path) as responsibilities:
            responsibility = responsibilities.get(args.responsibility_id)
        result = asdict(service.create_inbox_card(args.title or responsibility.outcome, args.payload_text, responsibility.conversation_url, idempotency_key=args.key, responsibility_id=responsibility.id, responsibility_version=responsibility.version))
    elif args.command == 'responsibility-publish-result':
        if not args.responsibility_id or not args.fence or not args.key or not args.file:
            raise ValueError('responsibility id, fence, stable key, and result text file required')
        delivery = service.publish_responsibility_result(args.responsibility_id, args.fence, args.payload_text, idempotency_key=args.key)
        result = {'allowed': delivery is not None, 'delivery': asdict(delivery) if delivery is not None else None}
    elif args.command == 'repair-inbox-card-link':
        if not args.key or not args.conversation_url:
            raise ValueError('stable idempotency key and conversation URL required')
        result = asdict(service.repair_inbox_card_link(args.key, args.conversation_url))
    elif args.command == 'test-defer-inbox-card':
        if not args.key or args.due_at is None:
            raise ValueError('test-only inbox card key and due time are required')
        result = asdict(service.test_defer_inbox_card(args.key, args.due_at))
    elif args.command == 'reconcile':
        if not args.key:
            raise ValueError('idempotency key required')
        result = asdict(service.reconcile_outgoing(args.key))
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('provision-runtime-env', 'check-config', 'listen', 'status', 'pending', 'poll', 'claim', 'relay-ack', 'source', 'read', 'complete', 'send', 'reconcile', 'recover', 'remind', 'reminders', 'deliver-reminders', 'canvas-sync', 'inbox-card', 'repair-inbox-card-link', 'test-defer-inbox-card', 'dispatch-reconcile', 'guardian-pending', 'guardian-approve', 'agent-publish', 'conversation-feed-import', 'conversation-feed-hide', 'responsibility-create', 'responsibility-update', 'responsibility-list', 'responsibility-get', 'responsibility-claim', 'responsibility-execution-gate', 'responsibility-publish-result', 'responsibility-complete', 'responsibility-cancel', 'responsibility-resume', 'responsibility-post-card'))
    parser.add_argument('--config', type=Path, default=Path(os.environ.get('DIRECTOR_CONFIG', Path(__file__).resolve().parent.parent / 'config/director.json')))
    parser.add_argument('--message-id', type=int)
    parser.add_argument('--revision', type=int)
    parser.add_argument('--file', type=Path, help='Text or lease JSON file, depending on operation')
    parser.add_argument('--key')
    parser.add_argument('--authority')
    parser.add_argument('--retry-if-stopped', action='store_const', const='retry')
    parser.add_argument('--due-at', type=float, help='Reminder due time as Unix seconds')
    parser.add_argument('--thread-ts')
    parser.add_argument('--title')
    parser.add_argument('--conversation-url')
    parser.add_argument('--conversation-title')
    parser.add_argument('--conversation-emoji')
    parser.add_argument('--conversation-preview')
    parser.add_argument('--feed-root')
    parser.add_argument('--holder', default='luna-recovery')
    parser.add_argument('--responsibility-id')
    parser.add_argument('--outcome')
    parser.add_argument('--next-action')
    parser.add_argument('--state')
    parser.add_argument('--current-thread')
    parser.add_argument('--source-message-id', type=int)
    parser.add_argument('--source-revision', type=int)
    parser.add_argument('--deadline-at', type=float)
    parser.add_argument('--lease-seconds', type=float, default=900)
    parser.add_argument('--fence')
    parser.add_argument('--summary')
    parser.add_argument('--runnable-only', action='store_true')
    parser.add_argument('--role', choices=('manager', 'relay'), default='manager')
    args = parser.parse_args()
    try:
        config = load_config(args.config)
        project = args.config.resolve().parent.parent
        path = database_path(config, project)
        result = None
        if args.command == 'provision-runtime-env':
            result = provision_runtime_environment(project, config)
        elif args.command == 'check-config':
            result = {'config_valid': True, 'receiver_enabled': config['enabled'], 'credentials_checked': False}
        elif args.command == 'listen':
            listen(config, project)
        elif args.command in ('responsibility-create', 'responsibility-update', 'responsibility-list', 'responsibility-get', 'responsibility-claim', 'responsibility-execution-gate', 'responsibility-complete', 'responsibility-cancel', 'responsibility-resume'):
            with ResponsibilityStore(path) as responsibilities:
                if args.command == 'responsibility-create':
                    if not args.responsibility_id or not args.outcome or not args.next_action or not args.conversation_url:
                        raise ValueError('responsibility id, outcome, next action, and conversation URL required')
                    result = asdict(responsibilities.create(args.responsibility_id, args.outcome, args.next_action, args.conversation_url, current_thread=args.current_thread, source_message_id=args.source_message_id, source_revision=args.source_revision, deadline_at=args.deadline_at, state=args.state or 'waiting_input'))
                elif args.command == 'responsibility-update':
                    if not args.responsibility_id:
                        raise ValueError('responsibility id required')
                    if not any((args.outcome, args.next_action, args.conversation_url, args.current_thread, args.source_message_id, args.source_revision, args.deadline_at, args.state)):
                        raise ValueError('at least one mutable responsibility field required')
                    result = asdict(responsibilities.update(args.responsibility_id, outcome=args.outcome, next_action=args.next_action, conversation_url=args.conversation_url, current_thread=args.current_thread, source_message_id=args.source_message_id, source_revision=args.source_revision, deadline_at=args.deadline_at, state=args.state))
                elif args.command == 'responsibility-list':
                    result = [asdict(item) for item in responsibilities.list(runnable_only=args.runnable_only)]
                elif args.command == 'responsibility-get':
                    if not args.responsibility_id:
                        raise ValueError('responsibility id required')
                    result = {'responsibility': asdict(responsibilities.get(args.responsibility_id)), 'history': [asdict(item) for item in responsibilities.history(args.responsibility_id)]}
                elif args.command == 'responsibility-claim':
                    if not args.responsibility_id:
                        raise ValueError('responsibility id required')
                    result = asdict(responsibilities.claim(args.responsibility_id, args.holder, args.lease_seconds))
                elif args.command == 'responsibility-execution-gate':
                    if not args.responsibility_id or not args.fence:
                        raise ValueError('responsibility id and fence required')
                    result = {'allowed': responsibilities.execution_gate(args.responsibility_id, args.fence)}
                elif args.command == 'responsibility-complete':
                    if not args.responsibility_id or not args.fence:
                        raise ValueError('responsibility id and fence required')
                    result = asdict(responsibilities.complete(args.responsibility_id, args.fence))
                elif args.command == 'responsibility-resume':
                    if not args.responsibility_id:
                        raise ValueError('responsibility id required')
                    result = asdict(responsibilities.resume(args.responsibility_id))
                elif args.command == 'responsibility-cancel':
                    if not args.responsibility_id:
                        raise ValueError('responsibility id required')
                    result = asdict(responsibilities.cancel(args.responsibility_id))
        elif args.command in ('status', 'pending', 'poll', 'claim', 'relay-ack', 'complete', 'remind', 'reminders'):
            with InboxStore(path) as store:
                if args.command in ('remind', 'reminders'):
                    from .reminders import ReminderStore
                    with ReminderStore(path) as reminders:
                        if args.command == 'reminders':
                            result = [asdict(item) for item in reminders.list_due()]
                        else:
                            if not args.key or not args.file or args.due_at is None:
                                raise ValueError('key, text file, and due-at required')
                            result = asdict(reminders.create(args.key, args.file.read_text(), args.due_at, thread_ts=args.thread_ts))
                elif args.command == 'status':
                    result = {'enabled': config['enabled'], 'pending_relays': store.pending_relay_count(), 'checkpoints': {key: asdict(value) if (value := store.get_checkpoint(key)) else None for key in ('receiver.loop', 'receiver.channel_error', 'receiver.test_environment_error', 'receiver.recovery', 'receiver.recovery_error', 'receiver.resurface_error', 'receiver.card_presence_error', 'receiver.card_render_error', 'receiver.card_nudge_error', 'receiver.conversation_feed_error', 'receiver.conversation_feed_queue_error', 'receiver.stopped', 'receiver.commands', 'dispatcher.loop', 'dispatcher.error', 'receiver.reminder_error', 'receiver.intake_notice_error', 'luna.scan', 'runtime.credentials')}}
                elif args.command == 'pending':
                    result = {'pending_relays': [asdict(item) for item in store.list_pending_relays()], 'open_messages': [asdict(item) for item in store.list_open_messages()]}
                elif args.command == 'poll':
                    result = collect_poll(path, role=args.role, enabled=config['enabled'])
                elif args.command == 'claim':
                    result = [asdict(item) for item in store.claim_pending_relay(args.holder, 300, 20)]
                elif args.command == 'relay-ack':
                    if not args.file:
                        raise ValueError('lease file required')
                    leases = json.loads(args.file.read_text())
                    result = []
                    for item in leases:
                        lease = RelayLease(**item)
                        if store.get_receipts(lease.message_id).is_relayed(lease.revision):
                            state = 'already_acknowledged'
                        else:
                            try:
                                store.acknowledge_relay(lease)
                                state = 'acknowledged'
                            except Exception as error:
                                state = type(error).__name__
                        result.append({'message_id': lease.message_id, 'revision': lease.revision, 'state': state})
                elif args.command == 'complete':
                    if args.message_id is None or args.revision is None:
                        raise ValueError('message-id and observed revision required')
                    result = {'completed': store.mark_completed_if_revision(args.message_id, args.revision)}
        else:
            from .service_queue import submit_command
            if not config['enabled']:
                raise RuntimeError('Receiver disabled')
            if args.command == 'dispatch-reconcile':
                args.retry_if_stopped = args.retry_if_stopped or 'verify'
            args.payload_text = args.file.read_text() if args.file else None
            if args.command == 'canvas-sync' and args.payload_text is None:
                args.payload_text = (project / 'state/canvas.md').read_text()
            result = submit_command(path, args, config)
        if result is not None:
            print(json.dumps(result))
        return 0
    except Exception as error:
        if isinstance(error, CredentialUnavailable):
            with InboxStore(path) as store:
                store.set_checkpoint('runtime.credentials', 'unavailable')
            print(json.dumps({'error_type': type(error).__name__, 'error_code': 'credential_unavailable', 'ok': False}))
        else:
            print(json.dumps({'error_type': type(error).__name__, 'ok': False}))
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
