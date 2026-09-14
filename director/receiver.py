"""One Socket Mode connection, isolated per-channel state and command queues."""
import argparse
from contextlib import ExitStack
import json
import signal
import queue
import threading
import time

from .inbox import InboxStore
from .receipt_ack import ReceiptAckWorker
from .runtime import credentials, database_path, needs_you_oauth_credentials, quiet_sdk, singleton, verify_identity, verify_conversation_feed_identity
from .slack_service import SlackService
from .slack_transport import SlackAllowlist, create_socket_mode_client, make_socket_mode_listener, _socket_mode_response


def channel_configs(primary, project, load):
    from .conversation_feed import feed_target
    from .needs_you import needs_you_target
    configs = [primary]
    for name in primary.get('additional_configs', []):
        path = (project / name).resolve()
        if path.parent != (project / 'config').resolve():
            raise ValueError('Additional config must be in project/config')
        config = load(path)
        if config.get('additional_configs'):
            raise ValueError('Nested channel configs are not supported')
        if any(config.get(k) != primary.get(k) for k in
               ('team_id', 'owner_user_id', 'bot_user_id', 'slack_app_id', 'workspace_domain')):
            raise ValueError('Additional channel identity mismatch')
        if config.get('environment') != 'test':
            raise ValueError('Additional channel must be an explicit test environment')
        if config['enabled']:
            configs.append(config)
    paths, channels, feed_channels = set(), set(), set()
    for config in configs:
        path = (project / config['database_path']).resolve()
        if path.parent in paths or config['channel_id'] in channels:
            raise ValueError('Channel/state directories must be unique')
        if config is not primary and (not path.is_relative_to((project / 'state/testing').resolve())
                                      or path.parent == (project / primary['database_path']).resolve().parent):
            raise ValueError('Test state must be isolated under state/testing')
        paths.add(path.parent)
        channels.add(config['channel_id'])
        target = feed_target(config)
        if target is not None:
            if target.channel_id in channels or target.channel_id in feed_channels:
                raise ValueError('Conversation feed channels must be distinct from every source and feed channel')
            feed_channels.add(target.channel_id)
        # App Home is one private surface per Slack app/team/owner.  Test
        # channels share the primary identity, so a second enabled projection
        # would expose the wrong local state to the owner.
        if config is not primary and needs_you_target(config) is not None:
            raise ValueError('Needs-you App Home must be configured only on the primary channel')
    if channels & feed_channels:
        raise ValueError('Conversation feed channels must be distinct from every source channel')
    return configs


def routed_listener(routes, response_factory=_socket_mode_response, *, home_routes=None):
    """Select exactly one listener before acknowledging an envelope."""
    def listener(client, request):
        payload = getattr(request, 'payload', None)
        if not isinstance(payload, dict):
            return
        if getattr(request, 'type', None) == 'events_api':
            event = payload.get('event')
            if not isinstance(event, dict):
                event = {}
            if event.get('type') == 'app_home_opened':
                selected = (home_routes or {}).get((payload.get('team_id'), event.get('user')))
                if selected:
                    selected(client, request)
                elif getattr(request, 'envelope_id', None):
                    client.send_socket_mode_response(response_factory(request.envelope_id))
                return
            key = (payload.get('team_id'), event.get('channel'))
        elif getattr(request, 'type', None) == 'interactive':
            team, channel = payload.get('team'), payload.get('channel')
            view, user = payload.get('view'), payload.get('user')
            if isinstance(view, dict) and isinstance(user, dict):
                selected = (home_routes or {}).get((team.get('id') if isinstance(team, dict) else None, user.get('id')))
                if selected:
                    selected(client, request)
                elif getattr(request, 'envelope_id', None):
                    client.send_socket_mode_response(response_factory(request.envelope_id))
                return
            key = (team.get('id') if isinstance(team, dict) else None,
                   channel.get('id') if isinstance(channel, dict) else None)
        else:
            return
        selected = routes.get(key)
        if selected:
            selected(client, request)
        elif getattr(request, 'envelope_id', None):
            client.send_socket_mode_response(response_factory(request.envelope_id))
    return listener


class FeedApprovalInteraction:
    """Fast owner-only feed callback gate; durable work stays on ChannelLoop."""

    def __init__(self, config, feed, requests):
        target = feed.target
        self.team_id = config['team_id']
        self.owner_user_id = config['owner_user_id']
        self.feed_channel_id = target.channel_id if target else ''
        self.feed = feed
        self.requests = requests

    def handle(self, payload):
        if not isinstance(payload, dict):
            return False
        team, user, channel = payload.get('team'), payload.get('user'), payload.get('channel')
        if (not isinstance(team, dict) or not isinstance(user, dict) or not isinstance(channel, dict)
                or team.get('id') != self.team_id or user.get('id') != self.owner_user_id
                or channel.get('id') != self.feed_channel_id):
            return False
        actions = payload.get('actions')
        if not isinstance(actions, list) or len(actions) != 1 or not isinstance(actions[0], dict):
            return False
        action = actions[0]
        token = action.get('value')
        card_ts = (payload.get('container') or {}).get('message_ts')
        if (action.get('action_id') != 'director_approve_once' or not isinstance(token, str)
                or not isinstance(card_ts, str) or not self.feed.reserve_guardian_click(token)):
            return False
        # Queue data has no authority, native event, payload, or review handle.
        self.requests.put((token, card_ts))
        return True


def make_feed_listener(handler, response_factory=_socket_mode_response):
    """Acknowledge output-only feed envelopes without feeding them into intake."""
    def listener(client, request):
        # Socket Mode's acknowledgement is independent of the later durable
        # approval queue. A malformed/replayed feed callback must not cause a
        # retry storm or delay original channel traffic.
        if getattr(request, 'envelope_id', None):
            client.send_socket_mode_response(response_factory(request.envelope_id))
        if getattr(request, 'type', None) == 'interactive':
            try:
                handler.handle(getattr(request, 'payload', None))
            except Exception:
                return
    return listener


def make_needs_you_listener(home, wake, response_factory=_socket_mode_response):
    """Acknowledge and durably handle only this owner's App Home callbacks."""
    def listener(client, request):
        payload = getattr(request, 'payload', None)
        try:
            if getattr(request, 'type', None) == 'events_api':
                if home.queue_open(payload):
                    wake()
            elif getattr(request, 'type', None) == 'interactive':
                home.handle_interaction(payload)
                wake()
            else:
                return
            if getattr(request, 'envelope_id', None):
                client.send_socket_mode_response(response_factory(request.envelope_id))
        except Exception:
            # A durable interaction must be retried if its state transition
            # could not be recorded.  Do not include Slack payloads in logs.
            return
    return listener


class ChannelLoop:
    def __init__(self, config, project, path, store, service, execute, *, feed_available=True):
        from .conversation_feed import ConversationFeed, ConversationFeedWorker
        from .needs_you import NeedsYouHome, NeedsYouHomeWorker
        from .service_queue import ServiceQueue
        from .dispatcher import Dispatcher
        self.config, self.project, self.path = config, project, path
        self.store, self.service, self.execute = store, service, execute
        self.queue = ServiceQueue(path)
        self.queue.recover_interrupted()
        self.feed = ConversationFeed(service._web_client, path, config)
        if not feed_available:
            self.feed.disable()
        self.feed_worker = ConversationFeedWorker(
            self.feed, lambda value: self.store.set_checkpoint('receiver.conversation_feed_error', value)
        )
        self.feed_worker.start()
        self.needs_you = NeedsYouHome(service._web_client, path, config)
        self.needs_you_worker = NeedsYouHomeWorker(
            self.needs_you, lambda value: self.store.set_checkpoint('receiver.needs_you_home_error', value)
        )
        self.needs_you_worker.start()
        self.needs_you_http = None
        if self.needs_you.target is not None and self.needs_you.target.action_url is not None:
            from .needs_you_http import NeedsYouActionServer
            oauth = needs_you_oauth_credentials(project)
            self.needs_you_http = NeedsYouActionServer(
                self.needs_you, service._web_client,
                client_id=oauth['SLACK_OPENID_CLIENT_ID'], client_secret=oauth['SLACK_OPENID_CLIENT_SECRET'],
                wake=self.needs_you_worker.wake,
            )
            self.needs_you_http.start()
        self.dispatcher = Dispatcher(
            project, config, store, service, feed=self.feed, feed_wake=self.feed_worker.wake,
            needs_you=self.needs_you, needs_you_wake=self.needs_you_worker.wake,
        ) if config.get('dispatcher', {}).get('enabled') else None
        if self.dispatcher is not None:
            self.dispatcher.recover_conversation_feed()
            self.dispatcher.recover_needs_you()
        self.feed_approval_requests = queue.SimpleQueue()
        self.feed_interactions = FeedApprovalInteraction(config, self.feed, self.feed_approval_requests)
        self.next_maintenance = self.next_recovery = 0

    def command(self, args):
        if args.command == 'dispatch-reconcile':
            if self.dispatcher is None:
                raise RuntimeError('Dispatcher unavailable')
            return self.dispatcher.reconcile_job(
                args.key, retry_if_stopped=args.retry_if_stopped == 'retry', operator_requested=True
            )
        if args.command == 'guardian-approve':
            if self.dispatcher is None:
                raise RuntimeError('Dispatcher unavailable')
            return self.dispatcher.approve_guardian_reply(args.key)
        if args.command == 'guardian-pending':
            if self.dispatcher is None:
                raise RuntimeError('Dispatcher unavailable')
            return self.dispatcher.pending_guardian_approvals()
        if args.command == 'agent-publish':
            if self.dispatcher is None:
                raise RuntimeError('Dispatcher unavailable')
            publish_args = (
                args.authority, args.payload_text, args.responsibility_id, args.fence,
                getattr(args, 'conversation_title', None), getattr(args, 'conversation_emoji', None),
                getattr(args, 'conversation_preview', None),
            )
            if getattr(args, 'owner_action_title', None) is None:
                return self.dispatcher.publish_agent_reply(*publish_args)
            return self.dispatcher.publish_agent_reply(
                *publish_args,
                needs_owner_action={'title': args.owner_action_title, 'detail': args.owner_action_detail},
            )
        if args.command == 'conversation-feed-import':
            if not args.payload_text:
                raise ValueError('conversation feed import requires a JSON file')
            result = self.feed.import_record(json.loads(args.payload_text))
            self.feed_worker.wake()
            return {'queued': result}
        if args.command == 'conversation-feed-hide':
            if not args.feed_root:
                raise ValueError('conversation feed hide requires a root')
            result = self.feed.hide(args.feed_root)
            self.feed_worker.wake()
            return {'hidden': result}
        return self.execute(args, self.config, self.project, self.path, self.store, self.service)

    def tick(self, connected):
        from .intake_health import notify_unread_sources
        self.queue.process_one(self.command, self.config)
        self._process_feed_approval()
        if self.dispatcher:
            try:
                self.dispatcher.tick()
                self.store.set_checkpoint('dispatcher.error', '')
                self.store.set_checkpoint('receiver.conversation_feed_queue_error', self.dispatcher.feed_error)
                self.store.set_checkpoint('receiver.needs_you_queue_error', self.dispatcher.needs_you_error)
            except Exception as error:
                self.store.set_checkpoint('dispatcher.error', type(error).__name__)
        if time.time() < self.next_maintenance:
            return
        self.next_maintenance = time.time() + 15
        self.store.set_checkpoint('receiver.commands', 'ready')
        self.store.set_checkpoint('receiver.loop', json.dumps({'connected': connected}))
        for checkpoint, operation in (
            ('receiver.reminder_error', lambda: self.command(argparse.Namespace(command='deliver-reminders'))),
            ('receiver.intake_notice_error', lambda: notify_unread_sources(self.store, self.service)),
            ('receiver.resurface_error', self.service.resurface_due_inbox_cards),
            ('receiver.card_presence_error', self.service.ensure_waiting_attention_cards),
            ('receiver.card_render_error', self.service.reconcile_pending_inbox_card_renders),
            ('receiver.card_nudge_error', self.service.deliver_pending_inbox_card_nudges),
            ('receiver.needs_you_resurface_error', self._resurface_needs_you),
        ):
            try:
                operation()
                self.store.set_checkpoint(checkpoint, '')
            except Exception as error:
                self.store.set_checkpoint(checkpoint, type(error).__name__)
        if time.time() >= self.next_recovery:
            try:
                with singleton(self.path.parent / 'recovery.lock'):
                    self.service.recover_owner_messages()
                    self.store.set_checkpoint('receiver.recovery', str(time.time()))
                    self.store.set_checkpoint('receiver.recovery_error', '')
            except Exception as error:
                self.store.set_checkpoint('receiver.recovery_error', type(error).__name__)
            self.next_recovery = time.time() + 900

    def _process_feed_approval(self):
        """Run one queued click on the loop/dispatcher owner thread."""
        try:
            approval_id, card_ts = self.feed_approval_requests.get_nowait()
        except queue.Empty:
            return
        job_key = self.feed.begin_guardian_approval(approval_id, card_ts)
        if job_key is None:
            return
        self.feed_worker.wake()
        if self.dispatcher is None:
            self.feed.set_guardian_approval_state(job_key, 'inactive')
            self.feed_worker.wake()
            return
        try:
            result = self.dispatcher.approve_guardian_reply(job_key)
        except Exception as error:
            self.feed.set_guardian_approval_state(job_key, 'failed')
            self.feed_worker.wake()
            self.store.set_checkpoint('receiver.conversation_feed_queue_error', type(error).__name__)
            return
        outcome = result.get('outcome') if isinstance(result, dict) else ''
        terminal = {
            'guardian_approval_submitted': None,
            'guardian_approval_expired': 'expired',
            'guardian_approval_runtime_stale': 'restart_inactive',
            'guardian_approval_runtime_unavailable': 'restart_inactive',
            'guardian_approval_context_unavailable': 'failed',
            'guardian_approval_source_stale': 'inactive',
            'guardian_approval_stale': 'inactive',
            'guardian_approval_root_busy': 'inactive',
            'guardian_approval_uncertain': 'failed',
            'guardian_approval_not_pending': 'inactive',
        }.get(outcome, 'failed')
        if terminal:
            self.feed.set_guardian_approval_state(job_key, terminal)
            self.feed_worker.wake()

    def close(self):
        if self.dispatcher:
            self.dispatcher.close()
        self.feed_worker.close()
        self.feed.close()
        if self.needs_you_http is not None:
            self.needs_you_http.close()
        self.needs_you_worker.close()
        self.needs_you.close()
        self.queue.close()
        self.store.set_checkpoint('receiver.stopped', str(time.time()))

    def _resurface_needs_you(self):
        if self.needs_you.resurface_due():
            self.needs_you_worker.wake()


def listen_channels(primary, project, load, execute):
    if not primary['enabled']:
        raise RuntimeError('Receiver disabled')
    if primary.get('environment') == 'test':
        raise ValueError('Test channels are hosted by the primary receiver; do not start a second socket')
    configs = channel_configs(primary, project, load)
    quiet_sdk()
    stop = threading.Event()
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, lambda *_: stop.set())
    with ExitStack() as stack:
        path = database_path(primary, project)
        stack.enter_context(singleton(path.parent / 'receiver.lock'))
        values = credentials(project)
        store = stack.enter_context(InboxStore(path))
        allow = SlackAllowlist(primary['team_id'], primary['channel_id'], primary['owner_user_id'], primary['workspace_domain'])
        client = create_socket_mode_client(values['SLACK_APP_TOKEN'], values['SLACK_BOT_TOKEN'], store, allow)
        stack.callback(client.close)
        routes, home_routes, loops, receipt_workers = {}, {}, [], []
        for index, config in enumerate(configs):
            try:
                verify_identity(client.web_client, config)
            except Exception as error:
                if not index:
                    raise
                # Loss of membership in the test channel must not stop the
                # real channel. Surface failure and require a verified restart.
                store.set_checkpoint('receiver.test_environment_error', type(error).__name__)
                continue
            if index:
                store.set_checkpoint('receiver.test_environment_error', '')
            target = database_path(config, project)
            if index:
                stack.enter_context(singleton(target.parent / 'receiver.lock'))
                current = stack.enter_context(InboxStore(target))
            else:
                current = store
            feed_available = True
            try:
                verify_conversation_feed_identity(client.web_client, config)
                current.set_checkpoint('receiver.conversation_feed_error', '')
            except Exception as error:
                # The feed is an optional projection. Its unavailable target
                # must not take source intake, receipts, or replies offline.
                feed_available = False
                current.set_checkpoint('receiver.conversation_feed_error', type(error).__name__)
            allow = SlackAllowlist(config['team_id'], config['channel_id'], config['owner_user_id'], config['workspace_domain'])
            service = stack.enter_context(SlackService(client.web_client, current, allow, target))
            receipt_worker = ReceiptAckWorker(target, client.web_client, allow)
            stack.callback(receipt_worker.close)
            receipt_workers.append(receipt_worker)
            routes[(config['team_id'], config['channel_id'])] = make_socket_mode_listener(
                current,
                allow,
                interaction_handler=service,
                intake_ack_notifier=receipt_worker.wake,
            )
            loop = ChannelLoop(config, project, target, current, service, execute, feed_available=feed_available)
            stack.callback(loop.close)
            loops.append(loop)
            if loop.feed.target is not None:
                routes[(config['team_id'], loop.feed.target.channel_id)] = make_feed_listener(loop.feed_interactions)
            if loop.needs_you.target is not None:
                home_key = (loop.needs_you.target.team_id, loop.needs_you.target.owner_user_id)
                if home_key in home_routes:
                    raise RuntimeError('Needs-you App Home route is ambiguous')
                home_routes[home_key] = make_needs_you_listener(loop.needs_you, loop.needs_you_worker.wake)
        client.socket_mode_request_listeners[:] = [routed_listener(routes, home_routes=home_routes)]
        client.connect()
        for receipt_worker in receipt_workers:
            receipt_worker.start()
        for loop in loops:
            loop.store.set_checkpoint('receiver.started', str(time.time()))
            loop.store.set_checkpoint('runtime.credentials', 'available')
        while not stop.is_set():
            for loop in loops:
                try:
                    loop.tick(client.is_connected())
                    loop.store.set_checkpoint('receiver.channel_error', '')
                except Exception as error:
                    loop.store.set_checkpoint('receiver.channel_error', type(error).__name__)
            stop.wait(0.25)
