"""Selection must target one existing ACP session and fail before execution on drift."""
import unittest
from types import SimpleNamespace
from unittest.mock import Mock
from director.agent_gateway import ACPDriver, GatewayError


def response(model, effort):
    return SimpleNamespace(config_options=[
        SimpleNamespace(id='model', current_value=model),
        SimpleNamespace(id='reasoning_effort', current_value=effort),
    ])


class SessionSelectionTests(unittest.TestCase):
    def driver(self, answer):
        driver = object.__new__(ACPDriver)
        driver.wait_ready = Mock()
        state = {'model': 'old', 'effort': 'medium'}
        calls = []
        def set_option(**kw):
            calls.append(kw)
            if kw['config_id'] == 'model':
                state.update(model=kw['value'], effort='low')
            else:
                state['effort'] = kw['value']
            return response(state['model'], state['effort']) if answer is None else answer
        driver.connection = SimpleNamespace(set_config_option=set_option)
        driver.call = lambda value: value
        return driver, calls

    def test_model_then_effort_use_the_same_existing_session(self):
        driver, calls = self.driver(None)
        result = driver.configure_session('thread-a', model='gpt-5.6-terra', effort='high')
        self.assertEqual(calls, [
            {'config_id': 'model', 'session_id': 'thread-a', 'value': 'gpt-5.6-terra'},
            {'config_id': 'reasoning_effort', 'session_id': 'thread-a', 'value': 'high'},
        ])
        self.assertEqual(result.config_options[1].current_value, 'high')

    def test_refuses_an_unconfirmed_model_or_effort(self):
        for answer in (response('wrong', 'high'), response('gpt-5.6-terra', 'low'), SimpleNamespace()):
            with self.subTest(answer=answer):
                driver, _ = self.driver(answer)
                with self.assertRaisesRegex(GatewayError, 'runtime_selection_unverified'):
                    driver.configure_session('thread-a', model='gpt-5.6-terra', effort='high')

    def test_bad_selection_sends_no_request(self):
        driver, calls = self.driver(None)
        with self.assertRaisesRegex(GatewayError, 'runtime_selection_invalid'):
            driver.configure_session('', model='gpt-5.6-terra', effort='high')
        self.assertEqual(calls, [])

    def test_failed_model_change_does_not_attempt_effort(self):
        driver, calls = self.driver(None)
        driver.call = Mock(side_effect=GatewayError('runtime_request_failed'))
        with self.assertRaisesRegex(GatewayError, 'runtime_request_failed'):
            driver.configure_session('thread-a', model='gpt-5.6-terra', effort='high')
        self.assertEqual(len(calls), 1)


class AsyncTurnSelectionTests(unittest.IsolatedAsyncioTestCase):
    def make_driver(self, set_option):
        import asyncio
        import queue
        from unittest.mock import AsyncMock
        driver = object.__new__(ACPDriver)
        driver.wait_ready = Mock()
        driver.generation = 9
        driver.turn = 0
        driver.loop = asyncio.get_running_loop()
        driver.profile = SimpleNamespace(request_timeout_seconds=.05)
        driver.events = queue.SimpleQueue()
        driver.connection = SimpleNamespace(set_config_option=set_option, prompt=AsyncMock())
        return driver

    async def run_turn(self, driver):
        import asyncio
        from unittest.mock import patch
        from director.model_selection import Selection
        tasks = []
        def schedule(coroutine, loop):
            task = loop.create_task(coroutine)
            tasks.append(task)
            return task
        reserved = []
        with patch('director.agent_gateway.asyncio.run_coroutine_threadsafe', side_effect=schedule):
            turn = driver.prompt('persisted-session', 'task', on_turn=reserved.append,
                                 selection=Selection('codex', 'standard', 'gpt-5.6-terra', 'high', 'llm_classifier'))
        await asyncio.gather(*tasks)
        self.assertEqual(reserved, [turn])
        return turn

    async def test_async_setters_finish_and_verify_before_prompt(self):
        import asyncio
        calls = []
        async def setter(**kw):
            calls.append((kw['config_id'], kw['session_id'], kw['value']))
            # Yield control to make premature prompt dispatch observable.
            await asyncio.sleep(0)
            driver.connection.prompt.assert_not_awaited()
            return response('gpt-5.6-terra', 'high' if kw['config_id'] == 'reasoning_effort' else 'low')
        driver = self.make_driver(setter)
        turn = await self.run_turn(driver)
        self.assertEqual(calls, [('model', 'persisted-session', 'gpt-5.6-terra'),
                                 ('reasoning_effort', 'persisted-session', 'high')])
        driver.connection.prompt.assert_awaited_once()
        self.assertEqual(driver.connection.prompt.await_args.kwargs['session_id'], 'persisted-session')
        self.assertEqual(driver.events.get_nowait(), ('configured', 'persisted-session', {
            'turn_id': turn, 'model': 'gpt-5.6-terra', 'effort': 'high'}))
        self.assertEqual(driver.events.get_nowait(), ('terminal', 'persisted-session', {'turn_id': turn}))
        self.assertTrue(driver.events.empty())

    async def test_echo_mismatch_never_submits_prompt(self):
        from unittest.mock import AsyncMock
        for answer in (response('wrong', 'high'), response('gpt-5.6-terra', 'low'), SimpleNamespace()):
            with self.subTest(answer=answer):
                driver = self.make_driver(AsyncMock(return_value=answer))
                turn = await self.run_turn(driver)
                driver.connection.prompt.assert_not_awaited()
                kind, session, detail = driver.events.get_nowait()
                self.assertEqual((kind, session, detail['turn_id']), ('error', 'persisted-session', turn))
                self.assertIs(detail['prompt_submitted'], False)
                self.assertEqual(detail['error'], 'selection_configuration_failed')
                self.assertTrue(driver.events.empty())

    async def test_setter_timeout_cancels_configuration_without_prompt(self):
        import asyncio
        calls = []
        cancelled = asyncio.Event()
        async def stalled(**kw):
            calls.append(kw['config_id'])
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()
        driver = self.make_driver(stalled)
        turn = await self.run_turn(driver)
        self.assertTrue(cancelled.is_set())
        self.assertEqual(calls, ['model'])
        driver.connection.prompt.assert_not_awaited()
        kind, session, detail = driver.events.get_nowait()
        self.assertEqual((kind, session, detail['turn_id']), ('error', 'persisted-session', turn))
        self.assertIs(detail['prompt_submitted'], False)
        self.assertEqual(detail['error_type'], 'TimeoutError')
        self.assertTrue(driver.events.empty())
