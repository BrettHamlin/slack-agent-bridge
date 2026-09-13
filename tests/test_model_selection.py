import os
os.environ['LITELLM_LOCAL_MODEL_COST_MAP'] = 'True'
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock
from director.model_selection import ModelSelector, MODELS


class ModelSelectionTests(unittest.IsolatedAsyncioTestCase):
    async def test_classifier_alone_is_called_and_tier_maps_within_bound_harness(self):
        for backend in MODELS:
            selector = ModelSelector(backend, router=SimpleNamespace())
            for tier, grade in [('SIMPLE','light'),('MEDIUM','standard'),('COMPLEX','heavy'),('REASONING','heavy')]:
                selector.router.acompletion = AsyncMock(return_value=SimpleNamespace(
                    choices=[SimpleNamespace(message=SimpleNamespace(content='{"tier":"'+tier+'"}'))],
                    _hidden_params={}))
                decision = await selector.select('Synthetic task to classify')
                self.assertEqual(decision.backend, backend)
                self.assertEqual(decision.model, MODELS[backend][grade])
                self.assertEqual(decision.effort, 'high')
                self.assertEqual(decision.cause, 'llm_classifier')
                self.assertEqual(selector.router.acompletion.await_count, 1)
                self.assertEqual(selector.router.acompletion.call_args.kwargs['model'], 'director-classifier')

    async def test_failed_classifier_uses_local_heuristic_and_preserves_backend(self):
        selector = ModelSelector('claude', router=SimpleNamespace())
        selector.router.acompletion = AsyncMock(side_effect=TimeoutError())
        decision = await selector.select('Implement async function database transaction algorithm')
        self.assertEqual(decision.backend, 'claude')
        self.assertIn(decision.model, MODELS['claude'].values())
        self.assertNotEqual(decision.cause, 'llm_classifier')
        self.assertEqual(selector.router.acompletion.await_count, 1)

    async def test_local_no_signal_defaults_standard(self):
        selector = ModelSelector(local_only=True)
        selector.router.acompletion = AsyncMock(side_effect=AssertionError('must not call provider'))
        decision = await selector.select('xyzzy')
        self.assertEqual(decision.grade, 'standard')
        selector.router.acompletion.assert_not_called()

    async def test_empty_prompt_does_not_call_classifier(self):
        selector = ModelSelector(router=SimpleNamespace())
        selector.router.acompletion = AsyncMock()
        with self.assertRaisesRegex(ValueError, 'selection_prompt_empty'):
            await selector.select(' ')
        selector.router.acompletion.assert_not_called()

    async def test_malformed_classifier_output_uses_local_heuristic(self):
        for content in ('not json', '{"tier":"FOREIGN"}', '{"model":"other"}'):
            selector = ModelSelector('claude', router=SimpleNamespace())
            selector.router.acompletion = AsyncMock(return_value=SimpleNamespace(
                choices=[SimpleNamespace(message=SimpleNamespace(content=content))],
                _hidden_params={}))
            decision = await selector.select('xyzzy')
            self.assertEqual(decision.backend, 'claude')
            self.assertEqual(decision.grade, 'standard')
            self.assertNotEqual(decision.cause, 'llm_classifier')

    def test_subscription_deployment_uses_responses_preserving_luna_xhigh(self):
        from unittest.mock import patch
        from litellm.main import responses_api_bridge_check
        from litellm.completion_extras.litellm_responses_transformation.transformation import LiteLLMResponsesTransformationHandler
        from litellm.llms.chatgpt.responses.transformation import ChatGPTResponsesAPIConfig
        from litellm.types.router import GenericLiteLLMParams

        with patch('litellm.Router', return_value=SimpleNamespace()) as router:
            ModelSelector()
        deployment = router.call_args.kwargs['model_list'][0]['litellm_params']
        provider, model = deployment['model'].split('/', 1)
        self.assertEqual(provider, 'chatgpt')
        info, wire_model = responses_api_bridge_check(
            model=model, custom_llm_provider=provider,
            reasoning_effort=deployment['reasoning_effort'])
        self.assertEqual(info['mode'], 'responses')
        self.assertEqual(wire_model, 'gpt-5.6-luna')
        reasoning = LiteLLMResponsesTransformationHandler()._map_reasoning_effort(deployment['reasoning_effort'])
        with patch('litellm.llms.chatgpt.responses.transformation.Authenticator'):
            config = ChatGPTResponsesAPIConfig()
        body = config.transform_responses_api_request(
            model=wire_model, input='Synthetic classification request',
            response_api_optional_request_params={'reasoning': reasoning,
                'text': {'format': {'type': 'json_object'}}},
            litellm_params=GenericLiteLLMParams(), headers={})
        self.assertEqual(body['model'], 'gpt-5.6-luna')
        self.assertEqual(body['reasoning']['effort'], 'xhigh')
        self.assertTrue(body['stream'])
        self.assertFalse(body['store'])
        self.assertNotIn('text', body)  # Pinned subscription transport drops schema.

    async def test_streaming_classifier_recovers_deltas_with_empty_terminal_output(self):
        from director.model_selection import _StreamingClassifierRouter
        from litellm.types.utils import ModelResponseStream
        async def chunks():
            yield ModelResponseStream(choices=[{'index':0,'delta':{'content':'{"tier":'},'finish_reason':None}])
            yield ModelResponseStream(choices=[{'index':0,'delta':{'content':'"SIMPLE"}'},'finish_reason':None}])
            yield ModelResponseStream(choices=[{'index':0,'delta':{'content':''},'finish_reason':'stop'}])
        router = SimpleNamespace(acompletion=AsyncMock(return_value=chunks()))
        result = await _StreamingClassifierRouter(router).acompletion(model='director-classifier', timeout=3)
        self.assertEqual(result.choices[0].message.content, '{"tier":"SIMPLE"}')
        self.assertTrue(router.acompletion.call_args.kwargs['stream'])

    async def test_streaming_classifier_rejects_partial_or_truncated_json(self):
        from director.model_selection import _StreamingClassifierRouter
        from litellm.types.utils import ModelResponseStream
        for terminal in (None, 'length', 'tool_calls'):
            async def chunks():
                yield ModelResponseStream(choices=[{'index':0,'delta':{'content':'{"tier":"SIMPLE"}'},'finish_reason':terminal}])
            router = SimpleNamespace(acompletion=AsyncMock(return_value=chunks()))
            with self.assertRaisesRegex(ValueError, 'selection_stream_incomplete'):
                await _StreamingClassifierRouter(router).acompletion(timeout=3)
