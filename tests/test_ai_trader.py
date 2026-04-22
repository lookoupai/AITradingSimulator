from __future__ import annotations

import json
import unittest
from unittest.mock import patch

from requests import RequestException
from requests import Response

from ai_trader import AIPredictor
from services.prediction_guard import AIPredictionError


class AIPredictorEncodingTests(unittest.TestCase):
    def setUp(self):
        AIPredictor._gateway_capability_cache.clear()
        AIPredictor._preferred_base_url_cache.clear()
        self.predictor = AIPredictor(
            api_key='test-key',
            api_url='https://example.com/v1',
            model_name='gpt-5.4'
        )

    def _build_response(self, body: str, content_type: str, status_code: int = 200) -> Response:
        response = Response()
        response.status_code = status_code
        response.headers['Content-Type'] = content_type
        response._content = body.encode('utf-8')
        response.encoding = 'ISO-8859-1'
        return response

    def test_parse_compatible_http_response_prefers_utf8_for_json(self):
        response = self._build_response(
            json.dumps({
                'issue_no': '3418102',
                'predicted_big_small': '大',
                'predicted_odd_even': '单',
                'predicted_combo': '大单',
                'reasoning_summary': '大单连出4次，追大单第17期'
            }, ensure_ascii=False),
            'application/json'
        )

        payload = self.predictor._parse_compatible_http_response(response, 'chat_completions')

        self.assertEqual(payload['predicted_big_small'], '大')
        self.assertEqual(payload['predicted_odd_even'], '单')
        self.assertEqual(payload['predicted_combo'], '大单')
        self.assertEqual(payload['reasoning_summary'], '大单连出4次，追大单第17期')

    def test_parse_compatible_http_response_prefers_utf8_for_sse(self):
        response = self._build_response(
            '\n'.join([
                'data: {"id":"resp_1","choices":[{"delta":{"content":"大单"},"finish_reason":null}]}',
                '',
                'data: {"choices":[{"delta":{},"finish_reason":"stop"}]}',
                '',
                'data: [DONE]',
                ''
            ]),
            'text/event-stream'
        )

        payload = self.predictor._parse_compatible_http_response(response, 'chat_completions')
        message = payload['choices'][0]['message']

        self.assertEqual(message['content'], '大单')
        self.assertEqual(payload['choices'][0]['finish_reason'], 'stop')

    def test_parse_compatible_http_response_accepts_json_with_sse_content_type(self):
        response = self._build_response(
            json.dumps({
                'id': 'resp_1',
                'object': 'chat.completion',
                'choices': [
                    {
                        'index': 0,
                        'message': {
                            'role': 'assistant',
                            'content': '{"predicted_combo":"大单"}'
                        },
                        'finish_reason': 'stop'
                    }
                ]
            }, ensure_ascii=False),
            'text/event-stream'
        )

        payload = self.predictor._parse_compatible_http_response(response, 'chat_completions')

        self.assertEqual(payload['id'], 'resp_1')
        self.assertEqual(payload['choices'][0]['message']['content'], '{"predicted_combo":"大单"}')

    def test_extract_http_error_message_prefers_utf8(self):
        response = self._build_response(
            json.dumps({
                'error': {
                    'message': '请求频率过高，请稍后重试'
                }
            }, ensure_ascii=False),
            'application/json'
        )
        response.status_code = 429

        message = self.predictor._extract_http_error_message(response)

        self.assertEqual(message, '请求频率过高，请稍后重试')

    def test_iter_token_limit_kwargs_for_chat_completions_keeps_third_party_compatibility_first(self):
        variants = self.predictor._iter_token_limit_kwargs(
            resolved_api_mode='chat_completions',
            max_output_tokens=512,
            prefer_legacy_chat_token_param=True
        )

        self.assertEqual(
            variants,
            [
                {'max_tokens': 512},
                {'max_completion_tokens': 512},
                {}
            ]
        )

    def test_iter_token_limit_kwargs_for_responses_falls_back_to_omitting_limit(self):
        variants = self.predictor._iter_token_limit_kwargs(
            resolved_api_mode='responses',
            max_output_tokens=256,
            prefer_legacy_chat_token_param=False
        )

        self.assertEqual(
            variants,
            [
                {'max_output_tokens': 256},
                {}
            ]
        )

    def test_build_compatible_payload_for_chat_completions_includes_response_format_when_json_output(self):
        payload = self.predictor._build_compatible_payload(
            resolved_api_mode='chat_completions',
            prompt='PROMPT',
            system_prompt='SYSTEM',
            max_output_tokens=None,
            json_output=True
        )

        self.assertEqual(payload['response_format'], {'type': 'json_object'})
        self.assertEqual(payload['messages'][0]['content'], 'SYSTEM')
        self.assertEqual(payload['messages'][1]['content'], 'PROMPT')

    def test_candidate_base_urls_prioritizes_cached_preferred_base_url(self):
        predictor = AIPredictor(
            api_key='test-key',
            api_url='https://example.com',
            model_name='gpt-5.4'
        )
        predictor._store_preferred_base_url('chat_completions', 'https://example.com/v1')

        self.assertEqual(
            predictor._candidate_base_urls('chat_completions'),
            ['https://example.com/v1', 'https://example.com']
        )

    def test_call_llm_with_metadata_remembers_successful_base_url(self):
        predictor = AIPredictor(
            api_key='test-key',
            api_url='https://example.com',
            model_name='gpt-5.4'
        )
        predictor.transport_attempts = 1
        call_order = []

        def call_side_effect(*args, **kwargs):
            base_url = kwargs['base_url']
            call_order.append(base_url)
            if base_url == 'https://example.com':
                raise Exception('HTTP 404：not found')
            return {
                'raw_response': '{"status":"ok"}',
                'api_mode': kwargs['resolved_api_mode'],
                'response_model': 'gpt-5.4',
                'finish_reason': 'stop',
                'latency_ms': 10
            }

        with patch.object(predictor, '_call_llm_via_compatible_http', side_effect=call_side_effect):
            predictor._call_llm_with_metadata('PROMPT', json_output=True)
            predictor._call_llm_with_metadata('PROMPT', json_output=True)

        self.assertEqual(
            call_order,
            [
                'https://example.com',
                'https://example.com/v1',
                'https://example.com/v1'
            ]
        )

    def test_call_llm_with_metadata_skips_same_host_sibling_base_url_after_transport_timeout(self):
        predictor = AIPredictor(
            api_key='test-key',
            api_url='https://example.com',
            model_name='gpt-5.4'
        )
        predictor.transport_attempts = 1
        call_order = []

        def call_side_effect(*args, **kwargs):
            call_order.append(kwargs['base_url'])
            raise RequestException('Read timed out')

        with patch.object(predictor, '_call_llm_via_compatible_http', side_effect=call_side_effect):
            with self.assertRaises(AIPredictionError):
                predictor._call_llm_with_metadata('PROMPT', json_output=True)

        self.assertEqual(call_order, ['https://example.com'])

    def test_call_with_token_limit_fallback_retries_on_unsupported_parameter(self):
        attempts = []

        def caller(request_kwargs):
            attempts.append(request_kwargs)
            if 'max_tokens' in request_kwargs:
                raise Exception('HTTP 400：Unsupported parameter: max_tokens')
            return {'status': 'ok', 'request_kwargs': request_kwargs}

        response, latency_ms = self.predictor._call_with_token_limit_fallback(
            capability_cache_key=('https://example.com/v1', 'gpt-5.4', 'chat_completions'),
            base_request_kwargs={'model': 'gpt-5.4', 'temperature': 0.7},
            resolved_api_mode='chat_completions',
            max_output_tokens=300,
            prefer_legacy_chat_token_param=True,
            caller=caller
        )

        self.assertEqual(
            attempts,
            [
                {'model': 'gpt-5.4', 'temperature': 0.7, 'max_tokens': 300},
                {'model': 'gpt-5.4', 'temperature': 0.7, 'max_completion_tokens': 300}
            ]
        )
        self.assertEqual(response['status'], 'ok')
        self.assertEqual(
            response['request_kwargs'],
            {'model': 'gpt-5.4', 'temperature': 0.7, 'max_completion_tokens': 300}
        )
        self.assertGreaterEqual(latency_ms, 0)

    def test_call_with_token_limit_fallback_retries_on_http_400_response(self):
        attempts = []

        def caller(request_kwargs):
            attempts.append(request_kwargs)
            if request_kwargs == {'model': 'gpt-5.4', 'temperature': 0.7, 'max_tokens': 300}:
                return self._build_response(
                    json.dumps({
                        'error': {
                            'message': 'Unsupported parameter: max_tokens'
                        }
                    }, ensure_ascii=False),
                    'application/json',
                    status_code=400
                )

            return self._build_response(
                json.dumps({
                    'id': 'chatcmpl_1',
                    'object': 'chat.completion',
                    'choices': [
                        {
                            'index': 0,
                            'message': {
                                'role': 'assistant',
                                'content': '{"status":"ok"}'
                            },
                            'finish_reason': 'stop'
                        }
                    ]
                }, ensure_ascii=False),
                'application/json'
                )

        response, latency_ms = self.predictor._call_with_token_limit_fallback(
            capability_cache_key=('https://example.com/v1', 'gpt-5.4', 'chat_completions'),
            base_request_kwargs={'model': 'gpt-5.4', 'temperature': 0.7},
            resolved_api_mode='chat_completions',
            max_output_tokens=300,
            prefer_legacy_chat_token_param=True,
            caller=caller
        )

        self.assertEqual(
            attempts,
            [
                {'model': 'gpt-5.4', 'temperature': 0.7, 'max_tokens': 300},
                {'model': 'gpt-5.4', 'temperature': 0.7, 'max_completion_tokens': 300}
            ]
        )
        self.assertEqual(response.status_code, 200)
        self.assertGreaterEqual(latency_ms, 0)

    def test_call_with_token_limit_fallback_drops_temperature_when_unsupported(self):
        attempts = []

        def caller(request_kwargs):
            attempts.append(request_kwargs)
            if 'temperature' in request_kwargs:
                return self._build_response(
                    json.dumps({
                        'error': {
                            'message': 'Unsupported parameter: temperature'
                        }
                    }, ensure_ascii=False),
                    'application/json',
                    status_code=400
                )

            return self._build_response(
                json.dumps({
                    'id': 'chatcmpl_1',
                    'object': 'chat.completion',
                    'choices': [
                        {
                            'index': 0,
                            'message': {
                                'role': 'assistant',
                                'content': '{"status":"ok"}'
                            },
                            'finish_reason': 'stop'
                        }
                    ]
                }, ensure_ascii=False),
                'application/json'
            )

        response, latency_ms = self.predictor._call_with_token_limit_fallback(
            capability_cache_key=('https://example.com/v1', 'gpt-5.4', 'chat_completions'),
            base_request_kwargs={'model': 'gpt-5.4', 'temperature': 0.7},
            resolved_api_mode='chat_completions',
            max_output_tokens=300,
            prefer_legacy_chat_token_param=True,
            caller=caller
        )

        self.assertEqual(
            attempts,
            [
                {'model': 'gpt-5.4', 'temperature': 0.7, 'max_tokens': 300},
                {'model': 'gpt-5.4', 'max_tokens': 300},
            ]
        )
        self.assertEqual(response.status_code, 200)
        self.assertGreaterEqual(latency_ms, 0)

    def test_call_with_token_limit_fallback_reuses_cached_disabled_parameters(self):
        cache_key = ('https://example.com/v1', 'gpt-5.4', 'chat_completions')
        attempts = []

        def caller(request_kwargs):
            attempts.append(request_kwargs)
            if 'temperature' in request_kwargs:
                return self._build_response(
                    json.dumps({
                        'error': {
                            'message': 'Unsupported parameter: temperature'
                        }
                    }, ensure_ascii=False),
                    'application/json',
                    status_code=400
                )

            return self._build_response(
                json.dumps({
                    'id': 'chatcmpl_1',
                    'object': 'chat.completion',
                    'choices': [
                        {
                            'index': 0,
                            'message': {
                                'role': 'assistant',
                                'content': '{"status":"ok"}'
                            },
                            'finish_reason': 'stop'
                        }
                    ]
                }, ensure_ascii=False),
                'application/json'
            )

        self.predictor._call_with_token_limit_fallback(
            capability_cache_key=cache_key,
            base_request_kwargs={'model': 'gpt-5.4', 'temperature': 0.7},
            resolved_api_mode='chat_completions',
            max_output_tokens=300,
            prefer_legacy_chat_token_param=True,
            caller=caller
        )
        self.assertEqual(
            attempts,
            [
                {'model': 'gpt-5.4', 'temperature': 0.7, 'max_tokens': 300},
                {'model': 'gpt-5.4', 'max_tokens': 300},
            ]
        )

        attempts.clear()
        self.predictor._call_with_token_limit_fallback(
            capability_cache_key=cache_key,
            base_request_kwargs={'model': 'gpt-5.4', 'temperature': 0.7},
            resolved_api_mode='chat_completions',
            max_output_tokens=300,
            prefer_legacy_chat_token_param=True,
            caller=caller
        )
        self.assertEqual(
            attempts,
            [
                {'model': 'gpt-5.4', 'max_tokens': 300},
            ]
        )

    def test_build_fallback_request_kwargs_removes_disabled_reasoning_split_from_extra_body(self):
        request_kwargs = self.predictor._build_fallback_request_kwargs(
            base_request_kwargs={
                'model': 'gpt-5.4',
                'extra_body': {
                    'reasoning_split': True
                }
            },
            token_limit_kwargs={'max_tokens': 300},
            disabled_parameters={'reasoning_split'}
        )

        self.assertEqual(
            request_kwargs,
            {
                'model': 'gpt-5.4',
                'max_tokens': 300
            }
        )

    def test_call_with_token_limit_fallback_drops_response_format_when_unsupported(self):
        attempts = []

        def caller(request_kwargs):
            attempts.append(request_kwargs)
            if 'response_format' in request_kwargs:
                return self._build_response(
                    json.dumps({
                        'error': {
                            'message': 'Unsupported parameter: response_format'
                        }
                    }, ensure_ascii=False),
                    'application/json',
                    status_code=400
                )

            return self._build_response(
                json.dumps({
                    'id': 'chatcmpl_1',
                    'object': 'chat.completion',
                    'choices': [
                        {
                            'index': 0,
                            'message': {
                                'role': 'assistant',
                                'content': '{"status":"ok"}'
                            },
                            'finish_reason': 'stop'
                        }
                    ]
                }, ensure_ascii=False),
                'application/json'
            )

        response, latency_ms = self.predictor._call_with_token_limit_fallback(
            capability_cache_key=('https://example.com/v1', 'gpt-5.4', 'chat_completions'),
            base_request_kwargs={
                'model': 'gpt-5.4',
                'temperature': 0.7,
                'response_format': {'type': 'json_object'}
            },
            resolved_api_mode='chat_completions',
            max_output_tokens=300,
            prefer_legacy_chat_token_param=True,
            caller=caller
        )

        self.assertEqual(
            attempts,
            [
                {
                    'model': 'gpt-5.4',
                    'temperature': 0.7,
                    'response_format': {'type': 'json_object'},
                    'max_tokens': 300
                },
                {
                    'model': 'gpt-5.4',
                    'temperature': 0.7,
                    'max_tokens': 300
                }
            ]
        )
        self.assertEqual(response.status_code, 200)
        self.assertGreaterEqual(latency_ms, 0)

    def test_prediction_max_output_tokens_uses_higher_budget_for_minimax_reasoning_model(self):
        minimax_predictor = AIPredictor(
            api_key='test-key',
            api_url='https://api.minimaxi.com',
            model_name='MiniMax-M2.7'
        )

        self.assertEqual(minimax_predictor._prediction_max_output_tokens(), 3200)
        self.assertEqual(minimax_predictor._build_provider_extra_body('chat_completions'), {'reasoning_split': True})

    def test_predict_next_issue_requests_json_output(self):
        predictor_config = {
            'prediction_targets': ['number', 'big_small', 'odd_even', 'combo']
        }
        llm_result = {
            'raw_response': json.dumps({
                'issue_no': '3418518',
                'predicted_number': 12,
                'predicted_big_small': '小',
                'predicted_odd_even': '双',
                'predicted_combo': '小双'
            }, ensure_ascii=False),
            'finish_reason': 'stop'
        }

        with patch.object(self.predictor, '_build_prompt', return_value='PROMPT'), patch.object(
            self.predictor,
            '_call_llm_with_metadata',
            return_value=llm_result
        ) as call_llm:
            prediction, raw_response, prompt = self.predictor.predict_next_issue(
                context={'next_issue_no': '3418518'},
                predictor_config=predictor_config
            )

        call_llm.assert_called_once_with(
            'PROMPT',
            max_output_tokens=1800,
            json_output=True,
            request_time_budget_seconds=None
        )
        self.assertEqual(prompt, 'PROMPT')
        self.assertEqual(raw_response, llm_result['raw_response'])
        self.assertEqual(prediction['prediction_number'], 12)
        self.assertEqual(prediction['prediction_combo'], '小双')

    def test_predict_next_issue_uses_countdown_budget_for_request_timeout(self):
        predictor_config = {
            'prediction_targets': ['number', 'big_small', 'odd_even', 'combo']
        }
        llm_result = {
            'raw_response': json.dumps({
                'issue_no': '3418518',
                'predicted_number': 12,
                'predicted_big_small': '小',
                'predicted_odd_even': '双',
                'predicted_combo': '小双'
            }, ensure_ascii=False),
            'finish_reason': 'stop'
        }

        with patch.object(self.predictor, '_build_prompt', return_value='PROMPT'), patch.object(
            self.predictor,
            '_call_llm_with_metadata',
            return_value=llm_result
        ) as call_llm:
            self.predictor.predict_next_issue(
                context={'next_issue_no': '3418518', 'countdown': '00:00:40'},
                predictor_config=predictor_config
            )

        self.assertAlmostEqual(
            call_llm.call_args.kwargs['request_time_budget_seconds'],
            20.0,
            delta=1.0
        )

    def test_predict_next_issue_skips_ai_call_when_countdown_window_too_small(self):
        predictor_config = {
            'prediction_targets': ['number', 'big_small', 'odd_even', 'combo']
        }

        with patch.object(self.predictor, '_build_prompt', return_value='PROMPT'), patch.object(
            self.predictor,
            '_call_llm_with_metadata'
        ) as call_llm:
            with self.assertRaises(AIPredictionError) as cm:
                self.predictor.predict_next_issue(
                    context={'next_issue_no': '3418518', 'countdown': '00:00:25'},
                    predictor_config=predictor_config
                )

        call_llm.assert_not_called()
        self.assertEqual(cm.exception.category, 'deadline')
        self.assertIn('已跳过 AI 调用', str(cm.exception))

    def test_predict_next_issue_deadline_skip_preserves_prompt_snapshot(self):
        predictor_config = {
            'prediction_targets': ['number', 'big_small', 'odd_even', 'combo']
        }

        with patch.object(self.predictor, '_build_prompt', return_value='PROMPT'), patch.object(
            self.predictor,
            '_call_llm_with_metadata'
        ) as call_llm:
            with self.assertRaises(AIPredictionError) as cm:
                self.predictor.predict_next_issue(
                    context={'next_issue_no': '3418518', 'countdown': '00:00:25'},
                    predictor_config=predictor_config
                )

        call_llm.assert_not_called()
        self.assertEqual(getattr(cm.exception, 'prompt_snapshot', ''), 'PROMPT')
        self.assertEqual(getattr(cm.exception, 'finish_reason', ''), 'deadline_guard')

    def test_predict_next_issue_uses_higher_budget_for_minimax_reasoning_model(self):
        minimax_predictor = AIPredictor(
            api_key='test-key',
            api_url='https://api.minimaxi.com',
            model_name='MiniMax-M2.7'
        )
        predictor_config = {
            'prediction_targets': ['number', 'big_small', 'odd_even', 'combo']
        }
        llm_result = {
            'raw_response': json.dumps({
                'issue_no': '3418524',
                'predicted_number': 18,
                'predicted_big_small': '大',
                'predicted_odd_even': '双',
                'predicted_combo': '大双'
            }, ensure_ascii=False),
            'finish_reason': 'stop'
        }

        with patch.object(minimax_predictor, '_build_prompt', return_value='PROMPT'), patch.object(
            minimax_predictor,
            '_call_llm_with_metadata',
            return_value=llm_result
        ) as call_llm:
            minimax_predictor.predict_next_issue(
                context={'next_issue_no': '3418524'},
                predictor_config=predictor_config
            )

        call_llm.assert_called_once_with(
            'PROMPT',
            max_output_tokens=3200,
            json_output=True,
            request_time_budget_seconds=None
        )

    def test_predict_next_issue_failure_preserves_debug_context(self):
        predictor_config = {
            'prediction_targets': ['number', 'big_small', 'odd_even', 'combo']
        }
        raw_response = '<think>only reasoning</think>'

        with patch.object(self.predictor, '_build_prompt', return_value='PROMPT'), patch.object(
            self.predictor,
            '_call_llm_with_metadata',
            return_value={
                'raw_response': raw_response,
                'finish_reason': 'length'
            }
        ):
            with self.assertRaises(AIPredictionError) as cm:
                self.predictor.predict_next_issue(
                    context={'next_issue_no': '3418519'},
                    predictor_config=predictor_config
                )

        self.assertEqual(getattr(cm.exception, 'raw_response', ''), raw_response)
        self.assertEqual(getattr(cm.exception, 'prompt_snapshot', ''), 'PROMPT')
        self.assertEqual(getattr(cm.exception, 'finish_reason', ''), 'length')
        self.assertIn('原始响应', str(cm.exception))

    def test_predict_next_issue_stops_outer_retry_after_transport_failure(self):
        predictor_config = {
            'prediction_targets': ['number', 'big_small', 'odd_even', 'combo']
        }

        with patch.object(self.predictor, '_build_prompt', return_value='PROMPT'), patch.object(
            self.predictor,
            '_call_llm_with_metadata',
            side_effect=AIPredictionError('API 连接失败：Read timed out', category='transport')
        ) as call_llm:
            with self.assertRaises(AIPredictionError):
                self.predictor.predict_next_issue(
                    context={'next_issue_no': '3418520'},
                    predictor_config=predictor_config
                )

        self.assertEqual(call_llm.call_count, 1)

    def test_run_json_task_parse_failure_preserves_debug_context(self):
        raw_response = '{"batch_key":"2026-04-16","predictions":[{"event_key":"2039116"'

        with patch.object(
            self.predictor,
            '_call_llm_with_metadata',
            return_value={
                'api_mode': 'chat_completions',
                'response_model': 'test-model',
                'finish_reason': 'length',
                'latency_ms': 123,
                'raw_response': raw_response
            }
        ):
            with self.assertRaises(AIPredictionError) as cm:
                self.predictor.run_json_task(
                    prompt='FOOTBALL_PROMPT',
                    system_prompt='只输出 JSON',
                    max_output_tokens=3200
                )

        self.assertEqual(getattr(cm.exception, 'raw_response', ''), raw_response)
        self.assertEqual(getattr(cm.exception, 'prompt_snapshot', ''), 'FOOTBALL_PROMPT')
        self.assertEqual(getattr(cm.exception, 'finish_reason', ''), 'length')

    def test_parse_response_rejects_schema_description_text(self):
        with self.assertRaises(ValueError):
            self.predictor._parse_response(
                raw_response=(
                    '请输出 JSON 字段：predicted_number(0-27), predicted_big_small(大/小), '
                    'predicted_odd_even(单/双), predicted_combo(大单/大双/小单/小双), confidence(0-1)'
                ),
                expected_issue_no='3419001',
                requested_targets=['number', 'big_small', 'odd_even', 'combo']
            )

    def test_parse_response_rejects_thinking_plus_schema_without_final_answer(self):
        with self.assertRaises(ValueError):
            self.predictor._parse_response(
                raw_response=(
                    '<think>先分析遗漏与走势，再决定候选号码</think>\n'
                    '输出 JSON 字段：predicted_number: 0-27, confidence: 0-1'
                ),
                expected_issue_no='3419002',
                requested_targets=['number', 'big_small', 'odd_even', 'combo']
            )

    def test_parse_response_accepts_plain_text_prediction(self):
        prediction = self.predictor._parse_response(
            raw_response='预测号码: 12，预测大小: 小，预测单双: 双，预测组合: 小双，置信度: 0.71',
            expected_issue_no='3419003',
            requested_targets=['number', 'big_small', 'odd_even', 'combo']
        )

        self.assertEqual(prediction['issue_no'], '3419003')
        self.assertEqual(prediction['prediction_number'], 12)
        self.assertEqual(prediction['prediction_big_small'], '小')
        self.assertEqual(prediction['prediction_odd_even'], '双')
        self.assertEqual(prediction['prediction_combo'], '小双')
        self.assertEqual(prediction['confidence'], 0.71)

    def test_parse_response_accepts_field_style_plain_text_prediction(self):
        prediction = self.predictor._parse_response(
            raw_response=(
                'predicted_number: 12, predicted_big_small: 小, predicted_odd_even: 双, '
                'predicted_combo: 小双, confidence: 0.71'
            ),
            expected_issue_no='3419003',
            requested_targets=['number', 'big_small', 'odd_even', 'combo']
        )

        self.assertEqual(prediction['prediction_number'], 12)
        self.assertEqual(prediction['prediction_big_small'], '小')
        self.assertEqual(prediction['prediction_odd_even'], '双')
        self.assertEqual(prediction['prediction_combo'], '小双')
        self.assertEqual(prediction['confidence'], 0.71)

    def test_parse_response_accepts_nested_json_string(self):
        prediction = self.predictor._parse_response(
            raw_response=(
                '"{\\"issue_no\\":\\"3419005\\",\\"predicted_number\\":16,\\"predicted_big_small\\":\\"大\\",'
                '\\"predicted_odd_even\\":\\"双\\",\\"predicted_combo\\":\\"大双\\",\\"confidence\\":0.6}"'
            ),
            expected_issue_no='3419005',
            requested_targets=['number', 'big_small', 'odd_even', 'combo']
        )

        self.assertEqual(prediction['issue_no'], '3419005')
        self.assertEqual(prediction['prediction_number'], 16)
        self.assertEqual(prediction['prediction_big_small'], '大')
        self.assertEqual(prediction['prediction_odd_even'], '双')
        self.assertEqual(prediction['prediction_combo'], '大双')
        self.assertEqual(prediction['confidence'], 0.6)

    def test_predict_next_issue_repairs_reasoning_drift_output(self):
        predictor_config = {
            'prediction_targets': ['number', 'big_small', 'odd_even', 'combo']
        }
        drift_response = (
            'We need to determine trend. Even streak is strongest. '
            'So the direction should stay big even, combo 大双.'
        )
        repaired_response = json.dumps({
            'issue_no': '3419006',
            'predicted_number': 16,
            'predicted_big_small': '大',
            'predicted_odd_even': '双',
            'predicted_combo': '大双',
            'confidence': 0.6,
            'reasoning_summary': '单双长龙延续大双'
        }, ensure_ascii=False)

        with patch.object(self.predictor, '_build_prompt', return_value='PROMPT'), patch.object(
            self.predictor,
            '_call_llm_with_metadata',
            side_effect=[
                {
                    'raw_response': drift_response,
                    'finish_reason': 'stop'
                },
                {
                    'raw_response': repaired_response,
                    'finish_reason': 'stop'
                }
            ]
        ) as call_llm:
            prediction, raw_response, prompt = self.predictor.predict_next_issue(
                context={'next_issue_no': '3419006'},
                predictor_config=predictor_config
            )

        self.assertEqual(prompt, 'PROMPT')
        self.assertEqual(prediction['issue_no'], '3419006')
        self.assertEqual(prediction['prediction_number'], 16)
        self.assertEqual(prediction['prediction_big_small'], '大')
        self.assertEqual(prediction['prediction_odd_even'], '双')
        self.assertEqual(prediction['prediction_combo'], '大双')
        self.assertIn('[original]', raw_response)
        self.assertIn('[repair_json]', raw_response)
        self.assertEqual(call_llm.call_count, 2)

    def test_predict_next_issue_treats_schema_text_as_parse_failure(self):
        predictor_config = {
            'prediction_targets': ['number', 'big_small', 'odd_even', 'combo']
        }
        raw_response = (
            '输出 JSON 字段：predicted_number: 0-27, predicted_big_small: 大/小, '
            'predicted_odd_even: 单/双, predicted_combo: 大单/大双/小单/小双, confidence: 0-1'
        )

        with patch.object(self.predictor, '_build_prompt', return_value='PROMPT'), patch.object(
            self.predictor,
            '_call_llm_with_metadata',
            return_value={
                'raw_response': raw_response,
                'finish_reason': 'stop'
            }
        ):
            with self.assertRaises(AIPredictionError) as cm:
                self.predictor.predict_next_issue(
                    context={'next_issue_no': '3419004'},
                    predictor_config=predictor_config
                )

        self.assertEqual(getattr(cm.exception, 'raw_response', ''), raw_response)
        self.assertEqual(getattr(cm.exception, 'prompt_snapshot', ''), 'PROMPT')
        self.assertEqual(getattr(cm.exception, 'finish_reason', ''), 'stop')


if __name__ == '__main__':
    unittest.main()
