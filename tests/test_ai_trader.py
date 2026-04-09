from __future__ import annotations

import json
import unittest
from unittest.mock import patch

from requests import Response

from ai_trader import AIPredictor
from services.prediction_guard import AIPredictionError


class AIPredictorEncodingTests(unittest.TestCase):
    def setUp(self):
        AIPredictor._gateway_capability_cache.clear()
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
            json_output=True
        )
        self.assertEqual(prompt, 'PROMPT')
        self.assertEqual(raw_response, llm_result['raw_response'])
        self.assertEqual(prediction['prediction_number'], 12)
        self.assertEqual(prediction['prediction_combo'], '小双')

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


if __name__ == '__main__':
    unittest.main()
