from __future__ import annotations

import json
import unittest

from requests import Response

from ai_trader import AIPredictor


class AIPredictorEncodingTests(unittest.TestCase):
    def setUp(self):
        self.predictor = AIPredictor(
            api_key='test-key',
            api_url='https://example.com/v1',
            model_name='gpt-5.4'
        )

    def _build_response(self, body: str, content_type: str) -> Response:
        response = Response()
        response.status_code = 200
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

        def caller(token_limit_kwargs):
            attempts.append(token_limit_kwargs)
            if 'max_tokens' in token_limit_kwargs:
                raise Exception('HTTP 400：Unsupported parameter: max_tokens')
            return {'status': 'ok', 'token_limit_kwargs': token_limit_kwargs}

        response, latency_ms = self.predictor._call_with_token_limit_fallback(
            resolved_api_mode='chat_completions',
            max_output_tokens=300,
            prefer_legacy_chat_token_param=True,
            caller=caller
        )

        self.assertEqual(
            attempts,
            [
                {'max_tokens': 300},
                {'max_completion_tokens': 300}
            ]
        )
        self.assertEqual(response['status'], 'ok')
        self.assertEqual(response['token_limit_kwargs'], {'max_completion_tokens': 300})
        self.assertGreaterEqual(latency_ms, 0)


if __name__ == '__main__':
    unittest.main()
