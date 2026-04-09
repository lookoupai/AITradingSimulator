"""
AI 预测调用与响应解析
"""
from __future__ import annotations

import json
import re
import threading
import time
from typing import Optional
from urllib.parse import urlparse

import requests
from openai import APIConnectionError, APIError, OpenAI
from requests import RequestException

from services.prediction_guard import AIPredictionError
from utils.pc28 import (
    build_combo,
    derive_pc28_attributes,
    normalize_big_small,
    normalize_api_mode,
    normalize_combo,
    normalize_injection_mode,
    normalize_odd_even,
    parse_pc28_triplet,
    normalize_target_list,
    parse_pc28_number
)
from utils.timezone import get_current_beijing_time, get_current_beijing_time_str


class AIPredictor:
    """基于 OpenAI 兼容接口的 PC28 预测器"""

    _gateway_capability_cache: dict[tuple[str, str, str], dict] = {}
    _gateway_capability_cache_lock = threading.Lock()
    _gateway_capability_cache_ttl_seconds = 24 * 60 * 60

    def __init__(
        self,
        api_key: str,
        api_url: str,
        model_name: str,
        api_mode: str = 'auto',
        temperature: float = 0.7
    ):
        self.api_key = api_key
        self.api_url = api_url.rstrip('/')
        self.model_name = model_name
        self.api_mode = normalize_api_mode(api_mode)
        self.temperature = temperature

    def predict_next_issue(self, context: dict, predictor_config: dict) -> tuple[dict, str, str]:
        """预测下一期开奖结果"""
        prompt = self._build_prompt(context, predictor_config)
        last_error = AIPredictionError('AI 未返回有效预测')
        last_response = ''

        for _ in range(3):
            try:
                raw_response = self._call_llm(prompt)
                last_response = raw_response
                prediction = self._parse_response(
                    raw_response=raw_response,
                    expected_issue_no=context.get('next_issue_no'),
                    requested_targets=predictor_config.get('prediction_targets')
                )
                if self._has_effective_prediction(prediction, predictor_config.get('prediction_targets')):
                    return prediction, raw_response, prompt
                last_error = AIPredictionError('AI 返回内容无法解析为有效预测', category='parse')
            except Exception as exc:
                last_error = self._normalize_ai_exception(exc)

        if last_response:
            raise AIPredictionError(
                f'{str(last_error)}；原始响应：{last_response[:300]}',
                category=getattr(last_error, 'category', 'ai_error')
            )
        raise last_error

    def run_connectivity_test(self) -> dict:
        """测试模型连通性与基础输出能力"""
        test_prompt = 'Return a minimal JSON object only: {"status":"ok"}'
        result = self._call_llm_with_metadata(test_prompt)
        raw_response = result['raw_response']
        preview = raw_response.strip()
        return {
            'success': bool(preview),
            'api_mode': result['api_mode'],
            'response_model': result['response_model'],
            'finish_reason': result['finish_reason'],
            'latency_ms': result['latency_ms'],
            'raw_response': raw_response,
            'response_preview': preview[:200]
        }

    def run_prompt_optimization(self, optimization_prompt: str) -> dict:
        """调用当前模型生成提示词优化建议"""
        result = self._call_llm_with_metadata(
            optimization_prompt,
            system_prompt='你是 PC28 提示词优化助手。你必须只输出单个 JSON 对象。',
            max_output_tokens=1800,
            json_output=True
        )
        try:
            payload = self._extract_json_object(result['raw_response'])
        except Exception as exc:
            raise self._normalize_ai_exception(exc, default_category='parse') from exc
        return {
            'api_mode': result['api_mode'],
            'response_model': result['response_model'],
            'finish_reason': result['finish_reason'],
            'latency_ms': result['latency_ms'],
            'raw_response': result['raw_response'],
            'payload': payload
        }

    def run_json_task(
        self,
        prompt: str,
        system_prompt: str,
        max_output_tokens: int = 1800
    ) -> dict:
        """执行一个通用 JSON 生成任务"""
        result = self._call_llm_with_metadata(
            prompt,
            system_prompt=system_prompt,
            max_output_tokens=max_output_tokens,
            json_output=True
        )
        try:
            payload = self._extract_json_payload(result['raw_response'])
        except Exception as exc:
            raise self._normalize_ai_exception(exc, default_category='parse') from exc
        return {
            'api_mode': result['api_mode'],
            'response_model': result['response_model'],
            'finish_reason': result['finish_reason'],
            'latency_ms': result['latency_ms'],
            'raw_response': result['raw_response'],
            'payload': payload
        }

    def _build_prompt(self, context: dict, predictor_config: dict) -> str:
        targets = normalize_target_list(predictor_config.get('prediction_targets'))
        target_labels = [self._target_label(target) for target in targets]
        method_name = predictor_config.get('prediction_method') or '自定义策略'
        custom_prompt = (predictor_config.get('system_prompt') or '').strip()
        data_injection_mode = normalize_injection_mode(predictor_config.get('data_injection_mode'))
        prompt_variables = self._build_prompt_variables(context, predictor_config, data_injection_mode)
        rendered_custom_prompt = self._render_prompt_template(custom_prompt, prompt_variables)
        placeholders_used = self._contains_placeholder(custom_prompt, prompt_variables)
        default_data_block = prompt_variables['default_data_block']

        return f"""你正在为加拿大28（PC28）预测下一期结果。

基础要求：
1. 只能基于我提供的历史数据、遗漏统计、今日统计和自定义策略做分析。
2. 预测目标：{', '.join(target_labels)}。
3. 预测期号：{context.get('next_issue_no') or '未知'}。
4. 你必须输出严格 JSON，不要输出 Markdown，不要输出解释性前缀。
5. 如果不确定某个字段，请输出 null，不要编造。
6. confidence 取值 0-1。
7. 你必须输出 predicted_number，且为 0-27 的有效和值；其余玩法可围绕该号码推导。

方案信息：
- 方案名称：{predictor_config.get('name', '')}
- 预测方法：{method_name}
- 历史窗口：最近 {prompt_variables['history_window']} 期
- 数据注入模式：{self._injection_mode_label(data_injection_mode)}
- 下一期倒计时：{context.get('countdown', '00:00:00')}
 - 当前北京时间：{prompt_variables['current_time_beijing']}
 - 下一期期号：{prompt_variables['next_issue_no']}

用户自定义策略提示词：
{rendered_custom_prompt or '无，按常规统计与模式识别进行分析'}

平台数据输入：
{default_data_block if not placeholders_used else '你在自定义提示词中已经使用了占位符，平台不再重复附加默认数据块。'}

输出 JSON 字段示例：
{{
  "issue_no": "{context.get('next_issue_no') or ''}",
  "predicted_number": 12,
  "predicted_big_small": "小",
  "predicted_odd_even": "双",
  "predicted_combo": "小双",
  "confidence": 0.68,
  "reasoning_summary": "简要说明依据"
}}

现在开始，仅输出 JSON。"""

    def _build_prompt_variables(self, context: dict, predictor_config: dict, injection_mode: str) -> dict:
        recent_draws = context.get('recent_draws') or []
        omission_preview = context.get('omission_preview') or {}
        today_preview = context.get('today_preview') or {}
        preview = context.get('preview') or {}
        beijing_now = get_current_beijing_time()
        beijing_now_str = context.get('current_time_beijing') or get_current_beijing_time_str()

        draw_summary_lines = []
        draw_csv_lines = ['期号, 数字1, 数字2, 数字3, 和值']
        draw_json_items = []

        for draw in recent_draws:
            triplet = parse_pc28_triplet(self._extract_draw_number_expression(draw))
            draw_summary_lines.append(
                f"- 第{draw['issue_no']}期: {draw['result_number_text']} ({draw['big_small']}/{draw['odd_even']}/{draw['combo']})"
            )

            if len(triplet) == 3:
                draw_csv_lines.append(
                    f"{draw['issue_no']}, {triplet[0]}, {triplet[1]}, {triplet[2]}, {draw['result_number']}"
                )
                draw_json_items.append({
                    'issue_no': draw['issue_no'],
                    'num1': triplet[0],
                    'num2': triplet[1],
                    'num3': triplet[2],
                    'sum': draw['result_number']
                })
            else:
                draw_csv_lines.append(
                    f"{draw['issue_no']}, , , , {draw['result_number']}"
                )
                draw_json_items.append({
                    'issue_no': draw['issue_no'],
                    'num1': None,
                    'num2': None,
                    'num3': None,
                    'sum': draw['result_number']
                })

        omission_lines = [
            f"{item['label']} 已遗漏 {item['value']} 期"
            for item in omission_preview.get('top_numbers', [])[:10]
        ]

        today_summary = today_preview.get('summary', {})
        today_lines = [f"{key}: {value}" for key, value in today_summary.items()]

        preview_lines = []
        dragon_summary = preview.get('dragon') or {}
        for key, value in dragon_summary.items():
            if isinstance(value, (str, int, float)):
                preview_lines.append(f"{key}: {value}")

        default_data_block = self._build_default_data_block(
            injection_mode=injection_mode,
            draw_summary_lines=draw_summary_lines,
            draw_csv_lines=draw_csv_lines,
            omission_lines=omission_lines,
            today_lines=today_lines,
            preview_lines=preview_lines,
            current_time_beijing=beijing_now_str,
            next_issue_no=context.get('next_issue_no') or '未知',
            countdown=context.get('countdown', '00:00:00')
        )

        return {
            'prediction_targets': ', '.join(self._target_label(target) for target in normalize_target_list(predictor_config.get('prediction_targets'))),
            'history_window': str(len(recent_draws)),
            'next_issue_no': str(context.get('next_issue_no') or '未知'),
            'countdown': str(context.get('countdown', '00:00:00')),
            'current_time_beijing': beijing_now_str,
            'current_year': str(beijing_now.year),
            'current_month': str(beijing_now.month),
            'current_day': str(beijing_now.day),
            'current_hour': str(beijing_now.hour),
            'current_minute': str(beijing_now.minute),
            'recent_draws_summary': '\n'.join(draw_summary_lines) or '- 暂无历史数据',
            'recent_draws_csv': '\n'.join(draw_csv_lines),
            'recent_draws_json': json.dumps(draw_json_items, ensure_ascii=False),
            'omission_summary': '\n'.join(omission_lines) or '- 暂无遗漏统计',
            'today_summary': '\n'.join(today_lines) or '- 暂无今日统计',
            'preview_summary': '\n'.join(preview_lines[:10]) or '- 暂无聚合走势',
            'latest_draw_summary': draw_summary_lines[0] if draw_summary_lines else '- 暂无历史数据',
            'default_data_block': default_data_block
        }

    def _extract_draw_number_expression(self, draw: dict) -> str:
        payload_text = draw.get('source_payload') or ''
        if not payload_text:
            return ''

        try:
            payload = json.loads(payload_text)
            return str(payload.get('number') or '')
        except (TypeError, json.JSONDecodeError):
            return ''

    def _render_prompt_template(self, prompt: str, prompt_variables: dict) -> str:
        rendered_prompt = prompt or ''
        for key, value in prompt_variables.items():
            rendered_prompt = rendered_prompt.replace(f'{{{{{key}}}}}', str(value))
        return rendered_prompt

    def _contains_placeholder(self, prompt: str, prompt_variables: dict) -> bool:
        if not prompt:
            return False
        return any(f'{{{{{key}}}}}' in prompt for key in prompt_variables)

    def _build_default_data_block(
        self,
        injection_mode: str,
        draw_summary_lines: list[str],
        draw_csv_lines: list[str],
        omission_lines: list[str],
        today_lines: list[str],
        preview_lines: list[str],
        current_time_beijing: str,
        next_issue_no: str,
        countdown: str
    ) -> str:
        if injection_mode == 'raw':
            return f"""输入：
- 当前北京时间：{current_time_beijing}
- 下一期期号：{next_issue_no}
- 倒计时：{countdown}
- 最近开奖数据：
{chr(10).join(draw_csv_lines) if draw_csv_lines else '期号, 数字1, 数字2, 数字3, 和值'}

补充统计：
- 遗漏：
{chr(10).join(omission_lines) if omission_lines else '- 暂无遗漏统计'}
- 今日统计：
{chr(10).join(today_lines) if today_lines else '- 暂无今日统计'}"""

        return f"""输入：
- 当前北京时间：{current_time_beijing}
- 下一期期号：{next_issue_no}
- 倒计时：{countdown}
- 最近开奖摘要：
{chr(10).join(draw_summary_lines) if draw_summary_lines else '- 暂无历史数据'}
- 遗漏统计：
{chr(10).join(omission_lines) if omission_lines else '- 暂无遗漏统计'}
- 今日统计：
{chr(10).join(today_lines) if today_lines else '- 暂无今日统计'}
- 聚合走势：
{chr(10).join(preview_lines[:10]) if preview_lines else '- 暂无聚合走势'}"""

    def _injection_mode_label(self, injection_mode: str) -> str:
        if injection_mode == 'raw':
            return '原始模式'
        return '摘要模式'

    def _call_llm(self, prompt: str) -> str:
        return self._call_llm_with_metadata(prompt)['raw_response']

    def _call_llm_with_metadata(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        max_output_tokens: int = 1200,
        json_output: bool = False
    ) -> dict:
        last_error: Optional[Exception] = None
        system_prompt = system_prompt or "你是 PC28 预测助手。你必须遵守字段契约，只输出单个 JSON 对象。"
        resolved_api_mode = self._resolve_api_mode()

        for base_url in self._candidate_base_urls():
            try:
                if self._should_use_compatible_http_transport(base_url):
                    return self._call_llm_via_compatible_http(
                        base_url=base_url,
                        resolved_api_mode=resolved_api_mode,
                        prompt=prompt,
                        system_prompt=system_prompt,
                        max_output_tokens=max_output_tokens,
                        json_output=json_output
                    )
                return self._call_llm_via_openai_sdk(
                    base_url=base_url,
                    resolved_api_mode=resolved_api_mode,
                    prompt=prompt,
                    system_prompt=system_prompt,
                    max_output_tokens=max_output_tokens,
                    json_output=json_output
                )
            except (APIConnectionError, APIError, RequestException, Exception) as exc:
                last_error = exc
                continue

        normalized_error = self._normalize_ai_exception(last_error)
        message = str(normalized_error)
        if isinstance(last_error, (APIConnectionError, RequestException)):
            message = f'API 连接失败：{last_error}'
        elif isinstance(last_error, APIError):
            message = f'API 调用失败（模式={resolved_api_mode}）：{last_error}'
        elif last_error is not None:
            message = f'LLM 调用失败（模式={resolved_api_mode}）：{last_error}'
        raise AIPredictionError(message, category=normalized_error.category)

    def _call_llm_via_openai_sdk(
        self,
        base_url: str,
        resolved_api_mode: str,
        prompt: str,
        system_prompt: str,
        max_output_tokens: int,
        json_output: bool
    ) -> dict:
        client = OpenAI(
            api_key=self.api_key,
            base_url=base_url
        )
        capability_cache_key = self._build_gateway_capability_cache_key(base_url, resolved_api_mode)
        if resolved_api_mode == 'responses':
            response_kwargs = {
                'model': self.model_name,
                'instructions': system_prompt,
                'input': prompt,
                'temperature': self.temperature
            }
            if json_output:
                response_kwargs['text'] = {
                    'format': {
                        'type': 'json_object'
                    }
                }

            response, latency_ms = self._call_with_token_limit_fallback(
                capability_cache_key=capability_cache_key,
                base_request_kwargs=response_kwargs,
                resolved_api_mode=resolved_api_mode,
                max_output_tokens=max_output_tokens,
                prefer_legacy_chat_token_param=False,
                caller=lambda request_kwargs: client.responses.create(**request_kwargs)
            )
            raw_response = self._extract_response_output_text(response)
            return {
                'raw_response': raw_response,
                'api_mode': resolved_api_mode,
                'response_model': getattr(response, 'model', self.model_name),
                'finish_reason': self._extract_responses_finish_reason(response),
                'latency_ms': latency_ms
            }

        response_kwargs = {
            'model': self.model_name,
            'messages': [
                {'role': 'system', 'content': system_prompt},
                {'role': 'user', 'content': prompt}
            ],
            'temperature': self.temperature
        }
        if json_output:
            response_kwargs['response_format'] = {'type': 'json_object'}

        response, latency_ms = self._call_with_token_limit_fallback(
            capability_cache_key=capability_cache_key,
            base_request_kwargs=response_kwargs,
            resolved_api_mode=resolved_api_mode,
            max_output_tokens=max_output_tokens,
            prefer_legacy_chat_token_param=False,
            caller=lambda request_kwargs: client.chat.completions.create(**request_kwargs)
        )
        raw_response = self._extract_message_text(response)
        return {
            'raw_response': raw_response,
            'api_mode': resolved_api_mode,
            'response_model': getattr(response, 'model', self.model_name),
            'finish_reason': self._extract_chat_finish_reason(response),
            'latency_ms': latency_ms
        }

    def _call_llm_via_compatible_http(
        self,
        base_url: str,
        resolved_api_mode: str,
        prompt: str,
        system_prompt: str,
        max_output_tokens: int,
        json_output: bool
    ) -> dict:
        endpoint = self._build_compatible_endpoint(base_url, resolved_api_mode)
        capability_cache_key = self._build_gateway_capability_cache_key(base_url, resolved_api_mode)
        payload = self._build_compatible_payload(
            resolved_api_mode=resolved_api_mode,
            prompt=prompt,
            system_prompt=system_prompt,
            max_output_tokens=None,
            json_output=json_output
        )
        response, latency_ms = self._call_with_token_limit_fallback(
            capability_cache_key=capability_cache_key,
            base_request_kwargs=payload,
            resolved_api_mode=resolved_api_mode,
            max_output_tokens=max_output_tokens,
            prefer_legacy_chat_token_param=True,
            caller=lambda request_kwargs: requests.post(
                endpoint,
                headers=self._build_compatible_headers(),
                json=request_kwargs,
                timeout=60
            )
        )
        if response.status_code >= 400:
            raise Exception(
                f'HTTP {response.status_code}：{self._extract_http_error_message(response)}'
            )

        payload = self._parse_compatible_http_response(
            response=response,
            resolved_api_mode=resolved_api_mode
        )

        if resolved_api_mode == 'responses':
            raw_response = self._extract_response_output_text(payload)
            finish_reason = self._extract_responses_finish_reason(payload)
        else:
            raw_response = self._extract_message_text(payload)
            finish_reason = self._extract_chat_finish_reason(payload)

        return {
            'raw_response': raw_response,
            'api_mode': resolved_api_mode,
            'response_model': payload.get('model', self.model_name),
            'finish_reason': finish_reason,
            'latency_ms': latency_ms
        }

    def _build_compatible_endpoint(self, base_url: str, resolved_api_mode: str) -> str:
        suffix = 'responses' if resolved_api_mode == 'responses' else 'chat/completions'
        return f"{base_url.rstrip('/')}/{suffix}"

    def _build_compatible_payload(
        self,
        resolved_api_mode: str,
        prompt: str,
        system_prompt: str,
        max_output_tokens: Optional[int],
        json_output: bool
    ) -> dict:
        if resolved_api_mode == 'responses':
            payload = {
                'model': self.model_name,
                'instructions': system_prompt,
                'input': prompt,
                'temperature': self.temperature,
                'stream': False
            }
            if max_output_tokens is not None:
                payload['max_output_tokens'] = max_output_tokens
            if json_output:
                payload['text'] = {
                    'format': {
                        'type': 'json_object'
                    }
                }
            return payload

        return {
            'model': self.model_name,
            'messages': [
                {'role': 'system', 'content': system_prompt},
                {'role': 'user', 'content': prompt}
            ],
            'temperature': self.temperature,
            'stream': False
        }

    def _call_with_token_limit_fallback(
        self,
        capability_cache_key: tuple[str, str, str],
        base_request_kwargs: dict,
        resolved_api_mode: str,
        max_output_tokens: int,
        prefer_legacy_chat_token_param: bool,
        caller
    ) -> tuple[object, int]:
        last_error: Optional[Exception] = None
        disabled_parameters = self._get_cached_disabled_parameters(capability_cache_key)
        attempted_request_keys: set[str] = set()

        while True:
            attempted_in_round = False
            should_restart_round = False
            for token_limit_kwargs in self._iter_token_limit_kwargs(
                resolved_api_mode=resolved_api_mode,
                max_output_tokens=max_output_tokens,
                prefer_legacy_chat_token_param=prefer_legacy_chat_token_param
            ):
                if any(parameter in disabled_parameters for parameter in token_limit_kwargs):
                    continue
                request_kwargs = self._build_fallback_request_kwargs(
                    base_request_kwargs=base_request_kwargs,
                    token_limit_kwargs=token_limit_kwargs,
                    disabled_parameters=disabled_parameters
                )
                request_key = json.dumps(request_kwargs, ensure_ascii=False, sort_keys=True)
                if request_key in attempted_request_keys:
                    continue

                attempted_request_keys.add(request_key)
                attempted_in_round = True
                start_time = time.perf_counter()
                try:
                    response = caller(request_kwargs)
                    unsupported_parameter = self._extract_unsupported_parameter_from_response(response)
                    if unsupported_parameter:
                        last_error = Exception(
                            f'HTTP {response.status_code}：{self._extract_http_error_message(response)}'
                        )
                        if unsupported_parameter not in disabled_parameters:
                            disabled_parameters.add(unsupported_parameter)
                            self._store_cached_disabled_parameters(capability_cache_key, disabled_parameters)
                            should_restart_round = True
                            break
                        continue
                    self._store_cached_disabled_parameters(capability_cache_key, disabled_parameters)
                    return response, int((time.perf_counter() - start_time) * 1000)
                except Exception as exc:
                    unsupported_parameter = self._extract_unsupported_parameter_name(str(exc))
                    if not unsupported_parameter:
                        raise
                    last_error = exc
                    if unsupported_parameter not in disabled_parameters:
                        disabled_parameters.add(unsupported_parameter)
                        self._store_cached_disabled_parameters(capability_cache_key, disabled_parameters)
                        should_restart_round = True
                        break
                    continue

            if should_restart_round:
                continue
            if not attempted_in_round:
                break

        self._store_cached_disabled_parameters(capability_cache_key, disabled_parameters)
        if last_error is not None:
            raise last_error
        raise RuntimeError('未生成可用的 token 限制参数')

    def _build_gateway_capability_cache_key(self, base_url: str, resolved_api_mode: str) -> tuple[str, str, str]:
        return (
            base_url.rstrip('/'),
            str(self.model_name or '').strip(),
            resolved_api_mode
        )

    def _get_cached_disabled_parameters(self, capability_cache_key: tuple[str, str, str]) -> set[str]:
        now = time.time()
        with self._gateway_capability_cache_lock:
            entry = self._gateway_capability_cache.get(capability_cache_key)
            if not entry:
                return set()

            expires_at = float(entry.get('expires_at') or 0)
            if expires_at and expires_at <= now:
                self._gateway_capability_cache.pop(capability_cache_key, None)
                return set()

            disabled_parameters = entry.get('disabled_parameters') or []
            return {
                str(parameter).strip()
                for parameter in disabled_parameters
                if str(parameter).strip()
            }

    def _store_cached_disabled_parameters(
        self,
        capability_cache_key: tuple[str, str, str],
        disabled_parameters: set[str]
    ) -> None:
        with self._gateway_capability_cache_lock:
            if not disabled_parameters:
                self._gateway_capability_cache.pop(capability_cache_key, None)
                return

            self._gateway_capability_cache[capability_cache_key] = {
                'disabled_parameters': sorted(disabled_parameters),
                'expires_at': time.time() + self._gateway_capability_cache_ttl_seconds
            }

    def _build_fallback_request_kwargs(
        self,
        base_request_kwargs: dict,
        token_limit_kwargs: dict,
        disabled_parameters: set[str]
    ) -> dict:
        request_kwargs = {
            **base_request_kwargs,
            **token_limit_kwargs
        }
        for parameter in disabled_parameters:
            request_kwargs.pop(parameter, None)
        return request_kwargs

    def _iter_token_limit_kwargs(
        self,
        resolved_api_mode: str,
        max_output_tokens: int,
        prefer_legacy_chat_token_param: bool
    ) -> list[dict]:
        if max_output_tokens <= 0:
            return [{}]

        if resolved_api_mode == 'responses':
            return [
                {'max_output_tokens': max_output_tokens},
                {}
            ]

        primary_key = 'max_tokens' if prefer_legacy_chat_token_param else 'max_completion_tokens'
        secondary_key = 'max_completion_tokens' if prefer_legacy_chat_token_param else 'max_tokens'
        return [
            {primary_key: max_output_tokens},
            {secondary_key: max_output_tokens},
            {}
        ]

    def _extract_unsupported_parameter_name(self, message: str) -> Optional[str]:
        message = str(message or '').lower()
        if not message:
            return None

        if not any(keyword in message for keyword in ['unsupported parameter', 'unknown parameter', 'extra inputs are not permitted']):
            return None

        supported_candidates = [
            'max_output_tokens',
            'max_completion_tokens',
            'max_tokens',
            'temperature',
            'response_format',
            'text.format',
            'text'
        ]
        for parameter in supported_candidates:
            if parameter in message:
                return parameter.split('.')[0]
        return None

    def _extract_unsupported_parameter_from_response(self, response) -> Optional[str]:
        status_code = getattr(response, 'status_code', 200)
        if not isinstance(status_code, int) or status_code < 400:
            return None

        try:
            message = self._extract_http_error_message(response)
        except Exception:
            return None

        return self._extract_unsupported_parameter_name(message)

    def _build_compatible_headers(self) -> dict:
        return {
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'User-Agent': 'AITradingSimulator/1.0'
        }

    def _decode_compatible_response_body(self, response: requests.Response) -> str:
        raw_bytes = response.content or b''
        if not raw_bytes:
            return response.text or ''

        candidate_encodings = []
        for encoding in ('utf-8', getattr(response, 'encoding', None), getattr(response, 'apparent_encoding', None)):
            normalized = str(encoding or '').strip()
            if normalized and normalized not in candidate_encodings:
                candidate_encodings.append(normalized)

        for encoding in candidate_encodings:
            try:
                return raw_bytes.decode(encoding)
            except (LookupError, UnicodeDecodeError):
                continue

        return raw_bytes.decode('utf-8', errors='replace')

    def _extract_http_error_message(self, response: requests.Response) -> str:
        body = self._decode_compatible_response_body(response).strip()
        if not body:
            return response.reason or '未知错误'

        try:
            payload = json.loads(body)
        except ValueError:
            return body[:500]

        if isinstance(payload, dict):
            error_payload = payload.get('error')
            if isinstance(error_payload, dict):
                message = error_payload.get('message') or error_payload.get('code')
                if message:
                    return str(message)
            if error_payload:
                return str(error_payload)
            message = payload.get('message')
            if message:
                return str(message)
        return body[:500]

    def _parse_compatible_http_response(
        self,
        response: requests.Response,
        resolved_api_mode: str
    ) -> dict:
        body = self._decode_compatible_response_body(response)
        parse_error = None
        try:
            parsed_payload = json.loads(body)
        except ValueError as exc:
            parsed_payload = None
            parse_error = exc

        if isinstance(parsed_payload, dict):
            return parsed_payload

        content_type = response.headers.get('Content-Type', '')
        if self._looks_like_sse_response(body, content_type):
            return self._parse_sse_payload(
                raw_body=body,
                resolved_api_mode=resolved_api_mode
            )

        if parsed_payload is None:
            raise ValueError(f'API 返回了非 JSON 内容：{body[:500]}') from parse_error
        if not isinstance(parsed_payload, dict):
            raise ValueError(f'API 返回的 JSON 不是对象：{body[:500]}')
        return parsed_payload

    def _looks_like_sse_response(self, body: str, content_type: str) -> bool:
        lowered_content_type = (content_type or '').lower()
        if 'text/event-stream' in lowered_content_type:
            return True

        stripped = (body or '').lstrip()
        return stripped.startswith('data:') or '\ndata:' in stripped

    def _parse_sse_payload(self, raw_body: str, resolved_api_mode: str) -> dict:
        event_payloads = self._extract_sse_event_payloads(raw_body)
        if not event_payloads:
            raise ValueError(f'API 返回了 SSE 流，但没有有效 JSON 事件：{raw_body[:500]}')

        if resolved_api_mode == 'responses':
            return self._build_responses_payload_from_sse(event_payloads, raw_body)
        return self._build_chat_payload_from_sse(event_payloads, raw_body)

    def _extract_sse_event_payloads(self, raw_body: str) -> list[dict]:
        payloads = []
        current_data_lines: list[str] = []

        def flush_current_event() -> None:
            if not current_data_lines:
                return

            data_block = '\n'.join(current_data_lines).strip()
            current_data_lines.clear()
            if not data_block or data_block == '[DONE]':
                return

            try:
                payload = json.loads(data_block)
            except json.JSONDecodeError:
                return

            if isinstance(payload, dict):
                payloads.append(payload)

        for raw_line in (raw_body or '').splitlines():
            line = raw_line.rstrip('\r')
            if not line.strip():
                flush_current_event()
                continue
            if line.startswith(':'):
                continue
            if line.startswith('data:'):
                current_data_lines.append(line[5:].lstrip())

        flush_current_event()
        return payloads

    def _build_chat_payload_from_sse(self, event_payloads: list[dict], raw_body: str) -> dict:
        response_id = None
        model = self.model_name
        created = None
        role = 'assistant'
        finish_reason = 'unknown'
        usage = None
        content_parts: list[str] = []
        reasoning_parts: list[str] = []

        for event_payload in event_payloads:
            response_id = event_payload.get('id') or response_id
            model = event_payload.get('model') or model
            created = event_payload.get('created') or created
            if event_payload.get('usage') is not None:
                usage = event_payload.get('usage')

            choices = event_payload.get('choices') or []
            for choice in choices:
                if not isinstance(choice, dict):
                    continue

                finish_reason = choice.get('finish_reason') or finish_reason
                delta = choice.get('delta') or {}
                if not isinstance(delta, dict):
                    continue

                role = delta.get('role') or role

                content_text = self._collect_stream_text(delta.get('content'))
                if content_text:
                    content_parts.append(content_text)

                reasoning_text = self._collect_stream_text(
                    delta.get('reasoning_content')
                    or delta.get('reasoning')
                    or delta.get('output_text')
                    or delta.get('text')
                )
                if reasoning_text:
                    reasoning_parts.append(reasoning_text)

        content = ''.join(content_parts)
        reasoning_content = ''.join(reasoning_parts)
        if not content and not reasoning_content:
            raise ValueError(f'API 返回了 SSE 流，但没有可用内容：{raw_body[:500]}')

        message = {
            'role': role or 'assistant',
            'content': content
        }
        if reasoning_content:
            message['reasoning_content'] = reasoning_content

        return {
            'id': response_id,
            'object': 'chat.completion',
            'created': created,
            'model': model,
            'choices': [
                {
                    'index': 0,
                    'message': message,
                    'finish_reason': finish_reason
                }
            ],
            'usage': usage
        }

    def _build_responses_payload_from_sse(self, event_payloads: list[dict], raw_body: str) -> dict:
        response_id = None
        model = self.model_name
        status = 'unknown'
        output_text_parts: list[str] = []

        for event_payload in event_payloads:
            response_id = event_payload.get('id') or response_id
            model = event_payload.get('model') or model
            status = event_payload.get('status') or status

            response_payload = event_payload.get('response')
            if isinstance(response_payload, dict):
                response_id = response_payload.get('id') or response_id
                model = response_payload.get('model') or model
                status = response_payload.get('status') or status
                if event_payload.get('type') == 'response.completed':
                    return response_payload

            if event_payload.get('type') == 'response.output_text.delta':
                delta = event_payload.get('delta')
                if isinstance(delta, str):
                    output_text_parts.append(delta)
                continue

            output_text = self._collect_stream_text(event_payload.get('output_text'))
            if output_text:
                output_text_parts.append(output_text)

        output_text = ''.join(output_text_parts)
        if not output_text:
            raise ValueError(f'API 返回了 SSE 流，但没有可用内容：{raw_body[:500]}')

        return {
            'id': response_id,
            'model': model,
            'status': status,
            'output_text': output_text
        }

    def _should_use_compatible_http_transport(self, base_url: str) -> bool:
        host = (urlparse(base_url).hostname or '').lower()
        return not self._is_official_openai_host(host)

    def _is_official_openai_host(self, host: str) -> bool:
        return host in {'api.openai.com', 'openai.com'} or host.endswith('.openai.com')

    def _extract_json_payload(self, raw_response: str):
        text = (raw_response or '').strip()
        if not text:
            raise ValueError('AI 返回为空')

        if '<think>' in text and '</think>' in text:
            text = text.split('</think>')[-1].strip()

        code_block_match = re.search(r'```(?:json)?\s*(\{.*\}|\[.*\])\s*```', text, re.DOTALL)
        if code_block_match:
            text = code_block_match.group(1).strip()

        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass

        json_match = re.search(r'(\{.*\}|\[.*\])', text, re.DOTALL)
        if json_match:
            try:
                return json.loads(json_match.group(0))
            except json.JSONDecodeError:
                pass

        raise ValueError(f'无法从模型响应中解析 JSON：{text[:300]}')

    def _extract_json_object(self, raw_response: str) -> dict:
        payload = self._extract_json_payload(raw_response)
        if not isinstance(payload, dict):
            raise ValueError('AI 返回的 JSON 不是对象')
        return payload

    def _resolve_api_mode(self) -> str:
        if self.api_mode != 'auto':
            return self.api_mode

        host = urlparse(self.api_url).hostname or ''
        host = host.lower()
        if self._is_official_openai_host(host):
            return 'responses'
        return 'chat_completions'

    def _normalize_ai_exception(self, exc: Exception | None, default_category: str = 'ai_error') -> AIPredictionError:
        if isinstance(exc, AIPredictionError):
            return exc

        message = str(exc or '未知 AI 错误')
        lowered_message = message.lower()
        category = default_category

        if any(keyword in lowered_message for keyword in ['quota', 'insufficient_quota', 'billing', '余额', '额度']):
            category = 'quota'
        elif any(keyword in lowered_message for keyword in ['invalid api key', 'incorrect api key', 'unauthorized', 'authentication', '401', 'key失效', '鉴权']):
            category = 'auth'
        elif any(keyword in lowered_message for keyword in ['rate limit', '429', 'too many requests']):
            category = 'rate_limit'
        elif any(keyword in lowered_message for keyword in ['无法解析', 'json', '格式', 'schema', 'parse', '字段缺失']):
            category = 'parse'
        elif any(keyword in lowered_message for keyword in ['timed out', 'timeout', '连接', 'connect', 'connection', 'network', 'dns']):
            category = 'transport'

        return AIPredictionError(message, category=category)

    def _extract_response_output_text(self, response) -> str:
        output_text = response.get('output_text') if isinstance(response, dict) else getattr(response, 'output_text', None)
        text = self._collect_text_fragments(output_text)
        if text:
            return text

        output_items = response.get('output') if isinstance(response, dict) else getattr(response, 'output', None)
        output_items = output_items or []
        parts = []
        for item in output_items:
            content_items = item.get('content') if isinstance(item, dict) else getattr(item, 'content', None)
            content_items = content_items or []
            for content_item in content_items:
                if isinstance(content_item, dict):
                    content_value = content_item.get('text') or content_item.get('content') or content_item
                else:
                    content_value = (
                        getattr(content_item, 'text', None)
                        or getattr(content_item, 'content', None)
                        or content_item
                    )
                candidate_text = self._collect_text_fragments(
                    content_value
                )
                if candidate_text:
                    parts.append(candidate_text)

        combined_text = '\n'.join(part for part in parts if part).strip()
        if combined_text:
            return combined_text

        response_dump = self._safe_model_dump(response)
        raise ValueError(f'Responses API 返回为空，原始响应片段：{response_dump[:500]}')

    def _extract_responses_finish_reason(self, response) -> str:
        status = response.get('status') if isinstance(response, dict) else getattr(response, 'status', None)
        incomplete_details = response.get('incomplete_details') if isinstance(response, dict) else getattr(response, 'incomplete_details', None)
        if incomplete_details is not None:
            reason = incomplete_details.get('reason') if isinstance(incomplete_details, dict) else getattr(incomplete_details, 'reason', None)
            if reason:
                return str(reason)
        if status:
            return str(status)
        return 'unknown'

    def _extract_message_text(self, response) -> str:
        choices = response.get('choices') if isinstance(response, dict) else getattr(response, 'choices', None)
        choices = choices or []
        if not choices:
            raise ValueError('AI 返回中没有 choices')

        choice = choices[0]
        message = choice.get('message') if isinstance(choice, dict) else getattr(choice, 'message', None)
        if message is None:
            raise ValueError('AI 返回中没有 message')

        content = message.get('content') if isinstance(message, dict) else getattr(message, 'content', None)
        text = self._collect_text_fragments(content)
        if text:
            return text

        fallback_fields = [
            'reasoning_content',
            'reasoning',
            'output_text',
            'text'
        ]
        for field_name in fallback_fields:
            value = message.get(field_name) if isinstance(message, dict) else getattr(message, field_name, None)
            text = self._collect_text_fragments(value)
            if text:
                return text

        tool_calls = message.get('tool_calls') if isinstance(message, dict) else getattr(message, 'tool_calls', None)
        tool_calls = tool_calls or []
        if tool_calls:
            tool_text_parts = []
            for tool_call in tool_calls:
                function_payload = tool_call.get('function') if isinstance(tool_call, dict) else getattr(tool_call, 'function', None)
                if function_payload is None:
                    continue
                arguments = function_payload.get('arguments') if isinstance(function_payload, dict) else getattr(function_payload, 'arguments', None)
                if arguments:
                    tool_text_parts.append(str(arguments).strip())
            tool_text = '\n'.join(part for part in tool_text_parts if part)
            if tool_text:
                return tool_text

        finish_reason = choice.get('finish_reason', 'unknown') if isinstance(choice, dict) else getattr(choice, 'finish_reason', 'unknown')
        usage = response.get('usage') if isinstance(response, dict) else getattr(response, 'usage', None)
        reasoning_tokens = self._extract_reasoning_tokens(usage)
        response_dump = self._safe_model_dump(response)

        if finish_reason == 'length' and reasoning_tokens:
            raise ValueError(
                f'AI 返回了纯推理结果，没有正文输出；finish_reason={finish_reason}，'
                f'reasoning_tokens={reasoning_tokens}。这通常是当前网关下的推理模型兼容性问题，'
                f'建议改用非推理聊天模型，或更换支持该模型完整输出的供应商。'
            )

        raise ValueError(
            f'AI 返回空 content，finish_reason={finish_reason}，'
            f'可用原始响应片段：{response_dump[:500]}'
        )

    def _extract_chat_finish_reason(self, response) -> str:
        choices = response.get('choices') if isinstance(response, dict) else getattr(response, 'choices', None)
        choices = choices or []
        if not choices:
            return 'unknown'

        choice = choices[0]
        if isinstance(choice, dict):
            return str(choice.get('finish_reason') or 'unknown')
        return getattr(choice, 'finish_reason', None) or 'unknown'

    def _collect_text_fragments(self, value) -> str:
        if value is None:
            return ''

        if isinstance(value, str):
            return value.strip()

        if isinstance(value, list):
            parts = []
            for item in value:
                if isinstance(item, str):
                    parts.append(item.strip())
                    continue
                if isinstance(item, dict):
                    parts.append(str(item.get('text') or item.get('content') or '').strip())
                    continue
                text_attr = getattr(item, 'text', None)
                if text_attr:
                    parts.append(str(text_attr).strip())
                    continue
                if hasattr(item, 'model_dump'):
                    dumped = item.model_dump()
                    parts.append(str(dumped.get('text') or dumped.get('content') or '').strip())
                    continue
                parts.append(str(item).strip())

            return '\n'.join(part for part in parts if part).strip()

        if hasattr(value, 'model_dump'):
            dumped = value.model_dump()
            return self._collect_text_fragments(dumped.get('text') or dumped.get('content'))

        return str(value).strip()

    def _collect_stream_text(self, value) -> str:
        if value is None:
            return ''

        if isinstance(value, str):
            return value

        if isinstance(value, list):
            return ''.join(self._collect_stream_text(item) for item in value)

        if isinstance(value, dict):
            if 'text' in value:
                return self._collect_stream_text(value.get('text'))
            if 'content' in value:
                return self._collect_stream_text(value.get('content'))
            if 'delta' in value:
                return self._collect_stream_text(value.get('delta'))
            return ''

        if hasattr(value, 'model_dump'):
            return self._collect_stream_text(value.model_dump())

        return str(value)

    def _safe_model_dump(self, value) -> str:
        if hasattr(value, 'model_dump_json'):
            try:
                return value.model_dump_json()
            except Exception:
                pass

        if hasattr(value, 'model_dump'):
            try:
                return json.dumps(value.model_dump(), ensure_ascii=False)
            except Exception:
                pass

        try:
            return json.dumps(value, ensure_ascii=False)
        except Exception:
            return str(value)

    def _extract_reasoning_tokens(self, usage) -> int:
        if usage is None:
            return 0

        completion_details = usage.get('completion_tokens_details') if isinstance(usage, dict) else getattr(usage, 'completion_tokens_details', None)
        if completion_details is None:
            return 0

        reasoning_tokens = completion_details.get('reasoning_tokens', 0) if isinstance(completion_details, dict) else getattr(completion_details, 'reasoning_tokens', 0)
        try:
            return int(reasoning_tokens or 0)
        except (TypeError, ValueError):
            return 0

    def _candidate_base_urls(self) -> list[str]:
        base_url = self.api_url.rstrip('/')
        candidates = [base_url]

        has_version_segment = '/v1' in base_url or '/openai' in base_url
        if not has_version_segment:
            candidates.append(f'{base_url}/v1')

        deduped = []
        for candidate in candidates:
            if candidate not in deduped:
                deduped.append(candidate)
        return deduped

    def _parse_response(self, raw_response: str, expected_issue_no: Optional[str], requested_targets) -> dict:
        text = (raw_response or '').strip()
        if not text:
            raise ValueError('AI 返回为空')

        if '<think>' in text and '</think>' in text:
            text = text.split('</think>')[-1].strip()

        code_block_match = re.search(r'```(?:json)?\s*(\{.*\})\s*```', text, re.DOTALL)
        if code_block_match:
            text = code_block_match.group(1).strip()

        try:
            payload = json.loads(text)
            return self._normalize_prediction(payload, expected_issue_no, requested_targets)
        except json.JSONDecodeError:
            pass

        json_match = re.search(r'\{.*\}', text, re.DOTALL)
        if json_match:
            try:
                payload = json.loads(json_match.group(0))
                return self._normalize_prediction(payload, expected_issue_no, requested_targets)
            except json.JSONDecodeError:
                pass

        return self._extract_from_text(text, expected_issue_no, requested_targets)

    def _normalize_prediction(self, payload: dict, expected_issue_no: Optional[str], requested_targets) -> dict:
        if 'prediction' in payload and isinstance(payload['prediction'], dict):
            payload = payload['prediction']

        targets = normalize_target_list(requested_targets)

        prediction_number = parse_pc28_number(
            payload.get('predicted_number', payload.get('number', payload.get('num')))
        )
        prediction_big_small = normalize_big_small(
            payload.get('predicted_big_small', payload.get('big_small'))
        )
        prediction_odd_even = normalize_odd_even(
            payload.get('predicted_odd_even', payload.get('odd_even'))
        )
        prediction_combo = normalize_combo(
            payload.get('predicted_combo', payload.get('combo'))
        )

        if prediction_number is not None:
            derived = derive_pc28_attributes(prediction_number)
            if 'big_small' in targets and not prediction_big_small:
                prediction_big_small = derived['big_small']
            if 'odd_even' in targets and not prediction_odd_even:
                prediction_odd_even = derived['odd_even']
            if 'combo' in targets and not prediction_combo:
                prediction_combo = derived['combo']

        if prediction_combo and ('big_small' in targets or 'odd_even' in targets):
            if not prediction_big_small:
                prediction_big_small = prediction_combo[0]
            if not prediction_odd_even:
                prediction_odd_even = prediction_combo[1]

        if not prediction_combo and prediction_big_small and prediction_odd_even:
            prediction_combo = build_combo(prediction_big_small, prediction_odd_even)

        confidence = self._normalize_confidence(payload.get('confidence'))
        reasoning_summary = str(
            payload.get('reasoning_summary')
            or payload.get('reasoning')
            or payload.get('analysis')
            or payload.get('justification')
            or ''
        ).strip()

        issue_no = str(payload.get('issue_no') or payload.get('nbr') or expected_issue_no or '').strip()

        return {
            'issue_no': issue_no,
            'prediction_number': prediction_number,
            'prediction_big_small': prediction_big_small,
            'prediction_odd_even': prediction_odd_even,
            'prediction_combo': prediction_combo,
            'confidence': confidence,
            'reasoning_summary': reasoning_summary
        }

    def _extract_from_text(self, text: str, expected_issue_no: Optional[str], requested_targets) -> dict:
        number_match = re.search(r'(?:号码|number|num)[^\d]{0,8}(\d{1,2})', text, re.IGNORECASE)
        number = parse_pc28_number(number_match.group(1)) if number_match else None

        big_small_match = re.search(r'(?:大小|big[_\s-]*small)[^大小单双]{0,8}(大|小|big|small)', text, re.IGNORECASE)
        odd_even_match = re.search(r'(?:单双|odd[_\s-]*even)[^大小单双]{0,8}(单|双|odd|even)', text, re.IGNORECASE)
        combo_match = re.search(r'(?:组合|combo)[^大小单双]{0,8}(大单|大双|小单|小双|big\s*odd|big\s*even|small\s*odd|small\s*even)', text, re.IGNORECASE)

        return self._normalize_prediction(
            {
                'issue_no': expected_issue_no,
                'predicted_number': number,
                'predicted_big_small': big_small_match.group(1) if big_small_match else None,
                'predicted_odd_even': odd_even_match.group(1) if odd_even_match else None,
                'predicted_combo': combo_match.group(1) if combo_match else None,
                'confidence': self._extract_confidence(text),
                'reasoning_summary': text[:240]
            },
            expected_issue_no,
            requested_targets
        )

    def _extract_confidence(self, text: str) -> Optional[float]:
        match = re.search(r'(?:confidence|置信度)[^0-9]{0,8}([0-9]+(?:\.[0-9]+)?)', text, re.IGNORECASE)
        if not match:
            return None
        return self._normalize_confidence(match.group(1))

    def _normalize_confidence(self, value) -> Optional[float]:
        if value is None or value == '':
            return None

        try:
            confidence = float(value)
        except (TypeError, ValueError):
            return None

        if confidence > 1:
            confidence = confidence / 100

        return max(0.0, min(confidence, 1.0))

    def _has_effective_prediction(self, prediction: dict, requested_targets) -> bool:
        targets = normalize_target_list(requested_targets)

        field_mapping = {
            'number': 'prediction_number',
            'big_small': 'prediction_big_small',
            'odd_even': 'prediction_odd_even',
            'combo': 'prediction_combo'
        }

        if 'number' in targets and prediction.get('prediction_number') is None:
            return False

        return any(prediction.get(field_mapping[target]) is not None for target in targets)

    def _target_label(self, target: str) -> str:
        mapping = {
            'number': '号码',
            'big_small': '大小',
            'odd_even': '单双',
            'combo': '组合'
        }
        return mapping.get(target, target)


AITrader = AIPredictor
