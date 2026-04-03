"""
AI 预测调用与响应解析
"""
from __future__ import annotations

import json
import re
from typing import Optional
from urllib.parse import urlparse

from openai import APIConnectionError, APIError, OpenAI

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
        last_error = 'AI 未返回有效预测'
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
                last_error = 'AI 返回内容无法解析为有效预测'
            except Exception as exc:
                last_error = str(exc)

        if last_response:
            raise Exception(f'{last_error}；原始响应：{last_response[:300]}')
        raise Exception(last_error)

    def run_connectivity_test(self) -> dict:
        """测试模型连通性与基础输出能力"""
        test_prompt = 'Return a minimal JSON object only: {"status":"ok"}'
        raw_response = self._call_llm(test_prompt)
        preview = raw_response.strip()
        return {
            'success': bool(preview),
            'api_mode': self._resolve_api_mode(),
            'raw_response': raw_response,
            'response_preview': preview[:200]
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
        last_error: Optional[Exception] = None
        system_prompt = (
            "你是 PC28 预测助手。你必须遵守字段契约，只输出单个 JSON 对象。"
        )
        resolved_api_mode = self._resolve_api_mode()

        for base_url in self._candidate_base_urls():
            try:
                client = OpenAI(
                    api_key=self.api_key,
                    base_url=base_url
                )
                if resolved_api_mode == 'responses':
                    response = client.responses.create(
                        model=self.model_name,
                        instructions=system_prompt,
                        input=prompt,
                        temperature=self.temperature,
                        max_output_tokens=1200,
                        text={
                            'format': {
                                'type': 'json_object'
                            }
                        }
                    )
                    return self._extract_response_output_text(response)

                response = client.chat.completions.create(
                    model=self.model_name,
                    messages=[
                        {'role': 'system', 'content': system_prompt},
                        {'role': 'user', 'content': prompt}
                    ],
                    temperature=self.temperature,
                    max_tokens=1200
                )
                return self._extract_message_text(response)
            except (APIConnectionError, APIError, Exception) as exc:
                last_error = exc
                continue

        if isinstance(last_error, APIConnectionError):
            raise Exception(f'API 连接失败：{last_error}')
        if isinstance(last_error, APIError):
            raise Exception(f'API 调用失败（模式={resolved_api_mode}）：{last_error}')
        raise Exception(f'LLM 调用失败（模式={resolved_api_mode}）：{last_error}')

    def _resolve_api_mode(self) -> str:
        if self.api_mode != 'auto':
            return self.api_mode

        host = urlparse(self.api_url).hostname or ''
        host = host.lower()
        if host in {'api.openai.com', 'openai.com'} or host.endswith('.openai.com'):
            return 'responses'
        return 'chat_completions'

    def _extract_response_output_text(self, response) -> str:
        output_text = getattr(response, 'output_text', None)
        text = self._collect_text_fragments(output_text)
        if text:
            return text

        output_items = getattr(response, 'output', None) or []
        parts = []
        for item in output_items:
            content_items = getattr(item, 'content', None) or []
            for content_item in content_items:
                candidate_text = self._collect_text_fragments(
                    getattr(content_item, 'text', None)
                    or getattr(content_item, 'content', None)
                    or content_item
                )
                if candidate_text:
                    parts.append(candidate_text)

        combined_text = '\n'.join(part for part in parts if part).strip()
        if combined_text:
            return combined_text

        response_dump = self._safe_model_dump(response)
        raise ValueError(f'Responses API 返回为空，原始响应片段：{response_dump[:500]}')

    def _extract_message_text(self, response) -> str:
        choices = getattr(response, 'choices', None) or []
        if not choices:
            raise ValueError('AI 返回中没有 choices')

        choice = choices[0]
        message = getattr(choice, 'message', None)
        if message is None:
            raise ValueError('AI 返回中没有 message')

        text = self._collect_text_fragments(getattr(message, 'content', None))
        if text:
            return text

        fallback_fields = [
            'reasoning_content',
            'reasoning',
            'output_text',
            'text'
        ]
        for field_name in fallback_fields:
            text = self._collect_text_fragments(getattr(message, field_name, None))
            if text:
                return text

        tool_calls = getattr(message, 'tool_calls', None) or []
        if tool_calls:
            tool_text_parts = []
            for tool_call in tool_calls:
                function_payload = getattr(tool_call, 'function', None)
                if function_payload is None:
                    continue
                arguments = getattr(function_payload, 'arguments', None)
                if arguments:
                    tool_text_parts.append(str(arguments).strip())
            tool_text = '\n'.join(part for part in tool_text_parts if part)
            if tool_text:
                return tool_text

        finish_reason = getattr(choice, 'finish_reason', 'unknown')
        usage = getattr(response, 'usage', None)
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

        completion_details = getattr(usage, 'completion_tokens_details', None)
        if completion_details is None:
            return 0

        reasoning_tokens = getattr(completion_details, 'reasoning_tokens', 0)
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
