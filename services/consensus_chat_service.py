"""
共识分析 AI 助手

复用 ai_trader.AIPredictor.run_json_task：让 AI 基于已经统计好的共识数据
+ 今日比赛明细做自然语言分析。

输出约定为 JSON，但只有一个 `reply` 字段保存自然语言文字，方便前端直接渲染，
也避免 AI 走偏到 Markdown / 代码块。
"""
from __future__ import annotations

import json
from typing import Any

from ai_trader import AIPredictor


SYSTEM_PROMPT = (
    '你是 AITradingSimulator 平台的竞彩足球方案共识分析师。'
    '你只能基于用户提供的统计数据和今日比赛明细回答问题，不要编造未给出的数据。'
    '禁止输出 Markdown、代码块、或任何 JSON 外的字符。'
    '你必须只输出一个 JSON 对象，且仅包含一个键 reply，值为面向用户的中文自然语言回答。'
)


def chat_consensus_analysis(
    *,
    api_key: str,
    api_url: str,
    model_name: str,
    api_mode: str,
    user_message: str,
    chat_history: list[dict] | None = None,
    consensus_summary: dict | None = None,
    today_matches_detail: list[dict] | None = None,
    historical_sample: list[dict] | None = None,
    temperature: float = 0.5
) -> dict:
    """
    返回结构：
        {
          'reply': '自然语言回复',
          'api_mode': ...,
          'response_model': ...,
          'finish_reason': ...,
          'latency_ms': ...,
          'raw_response': '...'
        }
    """
    if not user_message:
        raise ValueError('用户消息不能为空')

    prompt = _build_prompt(
        user_message=user_message,
        chat_history=chat_history or [],
        consensus_summary=consensus_summary or {},
        today_matches_detail=today_matches_detail or [],
        historical_sample=historical_sample or []
    )

    client = AIPredictor(
        api_key=api_key,
        api_url=api_url,
        model_name=model_name,
        api_mode=api_mode,
        temperature=temperature
    )
    result = client.run_json_task(
        prompt=prompt,
        system_prompt=SYSTEM_PROMPT,
        max_output_tokens=2200
    )
    payload = result.get('payload') or {}
    reply = ''
    if isinstance(payload, dict):
        reply = str(payload.get('reply') or '').strip()
    if not reply:
        # 兜底：把 raw_response 截断作为回复
        reply = (result.get('raw_response') or '')[:1200]

    return {
        'reply': reply,
        'api_mode': result.get('api_mode'),
        'response_model': result.get('response_model'),
        'finish_reason': result.get('finish_reason'),
        'latency_ms': result.get('latency_ms'),
        'raw_response': (result.get('raw_response') or '')[:1500]
    }


# ----------------------- 内部辅助 -----------------------

def _build_prompt(
    *,
    user_message: str,
    chat_history: list[dict],
    consensus_summary: dict,
    today_matches_detail: list[dict],
    historical_sample: list[dict]
) -> str:
    summary_block = _format_summary(consensus_summary)
    today_block = _format_today_detail(today_matches_detail)
    history_block = _format_history_sample(historical_sample)
    chat_block = _format_chat_history(chat_history)

    return f"""你将基于下面提供的"共识统计摘要"、"今日比赛明细"和"历史样本"回答用户的问题。

=== 共识统计摘要 ===
{summary_block}

=== 今日比赛明细（含每个方案的预测） ===
{today_block}

=== 最近历史样本（每条含各方案预测和命中情况） ===
{history_block}

=== 历史对话（如果有） ===
{chat_block}

=== 用户当前问题 ===
{user_message}

回答规范：
- 用中文自然语言。
- 数据要引用上面给出的真实数字，不要编造。
- 如果数据不足以回答，明确说不足。
- 把回答写在 JSON 的 reply 字段里，不输出其它任何字符。
- 输出格式严格如下：
{{"reply": "你的回答内容"}}
"""


def _format_summary(summary: dict) -> str:
    if not summary:
        return '（无）'
    keep_keys = ('lottery_label', 'window_days', 'sample_count',
                 'settled_item_count', 'pending_item_count', 'fields',
                 'predictors', 'per_predictor',
                 'consensus_by_count', 'pair_combinations')
    compact = {k: summary.get(k) for k in keep_keys if k in summary}
    return json.dumps(compact, ensure_ascii=False, indent=2)


def _format_today_detail(detail: list[dict]) -> str:
    if not detail:
        return '（今天没有可分析的比赛）'
    # 限制长度防止 token 爆炸
    return json.dumps(detail[:80], ensure_ascii=False, indent=2)


def _format_history_sample(sample: list[dict]) -> str:
    if not sample:
        return '（未提供历史样本）'
    return json.dumps(sample[:120], ensure_ascii=False, indent=2)


def _format_chat_history(chat_history: list[dict]) -> str:
    if not chat_history:
        return '（无）'
    lines = []
    for entry in chat_history[-10:]:
        role = entry.get('role') or 'user'
        content = entry.get('content') or ''
        lines.append(f'- {role}: {content}')
    return '\n'.join(lines)
