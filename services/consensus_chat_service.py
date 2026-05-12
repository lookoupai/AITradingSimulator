"""
共识分析 AI 助手

让 AI 基于已经统计好的多方案共识数据做自然语言分析。
聊天输出由后端包装成 JSON，模型本身只需要返回中文文本。
"""
from __future__ import annotations

import json
from typing import Any

from ai_trader import AIPredictor
from services.prediction_guard import AIPredictionError


MAX_PREDICTORS_IN_CHAT_CONTEXT = 8
MAX_TODAY_RECOMMENDATIONS_IN_CHAT_CONTEXT = 6
MAX_TODAY_MATCHES_IN_CHAT_CONTEXT = 8
MAX_PREDICTIONS_PER_MATCH_IN_CHAT_CONTEXT = 8
MAX_HISTORY_SAMPLES_IN_CHAT_CONTEXT = 12
MAX_ROWS_PER_FIELD_IN_CHAT_CONTEXT = 4
MAX_CHAT_REPLY_CHARS = 5000
DEFAULT_CHAT_REQUEST_BUDGET_SECONDS = 16
FALLBACK_CHAT_REQUEST_BUDGET_SECONDS = 10
REASONING_CHAT_REQUEST_BUDGET_SECONDS = 75


SYSTEM_PROMPT = (
    '你是 AITradingSimulator 平台的方案共识分析师。'
    '你的任务是解释多个预测方案之间的历史共识规律、强弱组合、共识陷阱和可执行决策规则。'
    '只能基于用户提供的统计数据回答，不要编造未给出的数据。'
    '请直接输出面向用户的最终中文结论。'
    '严禁输出你的推理过程、分析草稿、任务复述、JSON、Markdown 或代码块。'
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

    normalized_chat_history = chat_history or []
    normalized_summary = consensus_summary or {}
    normalized_today_detail = today_matches_detail or []
    normalized_history = historical_sample or []

    client = AIPredictor(
        api_key=api_key,
        api_url=api_url,
        model_name=model_name,
        api_mode=api_mode,
        temperature=temperature
    )
    use_reasoning_profile = client._is_slow_reasoning_model()
    prompt = _build_prompt(
        user_message=user_message,
        chat_history=[] if use_reasoning_profile else normalized_chat_history,
        consensus_summary=_build_fallback_summary(normalized_summary) if use_reasoning_profile else normalized_summary,
        today_matches_detail=normalized_today_detail[:3] if use_reasoning_profile else normalized_today_detail,
        historical_sample=[] if use_reasoning_profile else normalized_history
    )
    try:
        result = client.run_text_task(
            prompt=prompt,
            system_prompt=SYSTEM_PROMPT,
            max_output_tokens=900 if use_reasoning_profile else 1200,
            request_time_budget_seconds=(
                REASONING_CHAT_REQUEST_BUDGET_SECONDS
                if use_reasoning_profile
                else DEFAULT_CHAT_REQUEST_BUDGET_SECONDS
            )
        )
    except AIPredictionError as exc:
        if getattr(exc, 'category', '') != 'deadline':
            raise
        fallback_prompt = _build_prompt(
            user_message=user_message,
            chat_history=[],
            consensus_summary=_build_fallback_summary(normalized_summary),
            today_matches_detail=normalized_today_detail[:3],
            historical_sample=[]
        )
        result = client.run_text_task(
            prompt=fallback_prompt,
            system_prompt=SYSTEM_PROMPT,
            max_output_tokens=900,
            request_time_budget_seconds=FALLBACK_CHAT_REQUEST_BUDGET_SECONDS
    )
    raw_response = result.get('raw_response') or ''
    reply = _normalize_reply_text(raw_response)
    if _looks_like_reasoning_draft(reply):
        reply = _build_today_recommendation_fallback_reply(normalized_summary)

    return {
        'reply': reply,
        'api_mode': result.get('api_mode'),
        'response_model': result.get('response_model'),
        'finish_reason': result.get('finish_reason'),
        'latency_ms': result.get('latency_ms'),
        'raw_response': raw_response[:1500]
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
这个页面的核心目标是分析多个方案之间的历史共识规律，用这些规律辅助提高后续胜率；它不是单场比赛预测器。

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
- 第一句话必须以“结论：”开头。
- 只输出最终结论，不要写“首先分析”“需要评估”“从数据看”等推理过程。
- 总长度控制在 800 字以内。
- 如果是推荐/风险排序问题，固定输出 3 段：结论、风险排序、注意事项。
- 风险排序最多列 5 场，每场一行，格式为“低/中/高风险：比赛 - 推荐项 - 依据”。
- 优先围绕历史统计规律回答：单方案命中率、按共识数分布、两两方案组合、样本量可靠性、反指标和共识陷阱。
- 如果用户问今天/当前/哪场/推荐，再把今日推荐和今日明细作为这些历史规律的落地应用。
- 数据要引用上面给出的真实数字，不要编造。
- 如果数据不足以回答，明确说不足。
- 样本数不足时必须明确提示不可靠，不要把小样本高命中率说成稳定规律。
- 不要输出 JSON、Markdown、代码块或额外说明。
"""


def _format_summary(summary: dict) -> str:
    if not summary:
        return '（无）'
    fields = summary.get('fields') or []
    compact = {
        'lottery_label': summary.get('lottery_label'),
        'window_days': summary.get('window_days'),
        'sample_count': summary.get('sample_count'),
        'settled_item_count': summary.get('settled_item_count'),
        'pending_item_count': summary.get('pending_item_count'),
        'fields': fields[:4],
        'predictors': _compact_predictors(summary.get('predictors') or []),
        'per_predictor': _compact_per_predictor(summary.get('per_predictor') or [], fields),
        'today_recommendations': _compact_today_recommendations(summary.get('today_recommendations') or []),
        'consensus_by_count': _compact_rows_by_field(
            summary.get('consensus_by_count') or {},
            limit=MAX_ROWS_PER_FIELD_IN_CHAT_CONTEXT
        ),
        'pair_combinations': _compact_rows_by_field(
            summary.get('pair_combinations') or {},
            limit=MAX_ROWS_PER_FIELD_IN_CHAT_CONTEXT
        )
    }
    return _dump_compact_json(compact)


def _format_today_detail(detail: list[dict]) -> str:
    if not detail:
        return '（未提供今日比赛明细）'
    return _dump_compact_json([
        _compact_today_match_detail(item)
        for item in detail[:MAX_TODAY_MATCHES_IN_CHAT_CONTEXT]
    ])


def _format_history_sample(sample: list[dict]) -> str:
    if not sample:
        return '（未提供历史样本）'
    return _dump_compact_json([
        _compact_history_sample_item(item)
        for item in sample[:MAX_HISTORY_SAMPLES_IN_CHAT_CONTEXT]
    ])


def _format_chat_history(chat_history: list[dict]) -> str:
    if not chat_history:
        return '（无）'
    lines = []
    for entry in chat_history[-6:]:
        role = entry.get('role') or 'user'
        content = str(entry.get('content') or '').strip()
        if len(content) > 240:
            content = content[:240] + '…'
        lines.append(f'- {role}: {content}')
    return '\n'.join(lines)


def _build_fallback_summary(summary: dict) -> dict:
    if not summary:
        return {}
    return {
        'lottery_label': summary.get('lottery_label'),
        'window_days': summary.get('window_days'),
        'sample_count': summary.get('sample_count'),
        'settled_item_count': summary.get('settled_item_count'),
        'pending_item_count': summary.get('pending_item_count'),
        'fields': (summary.get('fields') or [])[:2],
        'predictors': (summary.get('predictors') or [])[:4],
        'per_predictor': (summary.get('per_predictor') or [])[:4],
        'today_recommendations': (summary.get('today_recommendations') or [])[:3],
        'consensus_by_count': _limit_rows_by_field(summary.get('consensus_by_count') or {}, limit=2),
        'pair_combinations': _limit_rows_by_field(summary.get('pair_combinations') or {}, limit=2)
    }


def _limit_rows_by_field(rows_by_field: dict, limit: int) -> dict:
    return {
        field_key: rows[:limit]
        for field_key, rows in (rows_by_field or {}).items()
        if isinstance(rows, list)
    }


def _compact_predictors(predictors: list[dict]) -> list[dict]:
    return [
        {
            'id': item.get('id'),
            'name': item.get('name'),
            'engine_type': item.get('engine_type')
        }
        for item in predictors[:MAX_PREDICTORS_IN_CHAT_CONTEXT]
    ]


def _compact_per_predictor(per_predictor: list[dict], fields: list[dict]) -> list[dict]:
    field_keys = [field.get('key') for field in fields if field.get('key')]
    compacted = []
    for item in per_predictor[:MAX_PREDICTORS_IN_CHAT_CONTEXT]:
        metrics = item.get('metrics') or {}
        compacted.append({
            'predictor_id': item.get('predictor_id'),
            'predictor_name': item.get('predictor_name'),
            'engine_type': item.get('engine_type'),
            'metrics': {
                key: {
                    'total': (metrics.get(key) or {}).get('total'),
                    'hit': (metrics.get(key) or {}).get('hit'),
                    'rate': (metrics.get(key) or {}).get('rate')
                }
                for key in field_keys
            }
        })
    return compacted


def _compact_today_recommendations(recommendations: list[dict]) -> list[dict]:
    compacted = []
    for item in recommendations[:MAX_TODAY_RECOMMENDATIONS_IN_CHAT_CONTEXT]:
        fields = []
        for field in (item.get('fields') or [])[:2]:
            fields.append({
                'field': field.get('field'),
                'field_label': field.get('field_label'),
                'consensus_value': field.get('consensus_value'),
                'agree_count': field.get('agree_count'),
                'historical_rate': field.get('historical_rate'),
                'historical_sample': field.get('historical_sample'),
                'weighted_strength': field.get('weighted_strength'),
                'is_reliable': field.get('is_reliable')
            })
        compacted.append({
            'run_key': item.get('run_key'),
            'event_key': item.get('event_key'),
            'title': item.get('title'),
            'fields': fields
        })
    return compacted


def _compact_today_match_detail(item: dict) -> dict:
    predictions = [
        _compact_match_prediction(prediction_item)
        for prediction_item in (item.get('predictions') or [])[:MAX_PREDICTIONS_PER_MATCH_IN_CHAT_CONTEXT]
    ]
    predictions = [prediction_item for prediction_item in predictions if prediction_item.get('prediction')]
    return {
        'run_key': item.get('run_key'),
        'event_key': item.get('event_key'),
        'title': item.get('title'),
        'consensus': _build_prediction_consensus(predictions),
        'predictions': predictions
    }


def _compact_match_prediction(item: dict) -> dict:
    return {
        'predictor_name': item.get('predictor_name'),
        'prediction': _compact_prediction_map(item.get('prediction') or {})
    }


def _compact_history_sample_item(item: dict) -> dict:
    return {
        'title': item.get('title'),
        'predictor_name': item.get('predictor_name'),
        'prediction': _compact_prediction_map(item.get('prediction') or {}),
        'actual': _compact_actual_map(item.get('actual') or {}),
        'hit': _compact_hit_map(item.get('hit') or {})
    }


def _compact_prediction_map(values: dict) -> dict:
    return {
        key: value
        for key, value in (values or {}).items()
        if value not in (None, '', 'null')
    }


def _compact_hit_map(values: dict) -> dict:
    return {
        key: value
        for key, value in (values or {}).items()
        if value is not None
    }


def _compact_actual_map(values: dict) -> dict:
    allowed_keys = {'spf', 'rqspf', 'score_text'}
    return {
        key: value
        for key, value in (values or {}).items()
        if key in allowed_keys and value not in (None, '', 'null')
    }


def _build_prediction_consensus(predictions: list[dict]) -> dict:
    counters: dict[str, dict[str, int]] = {}
    for prediction_item in predictions:
        prediction = prediction_item.get('prediction') or {}
        for field_key, value in prediction.items():
            if value in (None, '', 'null'):
                continue
            field_counter = counters.setdefault(field_key, {})
            field_counter[str(value)] = field_counter.get(str(value), 0) + 1

    consensus = {}
    for field_key, distribution in counters.items():
        consensus_value, agree_count = max(
            distribution.items(),
            key=lambda row: (row[1], row[0])
        )
        consensus[field_key] = {
            'value': consensus_value,
            'agree_count': agree_count,
            'distribution': distribution
        }
    return consensus


def _compact_rows_by_field(rows_by_field: dict, limit: int = 6) -> dict:
    compacted: dict[str, list[dict]] = {}
    for field_key, rows in (rows_by_field or {}).items():
        if not isinstance(rows, list):
            continue
        sorted_rows = sorted(
            rows,
            key=lambda row: (
                row.get('rate') or 0,
                row.get('total') or 0,
                row.get('agree_count') or 0
            ),
            reverse=True
        )
        compacted[field_key] = [
            {
                'agree_count': row.get('agree_count'),
                'value': row.get('value'),
                'pair': row.get('pair'),
                'total': row.get('total'),
                'hit': row.get('hit'),
                'rate': row.get('rate')
            }
            for row in sorted_rows[:limit]
        ]
    return compacted


def _dump_compact_json(payload) -> str:
    return json.dumps(payload, ensure_ascii=False, separators=(',', ':'))


def _looks_like_reasoning_draft(text: str) -> bool:
    normalized = (text or '').strip()
    if not normalized:
        return False
    draft_markers = (
        '用户要求',
        '用户询问',
        '我需要分析',
        '让我先',
        '首先分析',
        '需要评估',
        '共识统计摘要分析',
        '今日比赛明细分析'
    )
    return any(marker in normalized[:500] for marker in draft_markers)


def _build_today_recommendation_fallback_reply(summary: dict) -> str:
    recommendations = summary.get('today_recommendations') or []
    if not recommendations:
        sample_count = summary.get('sample_count')
        return (
            '结论：当前没有足够清晰的今日推荐可排序。\n'
            f'风险排序：暂无可用比赛；当前历史样本数为 {sample_count or 0}。\n'
            '注意事项：请先确认今日方案池是否已全部完成预测，再做跟单判断。'
        )

    rows = []
    for item in recommendations[:5]:
        best_field = _select_best_recommendation_field(item.get('fields') or [])
        if not best_field:
            continue
        rows.append(_format_recommendation_row(item, best_field))

    if not rows:
        return '结论：今日推荐缺少可解释字段，暂不建议跟单。\n风险排序：暂无。\n注意事项：等待更多方案预测后再看。'

    best = rows[0]
    lines = [
        f"结论：最值得优先关注的是 {best['title']}，推荐 {best['field_label']}={best['value']}；"
        f"历史命中率 {best['rate_text']}，样本 {best['sample_text']}，共识 {best['agree_count']} 个方案。",
        '风险排序：'
    ]
    for row in rows:
        lines.append(
            f"{row['risk']}：{row['title']} - {row['field_label']}={row['value']} - "
            f"{row['agree_count']} 方案共识，历史命中率 {row['rate_text']}，样本 {row['sample_text']}"
        )
    lines.append('注意事项：这是基于历史共识规律的排序，不是确定赛果；样本不足或临场阵容变化会显著增加风险。')
    return '\n'.join(lines)


def _select_best_recommendation_field(fields: list[dict]) -> dict | None:
    candidates = [field for field in fields if isinstance(field, dict)]
    if not candidates:
        return None
    return max(
        candidates,
        key=lambda field: (
            1 if field.get('is_reliable') else 0,
            float(field.get('historical_rate') or 0),
            int(field.get('historical_sample') or 0),
            int(field.get('agree_count') or 0),
            float(field.get('weighted_strength') or 0)
        )
    )


def _format_recommendation_row(item: dict, field: dict) -> dict:
    rate = field.get('historical_rate')
    sample = field.get('historical_sample')
    agree_count = int(field.get('agree_count') or 0)
    is_reliable = bool(field.get('is_reliable'))
    risk = '高风险'
    if is_reliable and agree_count >= 4 and (rate or 0) >= 55:
        risk = '低风险'
    elif is_reliable and agree_count >= 3 and (rate or 0) >= 45:
        risk = '中风险'

    return {
        'risk': risk,
        'title': item.get('title') or item.get('event_key') or '未知比赛',
        'field_label': field.get('field_label') or field.get('field') or '推荐项',
        'value': field.get('consensus_value'),
        'agree_count': agree_count,
        'rate_text': f'{rate}%' if rate is not None else '未知',
        'sample_text': str(sample if sample is not None else '未知')
    }


def _normalize_reply_text(raw_response: str) -> str:
    text = (raw_response or '').strip()
    if not text:
        return '（无回复）'

    if text.startswith('```') and text.endswith('```'):
        lines = text.splitlines()
        text = '\n'.join(lines[1:-1]).strip()

    try:
        payload = json.loads(text)
    except (TypeError, ValueError):
        return text[:MAX_CHAT_REPLY_CHARS]

    if isinstance(payload, dict):
        reply = str(payload.get('reply') or '').strip()
        if reply:
            return reply[:MAX_CHAT_REPLY_CHARS]
    return text[:MAX_CHAT_REPLY_CHARS]
