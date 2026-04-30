"""
用户算法 AI 助手
"""
from __future__ import annotations

import json

from ai_trader import AIPredictor
from services.algorithm_definition_validator import validate_algorithm_definition


SYSTEM_PROMPT = (
    '你是 AITradingSimulator 的用户算法设计助手。'
    '你只负责把用户自然语言策略转换成平台 DSL JSON。'
    '禁止输出 Markdown、代码块、解释性前缀或任意可执行代码。'
    '你必须只输出单个 JSON 对象。'
)


def generate_algorithm_draft(
    api_key: str,
    api_url: str,
    model_name: str,
    api_mode: str,
    lottery_type: str,
    user_message: str,
    current_definition: dict | None = None,
    chat_history: list[dict] | None = None,
    backtest_summary: dict | None = None,
    temperature: float = 0.2
) -> dict:
    prompt = _build_algorithm_prompt(
        lottery_type=lottery_type,
        user_message=user_message,
        current_definition=current_definition,
        chat_history=chat_history,
        backtest_summary=backtest_summary
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
        max_output_tokens=2600
    )
    payload = result['payload']
    if not isinstance(payload, dict):
        raise ValueError('AI 返回的内容不是 JSON 对象')

    algorithm = payload.get('algorithm')
    if not isinstance(algorithm, dict):
        algorithm = {}
    validation = validate_algorithm_definition(algorithm, lottery_type=lottery_type)
    return {
        'api_mode': result['api_mode'],
        'response_model': result['response_model'],
        'finish_reason': result['finish_reason'],
        'latency_ms': result['latency_ms'],
        'raw_response': result['raw_response'],
        'payload': payload,
        'algorithm': validation['normalized_definition'] if validation.get('normalized_definition') else algorithm,
        'validation': validation
    }


def _build_algorithm_prompt(
    lottery_type: str,
    user_message: str,
    current_definition: dict | None = None,
    chat_history: list[dict] | None = None,
    backtest_summary: dict | None = None
) -> str:
    current_block = json.dumps(current_definition or {}, ensure_ascii=False, indent=2)
    history_block = _format_chat_history(chat_history or [])
    backtest_block = _format_backtest_summary(backtest_summary or {})
    return f"""请把用户的预测思路转换成 AITradingSimulator 用户算法 DSL。

彩种：{lottery_type}

最近对话：
{history_block}

用户需求：
{user_message}

当前算法草稿：
{current_block}

最近回测摘要：
{backtest_block}

平台 DSL 约束：
1. 顶层必须输出 reply_type、message、questions、algorithm、change_summary、risk_notes。
2. reply_type 只能是 draft_algorithm 或 need_clarification。
3. 如果信息不足，reply_type=need_clarification，questions 给出 1 到 3 个问题，algorithm 输出 null。
4. 如果信息足够，reply_type=draft_algorithm，algorithm 输出完整 DSL 对象。
5. algorithm.schema_version 必须是 1。
6. algorithm.lottery_type 必须等于当前彩种。
7. 竞彩足球 targets 只能使用 spf、rqspf。
8. PC28 targets 只能使用 number、big_small、odd_even、combo。
9. 不允许输出 Python、JavaScript、SQL、Shell 或任何可执行代码。

竞彩足球可用字段：
- spf_odds.win / spf_odds.draw / spf_odds.lose
- rqspf.handicap / rqspf.odds.win / rqspf.odds.draw / rqspf.odds.lose
- home_recent_wins_6 / away_recent_wins_6
- home_goals_per_match_6 / away_goals_per_match_6
- home_conceded_per_match_6 / away_conceded_per_match_6
- home_goal_diff_per_match_6 / home_away_adjusted_form_6
- home_rank / away_rank / rank_gap
- home_injury_count / away_injury_count / injury_advantage
- implied_probability_spf_win / implied_probability_spf_draw / implied_probability_spf_lose
- market_odds_consistency
- euro.initial.win / euro.current.win / euro.drop.win

PC28 可用字段：
- number_frequency_n / combo_frequency_n / big_small_frequency_n / odd_even_frequency_n
- number_omission / combo_omission / big_small_omission / odd_even_omission
- number_moving_avg_n / number_stddev_n / combo_transition_score
- current_hour / current_minute / issue_index_today

支持的 filters op：eq、neq、gt、gte、lt、lte、in、between。
支持的 score transform：linear、inverse、bucket、bonus_if、penalty_if、normalize。

输出示例结构：
{{
  "reply_type": "draft_algorithm",
  "message": "已生成算法草稿。",
  "questions": [],
  "algorithm": {{
    "schema_version": 1,
    "method_name": "进球率预测法",
    "lottery_type": "{lottery_type}",
    "targets": ["spf"],
    "data_window": {{"recent_matches": 6, "history_matches": 30}},
    "filters": [],
    "score": [],
    "decision": {{"target": "spf", "pick": "max_score", "min_confidence": 0.58, "allow_skip": true}},
    "explain": {{"template": "简短依据"}}
  }},
  "change_summary": "生成初始算法。",
  "risk_notes": ["样本窗口较短时波动较大。"]
}}

现在只输出 JSON。"""


def _format_chat_history(chat_history: list[dict]) -> str:
    if not chat_history:
        return '无'

    lines = []
    for item in chat_history[-8:]:
        if not isinstance(item, dict):
            continue
        role = str(item.get('role') or '').strip()
        content = str(item.get('content') or '').strip()
        if role not in {'user', 'assistant'} or not content:
            continue
        label = '用户' if role == 'user' else '助手'
        lines.append(f'{label}: {content[:500]}')
    return '\n'.join(lines) or '无'


def _format_backtest_summary(backtest_summary: dict) -> str:
    if not backtest_summary:
        return '无'

    hit_rate = backtest_summary.get('hit_rate') or {}
    profit_summary = backtest_summary.get('profit_summary') or {}
    risk_flags = backtest_summary.get('risk_flags') or []
    lines = [
        f"样本数：{backtest_summary.get('sample_size', 0)}",
        f"产生预测：{backtest_summary.get('prediction_count', 0)}",
        f"跳过预测：{backtest_summary.get('skip_count', 0)}",
        f"跳过率：{backtest_summary.get('skip_rate', '--')}",
    ]
    for target in ('spf', 'rqspf'):
        stats = hit_rate.get(target) or {}
        profit = profit_summary.get(target) or {}
        if stats:
            lines.append(
                f"{target} 命中：{stats.get('ratio_text', '--')}，"
                f"命中率：{stats.get('hit_rate', '--')}%，"
                f"模拟净收益：{profit.get('net_profit', '--')}，"
                f"ROI：{profit.get('roi', '--')}"
            )
    if risk_flags:
        lines.append('风险提示：' + '；'.join(str(item)[:120] for item in risk_flags[:5]))
    return '\n'.join(lines)
