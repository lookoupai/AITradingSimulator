"""
用户算法模板与本地调参工具
"""
from __future__ import annotations

from copy import deepcopy


FOOTBALL_ALGORITHM_TEMPLATES = [
    {
        'key': 'goal_rate_v1',
        'name': '进球率预测法',
        'description': '按主队进球率、客队失球率、赔率隐含概率和伤停优势做主胜筛选。',
        'definition': {
            'schema_version': 1,
            'method_name': '进球率预测法',
            'lottery_type': 'jingcai_football',
            'targets': ['spf'],
            'data_window': {'recent_matches': 6, 'history_matches': 30},
            'filters': [
                {'field': 'spf_odds.win', 'op': 'gte', 'value': 1.35},
                {'field': 'home_goals_per_match_6', 'op': 'gte', 'value': 1.2}
            ],
            'score': [
                {'feature': 'home_goals_per_match_6', 'transform': 'linear', 'weight': 0.35},
                {'feature': 'away_conceded_per_match_6', 'transform': 'linear', 'weight': 0.30},
                {'feature': 'implied_probability_spf_win', 'transform': 'linear', 'weight': 0.20},
                {'feature': 'injury_advantage', 'transform': 'linear', 'weight': 0.15}
            ],
            'decision': {'target': 'spf', 'pick': '胜', 'min_confidence': 0.58, 'allow_skip': True},
            'explain': {'template': '进球率、失球率、赔率和伤停综合评分'}
        }
    },
    {
        'key': 'recent_six_v1',
        'name': '最近六场预测法',
        'description': '更重视近六场胜场、净胜球和主客状态差。',
        'definition': {
            'schema_version': 1,
            'method_name': '最近六场预测法',
            'lottery_type': 'jingcai_football',
            'targets': ['spf'],
            'data_window': {'recent_matches': 6, 'history_matches': 30},
            'filters': [],
            'score': [
                {'feature': 'home_recent_wins_6', 'transform': 'linear', 'weight': 0.30},
                {'feature': 'home_goal_diff_per_match_6', 'transform': 'linear', 'weight': 0.30},
                {'feature': 'home_away_adjusted_form_6', 'transform': 'linear', 'weight': 0.25},
                {'feature': 'rank_gap', 'transform': 'linear', 'weight': 0.15}
            ],
            'decision': {'target': 'spf', 'pick': '胜', 'min_confidence': 0.56, 'allow_skip': True},
            'explain': {'template': '近六场状态和主客强弱差综合评分'}
        }
    },
    {
        'key': 'odds_heat_filter_v1',
        'name': '赔率热度过滤法',
        'description': '过滤过热低赔率热门，优先选择赔率与状态一致的场次。',
        'definition': {
            'schema_version': 1,
            'method_name': '赔率热度过滤法',
            'lottery_type': 'jingcai_football',
            'targets': ['spf'],
            'data_window': {'recent_matches': 6, 'history_matches': 40},
            'filters': [
                {'field': 'spf_odds.win', 'op': 'between', 'value': [1.45, 2.35]}
            ],
            'score': [
                {'feature': 'implied_probability_spf_win', 'transform': 'linear', 'weight': 0.35},
                {'feature': 'market_odds_consistency', 'transform': 'linear', 'weight': 0.25},
                {'feature': 'home_away_adjusted_form_6', 'transform': 'linear', 'weight': 0.25},
                {'feature': 'injury_advantage', 'transform': 'linear', 'weight': 0.15}
            ],
            'decision': {'target': 'spf', 'pick': '胜', 'min_confidence': 0.60, 'allow_skip': True},
            'explain': {'template': '过滤低赔率热门后按赔率一致性评分'}
        }
    },
    {
        'key': 'home_away_strength_v1',
        'name': '主客强弱差法',
        'description': '按积分排名差、近期状态差和让球盘口做强弱判断。',
        'definition': {
            'schema_version': 1,
            'method_name': '主客强弱差法',
            'lottery_type': 'jingcai_football',
            'targets': ['spf', 'rqspf'],
            'data_window': {'recent_matches': 6, 'history_matches': 40},
            'filters': [{'field': 'rank_gap', 'op': 'gte', 'value': 3}],
            'score': [
                {'feature': 'rank_gap', 'transform': 'linear', 'weight': 0.30},
                {'feature': 'home_away_adjusted_form_6', 'transform': 'linear', 'weight': 0.30},
                {'feature': 'rqspf.handicap', 'transform': 'inverse', 'weight': 0.15},
                {'feature': 'implied_probability_spf_win', 'transform': 'linear', 'weight': 0.25}
            ],
            'decision': {'target': 'spf', 'pick': '胜', 'min_confidence': 0.58, 'allow_skip': True},
            'explain': {'template': '排名差、状态差和盘口方向综合评分'}
        }
    },
    {
        'key': 'injury_adjustment_v1',
        'name': '伤停修正法',
        'description': '以赔率为底座，对伤停优势和近期失球做修正。',
        'definition': {
            'schema_version': 1,
            'method_name': '伤停修正法',
            'lottery_type': 'jingcai_football',
            'targets': ['spf'],
            'data_window': {'recent_matches': 6, 'history_matches': 30},
            'filters': [{'field': 'injury_advantage', 'op': 'gte', 'value': 0}],
            'score': [
                {'feature': 'injury_advantage', 'transform': 'linear', 'weight': 0.35},
                {'feature': 'implied_probability_spf_win', 'transform': 'linear', 'weight': 0.30},
                {'feature': 'away_conceded_per_match_6', 'transform': 'linear', 'weight': 0.20},
                {'feature': 'home_recent_wins_6', 'transform': 'linear', 'weight': 0.15}
            ],
            'decision': {'target': 'spf', 'pick': '胜', 'min_confidence': 0.57, 'allow_skip': True},
            'explain': {'template': '伤停差和赔率底座修正'}
        }
    },
    {
        'key': 'odds_movement_v1',
        'name': '赔率变化法',
        'description': '按欧赔主胜下降、即时赔率和状态做市场变化筛选。',
        'definition': {
            'schema_version': 1,
            'method_name': '赔率变化法',
            'lottery_type': 'jingcai_football',
            'targets': ['spf'],
            'data_window': {'recent_matches': 6, 'history_matches': 40},
            'filters': [{'field': 'euro.drop.win', 'op': 'gte', 'value': 0.02}],
            'score': [
                {'feature': 'euro.drop.win', 'transform': 'linear', 'weight': 0.35},
                {'feature': 'implied_probability_spf_win', 'transform': 'linear', 'weight': 0.30},
                {'feature': 'home_away_adjusted_form_6', 'transform': 'linear', 'weight': 0.20},
                {'feature': 'injury_advantage', 'transform': 'linear', 'weight': 0.15}
            ],
            'decision': {'target': 'spf', 'pick': '胜', 'min_confidence': 0.59, 'allow_skip': True},
            'explain': {'template': '欧赔变化与基本面共振评分'}
        }
    }
]


def list_algorithm_templates(lottery_type: str = 'jingcai_football') -> list[dict]:
    if lottery_type != 'jingcai_football':
        return []
    return [
        {
            'key': item['key'],
            'name': item['name'],
            'description': item['description'],
            'definition': deepcopy(item['definition'])
        }
        for item in FOOTBALL_ALGORITHM_TEMPLATES
    ]


def apply_algorithm_adjustment(definition: dict, mode: str) -> tuple[dict, str]:
    adjusted = deepcopy(definition or {})
    decision = adjusted.setdefault('decision', {})
    filters = adjusted.setdefault('filters', [])
    score_items = adjusted.setdefault('score', [])
    mode = str(mode or '').strip()

    if mode == 'conservative':
        decision['min_confidence'] = _shift_confidence(decision.get('min_confidence'), 0.04)
        _upsert_filter(filters, 'spf_odds.win', 'gte', 1.45)
        return adjusted, '一键调参：更保守，提高信心门槛并过滤过低赔率。'
    if mode == 'aggressive':
        decision['min_confidence'] = _shift_confidence(decision.get('min_confidence'), -0.04)
        return adjusted, '一键调参：更激进，降低信心门槛以增加出手。'
    if mode == 'reduce_skip':
        decision['min_confidence'] = _shift_confidence(decision.get('min_confidence'), -0.06)
        if filters:
            filters.pop()
        return adjusted, '一键调参：降低跳过，放宽信心门槛并减少一条过滤条件。'
    if mode == 'value_odds':
        decision['min_confidence'] = _shift_confidence(decision.get('min_confidence'), 0.02)
        _upsert_filter(filters, 'spf_odds.win', 'between', [1.55, 3.2])
        _upsert_score(score_items, 'market_odds_consistency', 'linear', 0.25)
        return adjusted, '一键调参：提高赔率价值，过滤低赔率热门并增加赔率一致性权重。'
    return adjusted, '未识别调参模式，算法定义未改变。'


def build_backtest_adjustment_suggestions(backtest: dict) -> list[dict]:
    suggestions = []
    sample_size = int((backtest or {}).get('sample_size') or 0)
    prediction_count = int((backtest or {}).get('prediction_count') or 0)
    skip_rate = (backtest or {}).get('skip_rate')
    if sample_size < 20:
        suggestions.append({'mode': 'collect_data', 'label': '先补样本', 'reason': '有效样本不足，先补齐历史赛果和赔率。'})
    if skip_rate is not None and float(skip_rate) >= 45:
        suggestions.append({'mode': 'reduce_skip', 'label': '降低跳过', 'reason': '跳过率偏高，预测机会不足。'})
    if prediction_count >= 10:
        for target, stats in ((backtest or {}).get('hit_rate') or {}).items():
            hit_rate = stats.get('hit_rate')
            if hit_rate is not None and float(hit_rate) < 45:
                suggestions.append({'mode': 'conservative', 'label': '更保守', 'reason': f'{target.upper()} 命中率偏低，提高信心门槛。'})
    odds_perf = ((backtest or {}).get('odds_interval_performance') or {}).get('spf') or []
    low_odds = next((item for item in odds_perf if item.get('range') == '1.00-1.49'), None)
    if low_odds and low_odds.get('sample_count') and (low_odds.get('hit_rate') or 0) < 70:
        suggestions.append({'mode': 'value_odds', 'label': '提高赔率价值', 'reason': '低赔率热门表现不足，建议过滤过热区间。'})
    return suggestions


def _shift_confidence(value, delta: float) -> float:
    try:
        current = float(value)
    except (TypeError, ValueError):
        current = 0.58
    return round(max(0.35, min(0.85, current + delta)), 2)


def _upsert_filter(filters: list[dict], field: str, op: str, value):
    for item in filters:
        if item.get('field') == field:
            item['op'] = op
            item['value'] = value
            return
    filters.append({'field': field, 'op': op, 'value': value})


def _upsert_score(score_items: list[dict], feature: str, transform: str, weight: float):
    for item in score_items:
        if item.get('feature') == feature:
            item['transform'] = transform
            item['weight'] = weight
            return
    score_items.append({'feature': feature, 'transform': transform, 'weight': weight})
