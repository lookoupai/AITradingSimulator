"""
用户自定义算法 DSL 校验器
"""
from __future__ import annotations

from copy import deepcopy

from lotteries.registry import normalize_lottery_type, normalize_prediction_targets


ALGORITHM_SCHEMA_VERSION = 1
MAX_FILTERS = 30
MAX_SCORE_ITEMS = 40
MAX_DEFINITION_JSON_LENGTH = 32768

ALLOWED_FILTER_OPERATORS = {'eq', 'neq', 'gt', 'gte', 'lt', 'lte', 'in', 'between'}
ALLOWED_TRANSFORMS = {'linear', 'inverse', 'bucket', 'bonus_if', 'penalty_if', 'normalize'}
ALLOWED_DECISION_PICK_VALUES = {'max_score', '胜', '平', '负', '大', '小', '单', '双', '大单', '大双', '小单', '小双'}

FOOTBALL_ALLOWED_FIELDS = {
    'league',
    'match_time',
    'home_team',
    'away_team',
    'spf_odds.win',
    'spf_odds.draw',
    'spf_odds.lose',
    'rqspf.handicap',
    'rqspf.odds.win',
    'rqspf.odds.draw',
    'rqspf.odds.lose',
    'euro.initial.win',
    'euro.current.win',
    'euro.drop.win',
    'asia.line_changed',
    'asia.home_trend',
    'home_recent_wins_n',
    'away_recent_wins_n',
    'home_goals_per_match_n',
    'away_goals_per_match_n',
    'home_conceded_per_match_n',
    'away_conceded_per_match_n',
    'home_recent_wins_6',
    'away_recent_wins_6',
    'home_goals_per_match_6',
    'away_goals_per_match_6',
    'home_conceded_per_match_6',
    'away_conceded_per_match_6',
    'home_goal_diff_per_match_6',
    'home_away_adjusted_form_6',
    'home_rank',
    'away_rank',
    'rank_gap',
    'home_injury_count',
    'away_injury_count',
    'injury_advantage',
    'implied_probability_spf_win',
    'implied_probability_spf_draw',
    'implied_probability_spf_lose',
    'market_odds_consistency'
}

PC28_ALLOWED_FIELDS = {
    'result_number',
    'big_small',
    'odd_even',
    'combo',
    'number_frequency_n',
    'combo_frequency_n',
    'big_small_frequency_n',
    'odd_even_frequency_n',
    'number_omission',
    'combo_omission',
    'big_small_omission',
    'odd_even_omission',
    'number_moving_avg_n',
    'number_stddev_n',
    'combo_transition_score',
    'current_hour',
    'current_minute',
    'issue_index_today'
}

ALLOWED_FIELDS_BY_LOTTERY = {
    'pc28': PC28_ALLOWED_FIELDS,
    'jingcai_football': FOOTBALL_ALLOWED_FIELDS
}


def validate_algorithm_definition(definition: dict, lottery_type: str | None = None) -> dict:
    errors: list[str] = []
    warnings: list[str] = []

    if not isinstance(definition, dict):
        return {
            'valid': False,
            'errors': ['算法定义必须是 JSON 对象'],
            'warnings': [],
            'normalized_definition': {}
        }

    normalized = deepcopy(definition)
    resolved_lottery_type = normalize_lottery_type(lottery_type or normalized.get('lottery_type'))
    normalized['lottery_type'] = resolved_lottery_type

    if normalized.get('schema_version') != ALGORITHM_SCHEMA_VERSION:
        errors.append(f'算法 schema_version 必须为 {ALGORITHM_SCHEMA_VERSION}')

    method_name = str(normalized.get('method_name') or '').strip()
    if not method_name:
        errors.append('算法名称不能为空')
    normalized['method_name'] = method_name[:80]

    targets = normalize_prediction_targets(resolved_lottery_type, normalized.get('targets'))
    if not targets:
        errors.append('算法至少需要一个预测目标')
    normalized['targets'] = targets

    _validate_data_window(normalized.get('data_window'), errors)
    _validate_filters(normalized.get('filters'), resolved_lottery_type, errors)
    _validate_score(normalized.get('score'), resolved_lottery_type, errors, warnings)
    _validate_decision(normalized.get('decision'), targets, errors, warnings)
    _validate_explain(normalized.get('explain'), warnings)

    return {
        'valid': not errors,
        'errors': errors,
        'warnings': warnings,
        'normalized_definition': normalized
    }


def _validate_data_window(value, errors: list[str]):
    if value is None:
        return
    if not isinstance(value, dict):
        errors.append('data_window 必须是对象')
        return
    for key in ('recent_matches', 'history_matches'):
        if key not in value:
            continue
        try:
            number = int(value.get(key))
        except (TypeError, ValueError):
            errors.append(f'data_window.{key} 必须是整数')
            continue
        if number < 1 or number > 200:
            errors.append(f'data_window.{key} 必须在 1 到 200 之间')


def _validate_filters(value, lottery_type: str, errors: list[str]):
    if value is None:
        return
    if not isinstance(value, list):
        errors.append('filters 必须是数组')
        return
    if len(value) > MAX_FILTERS:
        errors.append(f'filters 最多允许 {MAX_FILTERS} 条')
        return
    for index, item in enumerate(value):
        if not isinstance(item, dict):
            errors.append(f'filters[{index}] 必须是对象')
            continue
        _validate_field(item.get('field'), lottery_type, f'filters[{index}].field', errors)
        operator = str(item.get('op') or '').strip()
        if operator not in ALLOWED_FILTER_OPERATORS:
            errors.append(f'filters[{index}].op 不支持: {operator or "空"}')
        if 'value' not in item:
            errors.append(f'filters[{index}].value 不能为空')


def _validate_score(value, lottery_type: str, errors: list[str], warnings: list[str]):
    if not isinstance(value, list) or not value:
        errors.append('score 必须是非空数组')
        return
    if len(value) > MAX_SCORE_ITEMS:
        errors.append(f'score 最多允许 {MAX_SCORE_ITEMS} 条')
        return

    total_weight = 0.0
    for index, item in enumerate(value):
        if not isinstance(item, dict):
            errors.append(f'score[{index}] 必须是对象')
            continue
        _validate_field(item.get('feature'), lottery_type, f'score[{index}].feature', errors)
        transform = str(item.get('transform') or 'linear').strip()
        if transform not in ALLOWED_TRANSFORMS:
            errors.append(f'score[{index}].transform 不支持: {transform or "空"}')
        try:
            weight = float(item.get('weight'))
        except (TypeError, ValueError):
            errors.append(f'score[{index}].weight 必须是数字')
            continue
        if weight < 0 or weight > 1:
            errors.append(f'score[{index}].weight 必须在 0 到 1 之间')
        total_weight += weight

    if total_weight <= 0:
        errors.append('score 权重总和必须大于 0')
    elif abs(total_weight - 1.0) > 0.05:
        warnings.append('score 权重总和建议接近 1')


def _validate_decision(value, targets: list[str], errors: list[str], warnings: list[str]):
    if not isinstance(value, dict):
        errors.append('decision 必须是对象')
        return

    target = str(value.get('target') or '').strip()
    if target and target not in targets:
        errors.append('decision.target 必须包含在 targets 中')
    if not target and targets:
        warnings.append('decision.target 为空，执行时将使用第一个预测目标')

    pick = str(value.get('pick') or 'max_score').strip()
    if pick not in ALLOWED_DECISION_PICK_VALUES:
        errors.append(f'decision.pick 不支持: {pick or "空"}')

    if 'min_confidence' in value:
        try:
            min_confidence = float(value.get('min_confidence'))
        except (TypeError, ValueError):
            errors.append('decision.min_confidence 必须是数字')
        else:
            if min_confidence < 0 or min_confidence > 1:
                errors.append('decision.min_confidence 必须在 0 到 1 之间')


def _validate_explain(value, warnings: list[str]):
    if value is None:
        return
    if not isinstance(value, dict):
        warnings.append('explain 不是对象，执行时将忽略解释模板')
        return
    template = str(value.get('template') or '')
    if len(template) > 500:
        warnings.append('explain.template 过长，建议控制在 500 字以内')


def _validate_field(field, lottery_type: str, label: str, errors: list[str]):
    text = str(field or '').strip()
    if not text:
        errors.append(f'{label} 不能为空')
        return
    allowed_fields = ALLOWED_FIELDS_BY_LOTTERY.get(lottery_type, set())
    if text not in allowed_fields:
        errors.append(f'{label} 不在当前彩种字段白名单中: {text}')
