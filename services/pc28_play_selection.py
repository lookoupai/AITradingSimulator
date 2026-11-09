"""模拟投注内容选择，与均注／倍投／过关资金计算分离。"""
from __future__ import annotations

from itertools import combinations
from utils.pc28 import GROUP_PLAY_TYPES, derive_group_bet

COMBOS = ('大单','大双','小单','小双')
VALUES = {'big_small': ('大','小'), 'odd_even': ('单','双'), 'combo': COMBOS,
          'number': tuple(str(i) for i in range(28)), 'kill_group': COMBOS,
          'double_group': tuple('+'.join(pair) for pair in combinations(COMBOS,2))}


def normalize_selection(metric, config=None):
    config = config or {}
    mode = config.get('selection_mode') or 'follow'
    if metric not in VALUES or mode not in ('follow','fixed','filter'):
        raise ValueError('投注玩法或方向选择方式无效')
    value = config.get('fixed_value')
    if mode != 'follow':
        value = str(value if value is not None else '').strip()
        if metric == 'number' and value.isdigit():
            value = str(int(value))
        if metric == 'double_group':
            parts = value.replace('＋','+').split('+')
            if len(parts) == 2 and len(set(parts)) == 2 and all(p in COMBOS for p in parts):
                value = '+'.join(p for p in COMBOS if p in parts)
        if value not in VALUES[metric]:
            raise ValueError('投注方向与玩法不匹配')
    else:
        value = None
    return {'selection_mode': mode, 'fixed_value': value}


def selected_bets(prediction, metric, config):
    mode = config.get('selection_mode') or 'follow'
    if mode == 'fixed':
        value = config['fixed_value']
    elif metric in GROUP_PLAY_TYPES:
        group = derive_group_bet(metric, prediction.get('prediction_combo'))
        if not group:
            return None
        value = group['excluded_combo'] if metric == 'kill_group' else '+'.join(group['selected_combos'])
    else:
        raw = prediction.get('prediction_'+metric)
        if raw is None:
            return None
        value = str(raw)
    if mode == 'filter' and value != config['fixed_value']:
        return None
    if value not in VALUES[metric]:
        return None
    if metric in GROUP_PLAY_TYPES:
        combos = [c for c in COMBOS if c != value] if metric == 'kill_group' else value.split('+')
        return [('combo', c) for c in combos]
    return [(metric,value)]


def selected_prediction(prediction, metric, config):
    bets = selected_bets(prediction, metric, config)
    if not bets:
        return None
    if metric in GROUP_PLAY_TYPES:
        selected = [value for _,value in bets]
        excluded = next((c for c in COMBOS if c not in selected),None) if metric == 'kill_group' else None
        group = {'selected_combos': selected, 'excluded_combo': excluded, 'unit_count': len(selected)}
        return {**prediction, '_simulation_group': group}
    return {**prediction, 'prediction_'+metric: bets[0][1]}
