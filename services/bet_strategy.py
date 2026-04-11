"""
统一下注策略定义
"""
from __future__ import annotations

DEFAULT_BASE_STAKE = 10.0
DEFAULT_BET_MODE = 'flat'
DEFAULT_BET_MULTIPLIER = 2.0
DEFAULT_BET_MAX_STEPS = 6
MAX_BET_STEPS = 12

BET_MODE_LABELS = {
    'flat': '均注',
    'martingale': '倍投'
}

REFUND_ACTION_LABELS = {
    'hold': '退本金保持当前手'
}

CAP_ACTION_LABELS = {
    'reset': '封顶后未中回到基础注'
}


def build_bet_strategy(
    bet_mode=None,
    base_stake=None,
    multiplier=None,
    max_steps=None,
    refund_action: str | None = None,
    cap_action: str | None = None
) -> dict:
    normalized_mode = str(bet_mode or '').strip().lower()
    if normalized_mode not in BET_MODE_LABELS:
        normalized_mode = DEFAULT_BET_MODE

    try:
        resolved_base_stake = float(base_stake)
    except (TypeError, ValueError):
        resolved_base_stake = DEFAULT_BASE_STAKE
    if resolved_base_stake <= 0:
        resolved_base_stake = DEFAULT_BASE_STAKE
    resolved_base_stake = round(min(resolved_base_stake, 1_000_000.0), 2)

    try:
        resolved_multiplier = float(multiplier)
    except (TypeError, ValueError):
        resolved_multiplier = DEFAULT_BET_MULTIPLIER
    if resolved_multiplier <= 1:
        resolved_multiplier = DEFAULT_BET_MULTIPLIER
    resolved_multiplier = round(min(resolved_multiplier, 20.0), 2)

    try:
        resolved_max_steps = int(max_steps)
    except (TypeError, ValueError):
        resolved_max_steps = DEFAULT_BET_MAX_STEPS
    resolved_max_steps = max(1, min(resolved_max_steps, MAX_BET_STEPS))

    normalized_refund_action = str(refund_action or 'hold').strip().lower()
    if normalized_refund_action not in REFUND_ACTION_LABELS:
        normalized_refund_action = 'hold'

    normalized_cap_action = str(cap_action or 'reset').strip().lower()
    if normalized_cap_action not in CAP_ACTION_LABELS:
        normalized_cap_action = 'reset'

    return {
        'mode': normalized_mode,
        'mode_label': BET_MODE_LABELS[normalized_mode],
        'base_stake': resolved_base_stake,
        'multiplier': resolved_multiplier,
        'max_steps': resolved_max_steps,
        'refund_action': normalized_refund_action,
        'refund_action_label': REFUND_ACTION_LABELS[normalized_refund_action],
        'cap_action': normalized_cap_action,
        'cap_action_label': CAP_ACTION_LABELS[normalized_cap_action]
    }


def build_bet_strategy_label(bet_strategy: dict) -> str:
    if bet_strategy.get('mode') == 'flat':
        return f"均注 {float(bet_strategy.get('base_stake') or DEFAULT_BASE_STAKE):.2f}U"
    return (
        f"倍投 {float(bet_strategy.get('base_stake') or DEFAULT_BASE_STAKE):.2f}U × "
        f"{float(bet_strategy.get('multiplier') or DEFAULT_BET_MULTIPLIER):.2f}"
        f" · {int(bet_strategy.get('max_steps') or DEFAULT_BET_MAX_STEPS)} 手封顶"
    )
