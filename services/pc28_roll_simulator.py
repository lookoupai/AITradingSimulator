"""PC28 过关的无副作用历史回放；与投注端的整数拆注、资金及关数口径一致。"""
from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone

from utils.timezone import BEIJING_FIXED_TZ, get_pc28_day_window, parse_beijing_time
from services.pc28_play_selection import normalize_selection, selected_bets, VALUES


ROLL_MODE = 'win_streak_roll'
STRUCTURES = {
    'A': ('单', '小双'), 'B': ('单', '大双'),
    'C': ('双', '小单'), 'D': ('双', '大单'),
}
COMBO_STRUCTURES = {'小单': 'A', '大单': 'B', '小双': 'C', '大双': 'D'}
ROLL_DEFAULTS = {
    'structure': 'follow_signal', 'roll_mode': 'chain', 'base_amount': 100.0,
    'main_share': 0.7, 'target_streak': 3, 'on_target': 'close_day',
    'on_bust': 'close_day', 'max_cycles_per_day': 1,
    'day_boundary': 'natural', 'days': 1,
    'play_type': 'big_small', 'selection_mode': 'follow', 'fixed_value': None,
}
REPLAY_NOTE = '按每期当前保存的预测回放，并排除已知晚于开奖的预测；历史覆盖版本无法完整还原。'


def normalize_roll_config(value=None):
    if value is not None and not isinstance(value, dict):
        raise ValueError('过关配置必须为对象')
    incoming = value or {}
    if set(incoming) - set(ROLL_DEFAULTS):
        raise ValueError('存在不支持的过关参数')
    config = {**ROLL_DEFAULTS, **incoming}
    enums = {
        'structure': ('single_play', 'follow_signal', *STRUCTURES), 'roll_mode': ('chain', 'all_in'),
        'on_target': ('close_day', 'new_cycle'), 'on_bust': ('close_day', 'new_cycle'),
        'day_boundary': ('natural', 'pc28'),
    }
    for key, allowed in enums.items():
        if config[key] not in allowed:
            raise ValueError(f'过关参数 {key} 无效')
    single = config['structure'] == 'single_play'
    if single:
        config.update(normalize_selection(config['play_type'], config))
    units = {'double_group':2,'kill_group':3}.get(config['play_type'],1) if single else 2
    bounds = {'base_amount': (units, 1_000_000), 'main_share': (0.01, 0.99),
              'target_streak': (1, 100), 'max_cycles_per_day': (1, 1000), 'days': (1, 30)}
    for key, (lower, upper) in bounds.items():
        try:
            number = float(config[key])
        except (ValueError, TypeError, OverflowError):
            raise ValueError(f'过关参数 {key} 必须为有效数字') from None
        if isinstance(config[key], bool) or not math.isfinite(number) or not lower <= number <= upper:
            raise ValueError(f'过关参数 {key} 必须在 {lower} 到 {upper} 之间')
        if key in ('target_streak', 'max_cycles_per_day', 'days'):
            if not number.is_integer():
                raise ValueError(f'过关参数 {key} 必须为整数')
            config[key] = int(number)
        else:
            config[key] = round(number, 4 if key == 'main_share' else 2)
    if config['days'] not in (1, 7, 30):
        raise ValueError('过关区间仅支持 1、7、30 日')
    if not single and split_roll_stakes(config['base_amount'], config['main_share'])[0] < 1:
        raise ValueError('起始金额按当前比例无法拆成两笔整数注')
    return config


def split_roll_stakes(amount, share):
    total = int(round(amount, 2) + 1e-9)
    main = int(total * share + 1e-9)
    if total < 2 or main < 1:
        return 0, 0
    if total - main < 1:
        main = total - 1
    return main, total - main


def roll_day_start(moment, boundary):
    if boundary == 'pc28':
        return get_pc28_day_window(moment)['start']
    return moment.replace(hour=0, minute=0, second=0, microsecond=0)


def roll_period(config, now):
    start = roll_day_start(now, config['day_boundary'])
    # Walk actual boundaries rather than subtracting 24h across DST changes.
    for _ in range(config['days'] - 1):
        start = roll_day_start(start - timedelta(seconds=1), config['day_boundary'])
    return start, now


def _prediction_time(prediction):
    """created_at 为 SQLite UTC；外部信号也必须在开奖前已抓取到。"""
    values = [prediction.get('created_at')]
    if prediction.get('external_fetched_at'):
        values.append(prediction['external_fetched_at'])
    moments = []
    for value in values:
        try:
            moment = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
            if moment.tzinfo is None:
                moment = moment.replace(tzinfo=timezone.utc)
            moments.append(moment.astimezone(BEIJING_FIXED_TZ).replace(tzinfo=None))
        except (ValueError, TypeError, OverflowError):
            return None
    return max(moments)


def replay_roll(predictions, draws, config, *, start, end, settle_leg, include_records=True):
    """settle_leg(prediction, draw, metric, value, stake) 返回单笔资金结算。"""
    records = []
    summary = dict.fromkeys((
        'bet_count', 'hit_count', 'refund_count', 'miss_count', 'skipped_count',
        'cycle_count', 'target_cycle_count', 'bust_cycle_count', 'days_with_bets',
        'total_stake', 'total_payout', 'net_profit', 'total_funding', 'total_withdrawn',
        'ending_balance', 'max_drawdown',
    ), 0)
    skips = {}
    balance = cycle_stake = 0.0
    current_day = None
    day_cycles = streak = 0
    stopped = False
    stop_reason = None
    peak_profit = 0.0
    seen = set()

    def skip(reason):
        skips[reason] = skips.get(reason, 0) + 1
        summary['skipped_count'] += 1

    ordered = sorted(predictions, key=lambda item: str(item.get('issue_no', '')).zfill(24))
    for prediction in ordered:
        issue = str(prediction.get('issue_no') or '')
        if prediction.get('status') != 'settled':
            continue
        draw = draws.get(issue)
        moment = parse_beijing_time((draw or {}).get('open_time'))
        if not moment:
            skip('missing_draw')
            continue
        if not start <= moment <= end:
            continue
        if issue in seen:
            skip('duplicate_issue')
            continue
        seen.add(issue)
        available_at = _prediction_time(prediction)
        if available_at is None or available_at >= moment:
            skip('unknown_prediction_time' if available_at is None else 'late_prediction')
            continue
        structure = config['structure']
        single = structure == 'single_play'
        bets = selected_bets(prediction, config['play_type'], config) if single else None
        if single and not bets:
            skip('missing_or_filtered_prediction')
            continue
        if structure == 'follow_signal':
            structure = COMBO_STRUCTURES.get(prediction.get('prediction_combo'))
        if not structure:
            skip('missing_combo')
            continue
        day = roll_day_start(moment, config['day_boundary']).strftime('%Y-%m-%d')
        new_day = day != current_day
        if not new_day and stopped:
            action = config['on_target' if stop_reason == 'target' else 'on_bust']
            if action == 'close_day' or day_cycles >= config['max_cycles_per_day']:
                skip('day_closed')
                continue
        starting = new_day or stopped
        available = config['base_amount'] if starting else (balance if config['roll_mode'] == 'all_in' else cycle_stake)
        if available > 1_000_000_000:
            raise ValueError('模拟滚动注额超过 10 亿，请减少关数或缩短区间')
        if single:
            per_unit = int(round(available,2) / len(bets) + 1e-9)
            stakes = [per_unit] * len(bets)
        else:
            bets = [('odd_even',STRUCTURES[structure][0]), ('combo',STRUCTURES[structure][1])]
            stakes = list(split_roll_stakes(available, config['main_share']))
        main_stake = stakes[0]
        if not main_stake:
            # Integer/share constraints can make a small positive payout unplaceable.
            stopped, stop_reason = True, 'bust'
            summary['bust_cycle_count'] += 1
            skip('insufficient_stake')
            continue
        legs = [settle_leg(prediction, draw, metric, value, amount) for (metric,value),amount in zip(bets,stakes)]
        if any(leg is None for leg in legs):
            skip('invalid_draw')
            continue

        funding = withdrawn = 0.0
        if starting:
            if new_day:
                current_day, day_cycles = day, 0
                summary['days_with_bets'] += 1
            if config['roll_mode'] == 'all_in':
                withdrawn, funding = balance, config['base_amount']
                balance = config['base_amount']
            else:
                funding = round(max(0, config['base_amount'] - balance), 2)
                balance = round(balance + funding, 2)
            cycle_stake = config['base_amount']
            summary['total_funding'] = round(summary['total_funding'] + funding, 2)
            summary['total_withdrawn'] = round(summary['total_withdrawn'] + withdrawn, 2)
            summary['cycle_count'] += 1
            day_cycles += 1
            streak, stopped, stop_reason = 0, False, None
        before = balance
        stake = sum(stakes)
        payout = round(sum(leg['payout_amount'] for leg in legs), 2)
        net = round(payout - stake, 2)
        balance = round(balance + net, 2)
        cycle_stake = payout
        next_stake = balance if config['roll_mode'] == 'all_in' else cycle_stake
        streak = streak + 1 if net > 0 else 0 if net < 0 else streak
        if streak >= config['target_streak']:
            stopped, stop_reason = True, 'target'
            summary['target_cycle_count'] += 1
        elif (single and int(round(next_stake,2) / len(bets) + 1e-9) < 1) or (not single and not split_roll_stakes(next_stake, config['main_share'])[0]):
            stopped, stop_reason = True, 'bust'
            summary['bust_cycle_count'] += 1
        result = 'hit' if net > 0 else 'miss' if net < 0 else 'refund'
        summary[result + '_count'] += 1
        summary['bet_count'] += 1
        for key, amount in (('total_stake', stake), ('total_payout', payout), ('net_profit', net)):
            summary[key] = round(summary[key] + amount, 2)
        peak_profit = max(peak_profit, summary['net_profit'])
        summary['max_drawdown'] = round(max(summary['max_drawdown'], peak_profit-summary['net_profit']), 2)
        if include_records:
            refunds = list(dict.fromkeys(leg['refund_reason'] for leg in legs if leg.get('refund_reason')))
            records.append({
                'issue_no': issue, 'open_time': moment.strftime('%Y-%m-%d %H:%M:%S'),
                'metric': 'roll', 'metric_label': '过关', 'structure': structure,
                'ticket_label': ' ＋ '.join(f'{value} {amount}U' for (_,value),amount in zip(bets,stakes)),
                'predicted_value': prediction.get('prediction_'+(config['play_type'] if single else 'combo')) or '固定／派生投注',
                'actual_value': draw.get('combo') or '--', 'odds': None, 'legs': legs,
                **({'prediction_hit': draw.get('combo') in [value for _,value in bets], 'per_combo_amount': stakes[0]} if single and config['play_type'] in ('double_group','kill_group') else {}),
                'result_type': result, 'result_label': '盈利' if net > 0 else '亏损' if net < 0 else '持平',
                'refund_reason': '；'.join(refunds) or None, 'stake_amount': stake,
                'payout_amount': payout, 'net_profit': net, 'cumulative_profit': summary['net_profit'],
                'balance_before': before, 'balance_after': balance, 'next_stake': next_stake,
                'win_streak': streak, 'target_streak': config['target_streak'],
                'cycle_number': summary['cycle_count'], 'day_cycle_number': day_cycles, 'stat_date': day,
                'bet_step': streak, 'bet_step_label': f'第 {day_cycles} 轮 · {streak}/{config["target_streak"]} 关',
                'stop_reason': stop_reason, 'funding_amount': funding, 'withdrawn_amount': withdrawn,
            })
    summary['ending_balance'] = balance
    summary['incomplete_cycle_count'] = summary['cycle_count'] - summary['target_cycle_count'] - summary['bust_cycle_count']
    summary['roi_percentage'] = round(summary['net_profit'] / summary['total_stake'] * 100, 2) if summary['total_stake'] else 0
    summary['average_profit'] = round(summary['net_profit'] / summary['bet_count'], 2) if summary['bet_count'] else 0
    return {'summary': summary, 'records': records, 'skip_reasons': skips}
