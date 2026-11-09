"""
PC28「主注 70% + 辅助注 30%」全仓滚三连策略模拟引擎。

策略规则（用户 spec，2026-11-02）：
- 每期总投注 = 当前可用余额的 100%，其中主注 70%、辅助注 30%
- 四种固定结构：单+小双 / 单+大双 / 双+小单 / 双+大单，主注为单双，辅助注为同侧组合
- 整期净利 > 0 记一次有效连中；< 0 清零；= 0 保持计数
- 连中 3 次当日止盈，余额 <= 0 当日止损，次日重新投入固定本金
- 止盈 / 止损后当日不再下注

口径全部显式参数化，不在代码里隐式假设：
- 赔率规则：默认 ``pc28_fullpay_netdisk``（1.99 / 3.71 / 4.32，0/27 中奖退本，组合不退）
- 日界线：``pc28``（盘日，跟随 get_pc28_day_window）/ ``natural``（北京时间自然日）/ ``sequential``（不按日历切分）
- 注额精度：``float``（全精度）/ ``cent``（分位）/ ``unit``（整数 U，只投整数部分）

结算一律走 ``services.profit_simulator`` 里已有的 pure function，避免复制一份赔率逻辑。
"""
from __future__ import annotations

import json
import math
import random
import sqlite3
import statistics
from collections import namedtuple
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence

from services.profit_simulator import (
    COMBO_GROUPS,
    PC28_PROFIT_RULES,
    build_pc28_special_flags,
    settle_pc28_metric
)
from utils.timezone import get_pc28_day_window


DEFAULT_PROFIT_RULE_ID = 'pc28_fullpay_netdisk'
DEFAULT_ODDS_PROFILE = 'regular'
DEFAULT_BANKROLL = 100.0
DEFAULT_MAIN_SHARE = 0.7
DEFAULT_TP_STREAK = 3
ALLOWED_PRECISIONS = ('float', 'cent', 'unit')
ALLOWED_DAY_BOUNDARIES = ('pc28', 'natural', 'sequential')
ALLOWED_STOP_REASONS = ('tp', 'bust', 'no_stake', 'window_end')

STRUCTURES = {
    'odd_small_even': {'label': '单+小双', 'main_value': '单', 'aux_value': '小双'},
    'odd_big_even': {'label': '单+大双', 'main_value': '单', 'aux_value': '大双'},
    'even_small_odd': {'label': '双+小单', 'main_value': '双', 'aux_value': '小单'},
    'even_big_odd': {'label': '双+大单', 'main_value': '双', 'aux_value': '大单'}
}

# 组合赔率按组给，需要用组名反查。
COMBO_GROUP_BY_VALUE = {
    value: group
    for group, config in COMBO_GROUPS.items()
    for value in config['combos']
}

_NEUTRAL_TOLERANCE = 1e-9

# 百万天模拟只保留紧凑记录，避免上百万个 dict 撑爆内存。
DayRecord = namedtuple('DayRecord', ['net', 'periods', 'reason', 'records', 'balance'])


def _day_field(day: Any, name: str) -> Any:
    if isinstance(day, dict):
        return day.get(name)
    return getattr(day, name)


def structure_options() -> List[Dict[str, str]]:
    return [
        {'key': key, 'label': config['label'], 'main_value': config['main_value'], 'aux_value': config['aux_value']}
        for key, config in STRUCTURES.items()
    ]


def normalize_precision(precision: Optional[str]) -> str:
    normalized = str(precision or 'float').strip().lower()
    if normalized not in ALLOWED_PRECISIONS:
        raise ValueError('不支持的注额精度: %s（可选 %s）' % (precision, '/'.join(ALLOWED_PRECISIONS)))
    return normalized


def normalize_day_boundary(day_boundary: Optional[str]) -> str:
    normalized = str(day_boundary or 'pc28').strip().lower()
    if normalized not in ALLOWED_DAY_BOUNDARIES:
        raise ValueError('不支持的日界线: %s（可选 %s）' % (day_boundary, '/'.join(ALLOWED_DAY_BOUNDARIES)))
    return normalized


def _metric_profile(profit_rule_id: str, metric: str, odds_profile: str) -> dict:
    rule = PC28_PROFIT_RULES.get(profit_rule_id)
    if not rule:
        raise ValueError('未知盈亏规则: %s' % profit_rule_id)
    profiles = rule.get('metrics', {}).get(metric) or {}
    profile = profiles.get(odds_profile) or profiles.get(DEFAULT_ODDS_PROFILE)
    if not profile:
        raise ValueError('规则 %s 缺少 %s/%s 赔率档' % (profit_rule_id, metric, odds_profile))
    return profile


def _actual_value(metric: str, total: int) -> str:
    odd_even = '单' if total % 2 else '双'
    if metric == 'odd_even':
        return odd_even
    return ('大' if total >= 14 else '小') + odd_even


def settle_leg(
    *,
    profit_rule_id: str,
    metric: str,
    value: str,
    triplet: Sequence[int],
    odds_profile: str = DEFAULT_ODDS_PROFILE
) -> Dict[str, Any]:
    """结算单笔注，返回 result_type / return_ratio（每 1 元本金的返还）。"""
    total = sum(triplet)
    profile = _metric_profile(profit_rule_id, metric, odds_profile)
    flags = build_pc28_special_flags(
        {
            'result_number': total,
            'source_payload': json.dumps({'number': '+'.join(str(part) for part in triplet)})
        },
        wraparound=bool(profile.get('straight_wraparound', True))
    )
    if metric == 'combo':
        group = COMBO_GROUP_BY_VALUE.get(value)
        if not group:
            raise ValueError('未知组合: %s' % value)
        odds = float((profile.get('group_odds') or {}).get(group, 0.0))
    else:
        odds = float(profile.get('odds') or 0.0)

    result_type, refund_reason, effective_odds = settle_pc28_metric(
        hit=value == _actual_value(metric, total),
        odds=odds,
        stake_amount=1.0,
        sum_value=total,
        special_flags=flags,
        profile=profile
    )
    if result_type == 'hit':
        return_ratio = float(effective_odds)
    elif result_type == 'refund':
        return_ratio = 1.0
    else:
        return_ratio = 0.0
    return {
        'result_type': result_type,
        'refund_reason': refund_reason,
        'odds': odds,
        'return_ratio': return_ratio
    }


def build_outcome_table(
    structure_key: str,
    *,
    profit_rule_id: str = DEFAULT_PROFIT_RULE_ID,
    odds_profile: str = DEFAULT_ODDS_PROFILE
) -> List[Dict[str, Any]]:
    """穷举 1000 种有序三球，逐期给出主注 / 辅助注各自的返还倍率（不含仓位权重）。"""
    config = STRUCTURES.get(structure_key)
    if not config:
        raise ValueError('未知投注结构: %s' % structure_key)

    table = []
    for code in range(1000):
        triplet = (code // 100, code // 10 % 10, code % 10)
        main_leg = settle_leg(
            profit_rule_id=profit_rule_id,
            metric='odd_even',
            value=config['main_value'],
            triplet=triplet,
            odds_profile=odds_profile
        )
        aux_leg = settle_leg(
            profit_rule_id=profit_rule_id,
            metric='combo',
            value=config['aux_value'],
            triplet=triplet,
            odds_profile=odds_profile
        )
        table.append(
            {
                'code': code,
                'triplet': triplet,
                'sum': sum(triplet),
                'main_result': main_leg['result_type'],
                'aux_result': aux_leg['result_type'],
                'main_return': main_leg['return_ratio'],
                'aux_return': aux_leg['return_ratio']
            }
        )
    return table


def period_multiplier(row: Dict[str, Any], main_share: float = DEFAULT_MAIN_SHARE) -> float:
    """以当期总投注为 1 时，这一期的总返还倍率。"""
    return main_share * row['main_return'] + (1.0 - main_share) * row['aux_return']


def split_stakes(balance: float, main_share: float, precision: str) -> tuple[float, float]:
    """按精度口径拆分主注 / 辅助注。"""
    normalized = normalize_precision(precision)
    if normalized == 'unit':
        total = float(math.floor(balance))
        if total <= 0:
            return 0.0, 0.0
        main_stake = float(round(total * main_share))
        return main_stake, total - main_stake
    if normalized == 'cent':
        main_stake = round(balance * main_share, 2)
        return main_stake, round(balance - main_stake, 2)
    main_stake = balance * main_share
    return main_stake, balance - main_stake


def _classify_multiplier(multiplier: float) -> str:
    if multiplier <= _NEUTRAL_TOLERANCE:
        return 'bust'
    if multiplier > 1.0 + _NEUTRAL_TOLERANCE:
        return 'win'
    if multiplier < 1.0 - _NEUTRAL_TOLERANCE:
        return 'lose'
    return 'neutral'


def exact_day_stats(
    table: Sequence[Dict[str, Any]],
    *,
    bankroll: float = DEFAULT_BANKROLL,
    main_share: float = DEFAULT_MAIN_SHARE,
    tp_streak: int = DEFAULT_TP_STREAK
) -> Dict[str, Any]:
    """1000 组穷举下的解析解：单期 EV、日止盈率、日均净利、日均期数。

    连中计数只在整期净利 > 0 时 +1、< 0 时清零、= 0 时保持不变，
    因此把「净利为 0」的期视为自环：概率归一化后按有效步数求解，期数再按 1/(1-N) 放大。
    整期倍率为 0（两笔全输）是吸收态：当天直接结束，不计入转移概率，也不贡献期数与收益。
    """
    multipliers = [period_multiplier(row, main_share) for row in table]
    total = len(multipliers)
    if total == 0:
        raise ValueError('结果表为空')

    win_values = [m for m in multipliers if _classify_multiplier(m) == 'win']
    lose_values = [m for m in multipliers if _classify_multiplier(m) == 'lose']
    bust_count = sum(1 for m in multipliers if _classify_multiplier(m) == 'bust')
    neutral_count = total - len(win_values) - len(lose_values) - bust_count
    win_prob = len(win_values) / total
    lose_prob = len(lose_values) / total
    neutral_prob = neutral_count / total

    if neutral_prob >= 1.0:
        raise ValueError('全部期次净利为 0，无法求解')

    # 去掉自环后的有效转移概率
    win_effective = win_prob / (1.0 - neutral_prob)
    lose_effective = lose_prob / (1.0 - neutral_prob)
    win_mean = statistics.fmean(win_values) if win_values else 0.0
    lose_mean = statistics.fmean(lose_values) if lose_values else 0.0

    # 期望乘积递推用的是无条件贡献：赢 a = W·E[m|win]，小亏 b = L·E[m|lose]；爆仓贡献 0。
    advance = win_effective * win_mean
    recoup = lose_effective * lose_mean

    probability_span = sum(win_effective ** index for index in range(tp_streak))
    value_span = sum(advance ** index for index in range(tp_streak))
    stop_denominator = 1.0 - lose_effective * probability_span
    value_denominator = 1.0 - recoup * value_span
    if stop_denominator <= 0 or value_denominator <= 0:
        raise ValueError('止盈概率或期望收益不收敛')

    tp_probability = win_effective ** tp_streak / stop_denominator
    expected_periods = probability_span / stop_denominator / (1.0 - neutral_prob)
    expected_multiplier = advance ** tp_streak / value_denominator

    return {
        'period_ev_pct': (statistics.fmean(multipliers) - 1.0) * 100.0,
        'win_probability': win_prob,
        'lose_probability': lose_prob,
        'neutral_probability': neutral_prob,
        'bust_probability': bust_count / total,
        'win_mean_multiplier': win_mean,
        'lose_mean_multiplier': lose_mean,
        'daily_tp_probability': tp_probability,
        'daily_bust_probability': 1.0 - tp_probability,
        'daily_expected_net': bankroll * (expected_multiplier - 1.0),
        'daily_expected_periods': expected_periods,
        'daily_expected_multiplier': expected_multiplier
    }


def simulate_days(
    table: Sequence[Dict[str, Any]],
    days: int,
    *,
    bankroll: float = DEFAULT_BANKROLL,
    main_share: float = DEFAULT_MAIN_SHARE,
    tp_streak: int = DEFAULT_TP_STREAK,
    precision: str = 'float',
    seed: Optional[Any] = None,
    keep_records: int = 0
) -> List[DayRecord]:
    """按 1000 组等概率独立抽样模拟若干天。

    返回紧凑的 ``DayRecord`` 列表；只有前 ``keep_records`` 天保留逐期明细。
    """
    if days <= 0:
        raise ValueError('天数必须为正')
    if tp_streak < 1:
        raise ValueError('连中止盈次数必须 >= 1')

    rng = random.Random(seed)
    table_size = len(table)
    days_out: List[DayRecord] = []
    for _ in range(days):
        day_index = len(days_out)
        balance = bankroll
        streak = 0
        periods = 0
        records = [] if day_index < keep_records else None
        reason = 'no_stake'

        while True:
            row = table[rng.randrange(table_size)]
            main_stake, aux_stake = split_stakes(balance, main_share, precision)
            total_stake = main_stake + aux_stake
            if total_stake <= 0:
                reason = 'no_stake'
                break

            payout = main_stake * row['main_return'] + aux_stake * row['aux_return']
            new_balance = balance - total_stake + payout
            periods += 1
            if new_balance > balance:
                streak += 1
            elif new_balance < balance:
                streak = 0

            if records is not None:
                records.append(
                    {
                        'period': periods,
                        'balance_before': balance,
                        'main_stake': main_stake,
                        'aux_stake': aux_stake,
                        'triplet': '%d+%d+%d' % row['triplet'],
                        'sum': row['sum'],
                        'main_result': row['main_result'],
                        'aux_result': row['aux_result'],
                        'payout': payout,
                        'period_net': new_balance - balance,
                        'streak': streak
                    }
                )

            balance = new_balance
            if balance <= 0:
                reason = 'bust'
                break
            if streak >= tp_streak:
                reason = 'tp'
                break

        days_out.append(
            DayRecord(net=balance - bankroll, periods=periods, reason=reason,
                      records=records if records else None, balance=balance)
        )

    return days_out


def _day_key(moment: datetime, day_boundary: str) -> Any:
    if day_boundary == 'natural':
        return moment.date()
    return get_pc28_day_window(moment)['start']


def replay_real_draws(
    table: Sequence[Dict[str, Any]],
    draws: Sequence[Dict[str, Any]],
    *,
    bankroll: float = DEFAULT_BANKROLL,
    main_share: float = DEFAULT_MAIN_SHARE,
    tp_streak: int = DEFAULT_TP_STREAK,
    precision: str = 'float',
    day_boundary: str = 'pc28',
    keep_records: int = 0
) -> Dict[str, Any]:
    """按真实开奖顺序回放。

    - ``sequential``：不按日历切分，一天结束立刻开下一天（样本量大，用于统计长期期望）
    - ``pc28`` / ``natural``：按盘日 / 自然日窗口切分，窗口结束仍未止盈止损则记为 window_end
    """
    boundary = normalize_day_boundary(day_boundary)
    if not draws:
        return {'days': [], 'draws_consumed': 0, 'windows': 0, 'windows_with_draws': 0, 'incomplete_days': 0}

    multiplier_by_code = {
        row['code']: main_share * row['main_return'] + (1.0 - main_share) * row['aux_return']
        for row in table
    }
    row_by_code = {row['code']: row for row in table}

    days_out: List[Dict[str, Any]] = []
    consumed = 0
    windows = 0
    windows_with_draws = 0
    incomplete = 0

    def play_window(window_draws: Sequence[Dict[str, Any]], *, day_index: int, window_key: Any) -> Dict[str, Any]:
        balance = bankroll
        streak = 0
        periods = 0
        records = [] if day_index < keep_records else None
        reason = 'window_end'
        for draw in window_draws:
            row = row_by_code[draw['code']]
            main_stake, aux_stake = split_stakes(balance, main_share, precision)
            total_stake = main_stake + aux_stake
            if total_stake <= 0:
                reason = 'no_stake'
                break
            payout = main_stake * row['main_return'] + aux_stake * row['aux_return']
            new_balance = balance - total_stake + payout
            periods += 1
            if new_balance > balance:
                streak += 1
            elif new_balance < balance:
                streak = 0
            if records is not None:
                records.append(
                    {
                        'period': periods,
                        'issue_no': draw.get('issue_no'),
                        'open_time': draw.get('open_time_text'),
                        'balance_before': balance,
                        'main_stake': main_stake,
                        'aux_stake': aux_stake,
                        'triplet': draw.get('triplet_text'),
                        'sum': draw['sum'],
                        'main_result': row['main_result'],
                        'aux_result': row['aux_result'],
                        'payout': payout,
                        'period_net': new_balance - balance,
                        'streak': streak
                    }
                )
            balance = new_balance
            if balance <= 0:
                reason = 'bust'
                break
            if streak >= tp_streak:
                reason = 'tp'
                break
        return {
            'day': day_index,
            'window': str(window_key),
            'net': balance - bankroll,
            'balance': balance,
            'periods': periods,
            'reason': reason,
            'records': records
        }

    if boundary == 'sequential':
        while consumed < len(draws):
            day = play_window(draws[consumed:consumed + 4096], day_index=len(days_out), window_key=None)
            days_out.append(day)
            consumed += day['periods']
            if day['periods'] == 0:
                break
    else:
        grouped: Dict[Any, List[Dict[str, Any]]] = {}
        for draw in draws:
            grouped.setdefault(_day_key(draw['moment'], boundary), []).append(draw)
        windows = len(grouped)
        for window_key, window_draws in grouped.items():
            windows_with_draws += 1
            day = play_window(window_draws, day_index=len(days_out), window_key=window_key)
            if day['reason'] == 'window_end':
                incomplete += 1
            days_out.append(day)
            consumed += day['periods']

    return {
        'days': days_out,
        'draws_consumed': consumed,
        'windows': windows,
        'windows_with_draws': windows_with_draws,
        'incomplete_days': incomplete
    }


def sample_day_nets(days: Sequence[Any], *, max_points: int = 4000) -> List[float]:
    """按固定步长抽样日净利，供资金曲线与分布图使用。"""
    step = max(1, len(days) // max_points)
    return [_day_field(day, 'net') for day in days[::step]]


def summarize_days(
    days: Sequence[Dict[str, Any]],
    *,
    blocks: Sequence[int] = (30, 100, 365)
) -> Dict[str, Any]:
    """日度汇总：止盈率 / 止损率 / 净利分布 / 回撤 / 分段累计收益。"""
    if not days:
        return {}

    nets = [_day_field(day, 'net') for day in days]
    tp_days = [_day_field(day, 'net') for day in days if _day_field(day, 'reason') == 'tp']
    bust_days = [_day_field(day, 'net') for day in days if _day_field(day, 'reason') == 'bust']
    incomplete_days = [_day_field(day, 'net') for day in days
                       if _day_field(day, 'reason') in ('window_end', 'no_stake')]

    cumulative = peak = drawdown = 0.0
    losing_run = longest_losing_run = 0
    for net in nets:
        cumulative += net
        peak = max(peak, cumulative)
        drawdown = max(drawdown, peak - cumulative)
        losing_run = losing_run + 1 if net < 0 else 0
        longest_losing_run = max(longest_losing_run, losing_run)

    summary = {
        'days': len(nets),
        'tp_rate': len(tp_days) / len(nets),
        'bust_rate': len(bust_days) / len(nets),
        'incomplete_rate': len(incomplete_days) / len(nets),
        'daily_net_mean': statistics.fmean(nets),
        'daily_net_median': statistics.median(nets),
        'daily_net_stdev': statistics.pstdev(nets) if len(nets) > 1 else 0.0,
        'daily_periods_mean': statistics.fmean(_day_field(day, 'periods') for day in days),
        'daily_periods_max': max(_day_field(day, 'periods') for day in days),
        'tp_day_net_mean': statistics.fmean(tp_days) if tp_days else 0.0,
        'tp_day_net_max': max(tp_days) if tp_days else 0.0,
        'tp_day_net_negative_count': sum(1 for net in tp_days if net <= 0),
        'bust_day_net_values': sorted({round(net, 6) for net in bust_days}),
        'cumulative_net': cumulative,
        'max_drawdown': drawdown,
        'longest_losing_day_run': longest_losing_run
    }
    for size in blocks:
        if len(nets) < size:
            continue
        windows = [sum(nets[index:index + size]) for index in range(0, len(nets) - size + 1, size)]
        if len(windows) < 20:
            continue
        quantiles = statistics.quantiles(windows, n=20)
        summary['cumulative_%d_days' % size] = {
            'windows': len(windows),
            'mean': statistics.fmean(windows),
            'p5': quantiles[0],
            'median': quantiles[9],
            'p95': quantiles[18],
            'positive_rate': sum(1 for value in windows if value > 0) / len(windows)
        }
    return summary


def outcome_groups(table: Sequence[Dict[str, Any]], main_share: float = DEFAULT_MAIN_SHARE) -> List[Dict[str, Any]]:
    """把 1000 组按「整期倍率 + 两注结果」归类，便于人工核对结算口径。"""
    grouped: Dict[tuple, Dict[str, Any]] = {}
    for row in table:
        multiplier = round(period_multiplier(row, main_share), 6)
        key = (multiplier, row['main_result'], row['aux_result'])
        entry = grouped.setdefault(key, {'multiplier': multiplier, 'main_result': row['main_result'],
                                         'aux_result': row['aux_result'], 'count': 0, 'sums': set()})
        entry['count'] += 1
        entry['sums'].add(row['sum'])
    return [
        {**entry, 'sums': sorted(entry['sums'])}
        for entry in sorted(grouped.values(), key=lambda item: item['multiplier'], reverse=True)
    ]


def load_real_draws(
    db_path: str,
    *,
    limit: Optional[int] = None,
    since: Optional[str] = None
) -> Dict[str, Any]:
    """读取真实 PC28 开奖，兼容标准 payload 与 BCLC 兜底 payload 两种形态。"""
    query = (
        "SELECT issue_no, open_time, result_number, source_payload "
        "FROM lottery_draws WHERE lottery_type='pc28'"
    )
    params: List[Any] = []
    if since:
        query += ' AND open_time >= ?'
        params.append(since)
    query += ' ORDER BY CAST(issue_no AS INTEGER)'
    if limit:
        query += ' LIMIT ?'
        params.append(int(limit))

    connection = sqlite3.connect('file:%s?mode=ro' % db_path, uri=True, timeout=10)
    try:
        rows = connection.execute(query, params).fetchall()
    finally:
        connection.close()

    draws: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []
    for issue_no, open_time, result_number, payload in rows:
        parsed = parse_draw_payload(payload)
        if not parsed:
            skipped.append({'issue_no': issue_no, 'reason': 'payload_missing_triplet'})
            continue
        triplet, triplet_text = parsed
        total = sum(triplet)
        if result_number is not None and int(result_number) != total:
            skipped.append({'issue_no': issue_no, 'reason': 'sum_mismatch'})
            continue
        try:
            moment = datetime.strptime(open_time, '%Y-%m-%d %H:%M:%S')
        except (TypeError, ValueError):
            skipped.append({'issue_no': issue_no, 'reason': 'bad_open_time'})
            continue
        draws.append(
            {
                'issue_no': issue_no,
                'moment': moment,
                'open_time_text': open_time,
                'sum': total,
                'triplet_text': triplet_text,
                'code': triplet[0] * 100 + triplet[1] * 10 + triplet[2]
            }
        )
    return {'draws': draws, 'skipped': skipped, 'raw_count': len(rows)}


def parse_draw_payload(payload: Optional[str]) -> Optional[tuple[tuple[int, int, int], str]]:
    try:
        data = json.loads(payload or '{}')
    except (TypeError, ValueError):
        return None
    if not isinstance(data, dict):
        return None
    if data.get('number'):
        text = str(data['number'])
        parts = text.split('+')
        if len(parts) == 3 and all(part.strip().isdigit() for part in parts):
            triplet = tuple(int(part) for part in parts)
            if all(0 <= value <= 9 for value in triplet):
                return triplet, text
        return None
    parts = [data.get('canada28_num%d' % index) for index in (1, 2, 3)]
    if all(isinstance(value, int) and 0 <= value <= 9 for value in parts):
        triplet = tuple(parts)
        return triplet, '%d+%d+%d' % triplet
    return None


def evaluate_structure(
    structure_key: str,
    *,
    profit_rule_id: str = DEFAULT_PROFIT_RULE_ID,
    odds_profile: str = DEFAULT_ODDS_PROFILE,
    bankroll: float = DEFAULT_BANKROLL,
    main_share: float = DEFAULT_MAIN_SHARE,
    tp_streak: int = DEFAULT_TP_STREAK,
    precision: str = 'float',
    monte_days: int = 0,
    seed: Optional[Any] = None,
    real_draws: Optional[Sequence[Dict[str, Any]]] = None,
    day_boundary: str = 'pc28',
    keep_records: int = 0
) -> Dict[str, Any]:
    table = build_outcome_table(structure_key, profit_rule_id=profit_rule_id, odds_profile=odds_profile)
    result: Dict[str, Any] = {
        'structure': structure_key,
        'label': STRUCTURES[structure_key]['label'],
        'main_value': STRUCTURES[structure_key]['main_value'],
        'aux_value': STRUCTURES[structure_key]['aux_value'],
        'profit_rule_id': profit_rule_id,
        'odds_profile': odds_profile,
        'bankroll': bankroll,
        'main_share': main_share,
        'tp_streak': tp_streak,
        'precision': precision,
        'outcome_groups': outcome_groups(table, main_share),
        'exact': exact_day_stats(table, bankroll=bankroll, main_share=main_share, tp_streak=tp_streak)
    }

    if monte_days:
        simulated = simulate_days(
            table,
            monte_days,
            bankroll=bankroll,
            main_share=main_share,
            tp_streak=tp_streak,
            precision=precision,
            seed=seed,
            keep_records=keep_records
        )
        result['monte_carlo'] = summarize_days(simulated)
        # 只保留画图需要的抽样点与少量逐期明细，丢掉百万级日记录。
        result['_sampled_nets'] = sample_day_nets(simulated, max_points=4000)
        result['_record_days'] = [day for day in simulated[:keep_records] if day.records]
        del simulated
    else:
        result['monte_carlo'] = None

    if real_draws is not None:
        replay = replay_real_draws(
            table,
            real_draws,
            bankroll=bankroll,
            main_share=main_share,
            tp_streak=tp_streak,
            precision=precision,
            day_boundary=day_boundary,
            keep_records=keep_records
        )
        result['real_replay'] = {
            'day_boundary': day_boundary,
            'draws_consumed': replay['draws_consumed'],
            'windows': replay['windows'],
            'incomplete_days': replay['incomplete_days'],
            'summary': summarize_days(replay['days'])
        }
        result['_replay_days'] = replay['days']

        # 日历回放的窗口数往往只有几十个，统计意义有限；连续回放（不按日历切分）
        # 才是真实数据上回答「长期期望」的口径，因此总是附带一份。
        if day_boundary == 'sequential':
            result['real_replay_sequential'] = result['real_replay']
        else:
            sequential = replay_real_draws(
                table,
                real_draws,
                bankroll=bankroll,
                main_share=main_share,
                tp_streak=tp_streak,
                precision=precision,
                day_boundary='sequential',
                keep_records=0
            )
            result['real_replay_sequential'] = {
                'day_boundary': 'sequential',
                'draws_consumed': sequential['draws_consumed'],
                'summary': summarize_days(sequential['days'])
            }
    else:
        result['real_replay'] = None
        result['real_replay_sequential'] = None

    return result


def public_result(result: Dict[str, Any]) -> Dict[str, Any]:
    """去掉体积大的内部字段，便于序列化。"""
    return {key: value for key, value in result.items() if not key.startswith('_')}
