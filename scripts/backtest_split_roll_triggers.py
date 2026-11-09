#!/usr/bin/env python3
"""
满赔2.8 + 主注70%/辅助注30% 全仓滚三关（每日 100U）—— 两个自动启动条件的真实数据回放。

启动条件：
  A. 定时：每天北京时间 08:01 后的第一期开滚（对齐规则 4 的 primary 窗口）。
  B. 连挂：跟单来源（算法）的「单双」信号当天第一次连续 5 期未中后，下一期开滚。
     未中序列连续跟踪（跨日不断）；13/14 等回本不计未中；每天只启动一次。

滚仓逻辑（用户 spec）：每日 100U；每期全仓 70/30；净利>0 计 1 关、<0 清零、=0 保持；
连中 3 关止盈；余额<=0 止损；触发后当日内不再启动第二次；结构由算法组合预测逐期指定：
主注 = C 的单双，辅助注 = C 翻转单双，放弃 = 对角组。

玩法：彩28满赔2.8（2.84/6.79/6.33 含本金；中奖遇 13/14+对子+顺子+豹子退本）。
"""
from __future__ import annotations

import csv
import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.pc28_split_roll import (  # noqa: E402
    DayRecord,
    STRUCTURES,
    build_outcome_table,
    load_real_draws,
    settle_leg,
    summarize_days
)

PREDICTOR_DB = str(Path(__file__).resolve().parents[1] / 'pc28_predictor.db')
RULE_ID = 'pc28_fullpay_2_8'
OUT_DIR = str(Path(__file__).resolve().parents[1] / 'reports' / 'pc28_split_roll_triggers_fullpay_2_8')
ALGORITHMS = {7: '频次趋势 V1', 8: '组合马尔可夫 V1', 9: '遗漏回补 V1'}
STRUCTURE_BY_COMBO = {
    '大单': 'odd_big_even', '小单': 'odd_small_even',
    '大双': 'even_big_odd', '小双': 'even_small_odd',
}
TP_STREAK = 3
BANKROLL = 100.0
MAIN_SHARE = 0.7
STREAK_TRIGGER = 5
ACCOUNT_INIT = 1000.0   # 会话账户（雨的截图：初始 1001.31，跨日结转）
MIN_ACCOUNT = 100.0     # 账户不足 100U 时无法开启新一天的 100U 计划 → 计划死亡


def _min_account(days: list) -> float:
    account = ACCOUNT_INIT
    lowest = ACCOUNT_INIT
    for day in days:
        account += day.net
        lowest = min(lowest, account)
    return lowest


def apply_account(days: list) -> tuple:
    """把每日盈亏结算到会话账户上；账户 < 100U 时计划死亡，截断后续。"""
    account = ACCOUNT_INIT
    alive = []
    ruin_day = None
    for index, day in enumerate(days):
        account += day.net
        alive.append(day)
        if account < MIN_ACCOUNT:
            ruin_day = index + 1
            break
    return alive, account, ruin_day


def load_predictions(db_path: str) -> dict:
    """{算法id: {期号: {'combo': C, 'odd_even': 单/双}}}，取开奖前最后一次。"""
    import sqlite3
    conn = sqlite3.connect('file:%s?mode=ro' % db_path, uri=True, timeout=30)
    latest = {}
    for predictor_id, issue_no, combo, odd_even, created_at in conn.execute(
            "SELECT predictor_id, issue_no, prediction_combo, prediction_odd_even, created_at "
            "FROM predictions WHERE lottery_type='pc28' AND prediction_combo IS NOT NULL "
            "AND predictor_id IN (7,8,9)"):
        combo = str(combo or '').strip()
        if combo not in STRUCTURE_BY_COMBO:
            continue
        odd = str(odd_even or '').strip()
        if odd not in ('单', '双'):
            odd = '单' if combo in ('大单', '小单') else '双'
        key = (predictor_id, str(issue_no))
        if key not in latest or created_at > latest[key][0]:
            latest[key] = (created_at, combo, odd)
    conn.close()
    result = {pid: {} for pid in ALGORITHMS}
    for (predictor_id, issue_no), (_t, combo, odd) in latest.items():
        result[predictor_id][issue_no] = {'combo': combo, 'odd_even': odd}
    return result


def cycle_from(start_index: int, draws: list, tables: dict, preds: dict) -> DayRecord:
    """从 start_index 起滚一轮：3 关止盈 / 归零止损，结构由当期组合预测指定。"""
    balance = BANKROLL
    streak = 0
    periods = 0
    records = []
    reason = 'window_end'
    for j in range(start_index, len(draws)):
        draw = draws[j]
        pred = preds.get(str(draw['issue_no']))
        if pred is None:
            continue
        row = tables[STRUCTURE_BY_COMBO[pred['combo']]][draw['code']]
        main_stake = balance * MAIN_SHARE
        aux_stake = balance - main_stake
        payout = main_stake * row['main_return'] + aux_stake * row['aux_return']
        new_balance = balance - main_stake - aux_stake + payout
        periods += 1
        if new_balance > balance:
            streak += 1
        elif new_balance < balance:
            streak = 0
        records.append({
            'period': periods, 'issue_no': draw['issue_no'], 'open_time': draw['open_time_text'],
            'structure': STRUCTURES[STRUCTURE_BY_COMBO[pred['combo']]]['label'],
            'balance_before': balance, 'main_stake': main_stake, 'aux_stake': aux_stake,
            'triplet': draw['triplet_text'], 'sum': draw['sum'],
            'main_result': row['main_result'], 'aux_result': row['aux_result'],
            'payout': payout, 'period_net': new_balance - balance, 'streak': streak,
        })
        balance = new_balance
        if balance <= 0:
            reason = 'bust'
            break
        if streak >= TP_STREAK:
            reason = 'tp'
            break
    return DayRecord(net=balance - BANKROLL, periods=periods, reason=reason,
                     records=records if records else None, balance=balance)


def run_schedule(draws: list, tables: dict, preds: dict) -> list:
    """条件 A：每天 08:01 后第一期开滚。"""
    days = []
    used_until = -1
    by_day = defaultdict(list)
    for index, draw in enumerate(draws):
        by_day[draw['moment'].date()].append(index)
    for day in sorted(by_day):
        start = next((i for i in by_day[day]
                      if draws[i]['moment'] >= datetime.combine(day, datetime.min.time()) + timedelta(hours=8, minutes=1)
                      and str(draws[i]['issue_no']) in preds), None)
        if start is None or start <= used_until:
            continue
        record = cycle_from(start, draws, tables, preds)
        used_until = start + record.periods - 1
        days.append(record)
    return days


def run_miss_streak(draws: list, tables: dict, preds: dict, odd_outcomes: dict) -> list:
    """条件 B：算法单双连挂 5 期（回本不计），每天第一次触发后下一期开滚。"""
    days = []
    streak = 0
    triggered_days = set()
    active_until = -1
    for index, draw in enumerate(draws):
        outcome = odd_outcomes.get(str(draw['issue_no']))
        if outcome == 'hit':
            streak = 0
        elif outcome == 'miss':
            streak += 1
        if index <= active_until or streak < STREAK_TRIGGER:
            continue
        day = draw['moment'].date()
        if day in triggered_days:
            continue
        start = next((j for j in range(index + 1, len(draws))
                      if str(draws[j]['issue_no']) in preds), None)
        if start is None:
            continue
        triggered_days.add(day)
        record = cycle_from(start, draws, tables, preds)
        active_until = start + record.periods - 1
        days.append(record)
    return days


def write_records_csv(path: str, runs: dict) -> None:
    columns = ['run', 'day', 'period', 'issue_no', 'open_time', 'structure', 'balance_before',
               'main_stake', 'aux_stake', 'triplet', 'sum', 'main_result', 'aux_result',
               'payout', 'period_net', 'streak']
    with open(path, 'w', encoding='utf-8-sig', newline='') as handle:
        writer = csv.writer(handle)
        writer.writerow(columns)
        for run_name, days in runs.items():
            for day_index, day in enumerate(days):
                for record in day.records or []:
                    writer.writerow([run_name, day_index + 1] + [record[c] for c in columns[2:]])


def build_charts(out_dir: str, runs: dict) -> list:
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from matplotlib import font_manager

    for font_path in ('/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc',
                      '/usr/share/fonts/truetype/droid/DroidSansFallbackFull.ttf'):
        if os.path.exists(font_path):
            font_manager.fontManager.addfont(font_path)
    plt.rcParams['font.sans-serif'] = ['WenQuanYi Zen Hei', 'Droid Sans Fallback', 'DejaVu Sans']
    plt.rcParams['axes.unicode_minus'] = False
    plt.rcParams['figure.dpi'] = 130

    colors = ['#2f6f9f', '#c46a2b', '#4b8f5a', '#8a4f9e', '#a63d40', '#3d7ea6']
    created = []
    figure, axis = plt.subplots(figsize=(11, 5.5))
    for index, (name, days) in enumerate(runs.items()):
        cumulative = []
        running = 0.0
        for day in days:
            running += day.net
            cumulative.append(running)
        axis.plot(range(1, len(cumulative) + 1), cumulative, linewidth=1.3,
                  color=colors[index % len(colors)], label=name)
    axis.axhline(0, color='#666666', linewidth=0.8, linestyle='--')
    axis.set_title('累计净利（满赔2.8 · 每日100U · 三关止盈）')
    axis.set_xlabel('启动日序号')
    axis.set_ylabel('累计净利 (U)')
    axis.legend(fontsize=8)
    axis.grid(alpha=0.25)
    figure.tight_layout()
    path = os.path.join(out_dir, 'equity_curve.png')
    figure.savefig(path)
    plt.close(figure)
    created.append(path)
    return created


def main() -> int:
    os.makedirs(OUT_DIR, exist_ok=True)
    loaded = load_real_draws(PREDICTOR_DB)
    draws = loaded['draws']
    tables = {key: build_outcome_table(key, profit_rule_id=RULE_ID) for key in STRUCTURES}
    predictions_by_algo = load_predictions(PREDICTOR_DB)

    runs = {}
    summary = {}
    draw_by_issue = {str(d['issue_no']): d for d in draws}
    for predictor_id, name in ALGORITHMS.items():
        preds = {str(issue): p for issue, p in predictions_by_algo[predictor_id].items()}
        # 算法单双信号的开奖结果（满赔2.8 口径：命中付费 / 命中退本 / 未中）
        odd_outcomes = {}
        for issue, pred in preds.items():
            draw = draw_by_issue.get(issue)
            if draw is None:
                continue
            leg = settle_leg(profit_rule_id=RULE_ID, metric='odd_even', value=pred['odd_even'],
                             triplet=tuple(int(x) for x in draw['triplet_text'].split('+')))
            odd_outcomes[issue] = leg['result_type']

        runs['A-8点 %s' % name] = run_schedule(draws, tables, preds)
        runs['B-连挂5 %s' % name] = run_miss_streak(draws, tables, preds, odd_outcomes)

    print('玩法: %s   真实开奖 %d 期   账户初始 %.0fU（跨日结转，<100U 计划死亡）' % (RULE_ID, len(draws), ACCOUNT_INIT))
    print('理论锚点（无信号）: 日止盈率 14.3%%~14.8%%, 日均EV -1.23~-1.28U, 止损日恒为 -100U')
    print()
    print('| 启动条件 × 算法 | 实际玩的天数 | 止盈率 | 日均净利±SE | 账户终值 | 账户最低 | 破产日 |')
    print('|---|---|---|---|---|---|---|')
    for name, all_days in runs.items():
        days, final_account, ruin_day = apply_account(all_days)
        stats = summarize_days(days)
        nets = [day.net for day in days]
        se = stats['daily_net_stdev'] / len(nets) ** 0.5 if len(nets) > 1 else 0.0
        summary[name] = {**stats, 'daily_net_se': se, 'final_account': final_account,
                         'ruin_day': ruin_day, 'days_total': len(all_days)}
        print('| %s | %d/%d | %.1f%% | %+.2f±%.2f | %+.2f | %+.2f | %s |' % (
            name, len(days), len(all_days), stats['tp_rate'] * 100,
            stats['daily_net_mean'], se, final_account,
            _min_account(days), ruin_day or '—'))

    sl_values = sorted({round(day.net, 2) for name, days in runs.items() for day in days
                        if day.reason == 'bust'})
    print('\n全部止损日净利取值: %s（应为且仅为 -100）' % sl_values)

    with open(os.path.join(OUT_DIR, 'summary.json'), 'w', encoding='utf-8') as handle:
        json.dump({'rule': RULE_ID, 'runs': summary}, handle, ensure_ascii=False, indent=2, default=str)
    write_records_csv(os.path.join(OUT_DIR, 'daily_records.csv'), runs)
    for path in build_charts(OUT_DIR, runs):
        print('图表 -> %s' % path)
    print('明细 -> %s' % os.path.join(OUT_DIR, 'daily_records.csv'))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
