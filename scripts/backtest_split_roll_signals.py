#!/usr/bin/env python3
"""
PC28 主注 70% + 辅助注 30% 全仓滚三连 —— 由本地机器算法逐期指定结构的真实数据回放。

资金管理（用户 spec，2026-11-02）：
- 每日初始本金 100U；每期总投注 = 当前余额 100%（主注 70% / 辅助注 30%），盈利滚仓
- 整期净利 > 0 记 1 关；< 0 清零；= 0 保持
- 连中 3 关当日止盈；余额 <= 0 当日止损；止盈/止损后当日不再下注；次日重置 100U

结构指定：算法每期预测组合 C → 主注 = C 的单双，辅助注 = C 翻转单双，放弃 = 对角组。
玩法：默认彩28满赔2.8（2.84/6.79/6.33 含本金，中奖遇 13/14+对子+顺子+豹子退本，组合同族退本）。

用法：
    .venv/bin/python scripts/backtest_split_roll_signals.py [--rule pc28_fullpay_2_8]
输出：
    reports/pc28_split_roll_signals_<rule>/{summary.json, daily_records.csv, *.png}
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.pc28_split_roll import (  # noqa: E402
    DEFAULT_BANKROLL,
    DEFAULT_MAIN_SHARE,
    DayRecord,
    STRUCTURES,
    build_outcome_table,
    load_real_draws,
    period_multiplier,
    summarize_days
)

PREDICTOR_DB = str(Path(__file__).resolve().parents[1] / 'pc28_predictor.db')
SIGNAL_DB = '/www/wwwroot/pc28touzhu/pc28touzhu.db'

# 三个本地机器算法（predictors.id）
ALGORITHMS = {7: '频次趋势 V1', 8: '组合马尔可夫 V1', 9: '遗漏回补 V1'}

# 预测组合 C -> 结构 key（主注 = C 的单双，辅助注 = C 翻转单双，放弃 = 对角组）
STRUCTURE_BY_COMBO = {
    '大单': 'odd_big_even',    # 单+大双，放弃小双
    '小单': 'odd_small_even',  # 单+小双，放弃大双
    '大双': 'even_big_odd',    # 双+大单，放弃小单
    '小双': 'even_small_odd',  # 双+小单，放弃大单
}


def load_predictions(db_path: str) -> dict:
    """{算法id: {期号: 组合}}，只取开奖前最后一次预测（防前视）。"""
    import sqlite3
    conn = sqlite3.connect('file:%s?mode=ro' % db_path, uri=True, timeout=30)
    latest = {}
    for predictor_id, issue_no, combo, created_at in conn.execute(
            "SELECT predictor_id, issue_no, prediction_combo, created_at FROM predictions "
            "WHERE lottery_type='pc28' AND prediction_combo IS NOT NULL "
            "AND predictor_id IN (7,8,9)"):
        combo = str(combo or '').strip()
        if combo not in STRUCTURE_BY_COMBO:
            continue
        key = (predictor_id, str(issue_no))
        if key not in latest or created_at > latest[key][0]:
            latest[key] = (created_at, combo)
    conn.close()
    result = {pid: {} for pid in ALGORITHMS}
    for (predictor_id, issue_no), (_created_at, combo) in latest.items():
        result[predictor_id][issue_no] = combo
    return result


def replay_signal_guided(tables: dict, predictions: dict, draws: list, *,
                         bankroll: float = DEFAULT_BANKROLL,
                         main_share: float = DEFAULT_MAIN_SHARE,
                         tp_streak: int = 3) -> list:
    """按北京自然日切分：每天一个策略日，逐期按算法组合指定结构全仓滚。"""
    by_day = defaultdict(list)
    for draw in draws:
        by_day[draw['moment'].date()].append(draw)

    days = []
    for day in sorted(by_day):
        balance = bankroll
        streak = 0
        periods = 0
        records = []
        reason = 'window_end'
        for draw in by_day[day]:
            combo = predictions.get(str(draw['issue_no']))
            if combo is None:
                continue  # 当期无算法信号则不下注
            row = tables[STRUCTURE_BY_COMBO[combo]][draw['code']]
            main_stake = balance * main_share
            aux_stake = balance - main_stake
            payout = main_stake * row['main_return'] + aux_stake * row['aux_return']
            new_balance = balance - main_stake - aux_stake + payout
            periods += 1
            if new_balance > balance:
                streak += 1
            elif new_balance < balance:
                streak = 0
            records.append({
                'period': periods, 'issue_no': draw['issue_no'],
                'open_time': draw['open_time_text'],
                'structure': STRUCTURES[STRUCTURE_BY_COMBO[combo]]['label'],
                'balance_before': balance, 'main_stake': main_stake, 'aux_stake': aux_stake,
                'triplet': draw['triplet_text'], 'sum': draw['sum'],
                'main_result': row['main_result'], 'aux_result': row['aux_result'],
                'payout': payout, 'period_net': new_balance - balance, 'streak': streak,
            })
            balance = new_balance
            if balance <= 0:
                reason = 'bust'
                break
            if streak >= tp_streak:
                reason = 'tp'
                break
        days.append(DayRecord(net=balance - bankroll, periods=periods, reason=reason,
                              records=records if records else None, balance=balance))
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

    created = []
    colors = ['#2f6f9f', '#c46a2b', '#4b8f5a', '#8a4f9e', '#a63d40', '#3d7ea6', '#7768ae']

    figure, axis = plt.subplots(figsize=(11, 5.5))
    for index, (name, days) in enumerate(runs.items()):
        cumulative = []
        running = 0.0
        for day in days:
            running += day.net
            cumulative.append(running)
        axis.plot(range(1, len(cumulative) + 1), cumulative, linewidth=1.4,
                  color=colors[index % len(colors)], label=name)
    axis.axhline(0, color='#666666', linewidth=0.8, linestyle='--')
    axis.set_title('累计净利曲线（每日重置 100U，真实开奖回放）')
    axis.set_xlabel('策略日（北京自然日）')
    axis.set_ylabel('累计净利 (U)')
    axis.legend(fontsize=8)
    axis.grid(alpha=0.25)
    figure.tight_layout()
    path = os.path.join(out_dir, 'equity_curve.png')
    figure.savefig(path)
    plt.close(figure)
    created.append(path)

    figure, axis = plt.subplots(figsize=(11, 5.0))
    width = 0.11
    names = list(runs)
    for index, name in enumerate(names):
        nets = [day.net for day in runs[name]]
        axis.bar([i + (index - len(names) / 2) * width for i in range(len(nets))],
                 nets, width=width, label=name, color=colors[index % len(colors)])
    axis.axhline(0, color='#666666', linewidth=0.8)
    axis.set_title('每日净利对比（正=止盈日，-100=止损日）')
    axis.set_xlabel('策略日')
    axis.set_ylabel('当日净利 (U)')
    axis.legend(fontsize=8)
    axis.grid(alpha=0.25, axis='y')
    figure.tight_layout()
    path = os.path.join(out_dir, 'daily_net_comparison.png')
    figure.savefig(path)
    plt.close(figure)
    created.append(path)
    return created


def main() -> int:
    parser = argparse.ArgumentParser(description='主注70%+辅助注30%全仓滚三连（算法指定结构）真实回放')
    parser.add_argument('--rule', default='pc28_fullpay_2_8',
                        help='盈亏规则 ID（默认彩28满赔2.8）')
    args = parser.parse_args()
    rule_id = args.rule
    out_dir = str(Path(__file__).resolve().parents[1] / 'reports' / ('pc28_split_roll_signals_' + rule_id))
    os.makedirs(out_dir, exist_ok=True)

    loaded = load_real_draws(PREDICTOR_DB)
    draws = loaded['draws']
    print('真实开奖: %d 期（%s → %s）' % (len(draws), draws[0]['open_time_text'], draws[-1]['open_time_text']))

    tables = {key: build_outcome_table(key, profit_rule_id=rule_id) for key in STRUCTURES}
    predictions_by_algo = load_predictions(PREDICTOR_DB)

    runs = {}
    # 四个固定结构基线（同一批真实开奖、同一套止盈止损）
    from services.pc28_split_roll import replay_real_draws
    for key, config in STRUCTURES.items():
        replay = replay_real_draws(tables[key], draws, day_boundary='natural', keep_records=0)
        runs['固定 %s' % config['label']] = [
            DayRecord(net=d['net'], periods=d['periods'], reason=d['reason'],
                      records=None, balance=d['balance'])
            for d in replay['days']]
    # 三个算法逐期指定结构
    for predictor_id, name in ALGORITHMS.items():
        preds = {str(issue): combo for issue, combo in predictions_by_algo[predictor_id].items()}
        covered = sum(1 for draw in draws if str(draw['issue_no']) in preds)
        days = replay_signal_guided(tables, preds, draws)
        runs['算法 %s' % name] = days
        print('算法 %s: 覆盖 %d/%d 期' % (name, covered, len(draws)))

    # 统计与输出
    summary = {}
    print()
    header = '| 口径 | 策略日 | 止盈率 | 止损率 | 日均净利±SE | 中位数 | 日均期数 | 止盈日均盈利 | 最大回撤 | 最长连亏 |'
    print(header)
    print('|---|---|---|---|---|---|---|---|---|---|')
    for name, days in runs.items():
        stats = summarize_days(days)
        nets = [day.net for day in days]
        se = stats['daily_net_stdev'] / len(nets) ** 0.5 if len(nets) > 1 else 0.0
        summary[name] = {**stats, 'daily_net_se': se}
        print('| %s | %d | %.1f%% | %.1f%% | %+.2f±%.2f | %+.1f | %.2f | %+.2f | %+.0f | %d |' % (
            name, stats['days'], stats['tp_rate'] * 100, stats['bust_rate'] * 100,
            stats['daily_net_mean'], se, stats['daily_net_median'],
            stats['daily_periods_mean'], stats['tp_day_net_mean'],
            stats['max_drawdown'], stats['longest_losing_day_run']))

    dump = {
        'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'profit_rule_id': rule_id,
        'draw_count': len(draws),
        'draw_range': [draws[0]['open_time_text'], draws[-1]['open_time_text']],
        'runs': {name: {k: v for k, v in stats.items()} for name, stats in summary.items()},
    }
    with open(os.path.join(out_dir, 'summary.json'), 'w', encoding='utf-8') as handle:
        json.dump(dump, handle, ensure_ascii=False, indent=2, default=str)

    write_records_csv(os.path.join(out_dir, 'daily_records.csv'), runs)
    for path in build_charts(out_dir, runs):
        print('图表 -> %s' % path)
    print('明细 -> %s' % os.path.join(out_dir, 'daily_records.csv'))
    print('汇总 -> %s' % os.path.join(out_dir, 'summary.json'))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
