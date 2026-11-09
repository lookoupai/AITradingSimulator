#!/usr/bin/env python3
"""
PC28「主注 70% + 辅助注 30%」全仓滚三连策略回测。

用法（仓库根目录）：
    .venv/bin/python scripts/backtest_pc28_split_roll.py --days 1000000
    .venv/bin/python scripts/backtest_pc28_split_roll.py --structures odd_small_even --days 200000
    .venv/bin/python scripts/backtest_pc28_split_roll.py --precision cent --day-boundary natural

输出：
    <out>/summary.json           全量统计
    <out>/structures.csv         四结构横向对比
    <out>/daily_records_*.csv    前 N 天逐期明细（含两注结算与连中计数）
    <out>/*.png                  资金曲线 / 日收益分布 / 结构对比
    <out>/report.md              可直接阅读的中文报告
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import os
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.pc28_split_roll import (  # noqa: E402
    ALLOWED_DAY_BOUNDARIES,
    ALLOWED_PRECISIONS,
    DEFAULT_BANKROLL,
    DEFAULT_MAIN_SHARE,
    DEFAULT_ODDS_PROFILE,
    DEFAULT_PROFIT_RULE_ID,
    DEFAULT_TP_STREAK,
    STRUCTURES,
    evaluate_structure,
    load_real_draws,
    normalize_day_boundary,
    normalize_precision,
    public_result
)

DEFAULT_DB_PATH = str(Path(__file__).resolve().parents[1] / 'pc28_predictor.db')
DEFAULT_OUT_DIR = str(Path(__file__).resolve().parents[1] / 'reports' / 'pc28_split_roll')

CHART_FONTS = ['Droid Sans Fallback', 'WenQuanYi Zen Hei', 'Noto Sans CJK SC', 'DejaVu Sans']


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='PC28 主注 70% + 辅助注 30% 全仓滚三连策略回测')
    parser.add_argument('--days', type=int, default=1_000_000, help='每种结构的随机模拟天数（默认 100 万）')
    parser.add_argument('--seed', default='20261102', help='随机种子，保证结果可复现')
    parser.add_argument('--rule', default=DEFAULT_PROFIT_RULE_ID, help='盈亏规则 ID')
    parser.add_argument('--odds-profile', default=DEFAULT_ODDS_PROFILE, help='赔率档位')
    parser.add_argument('--bankroll', type=float, default=DEFAULT_BANKROLL, help='每日初始本金')
    parser.add_argument('--main-share', type=float, default=DEFAULT_MAIN_SHARE, help='主注占比')
    parser.add_argument('--tp-streak', type=int, default=DEFAULT_TP_STREAK, help='连续有效中奖止盈次数')
    parser.add_argument('--precision', default='float', choices=ALLOWED_PRECISIONS, help='注额精度')
    parser.add_argument('--day-boundary', default='pc28', choices=ALLOWED_DAY_BOUNDARIES, help='真实回放的日界线')
    parser.add_argument('--structures', default='all', help='all 或逗号分隔的结构 key')
    parser.add_argument('--monte-days', type=int, default=None, help='覆盖 --days（便于快速试跑）')
    parser.add_argument('--db', default=DEFAULT_DB_PATH, help='真实开奖库路径')
    parser.add_argument('--out', default=DEFAULT_OUT_DIR, help='输出目录')
    parser.add_argument('--records-days', type=int, default=3, help='导出前 N 天的逐期明细，0 表示不导出')
    parser.add_argument('--no-charts', action='store_true', help='跳过图表生成')
    parser.add_argument('--no-replay', action='store_true', help='跳过真实开奖回放')
    return parser.parse_args(argv)


def resolve_structures(raw: str) -> list[str]:
    if raw.strip().lower() == 'all':
        return list(STRUCTURES)
    keys = [item.strip() for item in raw.split(',') if item.strip()]
    unknown = [key for key in keys if key not in STRUCTURES]
    if unknown:
        raise SystemExit('未知结构: %s（可选 %s）' % (', '.join(unknown), ' / '.join(STRUCTURES)))
    return keys


def ensure_dir(path: str) -> str:
    os.makedirs(path, exist_ok=True)
    return path


def dump_json(path: str, payload) -> None:
    with open(path, 'w', encoding='utf-8') as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, default=str)


def write_structures_csv(path: str, results: list[dict]) -> None:
    columns = [
        ('structure', lambda item: item['structure']),
        ('label', lambda item: item['label']),
        ('period_ev_pct', lambda item: item['exact']['period_ev_pct']),
        ('tp_rate_exact', lambda item: item['exact']['daily_tp_probability']),
        ('bust_rate_exact', lambda item: item['exact']['daily_bust_probability']),
        ('daily_net_exact', lambda item: item['exact']['daily_expected_net']),
        ('periods_exact', lambda item: item['exact']['daily_expected_periods']),
        ('tp_rate_mc', lambda item: _get(item, 'monte_carlo', 'tp_rate')),
        ('daily_net_mc', lambda item: _get(item, 'monte_carlo', 'daily_net_mean')),
        ('tp_day_net_mean_mc', lambda item: _get(item, 'monte_carlo', 'tp_day_net_mean')),
        ('max_drawdown_mc', lambda item: _get(item, 'monte_carlo', 'max_drawdown')),
        ('longest_losing_run_mc', lambda item: _get(item, 'monte_carlo', 'longest_losing_day_run')),
        ('daily_net_replay', lambda item: _get_replay(item, 'summary', 'daily_net_mean')),
        ('tp_rate_replay', lambda item: _get_replay(item, 'summary', 'tp_rate')),
        ('daily_net_replay_seq', lambda item: _get_seq(item, 'summary', 'daily_net_mean')),
        ('tp_rate_replay_seq', lambda item: _get_seq(item, 'summary', 'tp_rate')),
    ]
    with open(path, 'w', encoding='utf-8-sig', newline='') as handle:
        writer = csv.writer(handle)
        writer.writerow([name for name, _ in columns])
        for item in results:
            writer.writerow([_csv_value(getter(item)) for _, getter in columns])


def _csv_value(value):
    if isinstance(value, float):
        return round(value, 6)
    return value if value is not None else ''


def _get(item: dict, section: str, key: str):
    data = item.get(section) or {}
    return data.get(key)


def _get_replay(item: dict, section: str, key: str):
    replay = item.get('real_replay') or {}
    data = replay.get(section) or {}
    return data.get(key)


def _get_seq(item: dict, section: str, key: str):
    replay = item.get('real_replay_sequential') or {}
    data = replay.get(section) or {}
    return data.get(key)


def _records_of(day) -> list:
    if isinstance(day, dict):
        return day.get('records') or []
    return getattr(day, 'records', None) or []


def write_daily_records(path: str, results: list[dict]) -> int:
    rows_written = 0
    columns = [
        'source', 'structure', 'day', 'period', 'issue_no', 'open_time', 'balance_before',
        'main_stake', 'aux_stake', 'triplet', 'sum', 'main_result', 'aux_result',
        'payout', 'period_net', 'streak'
    ]
    with open(path, 'w', encoding='utf-8-sig', newline='') as handle:
        writer = csv.writer(handle)
        writer.writerow(columns)
        for item in results:
            for source, days in (('monte_carlo', item.get('_record_days') or []),
                                 ('real_replay', item.get('_replay_days') or [])):
                for day_index, day in enumerate(days):
                    for record in _records_of(day):
                        writer.writerow([
                            source, item['structure'], day_index, record.get('period'),
                            record.get('issue_no', ''), record.get('open_time', ''),
                            round(record['balance_before'], 6), round(record['main_stake'], 6),
                            round(record['aux_stake'], 6), record['triplet'], record['sum'],
                            record['main_result'], record['aux_result'],
                            round(record['payout'], 6), round(record['period_net'], 6), record['streak']
                        ])
                        rows_written += 1
    return rows_written


def build_charts(out_dir: str, results: list[dict]) -> list[str]:
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from matplotlib import font_manager

    # Droid Sans Fallback 缺 ASCII 数字/加号字形，显式注册文泉驿正黑兜底。
    for font_path in ('/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc',
                      '/usr/share/fonts/truetype/droid/DroidSansFallbackFull.ttf'):
        if os.path.exists(font_path):
            font_manager.fontManager.addfont(font_path)

    plt.rcParams['font.sans-serif'] = ['WenQuanYi Zen Hei', 'Droid Sans Fallback', 'DejaVu Sans']
    plt.rcParams['axes.unicode_minus'] = False
    plt.rcParams['figure.dpi'] = 130

    created = []
    colors = ['#2f6f9f', '#c46a2b', '#4b8f5a', '#8a4f9e']

    # 1. 资金曲线（按日净利累计）
    figure, axis = plt.subplots(figsize=(10, 5.2))
    max_points = 0
    for index, item in enumerate(results):
        nets = item.get('_sampled_nets')
        if not nets:
            continue
        max_points = max(max_points, len(nets))
        cumulative = []
        running = 0.0
        for net in nets:
            running += net
            cumulative.append(running)
        axis.plot(range(len(cumulative)), cumulative, linewidth=1.2,
                  color=colors[index % len(colors)], label=item['label'])
    axis.axhline(0, color='#666666', linewidth=0.8, linestyle='--')
    axis.set_title('累计净利曲线（每日重置本金，随机模拟抽样）')
    axis.set_xlabel('抽样点（共 %d 点）' % max_points)
    axis.set_ylabel('累计净利 (U)')
    axis.legend()
    axis.grid(alpha=0.25)
    figure.tight_layout()
    path = os.path.join(out_dir, 'equity_curve.png')
    figure.savefig(path)
    plt.close(figure)
    created.append(path)

    # 2. 每日净利分布
    figure, axes = plt.subplots(2, 2, figsize=(11, 7))
    for index, item in enumerate(results):
        axis = axes[index // 2][index % 2]
        nets = item.get('_sampled_nets') or []
        if not nets:
            axis.set_visible(False)
            continue
        axis.hist(nets, bins=60, color=colors[index % len(colors)], alpha=0.85)
        axis.set_yscale('log')
        axis.set_title('%s：日净利分布（对数轴）' % item['label'])
        axis.set_xlabel('当日净利 (U)')
        axis.set_ylabel('天数')
        axis.grid(alpha=0.2)
    figure.tight_layout()
    path = os.path.join(out_dir, 'daily_net_distribution.png')
    figure.savefig(path)
    plt.close(figure)
    created.append(path)

    # 3. 结构横向对比
    labels = [item['label'] for item in results]
    metrics = [
        ('日止盈率', [item['monte_carlo']['tp_rate'] * 100 if item.get('monte_carlo') else 0 for item in results], '%'),
        ('日均净利', [item['monte_carlo']['daily_net_mean'] if item.get('monte_carlo') else 0 for item in results], 'U'),
        ('日均期数', [item['monte_carlo']['daily_periods_mean'] if item.get('monte_carlo') else 0 for item in results], '期'),
        ('最大回撤', [item['monte_carlo']['max_drawdown'] / 1000 if item.get('monte_carlo') else 0 for item in results], '千U'),
    ]
    figure, axes = plt.subplots(2, 2, figsize=(11, 7))
    for position, (title, values, unit) in enumerate(metrics):
        axis = axes[position // 2][position % 2]
        bars = axis.bar(labels, values, color=colors[:len(labels)], alpha=0.9)
        axis.set_title('%s（%s）' % (title, unit))
        axis.grid(alpha=0.2, axis='y')
        for bar, value in zip(bars, values):
            axis.annotate('%.2f' % value, (bar.get_x() + bar.get_width() / 2, value),
                          ha='center', va='bottom' if value >= 0 else 'top', fontsize=8)
        if title == '日止盈率':
            axis.set_ylim(0, 100)
    figure.tight_layout()
    path = os.path.join(out_dir, 'structure_comparison.png')
    figure.savefig(path)
    plt.close(figure)
    created.append(path)

    return created


def format_pct(value) -> str:
    if value is None:
        return '-'
    return '%.2f%%' % (value * 100)


def format_num(value, digits=2) -> str:
    if value is None:
        return '-'
    return '%.*f' % (digits, value)


def format_money(value, digits=0) -> str:
    if value is None:
        return '-'
    return '{:,}'.format(round(value, digits))


def build_report_markdown(config: dict, results: list[dict], extra: dict) -> str:
    lines = [
        '# PC28 主注 70% + 辅助注 30% 全仓滚三连策略回测',
        '',
        '- 生成时间：%s' % config['generated_at'],
        '- 盈亏规则：`%s`（赔率档 %s）' % (config['profit_rule_id'], config['odds_profile']),
        '- 每日本金：%.2f U；主注占比：%.0f%%；连中止盈：%d 次；注额精度：%s'
        % (config['bankroll'], config['main_share'] * 100, config['tp_streak'], config['precision']),
        '- 随机模拟：每种结构 %s 天，种子 `%s`' % (format_int(config['monte_days']), config['seed']),
        '- 真实开奖：%s 期（%s → %s），回放日界线：%s'
        % (format_int(extra['draw_count']), extra['draw_start'] or '-', extra['draw_end'] or '-',
           config['day_boundary']),
        '',
        '## 四结构横向对比',
        '',
        '| 结构 | 单期EV | 日止盈率(解析) | 日均净利(解析) | 日均期数 | 百万天日均净利 | 百万天最大回撤 | 连续回放日均净利 |',
        '|---|---|---|---|---|---|---|---|',
    ]
    for item in results:
        exact = item['exact']
        monte = item.get('monte_carlo') or {}
        sequential = (item.get('real_replay_sequential') or {}).get('summary') or {}
        lines.append('| %s | %.3f%% | %s | %s U | %.3f | %s U | %s U | %s U |' % (
            item['label'],
            exact['period_ev_pct'],
            format_pct(exact['daily_tp_probability']),
            format_num(exact['daily_expected_net']),
            exact['daily_expected_periods'],
            format_num(monte.get('daily_net_mean')),
            format_money(monte.get('max_drawdown')),
            format_num(sequential.get('daily_net_mean'))
        ))

    lines += ['', '## 关键结论', '']
    for item in results:
        exact = item['exact']
        lines.append(
            '- **%s**：单期期望 %.3f%%，日止盈率 %.2f%%，日均净利 %.2f U；'
            '整期净赢概率 %.2f%%，整期爆仓概率 %.2f%%。' % (
                item['label'], exact['period_ev_pct'], exact['daily_tp_probability'] * 100,
                exact['daily_expected_net'], exact['win_probability'] * 100,
                exact['bust_probability'] * 100
            )
        )

    lines += ['', '## 结算口径核对（单+小双）', '', '| 整期倍率 | 主注结果 | 辅助注结果 | 组合数 | 和值 |', '|---|---|---|---|---|']
    for group in results[0]['outcome_groups']:
        lines.append('| %.4f | %s | %s | %d | %s |' % (
            group['multiplier'], group['main_result'], group['aux_result'], group['count'],
            ','.join(str(value) for value in group['sums'])
        ))

    if any(item.get('real_replay') for item in results):
        windows = (results[0].get('real_replay') or {}).get('windows') or 0
        lines += ['', '## 真实开奖回放', '']
        if 0 < windows < 100:
            lines += [
                '> **样本量警告**：按日界线切分后只有 %d 个窗口，日均净利标准误差约 ±16U，与下列均值同一量级，'
                '日历回放数字不具统计意义，只作口径演示；判断长期期望请看「连续回放」和解析解。' % windows,
                ''
            ]
        for item in results:
            replay = item.get('real_replay') or {}
            summary = replay.get('summary') or {}
            sequential = item.get('real_replay_sequential') or {}
            seq_summary = sequential.get('summary') or {}
            if summary:
                lines.append(
                    '- %s（日历，%d 窗口）：日均净利 %.2f U，止盈率 %.2f%%，最长连亏 %d 天。' % (
                        item['label'], replay.get('windows'),
                        summary.get('daily_net_mean', 0), summary.get('tp_rate', 0) * 100,
                        summary.get('longest_losing_day_run', 0)
                    )
                )
            if seq_summary:
                lines.append(
                    '- %s（连续回放，%d 个策略日）：日均净利 %.2f U，止盈率 %.2f%%，最长连亏 %d 天，最大回撤 %s U。' % (
                        item['label'], seq_summary.get('days'),
                        seq_summary.get('daily_net_mean', 0), seq_summary.get('tp_rate', 0) * 100,
                        seq_summary.get('longest_losing_day_run', 0),
                        format_money(seq_summary.get('max_drawdown'))
                    )
                )

    lines += [
        '',
        '## 说明',
        '',
        '- 单期期望由 1000 种有序三球穷举得出，不是抽样估计。',
        '- 日止盈率 / 日均净利（解析）按连中止盈的马尔可夫链闭式解计算，与随机模拟互为校验。',
        '- 连续回放把开奖序列当作连续流：一天结束立即开下一天，不按日历切分，样本量大，用于估计长期期望。',
        '- 日历回放严格按「每个日界线窗口只玩一天、止盈/止损后窗口内剩余期数不再下注」执行。',
        '- 真实回放按开奖顺序逐期下注，不含滑点、封盘、限额与取整损失。',
        ''
    ]
    return '\n'.join(lines)


def format_int(value) -> str:
    if value is None:
        return '-'
    return '{:,}'.format(int(value))


def print_table(results: list[dict]) -> None:
    header = '| 结构 | 单期EV | 日止盈率 | 日均净利(解析) | 日均期数 | 模拟日均净利 | 回放日均净利 |'
    print(header)
    print('|---|---|---|---|---|---|---|')
    for item in results:
        exact = item['exact']
        monte = item.get('monte_carlo') or {}
        replay = (item.get('real_replay') or {}).get('summary') or {}
        print('| %s | %.3f%% | %s | %sU | %.3f | %sU | %sU |' % (
            item['label'], exact['period_ev_pct'],
            format_pct(exact['daily_tp_probability']),
            format_num(exact['daily_expected_net']),
            exact['daily_expected_periods'],
            format_num(monte.get('daily_net_mean')),
            format_num(replay.get('daily_net_mean'))
        ))


def main(argv=None) -> int:
    args = parse_args(argv)
    precision = normalize_precision(args.precision)
    day_boundary = normalize_day_boundary(args.day_boundary)
    monte_days = args.monte_days if args.monte_days is not None else args.days
    structures = resolve_structures(args.structures)
    out_dir = ensure_dir(args.out)

    real_draws = []
    draw_meta = {'draw_count': 0, 'draw_start': None, 'draw_end': None, 'skipped': []}
    if not args.no_replay:
        loaded = load_real_draws(args.db)
        real_draws = loaded['draws']
        draw_meta = {
            'draw_count': len(real_draws),
            'draw_start': real_draws[0]['open_time_text'] if real_draws else None,
            'draw_end': real_draws[-1]['open_time_text'] if real_draws else None,
            'skipped': loaded['skipped'][:50],
            'skipped_count': len(loaded['skipped']),
            'raw_count': loaded['raw_count']
        }
        print('真实开奖：%d 期可用（跳过 %d 期）' % (len(real_draws), len(loaded['skipped'])))

    results = []
    for index, structure_key in enumerate(structures):
        result = evaluate_structure(
            structure_key,
            profit_rule_id=args.rule,
            odds_profile=args.odds_profile,
            bankroll=args.bankroll,
            main_share=args.main_share,
            tp_streak=args.tp_streak,
            precision=precision,
            monte_days=monte_days,
            seed='%s:%s' % (args.seed, structure_key),
            real_draws=real_draws if real_draws else None,
            day_boundary=day_boundary,
            keep_records=max(0, args.records_days)
        )
        results.append(result)
        print('已完成 %s（%d/%d）' % (result['label'], index + 1, len(structures)))

    write_structures_csv(os.path.join(out_dir, 'structures.csv'), results)
    dump_json(os.path.join(out_dir, 'summary.json'), {
        'config': {
            'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'profit_rule_id': args.rule,
            'odds_profile': args.odds_profile,
            'bankroll': args.bankroll,
            'main_share': args.main_share,
            'tp_streak': args.tp_streak,
            'precision': precision,
            'monte_days': monte_days,
            'seed': args.seed,
            'day_boundary': day_boundary
        },
        'draws': draw_meta,
        'structures': [public_result(item) for item in results]
    })

    if args.records_days > 0:
        rows = write_daily_records(os.path.join(out_dir, 'daily_records.csv'), results)
        print('逐期明细 %d 行 -> %s' % (rows, os.path.join(out_dir, 'daily_records.csv')))

    if not args.no_charts:
        for path in build_charts(out_dir, results):
            print('图表 -> %s' % path)

    config = {
        'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'profit_rule_id': args.rule,
        'odds_profile': args.odds_profile,
        'bankroll': args.bankroll,
        'main_share': args.main_share,
        'tp_streak': args.tp_streak,
        'precision': precision,
        'monte_days': monte_days,
        'seed': args.seed,
        'day_boundary': day_boundary
    }
    report = build_report_markdown(config, results, draw_meta)
    report_path = os.path.join(out_dir, 'report.md')
    with open(report_path, 'w', encoding='utf-8') as handle:
        handle.write(report)

    print()
    print_table(results)
    print()
    print('报告 -> %s' % report_path)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
