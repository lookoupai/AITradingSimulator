#!/usr/bin/env python3
"""
竞彩足球机器模型 walk-forward 回测。

直接复用 services.machine_prediction 里的算法函数（与线上预测同一代码路径）：
- Pi评级：逐日评估（在线评级，毫秒级）。
- 进球分布DC：按 N 天窗口评估，run_key 取窗口起始日（模型只用窗口前的比分，
  窗口内的比赛都用同一批分联赛模型预测，兼顾因果性与拟合开销）。

用法（仓库根目录）：
    .venv/bin/python scripts/backtest_football_models.py [--start 2026-06-01] [--end 2026-09-19] [--dc-window-days 7] [--include-intl]
"""
from __future__ import annotations

import argparse
import json
import math
import sys
from collections import defaultdict
from datetime import timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from penaltyblog.implied import calculate_implied  # noqa: E402

from database import Database  # noqa: E402
from services.machine_prediction import (  # noqa: E402
    _predict_jingcai_dixon_coles_v1,
    _predict_jingcai_pirating_v1
)
from utils import jingcai_football as football_utils  # noqa: E402

OUTCOMES = ('胜', '平', '负')


def is_international(league: str) -> bool:
    text = league or ''
    return ('世界杯' in text) or ('国际' in text) or ('亚运' in text)


def outcome_index(score1: int, score2: int) -> int:
    return 0 if score1 > score2 else (1 if score1 == score2 else 2)


def devig(odds: list[float]) -> list[float]:
    return [float(p) for p in calculate_implied(odds, method='multiplicative').probabilities]


def rps(probs: list[float], outcome: int) -> float:
    f1, f2 = probs[0], probs[0] + probs[1]
    o1, o2 = (1.0, 1.0) if outcome == 0 else ((0.0, 1.0) if outcome == 1 else (0.0, 0.0))
    return math.sqrt(((f1 - o1) ** 2 + (f2 - o2) ** 2) / 2)


def load_matches(db: Database) -> dict[str, list[dict]]:
    conn = db.get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT event_key, event_date, league, home_team, away_team, meta_payload, result_payload
            FROM lottery_events
            WHERE lottery_type = 'jingcai_football' AND source_provider = 'sina'
              AND json_extract(result_payload, '$.score1') IS NOT NULL
              AND json_extract(result_payload, '$.score2') IS NOT NULL
            ORDER BY event_date ASC, event_time ASC
            """
        )
        rows = cursor.fetchall()
    finally:
        conn.close()

    by_date: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        result = json.loads(row['result_payload'] or '{}')
        meta = json.loads(row['meta_payload'] or '{}')
        odds_map = meta.get('spf_odds') or {}
        odds = [odds_map.get(k) for k in OUTCOMES]
        if not all(isinstance(o, (int, float)) and o > 1 for o in odds):
            continue
        by_date[str(row['event_date'])].append({
            'event_key': row['event_key'],
            'match_no': '',
            'league': str(row['league'] or ''),
            'home_team': str(row['home_team'] or '').strip(),
            'away_team': str(row['away_team'] or '').strip(),
            'spf_odds': {k: float(odds[i]) for i, k in enumerate(OUTCOMES)},
            'rqspf': meta.get('rqspf') or {},
            'meta': meta,
            'outcome': outcome_index(int(result['score1']), int(result['score2'])),
            'odds': [float(o) for o in odds]
        })
    return by_date


def collect_records(model_name: str, run_key: str, day_matches: list[dict], items: list[dict], debug: dict, sink: list[dict]) -> None:
    probs_by_key = {row['event_key']: row for row in debug.get('items', [])}
    for match, item in zip(day_matches, items):
        row = probs_by_key.get(match['event_key'])
        if not row or not row.get('spf_probabilities'):
            continue
        probs = [row['spf_probabilities'].get(k, 0.0) for k in OUTCOMES]
        if sum(probs) <= 0:
            continue
        probs = [p / sum(probs) for p in probs]
        outcome = match['outcome']
        pick = max(range(3), key=lambda i: probs[i])
        single_ok = football_utils.is_metric_sellable(
            'spf', match['meta'], OUTCOMES[pick], play_mode='single', allow_settled=True
        )
        sink.append({
            'date': match['date'],
            'rps_model': rps(probs, outcome),
            'rps_market': rps(devig(match['odds']), outcome),
            'pick': OUTCOMES[pick],
            'pnl': (match['odds'][pick] - 1) if pick == outcome else -1,
            'single_ok': bool(single_ok)
        })


def evaluate(model_name: str, records: list[dict]) -> None:
    if not records:
        print(f'\n[{model_name}] 无可评估样本')
        return
    n = len(records)
    model_rps = sum(r['rps_model'] for r in records) / n
    market_rps = sum(r['rps_market'] for r in records) / n

    def roi(rows: list[dict]) -> tuple[int, float]:
        if not rows:
            return 0, 0.0
        return len(rows), sum(r['pnl'] for r in rows) / len(rows) * 100

    all_n, all_roi = roi(records)
    single_n, single_roi = roi([r for r in records if r['single_ok']])
    pick_dist = defaultdict(int)
    for r in records:
        pick_dist[r['pick']] += 1
    print(f'\n[{model_name}] 样本{n}场  RPS={model_rps:.4f} (市场 {market_rps:.4f}, 差值{model_rps - market_rps:+.4f})')
    print(f'  全量最大概率方向: {all_n}注 ROI {all_roi:+.2f}%')
    print(f'  仅单关可售场次:   {single_n}注 ROI {single_roi:+.2f}%')
    print(f'  方向分布: {dict(pick_dist)}')

    monthly: dict[str, list[dict]] = defaultdict(list)
    for r in records:
        monthly[r['date'][:7]].append(r)
    for month in sorted(monthly):
        rows = monthly[month]
        m_n = len(rows)
        m_rps = sum(r['rps_model'] for r in rows) / m_n
        _, m_roi = roi(rows)
        s_n, s_roi = roi([r for r in rows if r['single_ok']])
        print(f'  {month}: {m_n}场 RPS={m_rps:.4f} 全量ROI{m_roi:+.1f}% 单关{s_n}注 ROI{s_roi:+.1f}%')


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--start', default='2026-06-01')
    parser.add_argument('--end', default='2026-09-19')
    parser.add_argument('--dc-window-days', type=int, default=7)
    parser.add_argument('--include-intl', action='store_true', help='包含世界杯/国际赛场次')
    parser.add_argument('--db', default='pc28_predictor.db')
    args = parser.parse_args()

    db = Database(args.db)
    by_date = load_matches(db)
    dates = sorted(d for d in by_date if args.start <= d <= args.end)

    pi_records: list[dict] = []
    dc_records: list[dict] = []
    dc_failures = 0

    # Pi：逐日评估
    for date in dates:
        day_matches = [
            dict(m, date=date) for m in by_date[date]
            if args.include_intl or not is_international(m['league'])
        ]
        if not day_matches:
            continue
        try:
            items, debug = _predict_jingcai_pirating_v1(date, day_matches, db=db)
        except Exception as exc:  # noqa: BLE001
            print(f'Pi {date} 失败: {exc}')
            continue
        collect_records('Pi', date, day_matches, items, debug, pi_records)

    # DC：按窗口评估，run_key = 窗口起始日
    window = max(1, int(args.dc_window_days))
    window_start = dates[0] if dates else None
    while window_start and window_start <= args.end:
        window_end = (__import__('datetime').date.fromisoformat(window_start) + timedelta(days=window - 1)).isoformat()
        window_dates = [d for d in dates if window_start <= d <= window_end]
        window_matches = []
        for d in window_dates:
            for m in by_date[d]:
                if args.include_intl or not is_international(m['league']):
                    window_matches.append(dict(m, date=d))
        if window_matches:
            try:
                items, debug = _predict_jingcai_dixon_coles_v1(window_start, window_matches, db=db)
                collect_records('DC', window_start, window_matches, items, debug, dc_records)
            except Exception as exc:  # noqa: BLE001
                dc_failures += 1
                print(f'DC 窗口 {window_start} 失败: {exc}')
        next_start = (__import__('datetime').date.fromisoformat(window_start) + timedelta(days=window)).isoformat()
        window_start = next_start if next_start <= args.end else None

    print(f'评估区间 {args.start} ~ {args.end}（{"含" if args.include_intl else "剔除"}国际赛），比赛日 {len(dates)} 天，DC 窗口 {window} 天，失败窗口 {dc_failures}')
    evaluate('Pi评级 V1', pi_records)
    evaluate('进球分布DC V1', dc_records)


if __name__ == '__main__':
    main()
