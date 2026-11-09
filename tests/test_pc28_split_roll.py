from __future__ import annotations

import json
import unittest
from datetime import datetime

from services.pc28_split_roll import (
    DEFAULT_MAIN_SHARE,
    STRUCTURES,
    build_outcome_table,
    exact_day_stats,
    load_real_draws,
    outcome_groups,
    parse_draw_payload,
    period_multiplier,
    replay_real_draws,
    simulate_days,
    split_stakes,
    summarize_days
)
from services.profit_simulator import PC28_PROFIT_RULES


def _table(structure_key: str = 'odd_small_even'):
    return build_outcome_table(structure_key)


class OutcomeTableTests(unittest.TestCase):
    def test_table_covers_1000_ordered_triplets(self):
        table = _table()
        self.assertEqual(len(table), 1000)
        self.assertEqual(sorted(row['code'] for row in table), list(range(1000)))

    def test_fullpay_netdisk_multiplier_groups(self):
        """对照 1000 组穷举的手工口径：499×1.393 + 231×1.296 + 1×0.7 + 269×0。"""
        table = _table('odd_small_even')
        groups = {(round(row['multiplier'] if 'multiplier' in row else period_multiplier(row), 6),
                   row['main_result'], row['aux_result']) for row in table}
        self.assertIn((1.393, 'hit', 'miss'), groups)
        self.assertIn((1.296, 'miss', 'hit'), groups)
        self.assertIn((0.7, 'refund', 'miss'), groups)
        self.assertIn((0.0, 'miss', 'miss'), groups)

    def test_special_sum_refund_only_on_hit(self):
        """和值 27：主注「单」退本；和值 0：组合「小双」照常按 4.32 赔付。"""
        table = _table('odd_small_even')
        by_sum = {row['sum']: row for row in table if row['sum'] in (0, 27)}
        self.assertEqual(by_sum[27]['main_result'], 'refund')
        self.assertAlmostEqual(by_sum[27]['main_return'], 1.0)
        self.assertEqual(by_sum[27]['aux_result'], 'miss')
        self.assertEqual(by_sum[0]['main_result'], 'miss')
        self.assertEqual(by_sum[0]['aux_result'], 'hit')
        self.assertAlmostEqual(by_sum[0]['aux_return'], 4.32)

    def test_outcome_groups_sum_to_1000(self):
        total = sum(group['count'] for group in outcome_groups(_table()))
        self.assertEqual(total, 1000)


class ExactStatsTests(unittest.TestCase):
    def test_matches_closed_form_baseline(self):
        """与手推马尔可夫链闭式解对照（单+小双：W=0.73, L=0.001, B=0.269）。"""
        stats = exact_day_stats(_table('odd_small_even'))
        self.assertAlmostEqual(stats['period_ev_pct'], -0.4817, places=3)
        self.assertAlmostEqual(stats['daily_tp_probability'], 0.389898, places=4)
        self.assertAlmostEqual(stats['daily_expected_net'], -1.4401, places=3)
        self.assertAlmostEqual(stats['daily_expected_periods'], 2.268, places=2)
        self.assertAlmostEqual(stats['bust_probability'], 0.269, places=6)

    def test_mirrored_structures_share_expectation(self):
        first = exact_day_stats(_table('odd_small_even'))
        second = exact_day_stats(_table('even_big_odd'))
        self.assertAlmostEqual(first['daily_expected_net'], second['daily_expected_net'], places=6)
        self.assertAlmostEqual(first['daily_tp_probability'], second['daily_tp_probability'], places=6)


class SimulationTests(unittest.TestCase):
    def test_day_terminates_by_tp_or_bust(self):
        days = simulate_days(_table(), 500, seed='unit-test')
        self.assertEqual(len(days), 500)
        for day in days:
            self.assertIn(day.reason, ('tp', 'bust'))
            self.assertGreater(day.periods, 0)
            if day.reason == 'bust':
                self.assertAlmostEqual(day.balance, 0.0)
            else:
                self.assertGreater(day.balance, 0.0)

    def test_mean_converges_to_exact_expectation(self):
        stats = exact_day_stats(_table('odd_big_even'))
        days = simulate_days(_table('odd_big_even'), 200_000, seed='converge')
        mean = sum(day.net for day in days) / len(days)
        self.assertAlmostEqual(mean, stats['daily_expected_net'], delta=1.5)

    def test_precision_cent_keeps_total_stake_at_balance(self):
        main_stake, aux_stake = split_stakes(139.31, DEFAULT_MAIN_SHARE, 'cent')
        self.assertEqual(round(main_stake + aux_stake, 2), 139.31)
        self.assertAlmostEqual(main_stake, 97.52, places=2)

    def test_precision_unit_bets_only_whole_units(self):
        main_stake, aux_stake = split_stakes(139.31, DEFAULT_MAIN_SHARE, 'unit')
        self.assertEqual(main_stake + aux_stake, 139.0)
        main_stake, aux_stake = split_stakes(0.4, DEFAULT_MAIN_SHARE, 'unit')
        self.assertEqual(main_stake + aux_stake, 0.0)


class ReplayTests(unittest.TestCase):
    def _draws(self):
        payloads = [
            ('1', '2026-11-05 22:00:00', 5, {'number': '1+2+2'}),
            ('2', '2026-11-05 22:10:00', 14, {'number': '5+5+4'}),
            ('3', '2026-11-06 22:00:00', 7, {'number': '2+2+3'}),
            ('4', '2026-11-06 22:05:00', 0, {'number': '0+0+0'})
        ]
        draws = []
        for issue_no, open_time, total, payload in payloads:
            draws.append({
                'issue_no': issue_no,
                'moment': datetime.strptime(open_time, '%Y-%m-%d %H:%M:%S'),
                'open_time_text': open_time,
                'sum': total,
                'triplet_text': payload['number'],
                'code': int(payload['number'][0]) * 100 + int(payload['number'][2]) * 10 + int(payload['number'][4])
            })
        return draws

    def test_sequential_replay_counts_every_period(self):
        replay = replay_real_draws(_table(), self._draws(), day_boundary='sequential')
        self.assertEqual(replay['draws_consumed'], 4)
        self.assertGreaterEqual(len(replay['days']), 1)

    def test_calendar_replay_splits_by_day_window(self):
        """盘日窗口内一天结束后不再下注：第 2 期爆仓后，窗口剩余期数全部跳过。"""
        replay = replay_real_draws(_table(), self._draws(), day_boundary='pc28')
        self.assertEqual(replay['windows'], 2)
        self.assertEqual(replay['draws_consumed'], 4)
        reasons = [day['reason'] for day in replay['days']]
        self.assertEqual(reasons[0], 'bust')
        self.assertEqual(reasons[1], 'window_end')

    def test_payload_parser_handles_both_shapes(self):
        standard = parse_draw_payload(json.dumps({'number': '2+7+3'}))
        self.assertEqual(standard, ((2, 7, 3), '2+7+3'))
        bclc = parse_draw_payload(json.dumps({'canada28_num1': 2, 'canada28_num2': 8, 'canada28_num3': 7}))
        self.assertEqual(bclc, ((2, 8, 7), '2+8+7'))
        self.assertIsNone(parse_draw_payload(json.dumps({'num': '7'})))


class SummarizeTests(unittest.TestCase):
    def test_rates_are_consistent(self):
        days = simulate_days(_table(), 2000, seed='summary')
        summary = summarize_days(days)
        self.assertAlmostEqual(summary['tp_rate'] + summary['bust_rate'], 1.0, places=9)
        self.assertEqual(summary['days'], 2000)
        self.assertIn('cumulative_30_days', summary)


if __name__ == '__main__':
    unittest.main()
