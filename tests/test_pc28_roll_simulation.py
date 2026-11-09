from __future__ import annotations

import unittest
import json
import random
from datetime import datetime, timedelta
from unittest.mock import patch

from tests.support import create_predictor, fresh_app_harness


NOW = datetime(2026, 11, 10, 18, 0)


class ReplayDB:
    def __init__(self, totals, *, times=None, combos=None):
        self.predictor = {'id': 1, 'lottery_type': 'pc28', 'prediction_targets': ['combo'],
                          'profit_rule_id': 'pc28_fullpay_netdisk'}
        self.predictions = []
        self.draws = {}
        for index, total in enumerate(totals):
            moment = times[index] if times else NOW.replace(hour=12, minute=index)
            issue = str(1000 + index)
            odd = '单' if total % 2 else '双'
            combo = ('大' if total >= 14 else '小') + odd
            triplet = next((a, b, total-a-b) for a in range(10) for b in range(10) if 0 <= total-a-b <= 9)
            self.predictions.append({'issue_no': issue, 'status': 'settled', 'prediction_combo': combos[index] if combos else '小单',
                                     'created_at': (moment-timedelta(hours=8, minutes=1)).isoformat()})
            self.draws[issue] = {'issue_no': issue, 'open_time': moment.isoformat(), 'result_number': total,
                                'combo': combo, 'odd_even': odd, 'big_small': combo[0],
                                'source_payload': json.dumps({'number': '+'.join(map(str, triplet))})}

    def get_predictor(self, *args, **kwargs):
        return self.predictor

    def get_recent_predictions(self, *args, **kwargs):
        return self.predictions

    def get_pc28_simulation_predictions(self, *args, **kwargs):
        return self.predictions

    def get_draws_by_issues(self, *args):
        return self.draws


class RollReplayTests(unittest.TestCase):
    def simulate(self, db, **config):
        from services.profit_simulator import ProfitSimulator
        with patch('services.profit_simulator.get_current_beijing_time', return_value=NOW):
            return ProfitSimulator(db).build_profit_simulation(1, bet_mode='win_streak_roll', roll_config=config)

    def test_three_profitable_periods_stop_before_fourth(self):
        result = self.simulate(ReplayDB([5, 7, 9, 11]))
        self.assertEqual(len(result['records']), 3)
        self.assertEqual(result['summary']['target_cycle_count'], 1)
        self.assertEqual(result['records'][-1]['win_streak'], 3)
        self.assertEqual(result['summary']['net_profit'], 168.98)

    def test_integer_split_and_unbet_remainder(self):
        result = self.simulate(ReplayDB([5]), base_amount=101.75, roll_mode='all_in')
        row = result['records'][0]
        self.assertEqual([leg['stake_amount'] for leg in row['legs']], [70, 31])
        self.assertEqual(row['balance_after'], 140.05)
        self.assertEqual(row['net_profit'], 38.3)

    def test_partial_refund_is_loss_and_resets_streak(self):
        result = self.simulate(ReplayDB([5, 27]))
        row = result['records'][1]
        self.assertEqual(row['legs'][0]['result_type'], 'refund')
        self.assertEqual(row['net_profit'], -42)
        self.assertEqual(row['win_streak'], 0)
        self.assertEqual(row['next_stake'], 97)

    def test_neutral_period_holds_streak(self):
        db = ReplayDB([0, 5, 0])
        db.predictor['profit_rule_id'] = 'pc28_fullpay_2_0'
        result = self.simulate(db, main_share=0.5, target_streak=2)
        self.assertEqual([row['win_streak'] for row in result['records']], [1, 1, 2])
        self.assertEqual(result['records'][1]['net_profit'], 0)

    def test_odds_tier_uses_actual_stake(self):
        db = ReplayDB([13])
        db.predictor['profit_rule_id'] = 'pc28_fullpay_2_0'
        row = self.simulate(db, base_amount=3000)['records'][0]
        self.assertEqual(row['legs'][0]['odds'], 1.98)
        self.assertEqual(row['payout_amount'], 4158)

    def test_follow_signal_changes_structure(self):
        result = self.simulate(ReplayDB([5, 16], combos=['小单', '大双']))
        self.assertEqual([row['structure'] for row in result['records']], ['A', 'D'])

    def test_fixed_structure_does_not_require_combo(self):
        db = ReplayDB([5], combos=[None])
        db.predictor['prediction_targets'] = ['big_small']
        self.assertEqual(self.simulate(db, structure='A')['summary']['bet_count'], 1)
        with self.assertRaises(ValueError):
            self.simulate(db)

    def test_missing_late_and_unknown_time_predictions_are_skipped(self):
        db = ReplayDB([5, 5, 5, 5])
        db.predictions[0]['prediction_combo'] = None
        db.predictions[1]['created_at'] = '2026-11-10T04:01:00Z'
        db.predictions[2]['created_at'] = None
        result = self.simulate(db)
        self.assertEqual(result['summary']['bet_count'], 1)
        self.assertEqual(result['summary']['skipped_count'], 3)

    def test_bust_restart_funding_is_not_profit(self):
        result = self.simulate(ReplayDB([16, 16, 5]), on_bust='new_cycle', max_cycles_per_day=2)
        summary = result['summary']
        self.assertEqual(summary['bet_count'], 2)
        self.assertEqual(summary['bust_cycle_count'], 2)
        self.assertEqual(summary['total_funding'], 200)
        self.assertEqual(summary['net_profit'], -200)
        self.assertEqual(summary['ending_balance'], 0)

    def test_target_restart_all_in_withdrawal_preserves_profit(self):
        summary = self.simulate(ReplayDB([5, 5]), roll_mode='all_in', target_streak=1,
                                on_target='new_cycle', max_cycles_per_day=2)['summary']
        self.assertEqual(summary['net_profit'], 78.6)
        self.assertEqual(summary['total_funding'], 200)
        self.assertEqual(summary['total_withdrawn'], 139.3)
        self.assertAlmostEqual(summary['ending_balance'] + summary['total_withdrawn'] - summary['total_funding'], summary['net_profit'])

    def test_cross_day_restarts_stake_and_retains_chain_balance(self):
        times = [NOW.replace(day=9, hour=23), NOW.replace(hour=0)]
        result = self.simulate(ReplayDB([5, 5], times=times), days=7)
        self.assertEqual([row['stake_amount'] for row in result['records']], [100, 100])
        self.assertEqual(result['summary']['ending_balance'], 178.6)
        self.assertEqual(result['summary']['total_funding'], 100)

    def test_pc28_day_keeps_stake_across_midnight(self):
        times = [NOW.replace(day=9, hour=23), NOW.replace(hour=0)]
        result = self.simulate(ReplayDB([5, 5], times=times), day_boundary='pc28')
        self.assertEqual([row['stake_amount'] for row in result['records']], [100, 139])

    def test_invalid_config_is_rejected(self):
        for config in ({'base_amount': 'nan'}, {'base_amount': 'inf'}, {'base_amount': 1},
                       {'main_share': 1}, {'target_streak': 1.5}, {'structure': 'bad'},
                       {'on_bust': 'wait_condition'}, {'max_cycles_per_day': 1001}, {'days': 365},
                       {'roll_mode': 'bad'}, {'day_boundary': 'bad'}):
            with self.subTest(config=config), self.assertRaises(ValueError):
                self.simulate(ReplayDB([]), **config)

    def test_summary_calculation_matches_detail_calculation(self):
        from services.profit_simulator import ProfitSimulator
        db = ReplayDB([5, 27, 0, 16])
        with patch('services.profit_simulator.get_current_beijing_time', return_value=NOW):
            summary = ProfitSimulator(db).build_profit_simulation(1, bet_mode='win_streak_roll', include_records=False)
        full = self.simulate(db)
        self.assertEqual(summary['records'], [])
        self.assertEqual(summary['summary'], full['summary'])

    def test_empty_sample_has_no_fictional_funding(self):
        result = self.simulate(ReplayDB([]))
        self.assertEqual(result['summary']['total_funding'], 0)
        self.assertEqual(result['summary']['cycle_count'], 0)

    def test_high_odds_require_valid_triplet(self):
        db = ReplayDB([5])
        db.predictor['profit_rule_id'] = 'pc28_fullpay_2_8'
        db.draws['1000']['source_payload'] = '{}'
        result = self.simulate(db)
        self.assertEqual(result['summary']['bet_count'], 0)
        self.assertEqual(result['skip_reasons']['invalid_draw'], 1)

    def test_external_prediction_must_have_arrived_before_draw(self):
        db = ReplayDB([5])
        db.predictions[0]['external_fetched_at'] = '2026-11-10T05:00:00Z'
        self.assertEqual(self.simulate(db)['summary']['bet_count'], 0)

    def test_all_in_daily_reset_is_accounted_as_withdrawal_and_funding(self):
        times = [NOW.replace(day=9, hour=23), NOW.replace(hour=0)]
        summary = self.simulate(ReplayDB([5, 5], times=times), days=7, roll_mode='all_in')['summary']
        self.assertEqual(summary['total_funding'], 200)
        self.assertEqual(summary['total_withdrawn'], 139.3)
        self.assertEqual(summary['net_profit'], 78.6)

    def test_cycle_cap_includes_first_cycle(self):
        result = self.simulate(ReplayDB([5, 5, 5]), target_streak=1, on_target='new_cycle', max_cycles_per_day=2)
        self.assertEqual(result['summary']['cycle_count'], 2)
        self.assertEqual(result['summary']['target_cycle_count'], 2)

    def test_small_positive_payout_cannot_make_two_integer_bets(self):
        result = self.simulate(ReplayDB([27, 5]), base_amount=2, main_share=0.5)
        self.assertEqual(result['summary']['bet_count'], 1)
        self.assertEqual(result['summary']['bust_cycle_count'], 1)
        self.assertEqual(result['summary']['ending_balance'], 1)

    def test_cash_conservation_across_multiple_days_and_restarts(self):
        rng = random.Random(20261110)
        totals = [rng.randrange(28) for _ in range(40)]
        times = [NOW-timedelta(days=8-i//5, minutes=40-i%5) for i in range(40)]
        for mode in ('chain', 'all_in'):
            for amount in (2, 101.75, 3000):
                with self.subTest(mode=mode, amount=amount):
                    result = self.simulate(ReplayDB(totals, times=times), roll_mode=mode, base_amount=amount,
                                           days=30, on_target='new_cycle', on_bust='new_cycle', max_cycles_per_day=3)
                    s = result['summary']
                    self.assertAlmostEqual(s['ending_balance'] + s['total_withdrawn'] - s['total_funding'], s['net_profit'])
                    self.assertAlmostEqual(sum(row['net_profit'] for row in result['records']), s['net_profit'])
                    for row in result['records']:
                        self.assertEqual(row['stake_amount'], sum(leg['stake_amount'] for leg in row['legs']))
                        self.assertLessEqual(row['day_cycle_number'], 3)


class RollApiTests(unittest.TestCase):
    def seed(self, harness, user_id, **overrides):
        pid = create_predictor(harness, user_id, 'pc28', profit_rule_id='pc28_fullpay_netdisk', **overrides)
        db = ReplayDB([5, 7, 9, 16])
        for draw in db.draws.values():
            draw.update(draw_date='2026-11-10', draw_time=draw['open_time'][11:19], result_number_text=str(draw['result_number']))
        harness.db.upsert_draws('pc28', list(db.draws.values()))
        for row in db.predictions:
            harness.db.upsert_prediction({**row, 'predictor_id': pid})
            conn = harness.db.get_connection()
            conn.execute('UPDATE predictions SET created_at = ? WHERE predictor_id = ? AND issue_no = ?', (row['created_at'], pid, row['issue_no']))
            conn.commit()
            conn.close()
        return pid

    def test_roll_mode_is_not_silently_simulated_as_flat(self):
        with fresh_app_harness() as harness:
            client, user_id = harness.make_client()
            predictor_id = create_predictor(harness, user_id, 'pc28')
            response = client.get(f'/api/predictors/{predictor_id}/simulation?bet_mode=win_streak_roll')
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json['simulation']['bet_mode'], 'win_streak_roll')

    def test_server_advertises_roll_support_to_both_pages(self):
        with fresh_app_harness() as h:
            client, uid = h.make_client()
            pid = create_predictor(h, uid, 'pc28')
            for url in (f'/api/predictors/{pid}/dashboard', f'/api/public/predictors/{pid}'):
                response = client.get(url)
                self.assertEqual(response.status_code, 200)
                modes = response.json['predictor'].get('simulation_bet_modes', [])
                self.assertIn('win_streak_roll', [item['key'] for item in modes])

    def test_query_parameters_reach_replay(self):
        with fresh_app_harness() as h:
            client, uid = h.make_client()
            pid = self.seed(h, uid)
            with patch('services.profit_simulator.get_current_beijing_time', return_value=NOW):
                response = client.get(f'/api/predictors/{pid}/simulation', query_string={
                    'bet_mode': 'win_streak_roll', 'roll_mode': 'all_in', 'roll_base_amount': '101.75',
                    'roll_main_share': '0.5', 'roll_target_streak': '1', 'roll_structure': 'A',
                    'roll_days': '7', 'roll_day_boundary': 'pc28', 'roll_on_target': 'new_cycle',
                    'roll_on_bust': 'new_cycle', 'roll_max_cycles_per_day': '2',
                })
            self.assertEqual(response.status_code, 200, response.json)
            result = response.json['simulation']
            self.assertEqual(result['roll_config']['roll_mode'], 'all_in')
            self.assertEqual(result['roll_config']['base_amount'], 101.75)
            self.assertEqual(result['roll_config']['days'], 7)
            self.assertEqual(result['records'][0]['legs'][0]['stake_amount'], 50)

    def test_invalid_parameters_return_400_in_both_routes(self):
        with fresh_app_harness() as h:
            client, uid = h.make_client()
            pid = create_predictor(h, uid, 'pc28')
            for prefix in ('/api/predictors', '/api/public/predictors'):
                for key, value in (('roll_target_streak', '2.5'), ('roll_base_amount', 'nan'),
                                   ('roll_main_share', 'inf'), ('roll_max_cycles_per_day', '1001'),
                                   ('roll_mode', 'bad'), ('roll_structure', '<script>')):
                    with self.subTest(prefix=prefix, key=key):
                        response = client.get(f'{prefix}/{pid}/simulation', query_string={'bet_mode': 'win_streak_roll', key: value})
                        self.assertEqual(response.status_code, 400, response.json)

    def test_private_authorization_and_public_summary_privacy(self):
        with fresh_app_harness() as h:
            client, uid = h.make_client()
            pid = self.seed(h, uid, share_level='stats_only')
            other, _ = h.make_client('other')
            self.assertEqual(other.get(f'/api/predictors/{pid}/simulation?bet_mode=win_streak_roll').status_code, 403)
            anon = h.app.test_client()
            self.assertEqual(anon.get(f'/api/predictors/{pid}/simulation?bet_mode=win_streak_roll').status_code, 401)
            conn = h.db.get_connection()
            before = list(conn.iterdump())
            conn.close()
            with patch('services.profit_simulator.get_current_beijing_time', return_value=NOW):
                response = anon.get(f'/api/public/predictors/{pid}/simulation?bet_mode=win_streak_roll')
            self.assertEqual(response.status_code, 200, response.json)
            data = response.json
            self.assertFalse(data['can_view_records'])
            self.assertEqual(data['simulation']['records'], [])
            self.assertEqual(data['simulation']['summary']['bet_count'], 3)
            self.assertNotIn('legs', json.dumps(data))
            self.assertNotIn('1000', json.dumps(data))
            conn = h.db.get_connection()
            self.assertEqual(list(conn.iterdump()), before, '模拟请求不得写入数据库')
            conn.close()

    def test_football_rejects_roll(self):
        with fresh_app_harness() as h:
            client, uid = h.make_client()
            pid = create_predictor(h, uid, 'jingcai_football')
            self.assertEqual(client.get(f'/api/predictors/{pid}/simulation?bet_mode=win_streak_roll').status_code, 400)

    def test_unknown_roll_parameters_and_profile_mix_are_rejected(self):
        with fresh_app_harness() as h:
            client, uid = h.make_client()
            pid = create_predictor(h, uid, 'pc28')
            for extra in ({'roll_roll_mode': 'all_in'}, {'bet_profile_id': 'abc'}, {'bet_profile_id': '1'}):
                response = client.get(f'/api/predictors/{pid}/simulation', query_string={'bet_mode': 'win_streak_roll', **extra})
                self.assertEqual(response.status_code, 400, response.json)


if __name__ == '__main__':
    unittest.main()
