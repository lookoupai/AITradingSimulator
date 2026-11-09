from __future__ import annotations

import unittest

from tests.support import create_predictor, fresh_app_harness


class GroupPlayTests(unittest.TestCase):
    def test_execution_export_is_opt_in_and_uses_structured_groups(self):
        with fresh_app_harness() as harness:
            client, user = harness.make_client()
            pid = create_predictor(harness, user, 'pc28', profit_rule_id='pc28_fullpay_2_0')
            harness.db.upsert_prediction({
                'predictor_id': pid, 'lottery_type': 'pc28', 'issue_no': '1001',
                'requested_targets': ['combo'], 'prediction_combo': '大单',
                'prediction_number': None, 'prediction_big_small': None, 'prediction_odd_even': None,
                'status': 'pending', 'confidence': 0.7,
            })
            path = f'/api/export/predictors/{pid}/signals'
            old = client.get(path).get_json()['items'][0]
            self.assertEqual([s['bet_type'] for s in old['signals']], ['combo'])
            response = client.get(path + '?include_group_plays=1')
            self.assertEqual(response.status_code, 200)
            new = response.get_json()['items'][0]
            self.assertEqual(old['signal_id'], new['signal_id'])
            signals = {s['bet_type']: s for s in new['signals']}
            self.assertEqual(set(signals), {'combo', 'double_group', 'kill_group'})
            self.assertEqual(signals['double_group']['bet_value'], '大单+小双')
            self.assertEqual(signals['kill_group']['bet_value'], '小双')
            self.assertEqual(signals['kill_group']['normalized_payload']['selected_combos'], ['大单', '大双', '小单'])
            self.assertEqual(signals['kill_group']['normalized_payload']['source_hints']['settlement']['settlement_rule_id'], 'pc28_fullpay_2_0_regular')
            self.assertEqual(client.get(path + '?include_group_plays=invalid').status_code, 400)

    def test_performance_export_includes_group_coverage(self):
        with fresh_app_harness() as harness:
            client, user = harness.make_client()
            pid = create_predictor(harness, user, 'pc28')
            for issue, actual in [('1001', '大单'), ('1002', '大双'), ('1003', '小双')]:
                harness.db.upsert_prediction({
                    'predictor_id': pid, 'lottery_type': 'pc28', 'issue_no': issue,
                    'requested_targets': ['combo'], 'prediction_combo': '大单', 'actual_combo': actual,
                    'status': 'settled', 'hit_combo': int(actual == '大单'), 'settled_at': '2026-11-09T00:00:00Z',
                })
            data = client.get(f'/api/export/predictors/{pid}/performance').get_json()
            self.assertEqual(data['metrics']['double_group']['recent_20']['hit_count'], 2)
            self.assertEqual(data['metrics']['kill_group']['recent_20']['hit_count'], 2)
            self.assertEqual(data['metrics']['kill_group']['streaks']['current_miss_streak'], 1)
            self.assertEqual(data['metrics']['double_group']['label'], '双组')

    def test_simulation_counts_three_stakes_and_partial_refund(self):
        from services.profit_simulator import ProfitSimulator
        simulator = ProfitSimulator(None)
        policy = simulator._build_bet_strategy('martingale', 10, 2, 5)
        draw = {'result_number': 13, 'combo': '小单', 'open_time': '2026-11-09 12:00:00'}
        record = simulator._build_record({'issue_no': '1001', 'prediction_combo': '大单'}, draw, 'kill_group', 'pc28_fullpay_2_0', 'regular', policy, 1)
        self.assertIsNotNone(record)
        self.assertEqual((record['stake_amount'], record['payout_amount'], record['net_profit']), (30, 10, -20))
        self.assertTrue(record['prediction_hit'])
        self.assertEqual(record['result_type'], 'miss')
        self.assertEqual(len(record['legs']), 3)
        self.assertEqual(simulator._resolve_next_bet_step(policy, 1, record['result_type']), 2)
        winning = simulator._build_record({'issue_no': '1002', 'prediction_combo': '大单'}, {**draw, 'result_number': 15, 'combo': '大单'}, 'double_group', 'pc28_fullpay_2_0', 'regular', policy, 1)
        self.assertEqual((winning['stake_amount'], winning['net_profit']), (20, 23.2))

    def test_group_metrics_only_available_with_combo_prediction(self):
        from services.profit_simulator import ProfitSimulator
        simulator = ProfitSimulator(None)
        for metric in ('double_group', 'kill_group'):
            self.assertIn(metric, simulator.get_available_metrics({'lottery_type': 'pc28', 'prediction_targets': ['combo']}))
            self.assertNotIn(metric, simulator.get_available_metrics({'lottery_type': 'pc28', 'prediction_targets': ['big_small']}))

    def test_high_abc_special_refund_matches_execution_rules(self):
        import json
        from services.profit_simulator import ProfitSimulator
        simulator = ProfitSimulator(None)
        policy = simulator._build_bet_strategy('flat', 10, 2, 5)
        for digits, number, combo in [('0,0,0', 0, '小双'), ('8,9,0', 17, '大单'), ('4,4,5', 13, '小单')]:
            with self.subTest(digits=digits):
                draw = {'result_number': number, 'combo': combo, 'open_time': '2026-11-09 12:00:00', 'source_payload': json.dumps({'number': digits})}
                single = simulator._build_record({'issue_no': '1001', 'prediction_combo': combo}, draw, 'combo', 'pc28_high', 'abc', policy, 1)
                self.assertEqual(single['result_type'], 'refund')
                self.assertEqual(single['net_profit'], 0)
                group = simulator._build_record({'issue_no': '1001', 'prediction_combo': combo}, draw, 'double_group', 'pc28_high', 'abc', policy, 1)
                self.assertTrue(group['prediction_hit'])
                self.assertEqual(group['net_profit'], -10)

    def test_group_primary_and_profit_metric_can_be_saved_with_combo_target(self):
        with fresh_app_harness() as harness:
            client, _ = harness.make_client()
            payload = {
                'name': '杀组方案', 'engine_type': 'ai', 'lottery_type': 'pc28',
                'api_key': 'offline-test', 'api_url': 'https://example.com/v1', 'model_name': 'offline-test',
                'system_prompt': '输出组合预测', 'prediction_targets': ['combo'],
                'primary_metric': 'kill_group', 'profit_default_metric': 'kill_group',
            }
            response = client.post('/api/predictors', json=payload)
            self.assertEqual(response.status_code, 200, response.get_data(as_text=True))
            predictor = response.get_json()['predictor']
            self.assertEqual(predictor['primary_metric'], 'kill_group')
            self.assertEqual(predictor['profit_default_metric'], 'kill_group')
            rejected = client.post('/api/predictors', json={**payload, 'prediction_targets': ['big_small']})
            self.assertEqual(rejected.status_code, 400)

    def test_fractional_group_stake_uses_execution_profit_rounding(self):
        from services.profit_simulator import ProfitSimulator
        simulator = ProfitSimulator(None)
        policy = simulator._build_bet_strategy('flat', 0.15, 2, 5)
        record = simulator._build_record(
            {'issue_no': '1001', 'prediction_combo': '大单'},
            {'result_number': 0, 'combo': '小双', 'open_time': '2026-11-09 12:00:00'},
            'double_group', 'pc28_netdisk', 'abc', policy, 1,
        )
        self.assertEqual((record['stake_amount'], record['payout_amount'], record['net_profit']), (0.3, 0.47, 0.17))


if __name__ == '__main__':
    unittest.main()
