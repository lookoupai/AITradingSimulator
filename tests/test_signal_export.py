from __future__ import annotations

import unittest

from tests.support import create_predictor, fresh_app_harness


class SignalExportTests(unittest.TestCase):
    def test_export_execution_view_for_pc28_predictor(self):
        with fresh_app_harness() as harness:
            client, user_id = harness.make_client()
            predictor_id = create_predictor(harness, user_id, 'pc28')
            harness.db.upsert_prediction({
                'predictor_id': predictor_id,
                'lottery_type': 'pc28',
                'issue_no': '20260408001',
                'requested_targets': ['big_small', 'odd_even', 'combo'],
                'prediction_number': None,
                'prediction_big_small': '大',
                'prediction_odd_even': '单',
                'prediction_combo': '大单',
                'confidence': 0.78,
                'reasoning_summary': '测试说明',
                'raw_response': 'raw',
                'prompt_snapshot': 'prompt',
                'status': 'pending',
                'error_message': None,
                'actual_number': None,
                'actual_big_small': None,
                'actual_odd_even': None,
                'actual_combo': None,
                'hit_number': None,
                'hit_big_small': None,
                'hit_odd_even': None,
                'hit_combo': None,
                'settled_at': None
            })

            response = client.get(f'/api/export/predictors/{predictor_id}/signals?view=execution')
            data = response.get_json()

            self.assertEqual(response.status_code, 200)
            self.assertEqual(data['view'], 'execution')
            self.assertEqual(len(data['items']), 1)
            self.assertEqual(data['items'][0]['source_type'], 'ai_trading_simulator')
            self.assertEqual(data['items'][0]['signals'][0]['bet_type'], 'big_small')
            self.assertEqual(data['items'][0]['signals'][0]['bet_value'], '大')
            self.assertEqual(data['items'][0]['signals'][0]['normalized_payload']['profit_rule_id'], 'pc28_netdisk')
            self.assertEqual(data['items'][0]['signals'][0]['normalized_payload']['odds_profile'], 'regular')

    def test_export_analysis_view_contains_predictor_context(self):
        with fresh_app_harness() as harness:
            client, user_id = harness.make_client()
            predictor_id = create_predictor(harness, user_id, 'pc28', prediction_method='量化策略')
            harness.db.upsert_prediction({
                'predictor_id': predictor_id,
                'lottery_type': 'pc28',
                'issue_no': '20260408002',
                'requested_targets': ['number', 'big_small'],
                'prediction_number': 14,
                'prediction_big_small': '大',
                'prediction_odd_even': None,
                'prediction_combo': None,
                'confidence': 0.66,
                'reasoning_summary': '偏大',
                'raw_response': 'raw-analysis',
                'prompt_snapshot': 'prompt-analysis',
                'status': 'pending',
                'error_message': None,
                'actual_number': None,
                'actual_big_small': None,
                'actual_odd_even': None,
                'actual_combo': None,
                'hit_number': None,
                'hit_big_small': None,
                'hit_odd_even': None,
                'hit_combo': None,
                'settled_at': None
            })

            response = client.get(f'/api/export/predictors/{predictor_id}/signals?view=analysis')
            data = response.get_json()

            self.assertEqual(response.status_code, 200)
            self.assertEqual(data['view'], 'analysis')
            self.assertEqual(len(data['items']), 1)
            self.assertEqual(data['items'][0]['predictor']['prediction_method'], '量化策略')
            self.assertEqual(data['items'][0]['prediction']['prediction_number'], 14)
            self.assertEqual(data['items'][0]['raw']['raw_response'], 'raw-analysis')

    def test_export_performance_view_contains_recent_window_metrics(self):
        with fresh_app_harness() as harness:
            client, user_id = harness.make_client()
            predictor_id = create_predictor(harness, user_id, 'pc28')
            for index in range(100):
                issue_no = f'20260408{index + 1:03d}'
                hit_big_small = 1 if index < 38 else 0
                hit_odd_even = 1 if index < 61 else 0
                hit_combo = 1 if index < 19 else 0
                harness.db.upsert_prediction({
                    'predictor_id': predictor_id,
                    'lottery_type': 'pc28',
                    'issue_no': issue_no,
                    'requested_targets': ['big_small', 'odd_even', 'combo'],
                    'prediction_number': None,
                    'prediction_big_small': '大',
                    'prediction_odd_even': '单',
                    'prediction_combo': '大单',
                    'confidence': 0.7,
                    'reasoning_summary': '测试统计',
                    'raw_response': 'raw',
                    'prompt_snapshot': 'prompt',
                    'status': 'settled',
                    'error_message': None,
                    'actual_number': 27 if hit_big_small else 4,
                    'actual_big_small': '大' if hit_big_small else '小',
                    'actual_odd_even': '单' if hit_odd_even else '双',
                    'actual_combo': '大单' if hit_combo else '小双',
                    'hit_number': None,
                    'hit_big_small': hit_big_small,
                    'hit_odd_even': hit_odd_even,
                    'hit_combo': hit_combo,
                    'settled_at': '2026-04-08T12:00:00Z'
                })

            response = client.get(f'/api/export/predictors/{predictor_id}/performance')
            data = response.get_json()

            self.assertEqual(response.status_code, 200)
            self.assertEqual(data['schema_version'], '1.0')
            self.assertEqual(data['predictor_id'], predictor_id)
            self.assertEqual(data['latest_settled_issue'], '20260408100')
            self.assertEqual(data['metrics']['big_small']['recent_20']['sample_count'], 20)
            self.assertEqual(data['metrics']['big_small']['recent_20']['hit_rate'], 0.0)
            self.assertEqual(data['metrics']['odd_even']['recent_20']['hit_rate'], 0.0)
            self.assertEqual(data['metrics']['combo']['recent_20']['hit_rate'], 0.0)
            self.assertEqual(data['metrics']['big_small']['recent_50']['sample_count'], 50)
            self.assertEqual(data['metrics']['big_small']['recent_50']['hit_rate'], 0.0)
            self.assertEqual(data['metrics']['odd_even']['recent_50']['hit_rate'], 22.0)
            self.assertEqual(data['metrics']['combo']['recent_50']['hit_rate'], 0.0)
            self.assertEqual(data['metrics']['big_small']['recent_100']['sample_count'], 100)
            self.assertEqual(data['metrics']['big_small']['recent_100']['hit_rate'], 38.0)
            self.assertEqual(data['metrics']['odd_even']['recent_100']['hit_rate'], 61.0)
            self.assertEqual(data['metrics']['combo']['recent_100']['hit_rate'], 19.0)
            self.assertEqual(data['metrics']['big_small']['streaks']['current_miss_streak'], 62)
            self.assertEqual(data['metrics']['odd_even']['streaks']['current_miss_streak'], 39)
            self.assertEqual(data['metrics']['combo']['streaks']['current_miss_streak'], 81)
            self.assertEqual(data['metrics']['big_small']['streaks']['recent_100_max_miss_streak'], 62)

    def test_export_signals_rejects_non_pc28_predictor(self):
        with fresh_app_harness() as harness:
            client, user_id = harness.make_client()
            predictor_id = create_predictor(harness, user_id, 'jingcai_football')

            response = client.get(f'/api/export/predictors/{predictor_id}/signals')
            data = response.get_json()

            self.assertEqual(response.status_code, 400)
            self.assertIn('PC28', data['error'])


if __name__ == '__main__':
    unittest.main()
