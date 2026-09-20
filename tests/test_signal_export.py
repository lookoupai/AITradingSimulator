from __future__ import annotations

import unittest

from tests.support import create_predictor, fresh_app_harness


class SignalExportTests(unittest.TestCase):
    def test_export_pc28_best_pair_selects_highest_current_consensus_rate(self):
        with fresh_app_harness() as harness:
            client, user_id = harness.make_client()
            predictor_ids = [
                create_predictor(harness, user_id, 'pc28', name=name)
                for name in ('方案甲', '方案乙', '方案丙')
            ]
            for index in range(10):
                actual_combo = '大双' if index < 3 else '小单'
                for position, predictor_id in enumerate(predictor_ids):
                    prediction_combo = '大双'
                    if position == 2 and index >= 5:
                        prediction_combo = '小双'
                    harness.db.upsert_prediction({
                        'predictor_id': predictor_id,
                        'lottery_type': 'pc28',
                        'issue_no': f'202604070{index}',
                        'requested_targets': ['combo'],
                        'prediction_number': None,
                        'prediction_big_small': prediction_combo[0],
                        'prediction_odd_even': prediction_combo[1],
                        'prediction_combo': prediction_combo,
                        'confidence': 0.7,
                        'reasoning_summary': '方案对历史样本',
                        'raw_response': '',
                        'prompt_snapshot': '',
                        'status': 'settled',
                        'error_message': None,
                        'actual_number': None,
                        'actual_big_small': actual_combo[0],
                        'actual_odd_even': actual_combo[1],
                        'actual_combo': actual_combo,
                        'hit_number': None,
                        'hit_big_small': int(prediction_combo[0] == actual_combo[0]),
                        'hit_odd_even': int(prediction_combo[1] == actual_combo[1]),
                        'hit_combo': int(prediction_combo == actual_combo),
                        'settled_at': '2026-07-29T00:00:00Z',
                    })

            for predictor_id in predictor_ids:
                harness.db.upsert_prediction({
                    'predictor_id': predictor_id,
                    'lottery_type': 'pc28',
                    'issue_no': '20260730001',
                    'requested_targets': ['combo'],
                    'prediction_number': None,
                    'prediction_big_small': '大',
                    'prediction_odd_even': '双',
                    'prediction_combo': '大双',
                    'confidence': 0.7,
                    'reasoning_summary': '方案对待发信号',
                    'raw_response': '',
                    'prompt_snapshot': '',
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
                    'settled_at': None,
                })

            response = client.get(
                '/api/export/consensus/pc28/best-pair-signals'
                f'?predictor_ids={",".join(map(str, predictor_ids))}'
                '&min_historical_rate=28&min_historical_sample=5&window=30'
            )
            data = response.get_json()

            self.assertEqual(response.status_code, 200)
            self.assertEqual(data['selection_mode'], 'best_consensus_pair')
            self.assertEqual(
                data['selected_pair']['predictor_ids'],
                [predictor_ids[0], predictor_ids[2]],
            )
            self.assertEqual(data['selected_pair']['historical_rate'], 60.0)
            self.assertEqual(
                [item['predictor_ids'] for item in data['qualified_pairs']],
                [
                    [predictor_ids[0], predictor_ids[2]],
                    [predictor_ids[1], predictor_ids[2]],
                    [predictor_ids[0], predictor_ids[1]],
                ],
            )
            signal = data['items'][0]['signals'][0]
            self.assertEqual(signal['bet_type'], 'combo')
            self.assertEqual(signal['bet_value'], '大双')
            self.assertEqual(
                signal['normalized_payload']['supporter_predictor_ids'],
                [predictor_ids[0], predictor_ids[2]],
            )

    def test_export_pc28_best_pair_skips_when_no_pair_rate_qualifies(self):
        with fresh_app_harness() as harness:
            client, user_id = harness.make_client()
            predictor_ids = [
                create_predictor(harness, user_id, 'pc28', name=f'未达标方案{i}')
                for i in range(2)
            ]
            for index in range(10):
                actual_combo = '大双' if index < 2 else '小单'
                for predictor_id in predictor_ids:
                    harness.db.upsert_prediction({
                        'predictor_id': predictor_id,
                        'lottery_type': 'pc28',
                        'issue_no': f'202604080{index}',
                        'requested_targets': ['combo'],
                        'prediction_number': None,
                        'prediction_big_small': '大',
                        'prediction_odd_even': '双',
                        'prediction_combo': '大双',
                        'confidence': 0.7,
                        'reasoning_summary': '未达门槛历史样本',
                        'raw_response': '',
                        'prompt_snapshot': '',
                        'status': 'settled',
                        'error_message': None,
                        'actual_number': None,
                        'actual_big_small': actual_combo[0],
                        'actual_odd_even': actual_combo[1],
                        'actual_combo': actual_combo,
                        'hit_number': None,
                        'hit_big_small': int(actual_combo[0] == '大'),
                        'hit_odd_even': int(actual_combo[1] == '双'),
                        'hit_combo': int(actual_combo == '大双'),
                        'settled_at': '2026-07-29T00:00:00Z',
                    })

            for predictor_id in predictor_ids:
                harness.db.upsert_prediction({
                    'predictor_id': predictor_id,
                    'lottery_type': 'pc28',
                    'issue_no': '20260730001',
                    'requested_targets': ['combo'],
                    'prediction_number': None,
                    'prediction_big_small': '大',
                    'prediction_odd_even': '双',
                    'prediction_combo': '大双',
                    'confidence': 0.7,
                    'reasoning_summary': '未达门槛待发信号',
                    'raw_response': '',
                    'prompt_snapshot': '',
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
                    'settled_at': None,
                })

            response = client.get(
                '/api/export/consensus/pc28/best-pair-signals'
                f'?predictor_ids={",".join(map(str, predictor_ids))}'
                '&min_historical_rate=28&min_historical_sample=10&window=30'
            )
            data = response.get_json()

            self.assertEqual(response.status_code, 200)
            self.assertEqual(data['qualified_pairs'], [])
            self.assertEqual(data['items'], [])

    def test_export_pc28_consensus_execution_signal(self):
        with fresh_app_harness() as harness:
            client, user_id = harness.make_client()
            predictor_ids = [
                create_predictor(harness, user_id, 'pc28', name=f'共识方案{i}')
                for i in range(3)
            ]
            for predictor_id, value in zip(predictor_ids, ['双', '双', '单']):
                harness.db.upsert_prediction({
                    'predictor_id': predictor_id,
                    'lottery_type': 'pc28',
                    'issue_no': '20260408009',
                    'requested_targets': ['odd_even'],
                    'prediction_number': None,
                    'prediction_big_small': None,
                    'prediction_odd_even': value,
                    'prediction_combo': None,
                    'confidence': 0.7,
                    'reasoning_summary': '共识测试',
                    'raw_response': '',
                    'prompt_snapshot': '',
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

            response = client.get(
                '/api/export/consensus/pc28/signals'
                f'?predictor_ids={",".join(map(str, predictor_ids))}&field=odd_even&min_agreement=2'
            )
            data = response.get_json()

            self.assertEqual(response.status_code, 200)
            self.assertEqual(data['export_type'], 'consensus_execution')
            self.assertEqual(len(data['items']), 1)
            item = data['items'][0]
            self.assertEqual(item['issue_no'], '20260408009')
            self.assertEqual(item['signals'][0]['bet_type'], 'odd_even')
            self.assertEqual(item['signals'][0]['bet_value'], '双')
            self.assertEqual(item['source_ref']['supporter_predictor_ids'], predictor_ids[:2])

    def test_export_pc28_consensus_applies_historical_rate_and_sample_gates(self):
        with fresh_app_harness() as harness:
            client, user_id = harness.make_client()
            predictor_ids = [
                create_predictor(harness, user_id, 'pc28', name=f'组合门槛方案{i}')
                for i in range(3)
            ]

            for index, actual_combo in enumerate(['大双', '小双', '大双', '小单'], start=1):
                issue_no = f'2026040800{index}'
                for predictor_id in predictor_ids:
                    harness.db.upsert_prediction({
                        'predictor_id': predictor_id,
                        'lottery_type': 'pc28',
                        'issue_no': issue_no,
                        'requested_targets': ['combo'],
                        'prediction_number': None,
                        'prediction_big_small': '大',
                        'prediction_odd_even': '双',
                        'prediction_combo': '大双',
                        'confidence': 0.7,
                        'reasoning_summary': '组合门槛历史样本',
                        'raw_response': '',
                        'prompt_snapshot': '',
                        'status': 'settled',
                        'error_message': None,
                        'actual_number': None,
                        'actual_big_small': actual_combo[0],
                        'actual_odd_even': actual_combo[1],
                        'actual_combo': actual_combo,
                        'hit_number': None,
                        'hit_big_small': int(actual_combo[0] == '大'),
                        'hit_odd_even': int(actual_combo[1] == '双'),
                        'hit_combo': int(actual_combo == '大双'),
                        'settled_at': f'2026-04-08T00:0{index}:00Z',
                    })

            for predictor_id in predictor_ids:
                harness.db.upsert_prediction({
                    'predictor_id': predictor_id,
                    'lottery_type': 'pc28',
                    'issue_no': '20260408009',
                    'requested_targets': ['combo'],
                    'prediction_number': None,
                    'prediction_big_small': '大',
                    'prediction_odd_even': '双',
                    'prediction_combo': '大双',
                    'confidence': 0.7,
                    'reasoning_summary': '组合门槛待发信号',
                    'raw_response': '',
                    'prompt_snapshot': '',
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
                    'settled_at': None,
                })

            query = (
                '/api/export/consensus/pc28/signals'
                f'?predictor_ids={",".join(map(str, predictor_ids))}'
                '&field=combo&min_agreement=3&min_historical_rate=50'
                '&min_historical_sample=4'
            )
            response = client.get(query)
            data = response.get_json()

            self.assertEqual(response.status_code, 200)
            self.assertEqual(data['min_historical_rate'], 50.0)
            self.assertEqual(data['min_historical_sample'], 4)
            self.assertEqual(len(data['items']), 1)
            signal = data['items'][0]['signals'][0]
            self.assertEqual(signal['bet_type'], 'combo')
            self.assertEqual(signal['bet_value'], '大双')
            self.assertEqual(signal['normalized_payload']['historical_rate'], 50.0)
            self.assertEqual(signal['normalized_payload']['historical_sample'], 4)

            filtered = client.get(query.replace('min_historical_rate=50', 'min_historical_rate=60')).get_json()
            self.assertEqual(filtered['items'], [])

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

    def test_export_execution_view_maps_fullpay_rule_to_legacy_id(self):
        with fresh_app_harness() as harness:
            client, user_id = harness.make_client()
            predictor_id = create_predictor(harness, user_id, 'pc28', profit_rule_id='pc28_fullpay_2_8')
            harness.db.upsert_prediction({
                'predictor_id': predictor_id,
                'lottery_type': 'pc28',
                'issue_no': '20260408003',
                'requested_targets': ['big_small'],
                'prediction_number': None,
                'prediction_big_small': '大',
                'prediction_odd_even': None,
                'prediction_combo': None,
                'confidence': 0.7,
                'reasoning_summary': '满赔导出映射',
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

            execution = client.get(f'/api/export/predictors/{predictor_id}/signals?view=execution').get_json()
            analysis = client.get(f'/api/export/predictors/{predictor_id}/signals?view=analysis').get_json()

            self.assertEqual(
                execution['items'][0]['signals'][0]['normalized_payload']['profit_rule_id'],
                'pc28_high'
            )
            self.assertEqual(analysis['items'][0]['predictor']['profit_rule_id'], 'pc28_fullpay_2_8')

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
