from __future__ import annotations

import unittest
from unittest import mock

from tests.support import create_predictor, fresh_app_harness


def build_pc28_draw(issue_no: str, result_number: int) -> dict:
    big_small = '小' if result_number <= 13 else '大'
    odd_even = '双' if result_number % 2 == 0 else '单'
    combo = f'{big_small}{odd_even}'
    return {
        'issue_no': issue_no,
        'result_number': result_number,
        'result_number_text': f'{result_number:02d}',
        'big_small': big_small,
        'odd_even': odd_even,
        'combo': combo
    }


class MachinePredictorRouteTests(unittest.TestCase):
    def _build_football_match(self, with_detail: bool = False) -> dict:
        match = {
            'event_key': 'T001',
            'match_no': '周五001',
            'event_name': '[英超] 主队 vs 客队',
            'league': '英超',
            'home_team': '主队',
            'away_team': '客队',
            'team1_id': 'home-1',
            'team2_id': 'away-1',
            'match_time': '2026-04-24 20:00',
            'show_sell_status': '1',
            'spf_odds': {'胜': 1.62, '平': 3.75, '负': 5.20},
            'rqspf': {
                'handicap': -1,
                'handicap_text': '-1',
                'odds': {'胜': 2.95, '平': 3.35, '负': 2.10}
            }
        }
        if not with_detail:
            return match

        match['detail_bundle'] = {
            'odds_snapshots': {
                'euro': {
                    'company': '竞彩官方',
                    'initial': {'win': '1.72', 'draw': '3.55', 'lose': '4.60'},
                    'current': {'win': '1.62', 'draw': '3.75', 'lose': '5.20'}
                },
                'asia': {
                    'company': '澳门',
                    'initial': {'line': '半球', 'home': '0.92', 'away': '0.94'},
                    'current': {'line': '半一', 'home': '0.90', 'away': '0.96'}
                }
            },
            'team_table': {
                'team1': {
                    'items': {
                        'all': [{'position': '2', 'points': '68'}],
                        'home': [{'won': '10', 'draw': '2', 'loss': '1'}]
                    }
                },
                'team2': {
                    'items': {
                        'all': [{'position': '9', 'points': '46'}],
                        'away': [{'won': '3', 'draw': '3', 'loss': '7'}]
                    }
                }
            },
            'recent_form_team1': [
                {'team1': '主队', 'team2': '甲', 'team1Id': 'home-1', 'team2Id': 'x1', 'score1': '3', 'score2': '1'},
                {'team1': '乙', 'team2': '主队', 'team1Id': 'x2', 'team2Id': 'home-1', 'score1': '0', 'score2': '2'},
                {'team1': '主队', 'team2': '丙', 'team1Id': 'home-1', 'team2Id': 'x3', 'score1': '2', 'score2': '0'}
            ],
            'recent_form_team2': [
                {'team1': '客队', 'team2': '丁', 'team1Id': 'away-1', 'team2Id': 'y1', 'score1': '0', 'score2': '1'},
                {'team1': '戊', 'team2': '客队', 'team1Id': 'y2', 'team2Id': 'away-1', 'score1': '2', 'score2': '1'},
                {'team1': '客队', 'team2': '己', 'team1Id': 'away-1', 'team2Id': 'y3', 'score1': '1', 'score2': '1'}
            ],
            'injury': {
                'team1': [],
                'team2': [
                    {'playerShortName': '客队主力中卫', 'typeCn': '伤'},
                    {'playerShortName': '客队主力后腰', 'typeCn': '停'}
                ]
            }
        }
        return match

    def _build_value_edge_match(self) -> dict:
        match = {
            'event_key': 'T009',
            'match_no': '周五009',
            'event_name': '[西甲] 强队 vs 弱队',
            'league': '西甲',
            'home_team': '强队',
            'away_team': '弱队',
            'team1_id': 'strong-home',
            'team2_id': 'weak-away',
            'match_time': '2026-04-24 22:00',
            'show_sell_status': '1',
            'spf_odds': {'胜': 1.34, '平': 4.85, '负': 8.80},
            'rqspf': {
                'handicap': -1,
                'handicap_text': '-1',
                'odds': {'胜': 2.35, '平': 3.35, '负': 2.45}
            },
            'detail_bundle': {
                'odds_snapshots': {
                    'euro': {
                        'company': '竞彩官方',
                        'initial': {'win': '1.42', 'draw': '4.40', 'lose': '7.20'},
                        'current': {'win': '1.34', 'draw': '4.85', 'lose': '8.80'}
                    },
                    'asia': {
                        'company': '澳门',
                        'initial': {'line': '半一', 'home': '0.96', 'away': '0.90'},
                        'current': {'line': '一球', 'home': '0.92', 'away': '0.98'}
                    }
                },
                'team_table': {
                    'team1': {
                        'items': {
                            'all': [{'position': '1', 'points': '76'}],
                            'home': [{'won': '11', 'draw': '1', 'loss': '1'}]
                        }
                    },
                    'team2': {
                        'items': {
                            'all': [{'position': '14', 'points': '35'}],
                            'away': [{'won': '2', 'draw': '4', 'loss': '7'}]
                        }
                    }
                },
                'recent_form_team1': [
                    {'team1': '强队', 'team2': '甲', 'team1Id': 'strong-home', 'team2Id': 'a1', 'score1': '3', 'score2': '0'},
                    {'team1': '乙', 'team2': '强队', 'team1Id': 'a2', 'team2Id': 'strong-home', 'score1': '1', 'score2': '2'},
                    {'team1': '强队', 'team2': '丙', 'team1Id': 'strong-home', 'team2Id': 'a3', 'score1': '2', 'score2': '0'}
                ],
                'recent_form_team2': [
                    {'team1': '弱队', 'team2': '丁', 'team1Id': 'weak-away', 'team2Id': 'b1', 'score1': '0', 'score2': '1'},
                    {'team1': '戊', 'team2': '弱队', 'team1Id': 'b2', 'team2Id': 'weak-away', 'score1': '2', 'score2': '0'},
                    {'team1': '弱队', 'team2': '己', 'team1Id': 'weak-away', 'team2Id': 'b3', 'score1': '1', 'score2': '1'}
                ],
                'injury': {
                    'team1': [],
                    'team2': [{'playerShortName': '弱队主力前锋', 'typeCn': '伤'}]
                }
            }
        }
        return match

    def test_create_machine_predictor_without_ai_credentials(self):
        with fresh_app_harness() as harness:
            client, _ = harness.make_client()

            response = client.post('/api/predictors', json={
                'lottery_type': 'pc28',
                'engine_type': 'machine',
                'algorithm_key': 'pc28_frequency_v1',
                'name': 'pc28-machine',
                'prediction_method': '频次趋势',
                'api_key': '',
                'api_url': '',
                'model_name': '',
                'api_mode': 'auto',
                'primary_metric': 'big_small',
                'profit_default_metric': 'big_small',
                'profit_rule_id': 'pc28_netdisk',
                'share_level': 'records',
                'system_prompt': '',
                'data_injection_mode': 'summary',
                'prediction_targets': ['number', 'big_small', 'odd_even', 'combo'],
                'history_window': 30,
                'temperature': 0.2,
                'enabled': True
            })

            self.assertEqual(response.status_code, 200)
            payload = response.get_json()
            predictor = payload['predictor']
            self.assertEqual(predictor['engine_type'], 'machine')
            self.assertEqual(predictor['algorithm_key'], 'pc28_frequency_v1')
            self.assertEqual(predictor['algorithm_label'], '频次趋势 V1')
            self.assertEqual(predictor['execution_label'], '频次趋势 V1')
            self.assertIn('加权频次', predictor['algorithm_description'])
            self.assertIn('加权频次', predictor['execution_description'])

    def test_machine_predictor_test_endpoint_skips_model_connectivity(self):
        with fresh_app_harness() as harness:
            client, _ = harness.make_client()

            response = client.post('/api/predictors/test', json={
                'lottery_type': 'jingcai_football',
                'engine_type': 'machine',
                'algorithm_key': 'football_odds_baseline_v1',
                'name': 'football-machine',
                'prediction_method': '赔率基线',
                'api_key': '',
                'api_url': '',
                'model_name': '',
                'api_mode': 'auto',
                'primary_metric': 'spf',
                'profit_default_metric': 'spf',
                'profit_rule_id': 'jingcai_snapshot',
                'share_level': 'records',
                'system_prompt': '',
                'data_injection_mode': 'summary',
                'prediction_targets': ['spf', 'rqspf'],
                'history_window': 20,
                'temperature': 0.2,
                'enabled': True
            })

            self.assertEqual(response.status_code, 200)
            payload = response.get_json()
            self.assertEqual(payload['finish_reason'], 'builtin_machine')
            self.assertEqual(payload['response_model'], '赔率基线 V1')

    def test_predict_now_runs_pc28_machine_algorithm(self):
        with fresh_app_harness() as harness:
            client, user_id = harness.make_client()
            predictor_id = create_predictor(
                harness,
                user_id,
                'pc28',
                engine_type='machine',
                algorithm_key='pc28_frequency_v1',
                api_key='',
                api_url='',
                model_name='',
                system_prompt=''
            )
            draws = [
                build_pc28_draw('20260424001', 14),
                build_pc28_draw('20260424000', 18),
                build_pc28_draw('20260423999', 14),
                build_pc28_draw('20260423998', 12),
                build_pc28_draw('20260423997', 14),
                build_pc28_draw('20260423996', 16),
                build_pc28_draw('20260423995', 11),
                build_pc28_draw('20260423994', 14),
                build_pc28_draw('20260423993', 18),
                build_pc28_draw('20260423992', 14)
            ]
            context = {
                'latest_draw': draws[0],
                'next_issue_no': '20260424002',
                'countdown': '00:01:00',
                'recent_draws': draws,
                'omission_preview': {},
                'today_preview': {},
                'preview': {}
            }

            with mock.patch.object(harness.module.pc28_service, 'sync_recent_draws', return_value=draws), \
                 mock.patch.object(harness.module.prediction_engine, '_build_context', return_value=context):
                response = client.post(f'/api/predictors/{predictor_id}/predict-now')

            self.assertEqual(response.status_code, 200)
            payload = response.get_json()
            prediction = payload['prediction']
            self.assertEqual(prediction['status'], 'pending')
            self.assertIsNotNone(prediction['prediction_number'])
            self.assertIn('pc28_frequency_v1', prediction['raw_response'])
            runtime_state = harness.db.get_predictor_runtime_state(predictor_id)
            self.assertEqual(runtime_state['consecutive_ai_failures'], 0)

    def test_predict_now_runs_pc28_omission_reversion_algorithm(self):
        with fresh_app_harness() as harness:
            client, user_id = harness.make_client()
            predictor_id = create_predictor(
                harness,
                user_id,
                'pc28',
                engine_type='machine',
                algorithm_key='pc28_omission_reversion_v1',
                api_key='',
                api_url='',
                model_name='',
                system_prompt=''
            )
            draws = [
                build_pc28_draw('20260424011', 11),
                build_pc28_draw('20260424010', 9),
                build_pc28_draw('20260424009', 14),
                build_pc28_draw('20260424008', 7),
                build_pc28_draw('20260424007', 16),
                build_pc28_draw('20260424006', 5),
                build_pc28_draw('20260424005', 18),
                build_pc28_draw('20260424004', 3),
                build_pc28_draw('20260424003', 20),
                build_pc28_draw('20260424002', 8),
                build_pc28_draw('20260424001', 10),
                build_pc28_draw('20260424000', 12)
            ]
            context = {
                'latest_draw': draws[0],
                'next_issue_no': '20260424012',
                'countdown': '00:01:00',
                'recent_draws': draws,
                'omission_preview': {},
                'today_preview': {},
                'preview': {}
            }

            with mock.patch.object(harness.module.pc28_service, 'sync_recent_draws', return_value=draws), \
                 mock.patch.object(harness.module.prediction_engine, '_build_context', return_value=context):
                response = client.post(f'/api/predictors/{predictor_id}/predict-now')

            self.assertEqual(response.status_code, 200)
            payload = response.get_json()
            prediction = payload['prediction']
            self.assertEqual(prediction['status'], 'pending')
            self.assertIn('pc28_omission_reversion_v1', prediction['raw_response'])
            self.assertEqual(prediction['prediction_combo'], '大单')
            self.assertIn('遗漏偏高', prediction['reasoning_summary'])

    def test_predict_now_runs_pc28_combo_markov_algorithm(self):
        with fresh_app_harness() as harness:
            client, user_id = harness.make_client()
            predictor_id = create_predictor(
                harness,
                user_id,
                'pc28',
                engine_type='machine',
                algorithm_key='pc28_combo_markov_v1',
                api_key='',
                api_url='',
                model_name='',
                system_prompt=''
            )
            draws = [
                build_pc28_draw('20260424021', 12),
                build_pc28_draw('20260424020', 19),
                build_pc28_draw('20260424019', 10),
                build_pc28_draw('20260424018', 21),
                build_pc28_draw('20260424017', 8),
                build_pc28_draw('20260424016', 23),
                build_pc28_draw('20260424015', 6),
                build_pc28_draw('20260424014', 15),
                build_pc28_draw('20260424013', 4)
            ]
            context = {
                'latest_draw': draws[0],
                'next_issue_no': '20260424022',
                'countdown': '00:01:00',
                'recent_draws': draws,
                'omission_preview': {},
                'today_preview': {},
                'preview': {}
            }

            with mock.patch.object(harness.module.pc28_service, 'sync_recent_draws', return_value=draws), \
                 mock.patch.object(harness.module.prediction_engine, '_build_context', return_value=context):
                response = client.post(f'/api/predictors/{predictor_id}/predict-now')

            self.assertEqual(response.status_code, 200)
            payload = response.get_json()
            prediction = payload['prediction']
            self.assertEqual(prediction['status'], 'pending')
            self.assertIn('pc28_combo_markov_v1', prediction['raw_response'])
            self.assertEqual(prediction['prediction_combo'], '大单')
            self.assertEqual(prediction['prediction_big_small'], '大')
            self.assertEqual(prediction['prediction_odd_even'], '单')
            self.assertIn('历史转移更偏向大单', prediction['reasoning_summary'])

    def test_predict_now_runs_football_machine_algorithm(self):
        with fresh_app_harness() as harness:
            client, user_id = harness.make_client()
            predictor_id = create_predictor(
                harness,
                user_id,
                'jingcai_football',
                engine_type='machine',
                algorithm_key='football_odds_baseline_v1',
                api_key='',
                api_url='',
                model_name='',
                system_prompt=''
            )
            match = self._build_football_match(with_detail=False)
            batch_payload = {
                'batch_key': '2026-04-24',
                'dates': ['2026-04-24'],
                'matches': [match]
            }

            with mock.patch.object(harness.module.jingcai_football_service, 'sync_matches', return_value=batch_payload), \
                 mock.patch.object(harness.module.jingcai_football_service, '_fetch_recent_history', return_value=[]), \
                 mock.patch.object(harness.module.jingcai_football_service, 'enrich_matches_for_prediction', side_effect=lambda db, matches: matches):
                response = client.post(f'/api/predictors/{predictor_id}/predict-now')

            self.assertEqual(response.status_code, 200)
            payload = response.get_json()
            prediction = payload['prediction']
            self.assertEqual(prediction['status'], 'pending')
            self.assertIn('football_odds_baseline_v1', prediction['raw_response'])
            items = harness.db.get_recent_prediction_items(predictor_id, lottery_type='jingcai_football', limit=10)
            self.assertEqual(len(items), 1)
            self.assertEqual(items[0]['prediction_payload']['spf'], '胜')
            self.assertEqual(items[0]['prediction_payload']['rqspf'], '负')

    def test_predict_now_runs_weighted_football_machine_algorithm(self):
        with fresh_app_harness() as harness:
            client, user_id = harness.make_client()
            predictor_id = create_predictor(
                harness,
                user_id,
                'jingcai_football',
                engine_type='machine',
                algorithm_key='football_odds_form_weighted_v1',
                api_key='',
                api_url='',
                model_name='',
                system_prompt=''
            )
            match = self._build_football_match(with_detail=True)
            batch_payload = {
                'batch_key': '2026-04-24',
                'dates': ['2026-04-24'],
                'matches': [match]
            }

            with mock.patch.object(harness.module.jingcai_football_service, 'sync_matches', return_value=batch_payload), \
                 mock.patch.object(harness.module.jingcai_football_service, '_fetch_recent_history', return_value=[]), \
                 mock.patch.object(harness.module.jingcai_football_service, 'enrich_matches_for_prediction', side_effect=lambda db, matches: matches):
                response = client.post(f'/api/predictors/{predictor_id}/predict-now')

            self.assertEqual(response.status_code, 200)
            payload = response.get_json()
            prediction = payload['prediction']
            self.assertEqual(prediction['status'], 'pending')
            self.assertIn('football_odds_form_weighted_v1', prediction['raw_response'])
            items = harness.db.get_recent_prediction_items(predictor_id, lottery_type='jingcai_football', limit=10)
            self.assertEqual(len(items), 1)
            self.assertEqual(items[0]['prediction_payload']['spf'], '胜')
            self.assertEqual(items[0]['prediction_payload']['rqspf'], '胜')
            self.assertIn('近况差', items[0]['reasoning_summary'])

    def test_predict_now_runs_handicap_consistency_football_machine_algorithm(self):
        with fresh_app_harness() as harness:
            client, user_id = harness.make_client()
            predictor_id = create_predictor(
                harness,
                user_id,
                'jingcai_football',
                engine_type='machine',
                algorithm_key='football_handicap_consistency_v1',
                api_key='',
                api_url='',
                model_name='',
                system_prompt=''
            )
            match = self._build_football_match(with_detail=True)
            batch_payload = {
                'batch_key': '2026-04-24',
                'dates': ['2026-04-24'],
                'matches': [match]
            }

            with mock.patch.object(harness.module.jingcai_football_service, 'sync_matches', return_value=batch_payload), \
                 mock.patch.object(harness.module.jingcai_football_service, '_fetch_recent_history', return_value=[]), \
                 mock.patch.object(harness.module.jingcai_football_service, 'enrich_matches_for_prediction', side_effect=lambda db, matches: matches):
                response = client.post(f'/api/predictors/{predictor_id}/predict-now')

            self.assertEqual(response.status_code, 200)
            payload = response.get_json()
            prediction = payload['prediction']
            self.assertEqual(prediction['status'], 'pending')
            self.assertIn('football_handicap_consistency_v1', prediction['raw_response'])
            items = harness.db.get_recent_prediction_items(predictor_id, lottery_type='jingcai_football', limit=10)
            self.assertEqual(len(items), 1)
            self.assertEqual(items[0]['prediction_payload']['spf'], '胜')
            self.assertEqual(items[0]['prediction_payload']['rqspf'], '平')
            self.assertIn('盘路一致性', items[0]['reasoning_summary'])

    def test_predict_now_runs_value_edge_football_machine_algorithm(self):
        with fresh_app_harness() as harness:
            client, user_id = harness.make_client()
            predictor_id = create_predictor(
                harness,
                user_id,
                'jingcai_football',
                engine_type='machine',
                algorithm_key='football_value_edge_v1',
                api_key='',
                api_url='',
                model_name='',
                system_prompt=''
            )
            match = self._build_value_edge_match()
            batch_payload = {
                'batch_key': '2026-04-24',
                'dates': ['2026-04-24'],
                'matches': [match]
            }

            with mock.patch.object(harness.module.jingcai_football_service, 'sync_matches', return_value=batch_payload), \
                 mock.patch.object(harness.module.jingcai_football_service, '_fetch_recent_history', return_value=[]), \
                 mock.patch.object(harness.module.jingcai_football_service, 'enrich_matches_for_prediction', side_effect=lambda db, matches: matches):
                response = client.post(f'/api/predictors/{predictor_id}/predict-now')

            self.assertEqual(response.status_code, 200)
            payload = response.get_json()
            prediction = payload['prediction']
            self.assertEqual(prediction['status'], 'pending')
            self.assertIn('football_value_edge_v1', prediction['raw_response'])
            items = harness.db.get_recent_prediction_items(predictor_id, lottery_type='jingcai_football', limit=10)
            self.assertEqual(len(items), 1)
            self.assertIsNone(items[0]['prediction_payload']['spf'])
            self.assertEqual(items[0]['prediction_payload']['rqspf'], '胜')
            self.assertIn('SPF 无价值', items[0]['reasoning_summary'])
            self.assertIn('edge', items[0]['reasoning_summary'])


if __name__ == '__main__':
    unittest.main()
