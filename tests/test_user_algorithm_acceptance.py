from __future__ import annotations

import unittest
from unittest import mock

from tests.support import fresh_app_harness
from tests.test_user_algorithms import (
    _build_football_detail_bundle,
    _build_settled_football_event,
    build_football_algorithm_definition
)


def _user_algorithm_predictor_payload(algorithm_id: int) -> dict:
    return {
        'lottery_type': 'jingcai_football',
        'engine_type': 'machine',
        'algorithm_key': f'user:{algorithm_id}',
        'user_algorithm_fallback_strategy': 'fail',
        'name': '自建算法方案',
        'prediction_method': '进球率预测法',
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
        'prediction_targets': ['spf'],
        'history_window': 30,
        'temperature': 0.2,
        'enabled': True
    }


class UserAlgorithmAcceptanceTests(unittest.TestCase):
    def test_user_algorithm_operational_loop(self):
        with fresh_app_harness() as harness:
            client, _ = harness.make_client()
            harness.db.upsert_lottery_events([
                _build_settled_football_event('bt1', '周六001', '2026-04-25 18:00:00', 2, 1, '胜'),
                _build_settled_football_event('bt2', '周六002', '2026-04-25 20:00:00', 1, 1, '平')
            ])
            harness.db.upsert_lottery_event_details([
                {
                    'lottery_type': 'jingcai_football',
                    'event_key': event_key,
                    'detail_type': detail_type,
                    'source_provider': 'sina',
                    'payload': payload
                }
                for event_key in ('bt1', 'bt2')
                for detail_type, payload in _build_football_detail_bundle().items()
            ])

            with mock.patch.object(harness.module.AIPredictor, 'run_json_task', return_value={
                'api_mode': 'chat_completions',
                'response_model': 'test-model',
                'finish_reason': 'stop',
                'latency_ms': 10,
                'raw_response': '{}',
                'payload': {
                    'reply_type': 'draft_algorithm',
                    'message': '已生成进球率预测法。',
                    'questions': [],
                    'algorithm': build_football_algorithm_definition(),
                    'change_summary': '生成初始算法',
                    'risk_notes': []
                }
            }):
                draft_response = client.post('/api/user-algorithms/ai-draft', json={
                    'api_key': 'test-key',
                    'api_url': 'https://example.com/v1',
                    'model_name': 'test-model',
                    'lottery_type': 'jingcai_football',
                    'message': '帮我做进球率预测法'
                })
            self.assertEqual(draft_response.status_code, 200)
            draft = draft_response.get_json()

            save_response = client.post('/api/user-algorithms', json={
                'lottery_type': 'jingcai_football',
                'name': '进球率预测法',
                'description': '验收测试算法',
                'definition': draft['algorithm']
            })
            self.assertEqual(save_response.status_code, 200)
            algorithm_id = save_response.get_json()['id']

            backtest_response = client.post(f'/api/user-algorithms/{algorithm_id}/backtest', json={
                'limit': 20,
                'filters': {'recent_n': 2, 'market_type': 'spf'}
            })
            self.assertEqual(backtest_response.status_code, 200)
            self.assertEqual(backtest_response.get_json()['backtest']['sample_size'], 2)

            adjusted_definition = build_football_algorithm_definition()
            adjusted_definition['decision']['min_confidence'] = 0.54
            with mock.patch.object(harness.module.AIPredictor, 'run_json_task', return_value={
                'api_mode': 'chat_completions',
                'response_model': 'test-model',
                'finish_reason': 'stop',
                'latency_ms': 12,
                'raw_response': '{}',
                'payload': {
                    'reply_type': 'draft_algorithm',
                    'message': '已生成调参版本。',
                    'questions': [],
                    'algorithm': adjusted_definition,
                    'change_summary': '降低置信度减少跳过',
                    'risk_notes': []
                }
            }):
                adjust_response = client.post(f'/api/user-algorithms/{algorithm_id}/ai-adjust', json={
                    'api_key': 'test-key',
                    'api_url': 'https://example.com/v1',
                    'model_name': 'test-model',
                    'message': '降低跳过率'
                })
            self.assertEqual(adjust_response.status_code, 200)
            self.assertEqual(adjust_response.get_json()['algorithm']['active_version'], 2)

            compare_response = client.post(f'/api/user-algorithms/{algorithm_id}/compare-versions', json={
                'base_version': 1,
                'candidate_version': 2,
                'filters': {'recent_n': 2, 'market_type': 'spf'},
                'limit': 20
            })
            self.assertEqual(compare_response.status_code, 200)
            self.assertEqual(compare_response.get_json()['candidate']['sample_size'], 2)

            predictor_response = client.post('/api/predictors', json=_user_algorithm_predictor_payload(algorithm_id))
            self.assertEqual(predictor_response.status_code, 200)
            predictor_id = predictor_response.get_json()['id']
            predictor = harness.db.get_predictor(predictor_id, include_secret=True)
            match = {
                'event_key': 'UA001',
                'match_no': '周五001',
                'league': '英超',
                'home_team': '主队',
                'away_team': '客队',
                'team1_id': 'home-1',
                'team2_id': 'away-1',
                'match_time': '2026-04-30 20:00',
                'event_name': '[测试] 周五001',
                'show_sell_status': '1',
                'status': '1',
                'spf_odds': {'胜': 1.62, '平': 3.75, '负': 5.20},
                'rqspf': {'handicap': -1, 'odds': {'胜': 2.95, '平': 3.35, '负': 2.10}},
                'detail_bundle': {
                    'recent_form_team1': [
                        {'team1': '主队', 'team2': '甲', 'team1Id': 'home-1', 'score1': '3', 'score2': '1'},
                        {'team1': '乙', 'team2': '主队', 'team2Id': 'home-1', 'score1': '0', 'score2': '2'},
                        {'team1': '主队', 'team2': '丙', 'team1Id': 'home-1', 'score1': '2', 'score2': '0'}
                    ],
                    'recent_form_team2': [
                        {'team1': '客队', 'team2': '丁', 'team1Id': 'away-1', 'score1': '0', 'score2': '1'},
                        {'team1': '戊', 'team2': '客队', 'team2Id': 'away-1', 'score1': '2', 'score2': '1'},
                        {'team1': '客队', 'team2': '己', 'team1Id': 'away-1', 'score1': '1', 'score2': '1'}
                    ],
                    'injury': {'team1': [], 'team2': [{'playerShortName': '客队主力', 'typeCn': '伤'}]}
                }
            }
            with mock.patch.object(harness.module.jingcai_football_service, '_fetch_recent_history', return_value=[]), \
                 mock.patch.object(harness.module.jingcai_football_service, 'enrich_matches_for_prediction', return_value=[match]):
                run = harness.module.jingcai_football_service.generate_prediction(
                    harness.db,
                    predictor,
                    batch_payload={'batch_key': '2026-04-30', 'dates': [], 'matches': [match]}
                )
            self.assertEqual(run['status'], 'pending')

            logs_response = client.get(f'/api/user-algorithms/{algorithm_id}/execution-logs')
            self.assertEqual(logs_response.status_code, 200)
            logs = logs_response.get_json()
            self.assertEqual(logs[0]['status'], 'succeeded')
            self.assertEqual(logs[0]['prediction_count'], 1)

            rollback_response = client.post(f'/api/user-algorithms/{algorithm_id}/activate-version', json={'version': 1})
            self.assertEqual(rollback_response.status_code, 200)
            self.assertEqual(rollback_response.get_json()['algorithm']['active_version'], 1)
