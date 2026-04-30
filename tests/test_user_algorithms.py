from __future__ import annotations

import json
import unittest
from unittest import mock

from tests.support import fresh_app_harness


def build_football_algorithm_definition() -> dict:
    return {
        'schema_version': 1,
        'method_name': '进球率预测法',
        'lottery_type': 'jingcai_football',
        'targets': ['spf'],
        'data_window': {
            'recent_matches': 6,
            'history_matches': 30
        },
        'filters': [
            {'field': 'spf_odds.win', 'op': 'gte', 'value': 1.35},
            {'field': 'home_goals_per_match_6', 'op': 'gte', 'value': 1.2}
        ],
        'score': [
            {'feature': 'home_goals_per_match_6', 'transform': 'linear', 'weight': 0.34},
            {'feature': 'away_conceded_per_match_6', 'transform': 'linear', 'weight': 0.28},
            {'feature': 'implied_probability_spf_win', 'transform': 'linear', 'weight': 0.22},
            {'feature': 'injury_advantage', 'transform': 'linear', 'weight': 0.16}
        ],
        'decision': {
            'target': 'spf',
            'pick': '胜',
            'min_confidence': 0.58,
            'allow_skip': True
        },
        'explain': {
            'template': '主队近6场进球率 {home_goals_per_match_6}'
        }
    }


def _build_settled_football_event(event_key: str, match_no: str, event_time: str, score1: int, score2: int, actual_spf: str) -> dict:
    return {
        'lottery_type': 'jingcai_football',
        'event_key': event_key,
        'batch_key': '2026-04-25',
        'event_date': '2026-04-25',
        'event_time': event_time,
        'event_name': f'[测试] {match_no}',
        'league': '测试联赛',
        'home_team': '主队',
        'away_team': '客队',
        'status': '3',
        'status_label': '已开奖',
        'source_provider': 'sina',
        'result_payload': json.dumps({
            'score1': score1,
            'score2': score2,
            'actual_spf': actual_spf
        }, ensure_ascii=False),
        'meta_payload': json.dumps({
            'match_no': match_no,
            'spf_sell_status': '2',
            'spf_odds': {'胜': 1.62, '平': 3.75, '负': 5.2},
            'settled': True
        }, ensure_ascii=False),
        'source_payload': '{}'
    }


def _build_football_detail_bundle() -> dict:
    return {
        'recent_form_team1': [
            {'team1': '主队', 'team2': '甲队', 'score1': '3', 'score2': '1'},
            {'team1': '乙队', 'team2': '主队', 'score1': '0', 'score2': '2'},
            {'team1': '主队', 'team2': '丙队', 'score1': '2', 'score2': '0'}
        ],
        'recent_form_team2': [
            {'team1': '客队', 'team2': '丁队', 'score1': '0', 'score2': '1'},
            {'team1': '戊队', 'team2': '客队', 'score1': '2', 'score2': '1'},
            {'team1': '客队', 'team2': '己队', 'score1': '1', 'score2': '1'}
        ],
        'injury': {
            'team1': [],
            'team2': [{'playerShortName': '客队主力', 'typeCn': '伤'}]
        }
    }


class UserAlgorithmRouteTests(unittest.TestCase):
    def test_create_and_list_validated_user_algorithm(self):
        with fresh_app_harness() as harness:
            client, _ = harness.make_client()

            response = client.post('/api/user-algorithms', json={
                'lottery_type': 'jingcai_football',
                'name': '进球率预测法',
                'description': '按近六场进球率做主胜筛选',
                'definition': build_football_algorithm_definition()
            })

            self.assertEqual(response.status_code, 200)
            payload = response.get_json()
            self.assertEqual(payload['algorithm']['status'], 'validated')
            self.assertEqual(payload['algorithm']['key'], f"user:{payload['id']}")
            self.assertTrue(payload['validation']['valid'])

            list_response = client.get('/api/user-algorithms?lottery_type=jingcai_football')
            self.assertEqual(list_response.status_code, 200)
            algorithms = list_response.get_json()
            self.assertEqual(len(algorithms), 1)
            self.assertEqual(algorithms[0]['name'], '进球率预测法')

    def test_invalid_definition_is_saved_as_draft(self):
        with fresh_app_harness() as harness:
            client, _ = harness.make_client()

            definition = build_football_algorithm_definition()
            definition['score'][0]['feature'] = 'unsafe.field'
            response = client.post('/api/user-algorithms', json={
                'lottery_type': 'jingcai_football',
                'name': '错误字段算法',
                'definition': definition
            })

            self.assertEqual(response.status_code, 200)
            payload = response.get_json()
            self.assertEqual(payload['algorithm']['status'], 'draft')
            self.assertFalse(payload['validation']['valid'])
            self.assertIn('unsafe.field', '；'.join(payload['validation']['errors']))

    def test_validate_draft_does_not_create_algorithm(self):
        with fresh_app_harness() as harness:
            client, _ = harness.make_client()

            response = client.post('/api/user-algorithms/validate', json={
                'lottery_type': 'jingcai_football',
                'definition': build_football_algorithm_definition()
            })

            self.assertEqual(response.status_code, 200)
            self.assertTrue(response.get_json()['valid'])
            list_response = client.get('/api/user-algorithms?include_disabled=1')
            self.assertEqual(list_response.status_code, 200)
            self.assertEqual(list_response.get_json(), [])

    def test_dry_run_user_algorithm(self):
        with fresh_app_harness() as harness:
            client, _ = harness.make_client()

            response = client.post('/api/user-algorithms/dry-run', json={
                'lottery_type': 'jingcai_football',
                'definition': build_football_algorithm_definition()
            })

            self.assertEqual(response.status_code, 200)
            payload = response.get_json()
            self.assertEqual(payload['message'], '试跑完成')
            self.assertTrue(payload['validation']['valid'])
            self.assertEqual(payload['items'][0]['predicted_spf'], '胜')

    def test_backtest_user_algorithm_with_settled_football_events(self):
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

            response = client.post('/api/user-algorithms/backtest', json={
                'lottery_type': 'jingcai_football',
                'definition': build_football_algorithm_definition(),
                'limit': 20
            })

            self.assertEqual(response.status_code, 200)
            payload = response.get_json()
            backtest = payload['backtest']
            self.assertEqual(payload['message'], '回测完成')
            self.assertTrue(payload['validation']['valid'])
            self.assertEqual(backtest['sample_size'], 2)
            self.assertEqual(backtest['prediction_count'], 2)
            self.assertEqual(backtest['hit_rate']['spf']['ratio_text'], '1/2')
            self.assertEqual(backtest['records'][0]['predicted_spf'], '胜')

    def test_ai_draft_requires_user_api_credentials(self):
        with fresh_app_harness() as harness:
            client, _ = harness.make_client()

            response = client.post('/api/user-algorithms/ai-draft', json={
                'lottery_type': 'jingcai_football',
                'message': '帮我做进球率预测法'
            })

            self.assertEqual(response.status_code, 400)
            self.assertIn('API Key', response.get_json()['error'])

    def test_ai_draft_returns_validated_algorithm(self):
        with fresh_app_harness() as harness:
            client, _ = harness.make_client()

            with mock.patch.object(harness.module.AIPredictor, 'run_json_task', return_value={
                'api_mode': 'chat_completions',
                'response_model': 'test-model',
                'finish_reason': 'stop',
                'latency_ms': 12,
                'raw_response': '{}',
                'payload': {
                    'reply_type': 'draft_algorithm',
                    'message': '已生成进球率预测法。',
                    'questions': [],
                    'algorithm': build_football_algorithm_definition(),
                    'change_summary': '生成初始算法',
                    'risk_notes': ['样本窗口较短时波动较大']
                }
            }) as run_json_task:
                response = client.post('/api/user-algorithms/ai-draft', json={
                    'lottery_type': 'jingcai_football',
                    'api_key': 'test-key',
                    'api_url': 'https://example.com/v1',
                    'model_name': 'test-model',
                    'message': '帮我做进球率预测法',
                    'chat_history': [
                        {'role': 'user', 'content': '我想偏保守'},
                        {'role': 'assistant', 'content': '可以提高最低信心阈值'}
                    ]
                })

            self.assertEqual(response.status_code, 200)
            payload = response.get_json()
            self.assertEqual(payload['reply_type'], 'draft_algorithm')
            self.assertTrue(payload['validation']['valid'])
            self.assertEqual(payload['algorithm']['method_name'], '进球率预测法')
            prompt = run_json_task.call_args.kwargs['prompt']
            self.assertIn('我想偏保守', prompt)
            self.assertIn('可以提高最低信心阈值', prompt)

    def test_predictor_can_reference_owned_validated_algorithm(self):
        with fresh_app_harness() as harness:
            client, _ = harness.make_client()
            algorithm_response = client.post('/api/user-algorithms', json={
                'lottery_type': 'jingcai_football',
                'name': '进球率预测法',
                'definition': build_football_algorithm_definition()
            })
            algorithm_id = algorithm_response.get_json()['id']

            response = client.post('/api/predictors', json={
                'lottery_type': 'jingcai_football',
                'engine_type': 'machine',
                'algorithm_key': f'user:{algorithm_id}',
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
            })

            self.assertEqual(response.status_code, 200)
            predictor = response.get_json()['predictor']
            self.assertEqual(predictor['algorithm_key'], f'user:{algorithm_id}')
            self.assertEqual(predictor['algorithm_source'], 'user')
            self.assertEqual(predictor['algorithm_label'], '进球率预测法')
            self.assertEqual(predictor['user_algorithm_id'], algorithm_id)

    def test_predictor_cannot_reference_other_users_algorithm(self):
        with fresh_app_harness() as harness:
            owner_client, _ = harness.make_client(username='owner')
            algorithm_response = owner_client.post('/api/user-algorithms', json={
                'lottery_type': 'jingcai_football',
                'name': '进球率预测法',
                'definition': build_football_algorithm_definition()
            })
            algorithm_id = algorithm_response.get_json()['id']

            other_client, _ = harness.make_client(username='other')
            response = other_client.post('/api/predictors', json={
                'lottery_type': 'jingcai_football',
                'engine_type': 'machine',
                'algorithm_key': f'user:{algorithm_id}',
                'name': '越权方案',
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
            })

            self.assertEqual(response.status_code, 400)
            self.assertIn('无权使用此用户算法', response.get_json()['error'])

    def test_user_algorithm_can_execute_jingcai_prediction(self):
        from services.machine_prediction import predict_jingcai

        definition = build_football_algorithm_definition()
        predictor = {
            'id': 1,
            'engine_type': 'machine',
            'algorithm_key': 'user:7',
            'prediction_targets': ['spf'],
            'user_algorithm': {
                'id': 7,
                'name': '进球率预测法',
                'definition': definition
            }
        }
        matches = [{
            'event_key': 'T001',
            'match_no': '周五001',
            'league': '英超',
            'home_team': '主队',
            'away_team': '客队',
            'team1_id': 'home-1',
            'team2_id': 'away-1',
            'match_time': '2026-04-24 20:00',
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
        }]

        items, raw_response, prompt_snapshot = predict_jingcai('2026-04-24', matches, predictor)

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]['predicted_spf'], '胜')
        self.assertGreaterEqual(items[0]['confidence'], 0.35)
        self.assertIn('进球率预测法', raw_response)
        self.assertEqual(prompt_snapshot, '机器算法：进球率预测法')


if __name__ == '__main__':
    unittest.main()
