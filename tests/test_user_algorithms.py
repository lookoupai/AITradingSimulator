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
            'rqspf': {'handicap': -1, 'odds': {'胜': 2.95, '平': 3.35, '负': 2.10}},
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
            self.assertIn('spf', backtest['profit_summary'])
            self.assertIn('hit_rate_trend', backtest)
            self.assertIn('streaks', backtest)
            self.assertIn('max_drawdown', backtest)
            self.assertIn('skip_reason_stats', backtest)
            self.assertIn('odds_interval_performance', backtest)
            self.assertEqual(backtest['effective_sample_count'], 2)
            self.assertIn('field_completeness_rate', backtest['data_quality'])
            self.assertEqual(backtest['confidence_report']['level'], 'insufficient')
            self.assertIn('sample_bias_flags', backtest)

    def test_backtest_filters_by_market_league_and_recent_n(self):
        with fresh_app_harness() as harness:
            client, _ = harness.make_client()

            harness.db.upsert_lottery_events([
                _build_settled_football_event('bt1', '周六001', '2026-04-25 18:00:00', 2, 1, '胜'),
                {**_build_settled_football_event('bt2', '周六002', '2026-04-25 20:00:00', 1, 1, '平'), 'league': '其他联赛'}
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

            definition = build_football_algorithm_definition()
            definition['targets'] = ['spf', 'rqspf']
            response = client.post('/api/user-algorithms/backtest', json={
                'lottery_type': 'jingcai_football',
                'definition': definition,
                'recent_n': 1,
                'market_type': 'rqspf',
                'leagues': ['测试联赛'],
                'start_date': '2026-04-25',
                'end_date': '2026-04-25'
            })

            self.assertEqual(response.status_code, 200)
            backtest = response.get_json()['backtest']
            self.assertEqual(backtest['sample_size'], 1)
            self.assertEqual(backtest['targets'], ['rqspf'])
            self.assertEqual(backtest['records'][0]['league'], '测试联赛')
            self.assertIn('rqspf', backtest['hit_rate'])
            self.assertNotIn('spf', backtest['hit_rate'])

    def test_saved_backtest_is_stored_on_active_algorithm_version(self):
        with fresh_app_harness() as harness:
            client, _ = harness.make_client()
            harness.db.upsert_lottery_events([
                _build_settled_football_event('bt1', '周六001', '2026-04-25 18:00:00', 2, 1, '胜')
            ])
            harness.db.upsert_lottery_event_details([
                {
                    'lottery_type': 'jingcai_football',
                    'event_key': 'bt1',
                    'detail_type': detail_type,
                    'source_provider': 'sina',
                    'payload': payload
                }
                for detail_type, payload in _build_football_detail_bundle().items()
            ])
            algorithm_response = client.post('/api/user-algorithms', json={
                'lottery_type': 'jingcai_football',
                'name': '进球率预测法',
                'definition': build_football_algorithm_definition()
            })
            algorithm_id = algorithm_response.get_json()['id']

            response = client.post(f'/api/user-algorithms/{algorithm_id}/backtest', json={'limit': 20})

            self.assertEqual(response.status_code, 200)
            payload = response.get_json()
            self.assertEqual(payload['version'], 1)
            self.assertEqual(payload['algorithm']['active_backtest']['sample_size'], 1)
            versions_response = client.get(f'/api/user-algorithms/{algorithm_id}/versions')
            versions = versions_response.get_json()
            self.assertEqual(versions[0]['backtest']['sample_size'], 1)
            self.assertEqual(versions[0]['backtest']['confidence_report']['label'], '样本不足')

    def test_user_algorithm_version_comparison_summarizes_backtests(self):
        with fresh_app_harness() as harness:
            client, user_id = harness.make_client()
            algorithm_response = client.post('/api/user-algorithms', json={
                'lottery_type': 'jingcai_football',
                'name': '进球率预测法',
                'definition': build_football_algorithm_definition()
            })
            algorithm_id = algorithm_response.get_json()['id']
            updated_definition = build_football_algorithm_definition()
            updated_definition['decision']['min_confidence'] = 0.62
            client.put(f'/api/user-algorithms/{algorithm_id}', json={
                'lottery_type': 'jingcai_football',
                'name': '进球率预测法',
                'definition': updated_definition
            })
            harness.db.update_user_algorithm_version_backtest(algorithm_id, user_id, 1, {
                'sample_size': 30,
                'effective_sample_count': 25,
                'prediction_count': 20,
                'skip_rate': 33.33,
                'targets': ['spf'],
                'confidence_report': {'level': 'medium', 'label': '中等可信', 'score': 65},
                'data_quality': {'field_completeness_rate': 90},
                'hit_rate': {'spf': {'hit_rate': 50.0, 'ratio_text': '10/20'}},
                'profit_summary': {'spf': {'roi': -5.0, 'net_profit': -1.0}},
                'max_drawdown': {'spf': {'amount': 4.0}}
            })
            harness.db.update_user_algorithm_version_backtest(algorithm_id, user_id, 2, {
                'sample_size': 35,
                'effective_sample_count': 30,
                'prediction_count': 24,
                'skip_rate': 31.43,
                'targets': ['spf'],
                'confidence_report': {'level': 'high', 'label': '较高可信', 'score': 82},
                'data_quality': {'field_completeness_rate': 92},
                'hit_rate': {'spf': {'hit_rate': 58.33, 'ratio_text': '14/24'}},
                'profit_summary': {'spf': {'roi': 8.5, 'net_profit': 2.04}},
                'max_drawdown': {'spf': {'amount': 2.5}}
            })

            response = client.get(f'/api/user-algorithms/{algorithm_id}/version-comparison')

            self.assertEqual(response.status_code, 200)
            comparison = response.get_json()['comparison']
            self.assertEqual(comparison['rows'][0]['version'], 2)
            self.assertEqual(comparison['rows'][0]['summary']['confidence_label'], '较高可信')
            self.assertEqual(comparison['rows'][0]['delta_from_previous']['targets']['spf']['hit_rate'], 8.33)
            self.assertEqual(comparison['best_version']['version'], 2)

    def test_user_algorithm_ops_summary_aggregates_runtime_context(self):
        with fresh_app_harness() as harness:
            client, user_id = harness.make_client()
            algorithm_response = client.post('/api/user-algorithms', json={
                'lottery_type': 'jingcai_football',
                'name': '进球率预测法',
                'definition': build_football_algorithm_definition()
            })
            algorithm_id = algorithm_response.get_json()['id']
            harness.db.update_user_algorithm_version_backtest(algorithm_id, user_id, 1, {
                'sample_size': 25,
                'effective_sample_count': 20,
                'prediction_count': 15,
                'skip_count': 10,
                'skip_rate': 40,
                'targets': ['spf'],
                'confidence_report': {'level': 'medium', 'label': '中等可信', 'score': 65},
                'data_quality': {'field_completeness_rate': 88},
                'hit_rate': {'spf': {'hit_rate': 53.33, 'ratio_text': '8/15'}},
                'profit_summary': {'spf': {'roi': 4.5, 'net_profit': 0.68}},
                'max_drawdown': {'spf': {'amount': 2.0}},
                'risk_flags': ['跳过比例较高']
            })
            predictor_response = client.post('/api/predictors', json={
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
            predictor_id = predictor_response.get_json()['id']
            harness.db.create_user_algorithm_execution_log({
                'user_id': user_id,
                'algorithm_id': algorithm_id,
                'algorithm_version': 1,
                'predictor_id': predictor_id,
                'run_key': '2026-04-30',
                'status': 'succeeded',
                'match_count': 2,
                'prediction_count': 1,
                'skip_count': 1,
                'duration_ms': 12,
                'fallback_strategy': 'fail',
                'fallback_used': False,
                'debug': {'source': 'test'}
            })

            response = client.get(f'/api/user-algorithms/{algorithm_id}/ops-summary')

            self.assertEqual(response.status_code, 200)
            payload = response.get_json()
            self.assertEqual(payload['versions']['active_version'], 1)
            self.assertEqual(payload['recent_backtest']['summary']['sample_size'], 25)
            self.assertEqual(payload['recent_execution_logs'][0]['status'], 'succeeded')
            self.assertEqual(payload['bound_predictors'][0]['id'], predictor_id)
            self.assertEqual(payload['risk_summary']['bound_predictor_count'], 1)

    def test_compare_user_algorithm_versions_runs_same_filters(self):
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
            algorithm_response = client.post('/api/user-algorithms', json={
                'lottery_type': 'jingcai_football',
                'name': '进球率预测法',
                'definition': build_football_algorithm_definition()
            })
            algorithm_id = algorithm_response.get_json()['id']
            updated_definition = build_football_algorithm_definition()
            updated_definition['decision']['min_confidence'] = 0.62
            client.put(f'/api/user-algorithms/{algorithm_id}', json={
                'lottery_type': 'jingcai_football',
                'name': '进球率预测法',
                'definition': updated_definition
            })

            response = client.post(f'/api/user-algorithms/{algorithm_id}/compare-versions', json={
                'base_version': 1,
                'candidate_version': 2,
                'filters': {'recent_n': 2, 'market_type': 'spf'},
                'limit': 20
            })

            self.assertEqual(response.status_code, 200)
            payload = response.get_json()
            self.assertEqual(payload['message'], '版本对比回测完成')
            self.assertEqual(payload['base_version'], 1)
            self.assertEqual(payload['candidate_version'], 2)
            self.assertEqual(payload['base']['sample_size'], 2)
            self.assertEqual(payload['candidate']['sample_size'], 2)
            self.assertIn('hit_rate', payload['delta']['targets']['spf'])
            self.assertIn('odds_interval_performance', payload['delta'])

    def test_diagnose_user_algorithm_uses_recent_backtest_rules(self):
        with fresh_app_harness() as harness:
            client, _ = harness.make_client()
            harness.db.upsert_lottery_events([
                _build_settled_football_event('bt1', '周六001', '2026-04-25 18:00:00', 2, 1, '胜')
            ])
            harness.db.upsert_lottery_event_details([
                {
                    'lottery_type': 'jingcai_football',
                    'event_key': 'bt1',
                    'detail_type': detail_type,
                    'source_provider': 'sina',
                    'payload': payload
                }
                for detail_type, payload in _build_football_detail_bundle().items()
            ])
            algorithm_response = client.post('/api/user-algorithms', json={
                'lottery_type': 'jingcai_football',
                'name': '进球率预测法',
                'definition': build_football_algorithm_definition()
            })
            algorithm_id = algorithm_response.get_json()['id']
            client.post(f'/api/user-algorithms/{algorithm_id}/backtest', json={'limit': 20})

            response = client.post(f'/api/user-algorithms/{algorithm_id}/diagnose', json={})

            self.assertEqual(response.status_code, 200)
            diagnosis = response.get_json()['diagnosis']
            self.assertIn('sample_quality', diagnosis)
            self.assertIn('skip_analysis', diagnosis)
            self.assertIn('odds_analysis', diagnosis)
            self.assertIn('drawdown_analysis', diagnosis)
            self.assertIn('data_quality_analysis', diagnosis)
            self.assertGreaterEqual(len(diagnosis['recommended_actions']), 1)

    def test_algorithm_templates_and_adjustment_create_new_version(self):
        with fresh_app_harness() as harness:
            client, _ = harness.make_client()

            templates_response = client.get('/api/user-algorithms/templates?lottery_type=jingcai_football')
            self.assertEqual(templates_response.status_code, 200)
            templates = templates_response.get_json()
            self.assertGreaterEqual(len(templates), 6)

            algorithm_response = client.post('/api/user-algorithms', json={
                'lottery_type': 'jingcai_football',
                'name': '进球率预测法',
                'definition': templates[0]['definition']
            })
            algorithm_id = algorithm_response.get_json()['id']

            adjust_response = client.post(f'/api/user-algorithms/{algorithm_id}/adjust', json={'mode': 'reduce_skip'})
            self.assertEqual(adjust_response.status_code, 200)
            payload = adjust_response.get_json()
            self.assertEqual(payload['algorithm']['active_version'], 2)
            self.assertTrue(payload['validation']['valid'])
            versions = payload['versions']
            self.assertEqual(versions[0]['version'], 2)

    def test_can_activate_previous_user_algorithm_version(self):
        with fresh_app_harness() as harness:
            client, _ = harness.make_client()
            algorithm_response = client.post('/api/user-algorithms', json={
                'lottery_type': 'jingcai_football',
                'name': '进球率预测法',
                'definition': build_football_algorithm_definition()
            })
            algorithm_id = algorithm_response.get_json()['id']
            updated_definition = build_football_algorithm_definition()
            updated_definition['method_name'] = '新版进球率预测法'
            update_response = client.put(f'/api/user-algorithms/{algorithm_id}', json={
                'lottery_type': 'jingcai_football',
                'name': '新版进球率预测法',
                'definition': updated_definition
            })
            self.assertEqual(update_response.get_json()['algorithm']['active_version'], 2)

            response = client.post(f'/api/user-algorithms/{algorithm_id}/activate-version', json={'version': 1})

            self.assertEqual(response.status_code, 200)
            payload = response.get_json()
            self.assertEqual(payload['algorithm']['active_version'], 1)
            self.assertEqual(payload['algorithm']['definition']['method_name'], '进球率预测法')

            third_definition = build_football_algorithm_definition()
            third_definition['method_name'] = '第三版进球率预测法'
            third_response = client.put(f'/api/user-algorithms/{algorithm_id}', json={
                'lottery_type': 'jingcai_football',
                'name': '第三版进球率预测法',
                'definition': third_definition
            })
            self.assertEqual(third_response.status_code, 200)
            self.assertEqual(third_response.get_json()['algorithm']['active_version'], 3)

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
                    'backtest_summary': {
                        'sample_size': 20,
                        'prediction_count': 18,
                        'skip_count': 2,
                        'hit_rate': {'spf': {'ratio_text': '12/18', 'hit_rate': 66.67}},
                        'profit_summary': {'spf': {'net_profit': 3.2, 'roi': 17.78}},
                        'risk_flags': ['跳过比例较高']
                    },
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
            self.assertIn('样本数：20', prompt)
            self.assertIn('模拟净收益：3.2', prompt)

    def test_ai_adjust_creates_new_algorithm_version_from_backtest(self):
        with fresh_app_harness() as harness:
            client, user_id = harness.make_client()
            algorithm_response = client.post('/api/user-algorithms', json={
                'lottery_type': 'jingcai_football',
                'name': '进球率预测法',
                'definition': build_football_algorithm_definition()
            })
            algorithm_id = algorithm_response.get_json()['id']
            harness.db.update_user_algorithm_version_backtest(algorithm_id, user_id, 1, {
                'sample_size': 20,
                'prediction_count': 10,
                'skip_count': 10,
                'skip_rate': 50,
                'hit_rate': {'spf': {'ratio_text': '5/10', 'hit_rate': 50.0}},
                'profit_summary': {'spf': {'net_profit': -2.0, 'roi': -20.0}},
                'risk_flags': ['跳过比例较高']
            })
            adjusted_definition = build_football_algorithm_definition()
            adjusted_definition['decision']['min_confidence'] = 0.54

            with mock.patch.object(harness.module.AIPredictor, 'run_json_task', return_value={
                'api_mode': 'chat_completions',
                'response_model': 'test-model',
                'finish_reason': 'stop',
                'latency_ms': 18,
                'raw_response': '{}',
                'payload': {
                    'reply_type': 'draft_algorithm',
                    'message': '已降低跳过率。',
                    'questions': [],
                    'algorithm': adjusted_definition,
                    'change_summary': '降低最低置信度以减少跳过',
                    'risk_notes': []
                }
            }) as run_json_task:
                response = client.post(f'/api/user-algorithms/{algorithm_id}/ai-adjust', json={
                    'api_key': 'test-key',
                    'api_url': 'https://example.com/v1',
                    'model_name': 'test-model',
                    'message': '降低跳过率'
                })

            self.assertEqual(response.status_code, 200)
            payload = response.get_json()
            self.assertEqual(payload['algorithm']['active_version'], 2)
            self.assertEqual(payload['algorithm']['definition']['decision']['min_confidence'], 0.54)
            self.assertEqual(payload['versions'][0]['version'], 2)
            prompt = run_json_task.call_args.kwargs['prompt']
            self.assertIn('跳过率：50', prompt)
            self.assertIn('降低跳过率', prompt)

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
            self.assertEqual(predictor['user_algorithm_fallback_strategy'], 'fail')

    def test_user_algorithm_execution_log_records_fallback(self):
        with fresh_app_harness() as harness:
            client, _ = harness.make_client()
            algorithm_response = client.post('/api/user-algorithms', json={
                'lottery_type': 'jingcai_football',
                'name': '进球率预测法',
                'definition': build_football_algorithm_definition()
            })
            algorithm_id = algorithm_response.get_json()['id']
            predictor_response = client.post('/api/predictors', json={
                'lottery_type': 'jingcai_football',
                'engine_type': 'machine',
                'algorithm_key': f'user:{algorithm_id}',
                'user_algorithm_fallback_strategy': 'builtin_baseline',
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
            predictor_id = predictor_response.get_json()['id']
            predictor = harness.db.get_predictor(predictor_id, include_secret=True)
            match = {
                'event_key': 'UA001',
                'match_no': '周五001',
                'event_name': '[测试] 周五001',
                'show_sell_status': '1',
                'status': '1',
                'spf_odds': {'胜': 1.55, '平': 3.2, '负': 4.8},
                'rqspf': {'handicap': -1, 'odds': {'胜': 3.8, '平': 3.2, '负': 1.8}}
            }
            fallback_items = [{
                'event_key': 'UA001',
                'match_no': '周五001',
                'predicted_spf': '胜',
                'predicted_rqspf': None,
                'confidence': 0.6,
                'reasoning_summary': 'fallback'
            }]

            with mock.patch.object(harness.module.jingcai_football_service, '_fetch_recent_history', return_value=[]), \
                 mock.patch.object(harness.module.jingcai_football_service, 'enrich_matches_for_prediction', return_value=[match]), \
                 mock.patch('services.jingcai_football_service.machine_prediction.predict_jingcai', side_effect=[
                     ValueError('用户算法执行失败'),
                     (fallback_items, '{}', '机器算法：赔率基线')
                 ]):
                run = harness.module.jingcai_football_service.generate_prediction(
                    harness.db,
                    predictor,
                    batch_payload={'batch_key': '2026-04-30', 'dates': [], 'matches': [match]}
                )

            self.assertEqual(run['status'], 'pending')
            logs_response = client.get(f'/api/user-algorithms/{algorithm_id}/execution-logs')
            self.assertEqual(logs_response.status_code, 200)
            logs = logs_response.get_json()
            self.assertEqual(len(logs), 1)
            self.assertEqual(logs[0]['status'], 'fallback_succeeded')
            self.assertTrue(logs[0]['fallback_used'])
            self.assertEqual(logs[0]['fallback_strategy'], 'builtin_baseline')
            self.assertEqual(logs[0]['prediction_count'], 1)

    def test_disabling_algorithm_reports_bound_predictors(self):
        with fresh_app_harness() as harness:
            client, _ = harness.make_client()
            algorithm_response = client.post('/api/user-algorithms', json={
                'lottery_type': 'jingcai_football',
                'name': '进球率预测法',
                'definition': build_football_algorithm_definition()
            })
            algorithm_id = algorithm_response.get_json()['id']
            client.post('/api/predictors', json={
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

            response = client.delete(f'/api/user-algorithms/{algorithm_id}')

            self.assertEqual(response.status_code, 200)
            payload = response.get_json()
            self.assertEqual(len(payload['affected_predictors']), 1)
            self.assertIn('仍引用该算法', payload['warning'])

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

    def test_predictor_test_dry_runs_user_algorithm(self):
        with fresh_app_harness() as harness:
            client, _ = harness.make_client()
            algorithm_response = client.post('/api/user-algorithms', json={
                'lottery_type': 'jingcai_football',
                'name': '进球率预测法',
                'definition': build_football_algorithm_definition()
            })
            algorithm_id = algorithm_response.get_json()['id']

            response = client.post('/api/predictors/test', json={
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
            payload = response.get_json()
            self.assertEqual(payload['finish_reason'], 'user_algorithm')
            self.assertIn('样例试跑', payload['response_preview'])
            self.assertIn('胜', payload['raw_response'])

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
