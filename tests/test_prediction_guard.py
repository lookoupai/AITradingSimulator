from __future__ import annotations

import importlib
import json
import unittest
from unittest import mock

from tests.support import create_predictor, fresh_app_harness


class PredictionGuardTests(unittest.TestCase):
    def _pc28_context(self, issue_no: str) -> dict:
        return {
            'next_issue_no': issue_no,
            'countdown': '00:00:30',
            'recent_draws': [],
            'omission_preview': {},
            'today_preview': {},
            'preview': {}
        }

    def _build_jingcai_match(self, harness, issue_no: str = '周一001', ticai_id: str = '001') -> dict:
        return harness.module.jingcai_football_service._normalize_match({
            'matchId': f'match-{ticai_id}',
            'tiCaiId': ticai_id,
            'matchNo': issue_no,
            'matchNoValue': issue_no[-3:],
            'league': '测试联赛',
            'leagueOfficial': '测试联赛',
            'team1': f'{issue_no}主队',
            'team2': f'{issue_no}客队',
            'matchTimeFormat': '2026-04-06 18:00:00',
            'showSellStatus': '1',
            'showSellStatusCn': '已开售',
            'spfSellStatus': '1',
            'rqspfSellStatus': '1',
            'spf': '1.55,3.20,4.80',
            'rqspf': '-1,3.80,3.20,1.80',
            'score1': '',
            'score2': '',
            'halfScore1': '',
            'halfScore2': ''
        }, '2026-04-06')

    def test_pc28_auto_failures_pause_after_threshold_and_skip_same_issue(self):
        with fresh_app_harness() as harness:
            _, user_id = harness.make_client()
            predictor_id = create_predictor(harness, user_id, 'pc28')
            predictor = harness.db.get_predictor(predictor_id, include_secret=True)
            guard_module = importlib.import_module('services.prediction_guard')
            ai_error = guard_module.AIPredictionError('额度用尽', category='quota')

            with mock.patch('services.prediction_engine.AIPredictor.predict_next_issue', side_effect=ai_error):
                first_result = harness.module.prediction_engine._generate_prediction_locked(
                    predictor,
                    self._pc28_context('1001'),
                    auto_mode=True
                )

            self.assertEqual(first_result['status'], 'failed')
            predictor_after_first = harness.db.get_predictor(predictor_id, include_secret=False)
            self.assertEqual(predictor_after_first['consecutive_ai_failures'], 1)
            self.assertFalse(predictor_after_first['auto_paused'])

            with mock.patch('services.prediction_engine.AIPredictor.predict_next_issue', side_effect=AssertionError('不应再次调用 AI')):
                duplicate_result = harness.module.prediction_engine._generate_prediction_locked(
                    harness.db.get_predictor(predictor_id, include_secret=True),
                    self._pc28_context('1001'),
                    auto_mode=True
                )

            self.assertEqual(duplicate_result['status'], 'failed')
            predictor_after_duplicate = harness.db.get_predictor(predictor_id, include_secret=False)
            self.assertEqual(predictor_after_duplicate['consecutive_ai_failures'], 1)

            with mock.patch('services.prediction_engine.AIPredictor.predict_next_issue', side_effect=ai_error):
                harness.module.prediction_engine._generate_prediction_locked(
                    harness.db.get_predictor(predictor_id, include_secret=True),
                    self._pc28_context('1002'),
                    auto_mode=True
                )
                harness.module.prediction_engine._generate_prediction_locked(
                    harness.db.get_predictor(predictor_id, include_secret=True),
                    self._pc28_context('1003'),
                    auto_mode=True
                )

            predictor_after_threshold = harness.db.get_predictor(predictor_id, include_secret=False)
            self.assertTrue(predictor_after_threshold['auto_paused'])
            self.assertEqual(predictor_after_threshold['consecutive_ai_failures'], 3)
            self.assertEqual(
                harness.db.get_enabled_predictors(
                    lottery_type='pc28',
                    include_secret=False,
                    exclude_auto_paused=True
                ),
                []
            )

    def test_manual_predict_now_failure_does_not_increment_guard_counter(self):
        with fresh_app_harness() as harness:
            client, user_id = harness.make_client()
            predictor_id = create_predictor(harness, user_id, 'pc28')
            guard_module = importlib.import_module('services.prediction_guard')
            ai_error = guard_module.AIPredictionError('API Key 已失效', category='auth')

            with mock.patch.object(harness.module.pc28_service, 'sync_recent_draws', return_value=[]), \
                 mock.patch.object(harness.module.prediction_engine, '_build_context', return_value=self._pc28_context('2001')), \
                 mock.patch('services.prediction_engine.AIPredictor.predict_next_issue', side_effect=ai_error):
                response = client.post(f'/api/predictors/{predictor_id}/predict-now')

            data = response.get_json()
            self.assertEqual(response.status_code, 200)
            self.assertEqual(data['prediction']['status'], 'failed')
            predictor = harness.db.get_predictor(predictor_id, include_secret=False)
            self.assertEqual(predictor['consecutive_ai_failures'], 0)
            self.assertFalse(predictor['auto_paused'])

    def test_admin_can_update_guard_settings_and_resume_auto_pause(self):
        with fresh_app_harness() as harness:
            client, user_id = harness.make_client(username='admin', is_admin=True)
            predictor_id = create_predictor(harness, user_id, 'pc28')
            harness.db.update_predictor_runtime_state(predictor_id, {
                'consecutive_ai_failures': 3,
                'auto_paused': True,
                'auto_paused_at': '2026-04-07 00:00:00',
                'auto_pause_reason': 'AI 连续失败 3 次，已自动暂停'
            })

            settings_response = client.post('/api/admin/settings/prediction-guard', json={
                'enabled': False,
                'threshold': 5
            })
            settings_data = settings_response.get_json()
            self.assertEqual(settings_response.status_code, 200)
            self.assertFalse(settings_data['settings']['enabled'])
            self.assertEqual(settings_data['settings']['threshold'], 5)

            resume_response = client.post(f'/api/admin/predictors/{predictor_id}/resume-auto-pause')
            resume_data = resume_response.get_json()
            self.assertEqual(resume_response.status_code, 200)
            self.assertFalse(resume_data['predictor']['auto_paused'])
            self.assertEqual(resume_data['predictor']['consecutive_ai_failures'], 0)

    def test_jingcai_auto_failure_only_counts_once_per_batch(self):
        with fresh_app_harness() as harness:
            _, user_id = harness.make_client()
            predictor_id = create_predictor(harness, user_id, 'jingcai_football')
            predictor = harness.db.get_predictor(predictor_id, include_secret=True)
            match = self._build_jingcai_match(harness)
            batch_payload = {
                'batch_key': '2026-04-06',
                'dates': ['2026-04-06'],
                'matches': [match]
            }
            guard_module = importlib.import_module('services.prediction_guard')
            ai_error = guard_module.AIPredictionError('额度已用尽', category='quota')

            with mock.patch.object(harness.module.jingcai_football_service, '_fetch_recent_history', return_value=[]), \
                 mock.patch.object(harness.module.jingcai_football_service, 'enrich_matches_for_prediction', return_value=[match]), \
                 mock.patch.object(harness.module.jingcai_football_service, '_build_prediction_prompt', return_value='prompt'), \
                 mock.patch('services.jingcai_football_service.AIPredictor.run_json_task', side_effect=ai_error):
                first_run = harness.module.jingcai_football_service.generate_prediction(
                    harness.db,
                    predictor,
                    auto_mode=True,
                    batch_payload=batch_payload
                )

            self.assertEqual(first_run['status'], 'failed')
            predictor_after_first = harness.db.get_predictor(predictor_id, include_secret=False)
            self.assertEqual(predictor_after_first['consecutive_ai_failures'], 1)

            with mock.patch('services.jingcai_football_service.AIPredictor.run_json_task', side_effect=AssertionError('同一批次不应重复调用 AI')):
                second_run = harness.module.jingcai_football_service.generate_prediction(
                    harness.db,
                    harness.db.get_predictor(predictor_id, include_secret=True),
                    auto_mode=True,
                    batch_payload=batch_payload
                )

            self.assertEqual(second_run['status'], 'failed')
            predictor_after_second = harness.db.get_predictor(predictor_id, include_secret=False)
            self.assertEqual(predictor_after_second['consecutive_ai_failures'], 1)

    def test_jingcai_manual_success_clears_guard_state(self):
        with fresh_app_harness() as harness:
            _, user_id = harness.make_client()
            predictor_id = create_predictor(harness, user_id, 'jingcai_football')
            harness.db.update_predictor_runtime_state(predictor_id, {
                'consecutive_ai_failures': 2,
                'last_ai_error_category': 'parse',
                'last_ai_error_message': '旧的 JSON 解析失败',
                'last_ai_error_at': '2026-04-06 00:00:00'
            })
            predictor = harness.db.get_predictor(predictor_id, include_secret=True)
            match = self._build_jingcai_match(harness)
            batch_payload = {
                'batch_key': '2026-04-06',
                'dates': ['2026-04-06'],
                'matches': [match]
            }
            llm_result = {
                'raw_response': json.dumps({
                    'batch_key': '2026-04-06',
                    'predictions': [
                        {
                            'event_key': match['event_key'],
                            'match_no': match['match_no'],
                            'predicted_spf': '胜',
                            'predicted_rqspf': '负',
                            'confidence': 0.66,
                            'reasoning_summary': '测试'
                        }
                    ]
                }, ensure_ascii=False),
                'payload': {
                    'batch_key': '2026-04-06',
                    'predictions': [
                        {
                            'event_key': match['event_key'],
                            'match_no': match['match_no'],
                            'predicted_spf': '胜',
                            'predicted_rqspf': '负',
                            'confidence': 0.66,
                            'reasoning_summary': '测试'
                        }
                    ]
                }
            }

            with mock.patch.object(harness.module.jingcai_football_service, '_fetch_recent_history', return_value=[]), \
                 mock.patch.object(harness.module.jingcai_football_service, 'enrich_matches_for_prediction', return_value=[match]), \
                 mock.patch.object(harness.module.jingcai_football_service, '_build_prediction_prompt', return_value='prompt'), \
                 mock.patch('services.jingcai_football_service.AIPredictor.run_json_task', return_value=llm_result):
                run = harness.module.jingcai_football_service.generate_prediction(
                    harness.db,
                    predictor,
                    auto_mode=False,
                    batch_payload=batch_payload
                )

            self.assertEqual(run['status'], 'pending')
            predictor_after = harness.db.get_predictor(predictor_id, include_secret=False)
            self.assertEqual(predictor_after['consecutive_ai_failures'], 0)
            self.assertIsNone(predictor_after['last_ai_error_category'])
            self.assertIsNone(predictor_after['last_ai_error_message'])
            self.assertIsNone(predictor_after['last_ai_error_at'])

    def test_pc28_manual_success_clears_guard_state(self):
        with fresh_app_harness() as harness:
            _, user_id = harness.make_client()
            predictor_id = create_predictor(harness, user_id, 'pc28')
            harness.db.update_predictor_runtime_state(predictor_id, {
                'consecutive_ai_failures': 2,
                'last_ai_error_category': 'parse',
                'last_ai_error_message': '旧的格式错误',
                'last_ai_error_at': '2026-04-06 00:00:00'
            })
            predictor = harness.db.get_predictor(predictor_id, include_secret=True)

            with mock.patch('services.prediction_engine.AIPredictor.predict_next_issue', return_value=(
                {
                    'issue_no': '2001',
                    'prediction_number': 12,
                    'prediction_big_small': '小',
                    'prediction_odd_even': '双',
                    'prediction_combo': '小双',
                    'confidence': 0.72,
                    'reasoning_summary': '测试'
                },
                '{"issue_no":"2001"}',
                'PROMPT'
            )):
                prediction = harness.module.prediction_engine._generate_prediction_locked(
                    predictor,
                    self._pc28_context('2001'),
                    auto_mode=False
                )

            self.assertEqual(prediction['status'], 'pending')
            predictor_after = harness.db.get_predictor(predictor_id, include_secret=False)
            self.assertEqual(predictor_after['consecutive_ai_failures'], 0)
            self.assertIsNone(predictor_after['last_ai_error_category'])
            self.assertIsNone(predictor_after['last_ai_error_message'])
            self.assertIsNone(predictor_after['last_ai_error_at'])
