from __future__ import annotations

import importlib
import json
import unittest
from datetime import datetime
from unittest import mock

from tests.support import create_predictor, fresh_app_harness


class JingcaiFootballServicePromptTests(unittest.TestCase):
    def _build_match(self, harness, issue_no: str = '周四001', ticai_id: str = '2039116') -> dict:
        base_match = harness.module.jingcai_football_service._normalize_match({
            'matchId': f'match-{ticai_id}',
            'tiCaiId': ticai_id,
            'matchNo': issue_no,
            'matchNoValue': issue_no[-3:],
            'league': '欧罗巴',
            'leagueOfficial': '欧罗巴',
            'team1': '塞尔塔',
            'team2': '弗赖堡',
            'team1Id': 't1',
            'team2Id': 't2',
            'matchTimeFormat': '2026-04-16 18:00:00',
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
        }, '2026-04-16')
        base_match['detail_bundle'] = {
            'detail': {
                'league': '欧罗巴',
                'round': '1/4决赛',
                'team1Position': '3',
                'team2Position': '6',
                'isNeutral': '0'
            },
            'battle_history': [
                {'team1': '塞尔塔', 'score1': '2', 'score2': '1', 'team2': '弗赖堡'}
            ],
            'odds_euro': [
                {
                    'companyName': '竞彩官方',
                    'o1Ini': '1.70',
                    'o2Ini': '3.40',
                    'o3Ini': '4.80',
                    'o1New': '1.65',
                    'o2New': '3.50',
                    'o3New': '5.10'
                }
            ],
            'odds_asia': [
                {
                    'companyName': '澳门',
                    'o3IniCn': '半球',
                    'o1Ini': '0.92',
                    'o2Ini': '0.94',
                    'o3NewCn': '半一',
                    'o1New': '0.90',
                    'o2New': '0.96'
                }
            ],
            'odds_totals': [
                {
                    'companyName': '澳门',
                    'o3IniCn': '2.5',
                    'o1Ini': '0.90',
                    'o2Ini': '0.96',
                    'o3NewCn': '2.75',
                    'o1New': '0.88',
                    'o2New': '0.98'
                }
            ],
            'odds_snapshots': {
                'euro': {
                    'company': '竞彩官方',
                    'initial': {'win': '1.70', 'draw': '3.40', 'lose': '4.80'},
                    'current': {'win': '1.65', 'draw': '3.50', 'lose': '5.10'}
                }
            },
            'team_table': {
                'team1': {'items': {'all': [{'position': '3', 'points': '58'}], 'home': [{'won': '8', 'draw': '3', 'loss': '2'}]}},
                'team2': {'items': {'all': [{'position': '6', 'points': '49'}], 'away': [{'won': '5', 'draw': '4', 'loss': '4'}]}}
            },
            'recent_form_team1': [
                {'team1': '塞尔塔', 'team2': '主队A', 'team1Id': 't1', 'team2Id': 'tX', 'score1': '2', 'score2': '0'}
            ],
            'recent_form_team2': [
                {'team1': '客队A', 'team2': '弗赖堡', 'team1Id': 'tY', 'team2Id': 't2', 'score1': '1', 'score2': '1'}
            ],
            'injury': {
                'team1': [{'playerShortName': '主力前锋', 'typeCn': '伤'}],
                'team2': [{'playerShortName': '主力中卫', 'typeCn': '停'}]
            },
            'intelligence': {
                'team1': {'good': [{'content': '主队主场状态稳定'}], 'bad': []},
                'team2': {'good': [], 'bad': [{'content': '客队连续客场作战'}]},
                'neutral': [{'content': '盘口持续向主队倾斜'}]
            },
            'recent_matches': {
                'team1': [{'matchTimeFormat': '2026-04-20 20:00:00', 'team1': '塞尔塔', 'team2': '后续对手A'}],
                'team2': [{'matchTimeFormat': '2026-04-21 20:00:00', 'team1': '后续对手B', 'team2': '弗赖堡'}]
            }
        }
        return base_match

    def _build_matches(self, harness, count: int) -> list[dict]:
        return [
            self._build_match(
                harness,
                issue_no=f'周四{index + 1:03d}',
                ticai_id=f'20391{index + 10:02d}'
            )
            for index in range(count)
        ]

    def _build_batch_response(self, matches: list[dict], predicted_spf: str = '胜', predicted_rqspf: str = '负') -> dict:
        predictions = [
            {
                'event_key': match['event_key'],
                'match_no': match['match_no'],
                'predicted_spf': predicted_spf,
                'predicted_rqspf': predicted_rqspf,
                'confidence': 0.66,
                'reasoning_summary': f"{match['match_no']} 测试摘要"
            }
            for match in matches
        ]
        return {
            'raw_response': json.dumps({
                'batch_key': '2026-04-16',
                'predictions': predictions
            }, ensure_ascii=False),
            'payload': {
                'batch_key': '2026-04-16',
                'predictions': predictions
            }
        }

    def _create_existing_run(self, harness, predictor_id: int, matches: list[dict], status: str = 'pending') -> int:
        run_id = harness.db.upsert_prediction_run({
            'predictor_id': predictor_id,
            'lottery_type': 'jingcai_football',
            'run_key': '2026-04-16',
            'title': '2026-04-16 竞彩足球批次预测',
            'requested_targets': ['spf', 'rqspf'],
            'status': status,
            'total_items': len(matches),
            'settled_items': 0,
            'hit_items': 0,
            'confidence': 0.61,
            'reasoning_summary': '',
            'raw_response': '',
            'prompt_snapshot': '',
            'error_message': None,
            'settled_at': None
        })
        return run_id

    def test_build_prediction_prompt_renders_placeholders_and_avoids_duplicate_context(self):
        with fresh_app_harness() as harness:
            match = self._build_match(harness)
            predictor = {
                'name': '埃罗预测法',
                'prediction_method': '埃罗预测法',
                'prediction_targets': ['spf', 'rqspf'],
                'system_prompt': (
                    '先看{{match_batch_summary}}，再结合{{market_odds_summary}}、{{match_detail_summary}}、'
                    '{{injury_summary}}、{{intelligence_summary}}与{{recent_results_summary}}，'
                    '围绕{{prediction_targets}}完成判断，历史窗口={{history_window}}，时间={{current_time_beijing}}。'
                )
            }

            prompt = harness.module.jingcai_football_service._build_prediction_prompt(
                predictor=predictor,
                run_key='2026-04-16',
                upcoming_matches=[match],
                history_matches=[{
                    'match_no': '周三001',
                    'league': '欧罗巴',
                    'home_team': '主队A',
                    'away_team': '客队B',
                    'score_text': '2:1',
                    'actual_spf': '胜',
                    'actual_rqspf': '平'
                }],
                data_injection_mode='raw'
            )

        self.assertIn('event_key=2039116', prompt)
        self.assertIn('市场赔率=', prompt)
        self.assertIn('主队主场状态稳定', prompt)
        self.assertIn('盘口持续向主队倾斜', prompt)
        self.assertIn('历史窗口=1', prompt)
        self.assertNotIn('{{match_batch_summary}}', prompt)
        self.assertIn('平台不再重复拼接整批比赛上下文', prompt)

    def test_split_prediction_batches_respects_max_matches(self):
        with fresh_app_harness() as harness, \
             mock.patch('services.jingcai_football_service.PREDICTION_BATCH_MAX_MATCHES', 2), \
             mock.patch('services.jingcai_football_service.PREDICTION_PROMPT_SOFT_LIMIT', 999999):
            matches = self._build_matches(harness, 5)
            predictor = {
                'prediction_targets': ['spf', 'rqspf'],
                'system_prompt': 'test',
                'prediction_method': 'test'
            }

            batches = harness.module.jingcai_football_service._split_prediction_batches(
                predictor=predictor,
                run_key='2026-04-16',
                matches=matches,
                history_matches=[],
                data_injection_mode='summary'
            )

        self.assertEqual([len(batch['matches']) for batch in batches], [2, 2, 1])
        self.assertEqual([batch['item_order_offset'] for batch in batches], [0, 2, 4])

    def test_generate_prediction_splits_large_batch_and_merges_results(self):
        with fresh_app_harness() as harness, \
             mock.patch('services.jingcai_football_service.PREDICTION_BATCH_MAX_MATCHES', 2), \
             mock.patch('services.jingcai_football_service.PREDICTION_PROMPT_SOFT_LIMIT', 999999):
            _, user_id = harness.make_client()
            predictor_id = create_predictor(
                harness,
                user_id,
                'jingcai_football',
                data_injection_mode='summary'
            )
            predictor = harness.db.get_predictor(predictor_id, include_secret=True)
            matches = self._build_matches(harness, 3)
            batch_payload = {
                'batch_key': '2026-04-16',
                'dates': ['2026-04-16'],
                'matches': matches
            }
            side_effect = [
                self._build_batch_response(matches[:2]),
                self._build_batch_response(matches[2:])
            ]

            with mock.patch.object(harness.module.jingcai_football_service, '_fetch_recent_history', return_value=[]), \
                 mock.patch.object(harness.module.jingcai_football_service, 'enrich_matches_for_prediction', return_value=matches), \
                 mock.patch('services.jingcai_football_service.AIPredictor.run_json_task', side_effect=side_effect) as run_json_task:
                run = harness.module.jingcai_football_service.generate_prediction(
                    harness.db,
                    predictor,
                    auto_mode=False,
                    batch_payload=batch_payload
                )

            self.assertEqual(run_json_task.call_count, 2)
            self.assertEqual(run['status'], 'pending')
            saved_items = harness.db.get_prediction_run_items(run['id'])
            self.assertEqual(len(saved_items), 3)
            self.assertTrue(all(item['status'] == 'pending' for item in saved_items))
            self.assertEqual([item['item_order'] for item in saved_items], [0, 1, 2])

    def test_generate_prediction_retries_with_smaller_batches_after_parse_failure(self):
        with fresh_app_harness() as harness, \
             mock.patch('services.jingcai_football_service.PREDICTION_BATCH_MAX_MATCHES', 10), \
             mock.patch('services.jingcai_football_service.PREDICTION_PROMPT_SOFT_LIMIT', 999999):
            _, user_id = harness.make_client()
            predictor_id = create_predictor(
                harness,
                user_id,
                'jingcai_football',
                data_injection_mode='summary'
            )
            predictor = harness.db.get_predictor(predictor_id, include_secret=True)
            matches = self._build_matches(harness, 4)
            batch_payload = {
                'batch_key': '2026-04-16',
                'dates': ['2026-04-16'],
                'matches': matches
            }
            guard_module = importlib.import_module('services.prediction_guard')
            parse_error = guard_module.AIPredictionError('无法从模型响应中解析 JSON', category='parse')
            setattr(parse_error, 'finish_reason', 'length')
            setattr(parse_error, 'raw_response', '{"batch_key":"2026-04-16"')
            setattr(parse_error, 'prompt_snapshot', 'FULL_PROMPT')

            with mock.patch.object(harness.module.jingcai_football_service, '_fetch_recent_history', return_value=[]), \
                 mock.patch.object(harness.module.jingcai_football_service, 'enrich_matches_for_prediction', return_value=matches), \
                 mock.patch('services.jingcai_football_service.AIPredictor.run_json_task', side_effect=[
                     parse_error,
                     self._build_batch_response(matches[:2]),
                     self._build_batch_response(matches[2:])
                 ]) as run_json_task:
                run = harness.module.jingcai_football_service.generate_prediction(
                    harness.db,
                    predictor,
                    auto_mode=False,
                    batch_payload=batch_payload
                )

            self.assertEqual(run_json_task.call_count, 3)
            self.assertEqual(run['status'], 'pending')
            saved_items = harness.db.get_prediction_run_items(run['id'])
            self.assertEqual(len(saved_items), 4)
            self.assertTrue(all(item['status'] == 'pending' for item in saved_items))

    def test_generate_prediction_keeps_partial_results_when_smallest_batch_fails(self):
        with fresh_app_harness() as harness, \
             mock.patch('services.jingcai_football_service.PREDICTION_BATCH_MAX_MATCHES', 2), \
             mock.patch('services.jingcai_football_service.PREDICTION_PROMPT_SOFT_LIMIT', 999999):
            _, user_id = harness.make_client()
            predictor_id = create_predictor(
                harness,
                user_id,
                'jingcai_football',
                data_injection_mode='summary'
            )
            predictor = harness.db.get_predictor(predictor_id, include_secret=True)
            matches = self._build_matches(harness, 3)
            batch_payload = {
                'batch_key': '2026-04-16',
                'dates': ['2026-04-16'],
                'matches': matches
            }
            guard_module = importlib.import_module('services.prediction_guard')
            parse_error = guard_module.AIPredictionError('无法从模型响应中解析 JSON', category='parse')
            setattr(parse_error, 'finish_reason', 'length')
            setattr(parse_error, 'raw_response', '{"batch_key":"2026-04-16"')
            setattr(parse_error, 'prompt_snapshot', 'FAILED_PROMPT')

            with mock.patch.object(harness.module.jingcai_football_service, '_fetch_recent_history', return_value=[]), \
                 mock.patch.object(harness.module.jingcai_football_service, 'enrich_matches_for_prediction', return_value=matches), \
                 mock.patch('services.jingcai_football_service.AIPredictor.run_json_task', side_effect=[
                     self._build_batch_response(matches[:2]),
                     parse_error
                 ]):
                run = harness.module.jingcai_football_service.generate_prediction(
                    harness.db,
                    predictor,
                    auto_mode=False,
                    batch_payload=batch_payload
                )

            self.assertEqual(run['status'], 'pending')
            self.assertIn('子批次 2/2', run.get('error_message') or '')
            saved_items = harness.db.get_prediction_run_items(run['id'])
            self.assertEqual([item['status'] for item in saved_items], ['pending', 'pending', 'failed'])
            self.assertIn('子批次 2/2 失败', saved_items[-1]['error_message'])
            view_model = harness.module.jingcai_football_service.build_run_view_model(harness.db, run)
            self.assertEqual(len(view_model.get('failed_batches') or []), 1)
            self.assertEqual(view_model['failed_batches'][0]['batch_label'], '2/2')
            self.assertIn('周四003', view_model['failed_batches'][0]['match_text'])

    def test_generate_prediction_manual_retries_only_failed_items(self):
        with fresh_app_harness() as harness:
            _, user_id = harness.make_client()
            predictor_id = create_predictor(
                harness,
                user_id,
                'jingcai_football',
                data_injection_mode='summary'
            )
            predictor = harness.db.get_predictor(predictor_id, include_secret=True)
            matches = self._build_matches(harness, 3)
            run_id = self._create_existing_run(harness, predictor_id, matches, status='pending')
            harness.db.upsert_prediction_items([
                {
                    'run_id': run_id,
                    'predictor_id': predictor_id,
                    'lottery_type': 'jingcai_football',
                    'run_key': '2026-04-16',
                    'event_key': matches[0]['event_key'],
                    'item_order': 0,
                    'issue_no': matches[0]['match_no'],
                    'title': matches[0]['event_name'],
                    'requested_targets': ['spf', 'rqspf'],
                    'prediction_payload': {'spf': '胜', 'rqspf': '负'},
                    'actual_payload': {},
                    'hit_payload': {},
                    'confidence': 0.62,
                    'reasoning_summary': '成功场次',
                    'raw_response': '{}',
                    'status': 'pending',
                    'error_message': None,
                    'retry_count': 0,
                    'last_retry_at': None,
                    'last_retry_error': None,
                    'settled_at': None
                },
                {
                    'run_id': run_id,
                    'predictor_id': predictor_id,
                    'lottery_type': 'jingcai_football',
                    'run_key': '2026-04-16',
                    'event_key': matches[1]['event_key'],
                    'item_order': 1,
                    'issue_no': matches[1]['match_no'],
                    'title': matches[1]['event_name'],
                    'requested_targets': ['spf', 'rqspf'],
                    'prediction_payload': {},
                    'actual_payload': {},
                    'hit_payload': {},
                    'confidence': None,
                    'reasoning_summary': '',
                    'raw_response': '{}',
                    'status': 'failed',
                    'error_message': '旧失败',
                    'retry_count': 0,
                    'last_retry_at': None,
                    'last_retry_error': None,
                    'settled_at': None
                },
                {
                    'run_id': run_id,
                    'predictor_id': predictor_id,
                    'lottery_type': 'jingcai_football',
                    'run_key': '2026-04-16',
                    'event_key': matches[2]['event_key'],
                    'item_order': 2,
                    'issue_no': matches[2]['match_no'],
                    'title': matches[2]['event_name'],
                    'requested_targets': ['spf', 'rqspf'],
                    'prediction_payload': {'spf': '平', 'rqspf': '负'},
                    'actual_payload': {},
                    'hit_payload': {},
                    'confidence': 0.58,
                    'reasoning_summary': '成功场次2',
                    'raw_response': '{}',
                    'status': 'pending',
                    'error_message': None,
                    'retry_count': 0,
                    'last_retry_at': None,
                    'last_retry_error': None,
                    'settled_at': None
                }
            ])
            batch_payload = {
                'batch_key': '2026-04-16',
                'dates': ['2026-04-16'],
                'matches': matches
            }

            with mock.patch.object(harness.module.jingcai_football_service, '_fetch_recent_history', return_value=[]), \
                 mock.patch.object(harness.module.jingcai_football_service, 'enrich_matches_for_prediction', side_effect=lambda _db, selected: selected), \
                 mock.patch('services.jingcai_football_service.AIPredictor.run_json_task', return_value=self._build_batch_response(matches[1:2])) as run_json_task:
                run = harness.module.jingcai_football_service.generate_prediction(
                    harness.db,
                    predictor,
                    auto_mode=False,
                    batch_payload=batch_payload
                )

            self.assertEqual(run_json_task.call_count, 1)
            self.assertIn(matches[1]['match_no'], run_json_task.call_args.kwargs['prompt'])
            self.assertNotIn(matches[0]['match_no'], run_json_task.call_args.kwargs['prompt'])
            self.assertEqual(run['id'], run_id)
            saved_items = harness.db.get_prediction_run_items(run_id)
            self.assertEqual([item['status'] for item in saved_items], ['pending', 'pending', 'pending'])
            self.assertEqual(saved_items[1]['retry_count'], 1)
            self.assertTrue(saved_items[1]['last_retry_at'])
            self.assertIsNone(saved_items[1]['last_retry_error'])

    def test_generate_prediction_auto_mode_skips_recent_failed_item_retry(self):
        with fresh_app_harness() as harness:
            _, user_id = harness.make_client()
            predictor_id = create_predictor(
                harness,
                user_id,
                'jingcai_football',
                data_injection_mode='summary'
            )
            predictor = harness.db.get_predictor(predictor_id, include_secret=True)
            matches = self._build_matches(harness, 1)
            run_id = self._create_existing_run(harness, predictor_id, matches, status='pending')
            harness.db.upsert_prediction_items([
                {
                    'run_id': run_id,
                    'predictor_id': predictor_id,
                    'lottery_type': 'jingcai_football',
                    'run_key': '2026-04-16',
                    'event_key': matches[0]['event_key'],
                    'item_order': 0,
                    'issue_no': matches[0]['match_no'],
                    'title': matches[0]['event_name'],
                    'requested_targets': ['spf', 'rqspf'],
                    'prediction_payload': {},
                    'actual_payload': {},
                    'hit_payload': {},
                    'confidence': None,
                    'reasoning_summary': '',
                    'raw_response': '{}',
                    'status': 'failed',
                    'error_message': '旧失败',
                    'retry_count': 1,
                    'last_retry_at': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
                    'last_retry_error': '旧错误',
                    'settled_at': None
                }
            ])
            batch_payload = {
                'batch_key': '2026-04-16',
                'dates': ['2026-04-16'],
                'matches': matches
            }

            with mock.patch.object(harness.module.jingcai_football_service, '_fetch_recent_history', return_value=[]), \
                 mock.patch.object(harness.module.jingcai_football_service, 'enrich_matches_for_prediction', side_effect=lambda _db, selected: selected), \
                 mock.patch('services.jingcai_football_service.AIPredictor.run_json_task', side_effect=AssertionError('冷却期内不应重试')):
                run = harness.module.jingcai_football_service.generate_prediction(
                    harness.db,
                    predictor,
                    auto_mode=True,
                    batch_payload=batch_payload
                )

            self.assertEqual(run['id'], run_id)
            saved_items = harness.db.get_prediction_run_items(run_id)
            self.assertEqual(saved_items[0]['retry_count'], 1)

    def test_run_auto_cycle_retries_existing_failed_items(self):
        with fresh_app_harness() as harness:
            _, user_id = harness.make_client()
            predictor_id = create_predictor(
                harness,
                user_id,
                'jingcai_football',
                data_injection_mode='summary'
            )
            matches = self._build_matches(harness, 1)
            run_id = self._create_existing_run(harness, predictor_id, matches, status='pending')
            harness.db.upsert_prediction_items([
                {
                    'run_id': run_id,
                    'predictor_id': predictor_id,
                    'lottery_type': 'jingcai_football',
                    'run_key': '2026-04-16',
                    'event_key': matches[0]['event_key'],
                    'item_order': 0,
                    'issue_no': matches[0]['match_no'],
                    'title': matches[0]['event_name'],
                    'requested_targets': ['spf', 'rqspf'],
                    'prediction_payload': {},
                    'actual_payload': {},
                    'hit_payload': {},
                    'confidence': None,
                    'reasoning_summary': '',
                    'raw_response': '{}',
                    'status': 'failed',
                    'error_message': '旧失败',
                    'retry_count': 0,
                    'last_retry_at': '2026-04-15 00:00:00',
                    'last_retry_error': None,
                    'settled_at': None
                }
            ])
            batch_payload = {
                'batch_key': '2026-04-16',
                'dates': ['2026-04-16'],
                'matches': matches
            }

            with mock.patch.object(harness.module.jingcai_football_service, 'settle_pending_predictions', return_value=[]), \
                 mock.patch.object(harness.module.jingcai_football_service, 'sync_matches', return_value=batch_payload), \
                 mock.patch.object(harness.module.jingcai_football_service, 'generate_prediction', return_value={'run_key': '2026-04-16', 'status': 'pending'}) as generate_prediction:
                result = harness.module.jingcai_football_service.run_auto_cycle(harness.db)

            self.assertEqual(generate_prediction.call_count, 1)
            self.assertEqual(result['predictions'][0]['status'], 'pending')

    def test_sync_matches_best_effort_retries_history_batch_with_cache_bust_for_overdue_pending_matches(self):
        with fresh_app_harness() as harness, \
             mock.patch('services.jingcai_football_service.HISTORY_RESULT_REFRESH_GRACE_MINUTES', 30):
            stale_match = harness.module.jingcai_football_service._normalize_match({
                'matchId': 'match-2039119',
                'tiCaiId': '2039119',
                'matchNo': '周四004',
                'matchNoValue': '4004',
                'league': '欧罗巴',
                'leagueOfficial': '欧罗巴',
                'team1': '维拉',
                'team2': '博洛尼亚',
                'team1Id': 't1',
                'team2Id': 't2',
                'matchTimeFormat': '2026-04-17 03:00:00',
                'showSellStatus': '2',
                'showSellStatusCn': '待开奖',
                'spfSellStatus': '3',
                'rqspfSellStatus': '3',
                'spf': '1.49,4.20,5.40',
                'rqspf': '-1,2.56,3.55,2.20',
                'score1': '',
                'score2': '',
                'halfScore1': '',
                'halfScore2': ''
            }, '2026-04-16')
            settled_match = harness.module.jingcai_football_service._normalize_match({
                'matchId': 'match-2039119',
                'tiCaiId': '2039119',
                'matchNo': '周四004',
                'matchNoValue': '4004',
                'league': '欧罗巴',
                'leagueOfficial': '欧罗巴',
                'team1': '维拉',
                'team2': '博洛尼亚',
                'team1Id': 't1',
                'team2Id': 't2',
                'matchTimeFormat': '2026-04-17 03:00:00',
                'showSellStatus': '3',
                'showSellStatusCn': '已开奖',
                'spfSellStatus': '3',
                'rqspfSellStatus': '3',
                'spf': '1.49,4.20,5.40',
                'rqspf': '-1,2.56,3.55,2.20',
                'score1': '4',
                'score2': '0',
                'halfScore1': '3',
                'halfScore2': '0'
            }, '2026-04-16')

            with mock.patch.object(harness.module.jingcai_football_service, 'current_sale_date', return_value='2026-04-17'), \
                 mock.patch('services.jingcai_football_service.get_current_beijing_time', return_value=datetime(2026, 4, 17, 12, 0, 0)), \
                 mock.patch.object(
                     harness.module.jingcai_football_service,
                     'sync_matches',
                     side_effect=[
                         {
                             'lottery_type': 'jingcai_football',
                             'batch_key': '2026-04-16',
                             'matches': [stale_match]
                         },
                         {
                             'lottery_type': 'jingcai_football',
                             'batch_key': '2026-04-16',
                             'matches': [settled_match]
                         }
                     ]
                 ) as sync_matches:
                payload, used_is_prized = harness.module.jingcai_football_service._sync_matches_best_effort(
                    harness.db,
                    '2026-04-16'
                )

            self.assertEqual(used_is_prized, '1')
            self.assertEqual(sync_matches.call_count, 2)
            self.assertFalse(sync_matches.call_args_list[0].kwargs['cache_bust'])
            self.assertTrue(sync_matches.call_args_list[1].kwargs['cache_bust'])
            self.assertTrue(payload['matches'][0]['settled'])


if __name__ == '__main__':
    unittest.main()
