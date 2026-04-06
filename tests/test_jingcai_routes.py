from __future__ import annotations

import json
import unittest
from unittest import mock

from tests.support import create_predictor, fresh_app_harness


class JingcaiRouteTests(unittest.TestCase):
    def _build_match(
        self,
        harness,
        *,
        ticai_id: str,
        issue_no: str,
        show_sell_status: str,
        show_sell_status_cn: str,
        match_time: str,
        score1: str = '',
        score2: str = '',
        half_score1: str = '',
        half_score2: str = ''
    ) -> dict:
        return harness.module.jingcai_football_service._normalize_match({
            'matchId': f'match-{ticai_id}',
            'tiCaiId': ticai_id,
            'matchNo': issue_no,
            'matchNoValue': issue_no[-3:],
            'league': '测试联赛',
            'leagueOfficial': '测试联赛',
            'team1': f'{issue_no}主队',
            'team2': f'{issue_no}客队',
            'matchTimeFormat': match_time,
            'showSellStatus': show_sell_status,
            'showSellStatusCn': show_sell_status_cn,
            'spfSellStatus': '1' if show_sell_status == '1' else '3',
            'rqspfSellStatus': '1' if show_sell_status == '1' else '3',
            'spf': '1.55,3.20,4.80',
            'rqspf': '-1,3.80,3.20,1.80',
            'score1': score1,
            'score2': score2,
            'halfScore1': half_score1,
            'halfScore2': half_score2
        }, '2026-04-06')

    def _seed_event(
        self,
        harness,
        *,
        event_key: str,
        batch_key: str,
        issue_no: str,
        settled: bool,
        spf_sell_status: str = '2',
        rqspf_sell_status: str = '1'
    ):
        harness.db.upsert_lottery_events([
            {
                'lottery_type': 'jingcai_football',
                'event_key': event_key,
                'batch_key': batch_key,
                'event_date': batch_key,
                'event_time': f'{batch_key} 18:00:00',
                'event_name': f'[测试] {issue_no}',
                'league': '测试联赛',
                'home_team': '主队',
                'away_team': '客队',
                'status': '3' if settled else '1',
                'status_label': '已开奖' if settled else '已开售',
                'source_provider': 'sina',
                'result_payload': json.dumps({
                    'score1': 2 if settled else None,
                    'score2': 1 if settled else None,
                    'actual_spf': '胜' if settled else None,
                    'actual_rqspf': '平' if settled else None
                }, ensure_ascii=False),
                'meta_payload': json.dumps({
                    'match_no': issue_no,
                    'spf_sell_status': spf_sell_status,
                    'rqspf_sell_status': rqspf_sell_status,
                    'spf_odds': {'胜': 1.55, '平': 3.2, '负': 4.8},
                    'rqspf': {
                        'handicap': -1,
                        'handicap_text': '-1',
                        'odds': {'胜': 3.8, '平': 3.2, '负': 1.8}
                    },
                    'settled': settled
                }, ensure_ascii=False),
                'source_payload': '{}'
            }
        ])

    def test_replay_route_returns_cached_batch_when_sync_unavailable(self):
        with fresh_app_harness() as harness:
            client, user_id = harness.make_client()
            predictor_id = create_predictor(harness, user_id, 'jingcai_football')
            self._seed_event(
                harness,
                event_key='event-1',
                batch_key='2026-04-06',
                issue_no='周一001',
                settled=False
            )

            with mock.patch.object(harness.module.jingcai_football_service, 'sync_matches', side_effect=RuntimeError('offline')):
                response = client.post(f'/api/predictors/{predictor_id}/jingcai/replay', json={'date': '2026-04-06'})

            data = response.get_json()
            self.assertEqual(response.status_code, 200)
            self.assertEqual(data['used_is_prized'], 'cache')
            self.assertIn('回退本地缓存', data['warning'])
            self.assertEqual(data['overview']['match_count'], 1)

    def test_settle_route_uses_cached_events_and_settles_pending_item(self):
        with fresh_app_harness() as harness:
            client, user_id = harness.make_client()
            predictor_id = create_predictor(harness, user_id, 'jingcai_football')
            self._seed_event(
                harness,
                event_key='event-2',
                batch_key='2026-04-06',
                issue_no='周一002',
                settled=True
            )

            run_id = harness.db.upsert_prediction_run({
                'predictor_id': predictor_id,
                'lottery_type': 'jingcai_football',
                'run_key': '2026-04-06',
                'requested_targets': ['spf', 'rqspf'],
                'status': 'pending',
                'total_items': 1,
                'settled_items': 0,
                'hit_items': 0
            })
            harness.db.upsert_prediction_items([
                {
                    'run_id': run_id,
                    'predictor_id': predictor_id,
                    'lottery_type': 'jingcai_football',
                    'run_key': '2026-04-06',
                    'event_key': 'event-2',
                    'item_order': 0,
                    'issue_no': '周一002',
                    'title': '[测试] 周一002',
                    'requested_targets': ['spf', 'rqspf'],
                    'prediction_payload': {'spf': '胜', 'rqspf': '平'},
                    'actual_payload': {},
                    'hit_payload': {},
                    'confidence': 0.7,
                    'reasoning_summary': 'test',
                    'raw_response': '{}',
                    'status': 'pending',
                    'error_message': None,
                    'settled_at': None
                }
            ])

            with mock.patch.object(harness.module.jingcai_football_service, 'sync_matches', side_effect=RuntimeError('offline')):
                response = client.post(f'/api/predictors/{predictor_id}/jingcai/settle', json={'run_key': '2026-04-06'})

            data = response.get_json()
            self.assertEqual(response.status_code, 200)
            self.assertEqual(data['settled_items_count'], 1)
            self.assertEqual(data['pending_runs_count'], 0)

    def test_public_predictor_detail_exposes_jingcai_single_and_parlay_metrics(self):
        with fresh_app_harness() as harness:
            user_id = harness.db.create_user('public-user', harness.module.hash_password('password'))
            predictor_id = create_predictor(
                harness,
                user_id,
                'jingcai_football',
                share_level='records'
            )

            response = harness.app.test_client().get(f'/api/public/predictors/{predictor_id}')
            data = response.get_json()

            self.assertEqual(response.status_code, 200)
            labels = [item['label'] for item in data['predictor']['simulation_metrics']]
            self.assertIn('胜平负单关', labels)
            self.assertIn('胜平负二串一', labels)
            self.assertIn('让球胜平负单关', labels)
            self.assertIn('让球胜平负二串一', labels)

    def test_dashboard_current_prediction_exposes_odds_snapshots_in_market_snapshot(self):
        with fresh_app_harness() as harness:
            _, user_id = harness.make_client()
            predictor_id = create_predictor(harness, user_id, 'jingcai_football')

            self._seed_event(
                harness,
                event_key='event-4',
                batch_key='2026-04-06',
                issue_no='周一004',
                settled=False
            )
            harness.db.upsert_lottery_event_details([
                {
                    'lottery_type': 'jingcai_football',
                    'event_key': 'event-4',
                    'detail_type': 'odds_snapshots',
                    'source_provider': 'sina',
                    'payload': {
                        'euro': {
                            'company': '竞彩官方',
                            'initial': {'win': 1.8, 'draw': 3.3, 'lose': 4.2},
                            'current': {'win': 1.7, 'draw': 3.4, 'lose': 4.5},
                            'updated_at': '2026-04-06 17:55:00'
                        }
                    }
                }
            ])

            run_id = harness.db.upsert_prediction_run({
                'predictor_id': predictor_id,
                'lottery_type': 'jingcai_football',
                'run_key': '2026-04-06',
                'requested_targets': ['spf', 'rqspf'],
                'status': 'pending',
                'total_items': 1,
                'settled_items': 0,
                'hit_items': 0
            })
            harness.db.upsert_prediction_items([
                {
                    'run_id': run_id,
                    'predictor_id': predictor_id,
                    'lottery_type': 'jingcai_football',
                    'run_key': '2026-04-06',
                    'event_key': 'event-4',
                    'item_order': 0,
                    'issue_no': '周一004',
                    'title': '[测试] 周一004',
                    'requested_targets': ['spf', 'rqspf'],
                    'prediction_payload': {'spf': '胜', 'rqspf': '平'},
                    'actual_payload': {},
                    'hit_payload': {},
                    'confidence': 0.65,
                    'reasoning_summary': 'test',
                    'raw_response': '{}',
                    'status': 'pending',
                    'error_message': None,
                    'settled_at': None
                }
            ])

            dashboard = harness.module._get_predictor_dashboard_data(predictor_id)
            market_snapshot = dashboard['current_prediction']['items'][0]['market_snapshot']

            self.assertIn('odds_snapshots', market_snapshot)
            self.assertEqual(market_snapshot['odds_snapshots']['euro']['company'], '竞彩官方')

    def test_overview_route_falls_back_to_cached_events(self):
        with fresh_app_harness() as harness:
            self._seed_event(
                harness,
                event_key='event-3',
                batch_key='2026-04-06',
                issue_no='周一003',
                settled=False
            )

            with mock.patch.object(harness.module.jingcai_football_service, 'build_overview', side_effect=RuntimeError('offline')):
                response = harness.app.test_client().get('/api/lotteries/jingcai_football/overview')

            data = response.get_json()
            self.assertEqual(response.status_code, 200)
            self.assertEqual(data['match_count'], 1)
            self.assertIn('回退本地缓存', data['warning'])

    def test_overview_tracks_sale_open_and_awaiting_result_statuses(self):
        with fresh_app_harness() as harness:
            service = harness.module.jingcai_football_service
            matches = [
                self._build_match(
                    harness,
                    ticai_id='2038862',
                    issue_no='周一001',
                    show_sell_status='2',
                    show_sell_status_cn='待开奖',
                    match_time='2026-04-06 18:15:00'
                ),
                self._build_match(
                    harness,
                    ticai_id='2038863',
                    issue_no='周一002',
                    show_sell_status='3',
                    show_sell_status_cn='已开奖',
                    match_time='2026-04-06 18:30:00',
                    score1='0',
                    score2='0',
                    half_score1='0',
                    half_score2='0'
                ),
                self._build_match(
                    harness,
                    ticai_id='2038869',
                    issue_no='周一008',
                    show_sell_status='1',
                    show_sell_status_cn='已开售',
                    match_time='2026-04-06 22:00:00'
                )
            ]

            overview = service._build_overview_from_matches(matches, batch_key='2026-04-06', limit=10)

            self.assertEqual(overview['open_match_count'], 1)
            self.assertEqual(overview['sale_open_match_count'], 1)
            self.assertEqual(overview['awaiting_result_match_count'], 1)
            self.assertEqual(overview['settled_match_count'], 1)
            self.assertEqual(overview['next_match_name'], '[测试联赛] 周一008主队 vs 周一008客队')

    def test_generate_prediction_requires_sale_open_matches(self):
        with fresh_app_harness() as harness:
            _, user_id = harness.make_client()
            predictor_id = create_predictor(harness, user_id, 'jingcai_football')
            predictor = harness.db.get_predictor(predictor_id, include_secret=True)
            payload = {
                'lottery_type': 'jingcai_football',
                'batch_key': '2026-04-06',
                'dates': ['2026-04-06'],
                'matches': [
                    self._build_match(
                        harness,
                        ticai_id='2038862',
                        issue_no='周一001',
                        show_sell_status='2',
                        show_sell_status_cn='待开奖',
                        match_time='2026-04-06 18:15:00'
                    )
                ]
            }

            with mock.patch.object(harness.module.jingcai_football_service, 'sync_matches', return_value=payload):
                with self.assertRaisesRegex(ValueError, '已开售状态'):
                    harness.module.jingcai_football_service.generate_prediction(harness.db, predictor)


if __name__ == '__main__':
    unittest.main()
