from __future__ import annotations

import json
import sys
import tempfile
import types
import unittest

if 'dotenv' not in sys.modules:
    dotenv_stub = types.ModuleType('dotenv')
    dotenv_stub.load_dotenv = lambda *args, **kwargs: None
    sys.modules['dotenv'] = dotenv_stub

from database import Database
from services.consensus_analysis_service import build_consensus_analysis


class ConsensusAnalysisServiceTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.db = Database(f'{self.tempdir.name}/test.db')
        self.db.init_db()
        self.user_id = self.db.create_user('tester', 'hash')

    def tearDown(self):
        self.tempdir.cleanup()

    def _seed_event(
        self,
        *,
        event_key: str,
        spf_odds: dict,
        rqspf_handicap: int,
        rqspf_odds: dict,
        settled: bool
    ) -> None:
        self.db.upsert_lottery_events([
            {
                'lottery_type': 'jingcai_football',
                'event_key': event_key,
                'batch_key': '2026-04-06',
                'event_date': '2026-04-06',
                'event_time': '2026-04-06 18:00:00',
                'event_name': f'[测试] {event_key}',
                'league': '测试联赛',
                'home_team': '主队',
                'away_team': '客队',
                'status': '3' if settled else '1',
                'status_label': '已开奖' if settled else '已开售',
                'source_provider': 'sina',
                'result_payload': json.dumps({
                    'score1': 2 if settled else None,
                    'score2': 0 if settled else None,
                    'actual_spf': '胜' if settled else None,
                    'actual_rqspf': '胜' if settled else None
                }, ensure_ascii=False),
                'meta_payload': json.dumps({
                    'match_no': event_key,
                    'spf_odds': spf_odds,
                    'rqspf': {
                        'handicap': rqspf_handicap,
                        'handicap_text': str(rqspf_handicap),
                        'odds': rqspf_odds
                    },
                    'settled': settled
                }, ensure_ascii=False),
                'source_payload': '{}'
            }
        ])

    def _seed_prediction_item(
        self,
        *,
        predictor_id: int,
        run_id: int,
        event_key: str,
        spf: str | None,
        rqspf: str | None,
        hit_spf: int | None,
        hit_rqspf: int | None,
        status: str
    ) -> None:
        self.db.upsert_prediction_items([
            {
                'run_id': run_id,
                'predictor_id': predictor_id,
                'lottery_type': 'jingcai_football',
                'run_key': '2026-04-06',
                'event_key': event_key,
                'item_order': 0,
                'issue_no': event_key,
                'title': f'[测试] {event_key}',
                'requested_targets': ['spf', 'rqspf'],
                'prediction_payload': {'spf': spf, 'rqspf': rqspf},
                'actual_payload': {'actual_spf': '胜', 'actual_rqspf': '胜'} if status == 'settled' else {},
                'hit_payload': {'spf': hit_spf, 'rqspf': hit_rqspf},
                'confidence': 0.7,
                'reasoning_summary': 'test',
                'raw_response': '{}',
                'status': status,
                'error_message': None,
                'settled_at': '2026-04-06 20:00:00' if status == 'settled' else None
            }
        ])

    def test_build_consensus_analysis_segments_spf_and_rqspf(self):
        predictor_ids = [
            self.db.create_predictor(
                user_id=self.user_id,
                name=f'方案{i}',
                lottery_type='jingcai_football',
                engine_type='ai',
                algorithm_key='',
                api_key='key',
                api_url='https://example.com',
                model_name='model',
                api_mode='auto',
                primary_metric='spf',
                profit_default_metric='spf',
                profit_rule_id='jingcai_snapshot',
                share_level='records',
                prediction_method='test',
                system_prompt='test',
                data_injection_mode='summary',
                prediction_targets=['spf', 'rqspf'],
                history_window=20,
                temperature=0.3,
                enabled=True
            )
            for i in range(1, 4)
        ]
        run_ids = []
        for predictor_id in predictor_ids:
            run_ids.append(self.db.upsert_prediction_run({
                'predictor_id': predictor_id,
                'lottery_type': 'jingcai_football',
                'run_key': '2026-04-06',
                'requested_targets': ['spf', 'rqspf'],
                'status': 'settled',
                'total_items': 1,
                'settled_items': 1,
                'hit_items': 1
            }))

        self._seed_event(
            event_key='settled-favorite',
            spf_odds={'胜': 1.42, '平': 4.10, '负': 6.20},
            rqspf_handicap=-1,
            rqspf_odds={'胜': 2.05, '平': 3.40, '负': 3.10},
            settled=True
        )
        self._seed_event(
            event_key='settled-underdog',
            spf_odds={'胜': 4.60, '平': 3.30, '负': 1.66},
            rqspf_handicap=1,
            rqspf_odds={'胜': 1.72, '平': 3.25, '负': 4.25},
            settled=True
        )

        for predictor_id, run_id in zip(predictor_ids, run_ids):
            self._seed_prediction_item(
                predictor_id=predictor_id,
                run_id=run_id,
                event_key='settled-favorite',
                spf='胜',
                rqspf='胜',
                hit_spf=1,
                hit_rqspf=1,
                status='settled'
            )
            self._seed_prediction_item(
                predictor_id=predictor_id,
                run_id=run_id,
                event_key='settled-underdog',
                spf='胜',
                rqspf='胜',
                hit_spf=0,
                hit_rqspf=1,
                status='settled'
            )

        pending_run_ids = []
        for predictor_id in predictor_ids:
            pending_run_ids.append(self.db.upsert_prediction_run({
                'predictor_id': predictor_id,
                'lottery_type': 'jingcai_football',
                'run_key': '2026-04-07',
                'requested_targets': ['spf', 'rqspf'],
                'status': 'pending',
                'total_items': 1,
                'settled_items': 0,
                'hit_items': 0
            }))

        self._seed_event(
            event_key='pending-favorite',
            spf_odds={'胜': 1.48, '平': 4.00, '负': 5.80},
            rqspf_handicap=-1,
            rqspf_odds={'胜': 2.10, '平': 3.35, '负': 3.00},
            settled=False
        )
        for predictor_id, run_id in zip(predictor_ids, pending_run_ids):
            self._seed_prediction_item(
                predictor_id=predictor_id,
                run_id=run_id,
                event_key='pending-favorite',
                spf='胜',
                rqspf='胜',
                hit_spf=None,
                hit_rqspf=None,
                status='pending'
            )

        analysis = build_consensus_analysis(
            self.db,
            user_id=self.user_id,
            lottery_type='jingcai_football',
            time_window_days=30
        )

        spf_rows = analysis['consensus_by_count']['spf']
        favorite_row = next(
            row for row in spf_rows
            if row['agree_count'] == 3
            and row['value'] == '胜'
            and row['market_segment'] == 'spf:favorite:ultra_low'
        )
        underdog_row = next(
            row for row in spf_rows
            if row['agree_count'] == 3
            and row['value'] == '胜'
            and row['market_segment'] == 'spf:underdog:high'
        )
        self.assertEqual(favorite_row['match_total'], 1)
        self.assertEqual(favorite_row['match_hit'], 1)
        self.assertEqual(underdog_row['match_total'], 1)
        self.assertEqual(underdog_row['match_hit'], 0)

        rqspf_rows = analysis['consensus_by_count']['rqspf']
        give_row = next(
            row for row in rqspf_rows
            if row['agree_count'] == 3
            and row['value'] == '胜'
            and row['market_segment'] == 'rqspf:home_give:cover'
        )
        receive_row = next(
            row for row in rqspf_rows
            if row['agree_count'] == 3
            and row['value'] == '胜'
            and row['market_segment'] == 'rqspf:home_receive:protected_win'
        )
        self.assertEqual(give_row['market_segment_label'], '主让/打穿')
        self.assertEqual(receive_row['market_segment_label'], '主受让/受让方赢盘')

        pair_segment_rows = analysis['pair_segment_combinations']['spf']
        pair_favorite_row = next(
            row for row in pair_segment_rows
            if row['value'] == '胜'
            and row['market_segment'] == 'spf:favorite:ultra_low'
        )
        self.assertEqual(pair_favorite_row['total'], 1)
        self.assertEqual(pair_favorite_row['hit'], 1)

        predictor_segment_rows = analysis['per_predictor_segments']['rqspf']
        predictor_give_row = next(
            row for row in predictor_segment_rows
            if row['value'] == '胜'
            and row['market_segment'] == 'rqspf:home_give:cover'
        )
        self.assertEqual(predictor_give_row['market_segment_label'], '主让/打穿')
        self.assertEqual(predictor_give_row['total'], 1)
        self.assertEqual(predictor_give_row['hit'], 1)

        today_spf = next(
            field for field in analysis['today_recommendations'][0]['fields']
            if field['field'] == 'spf'
        )
        today_rqspf = next(
            field for field in analysis['today_recommendations'][0]['fields']
            if field['field'] == 'rqspf'
        )
        self.assertEqual(today_spf['market_segment'], 'spf:favorite:ultra_low')
        self.assertEqual(today_spf['market_segment_label'], '热门项/超低赔')
        self.assertEqual(today_rqspf['market_segment'], 'rqspf:home_give:cover')
        self.assertEqual(today_rqspf['market_segment_label'], '主让/打穿')


if __name__ == '__main__':
    unittest.main()
