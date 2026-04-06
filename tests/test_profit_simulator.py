from __future__ import annotations

import importlib
import json
import unittest
from datetime import timedelta

from tests.support import create_predictor, fresh_app_harness


class ProfitSimulatorTests(unittest.TestCase):
    def test_pc28_martingale_stake_progression(self):
        with fresh_app_harness() as harness:
            _, user_id = harness.make_client()
            predictor_id = create_predictor(
                harness,
                user_id,
                'pc28',
                primary_metric='big_small',
                profit_default_metric='big_small',
                prediction_targets=['big_small']
            )

            timezone = importlib.import_module('utils.timezone')
            period = timezone.get_pc28_day_window(timezone.get_current_beijing_time())
            start = period['start']

            harness.db.upsert_draws('pc28', [
                {
                    'issue_no': '1001',
                    'draw_date': start.strftime('%Y-%m-%d'),
                    'draw_time': '00:01:00',
                    'open_time': (start + timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S'),
                    'result_number': 20,
                    'result_number_text': '20',
                    'big_small': '大',
                    'odd_even': '双',
                    'combo': '大双',
                    'source_payload': json.dumps({'number': '6,7,7'}, ensure_ascii=False)
                },
                {
                    'issue_no': '1002',
                    'draw_date': start.strftime('%Y-%m-%d'),
                    'draw_time': '00:02:00',
                    'open_time': (start + timedelta(minutes=2)).strftime('%Y-%m-%d %H:%M:%S'),
                    'result_number': 21,
                    'result_number_text': '21',
                    'big_small': '大',
                    'odd_even': '单',
                    'combo': '大单',
                    'source_payload': json.dumps({'number': '7,7,7'}, ensure_ascii=False)
                },
                {
                    'issue_no': '1003',
                    'draw_date': start.strftime('%Y-%m-%d'),
                    'draw_time': '00:03:00',
                    'open_time': (start + timedelta(minutes=3)).strftime('%Y-%m-%d %H:%M:%S'),
                    'result_number': 22,
                    'result_number_text': '22',
                    'big_small': '大',
                    'odd_even': '双',
                    'combo': '大双',
                    'source_payload': json.dumps({'number': '7,7,8'}, ensure_ascii=False)
                }
            ])

            harness.db.upsert_prediction({
                'predictor_id': predictor_id,
                'lottery_type': 'pc28',
                'issue_no': '1001',
                'requested_targets': ['big_small'],
                'prediction_big_small': '小',
                'status': 'settled'
            })
            harness.db.upsert_prediction({
                'predictor_id': predictor_id,
                'lottery_type': 'pc28',
                'issue_no': '1002',
                'requested_targets': ['big_small'],
                'prediction_big_small': '小',
                'status': 'settled'
            })
            harness.db.upsert_prediction({
                'predictor_id': predictor_id,
                'lottery_type': 'pc28',
                'issue_no': '1003',
                'requested_targets': ['big_small'],
                'prediction_big_small': '大',
                'status': 'settled'
            })

            simulation = harness.module.profit_simulator.build_today_simulation(
                predictor_id,
                requested_metric='big_small',
                bet_mode='martingale',
                base_stake=10,
                multiplier=2,
                max_steps=6,
                include_records=True
            )

            self.assertEqual([item['bet_step'] for item in simulation['records'][:3]], [1, 2, 3])
            self.assertEqual([item['stake_amount'] for item in simulation['records'][:3]], [10.0, 20.0, 40.0])

    def test_jingcai_sell_status_rules_for_single_and_parlay(self):
        football = importlib.import_module('utils.jingcai_football')

        single_meta = {'spf_sell_status': '2', 'spf_odds': {'胜': 1.55}, 'settled': False}
        non_single_meta = {'spf_sell_status': '1', 'spf_odds': {'胜': 1.55}, 'settled': False}
        closed_meta = {'spf_sell_status': '0', 'spf_odds': {'胜': 1.55}, 'settled': False}
        settled_status_meta = {'spf_sell_status': '3', 'spf_odds': {'胜': 1.55}, 'settled': False}
        settled_meta = {'spf_sell_status': '2', 'spf_odds': {'胜': 1.55}, 'settled': True}

        self.assertTrue(football.is_metric_sellable('spf', single_meta, '胜', play_mode='single'))
        self.assertFalse(football.is_metric_sellable('spf', non_single_meta, '胜', play_mode='single'))
        self.assertTrue(football.is_metric_sellable('spf', non_single_meta, '胜', play_mode='parlay'))
        self.assertTrue(football.is_metric_sellable('spf', single_meta, '胜', play_mode='parlay'))
        self.assertFalse(football.is_metric_sellable('spf', closed_meta, '胜', play_mode='parlay'))
        self.assertFalse(football.is_metric_sellable('spf', settled_status_meta, '胜', play_mode='single'))
        self.assertFalse(football.is_metric_sellable('spf', settled_status_meta, '胜', play_mode='parlay'))
        self.assertFalse(football.is_metric_sellable('spf', settled_meta, '胜', play_mode='single'))
        self.assertTrue(football.is_metric_sellable('spf', settled_meta, '胜', play_mode='single', allow_settled=True))

    def test_jingcai_profit_simulation_supports_settled_history_for_single_and_parlay(self):
        with fresh_app_harness() as harness:
            _, user_id = harness.make_client()
            predictor_id = create_predictor(
                harness,
                user_id,
                'jingcai_football',
                primary_metric='spf',
                profit_default_metric='spf',
                prediction_targets=['spf', 'rqspf']
            )

            harness.db.upsert_lottery_events([
                {
                    'lottery_type': 'jingcai_football',
                    'event_key': 'e1',
                    'batch_key': '2026-04-06',
                    'event_date': '2026-04-06',
                    'event_time': '2026-04-06 18:00:00',
                    'event_name': '[测试] 主队A vs 客队A',
                    'league': '测试联赛',
                    'home_team': '主队A',
                    'away_team': '客队A',
                    'status': '3',
                    'status_label': '已开奖',
                    'source_provider': 'sina',
                    'result_payload': json.dumps({
                        'score1': 2,
                        'score2': 1,
                        'actual_spf': '胜',
                        'actual_rqspf': '平'
                    }, ensure_ascii=False),
                    'meta_payload': json.dumps({
                        'match_no': '周一001',
                        'spf_sell_status': '2',
                        'rqspf_sell_status': '1',
                        'spf_odds': {'胜': 1.55, '平': 3.3, '负': 4.8},
                        'rqspf': {
                            'handicap': -1,
                            'handicap_text': '-1',
                            'odds': {'胜': 3.8, '平': 3.2, '负': 1.8}
                        },
                        'settled': True
                    }, ensure_ascii=False),
                    'source_payload': '{}'
                },
                {
                    'lottery_type': 'jingcai_football',
                    'event_key': 'e2',
                    'batch_key': '2026-04-06',
                    'event_date': '2026-04-06',
                    'event_time': '2026-04-06 20:00:00',
                    'event_name': '[测试] 主队B vs 客队B',
                    'league': '测试联赛',
                    'home_team': '主队B',
                    'away_team': '客队B',
                    'status': '3',
                    'status_label': '已开奖',
                    'source_provider': 'sina',
                    'result_payload': json.dumps({
                        'score1': 1,
                        'score2': 1,
                        'actual_spf': '平',
                        'actual_rqspf': '平'
                    }, ensure_ascii=False),
                    'meta_payload': json.dumps({
                        'match_no': '周一002',
                        'spf_sell_status': '2',
                        'rqspf_sell_status': '1',
                        'spf_odds': {'胜': 2.3, '平': 2.8, '负': 2.9},
                        'rqspf': {
                            'handicap': -1,
                            'handicap_text': '-1',
                            'odds': {'胜': 5.7, '平': 3.6, '负': 1.46}
                        },
                        'settled': True
                    }, ensure_ascii=False),
                    'source_payload': '{}'
                }
            ])

            run_id = harness.db.upsert_prediction_run({
                'predictor_id': predictor_id,
                'lottery_type': 'jingcai_football',
                'run_key': '2026-04-06',
                'requested_targets': ['spf', 'rqspf'],
                'status': 'settled',
                'total_items': 2,
                'settled_items': 2,
                'hit_items': 3,
                'confidence': 0.66,
                'reasoning_summary': 'test'
            })
            harness.db.upsert_prediction_items([
                {
                    'run_id': run_id,
                    'predictor_id': predictor_id,
                    'lottery_type': 'jingcai_football',
                    'run_key': '2026-04-06',
                    'event_key': 'e1',
                    'item_order': 0,
                    'issue_no': '周一001',
                    'title': '[测试] 主队A vs 客队A',
                    'requested_targets': ['spf', 'rqspf'],
                    'prediction_payload': {'spf': '胜', 'rqspf': '平'},
                    'actual_payload': {'spf': '胜', 'rqspf': '平'},
                    'hit_payload': {'spf': 1, 'rqspf': 1},
                    'confidence': 0.7,
                    'reasoning_summary': 'test',
                    'raw_response': '{}',
                    'status': 'settled',
                    'error_message': None,
                    'settled_at': '2026-04-06 12:00:00'
                },
                {
                    'run_id': run_id,
                    'predictor_id': predictor_id,
                    'lottery_type': 'jingcai_football',
                    'run_key': '2026-04-06',
                    'event_key': 'e2',
                    'item_order': 1,
                    'issue_no': '周一002',
                    'title': '[测试] 主队B vs 客队B',
                    'requested_targets': ['spf', 'rqspf'],
                    'prediction_payload': {'spf': '平', 'rqspf': '平'},
                    'actual_payload': {'spf': '平', 'rqspf': '平'},
                    'hit_payload': {'spf': 1, 'rqspf': 1},
                    'confidence': 0.68,
                    'reasoning_summary': 'test',
                    'raw_response': '{}',
                    'status': 'settled',
                    'error_message': None,
                    'settled_at': '2026-04-06 12:10:00'
                }
            ])

            single_simulation = harness.module.profit_simulator.build_today_simulation(
                predictor_id,
                requested_metric='spf',
                include_records=True
            )
            parlay_simulation = harness.module.profit_simulator.build_today_simulation(
                predictor_id,
                requested_metric='rqspf_parlay',
                include_records=True
            )

            self.assertEqual(single_simulation['metric'], 'spf')
            self.assertEqual(len(single_simulation['records']), 2)
            self.assertEqual(single_simulation['summary']['bet_count'], 2)
            self.assertEqual(single_simulation['records'][0]['result_type'], 'hit')
            self.assertEqual(parlay_simulation['metric'], 'rqspf_parlay')
            self.assertEqual(len(parlay_simulation['records']), 1)
            self.assertEqual(parlay_simulation['summary']['bet_count'], 1)
            self.assertEqual(parlay_simulation['records'][0]['result_type'], 'hit')


if __name__ == '__main__':
    unittest.main()
