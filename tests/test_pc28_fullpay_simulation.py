from __future__ import annotations

import json
import unittest
from datetime import timedelta

from services.profit_simulator import (
    PC28_PROFIT_RULES,
    build_pc28_special_flags,
    settle_pc28_metric
)
from tests.support import create_predictor, fresh_app_harness
from utils.pc28 import is_pc28_straight, normalize_profit_rule, to_export_profit_rule_id
from utils.timezone import get_current_beijing_time, get_pc28_day_window


def _profile(rule_id: str, metric: str) -> dict:
    return PC28_PROFIT_RULES[rule_id]['metrics'][metric]['regular']


def _flags(result_number: int, number_text: str, *, wraparound: bool) -> dict:
    return build_pc28_special_flags(
        {
            'result_number': result_number,
            'source_payload': json.dumps({'number': number_text}, ensure_ascii=False)
        },
        wraparound=wraparound
    )


def _settle(rule_id: str, metric: str, *, hit: bool, result_number: int, number_text: str, stake: float = 10.0):
    profile = _profile(rule_id, metric)
    flags = _flags(result_number, number_text, wraparound=bool(profile.get('straight_wraparound', True)))
    if metric == 'combo':
        odds = next(iter((profile.get('group_odds') or {}).values()))
    else:
        odds = float(profile['odds'])
    return settle_pc28_metric(
        hit=hit,
        odds=float(odds),
        stake_amount=stake,
        sum_value=result_number,
        special_flags=flags,
        profile=profile
    )


class Pc28FullpaySimulationTests(unittest.TestCase):
    def test_export_mapping_keeps_legacy_protocol_ids(self):
        self.assertEqual(to_export_profit_rule_id('pc28_fullpay_netdisk'), 'pc28_netdisk')
        self.assertEqual(to_export_profit_rule_id('pc28_fullpay_2_0'), 'pc28_netdisk')
        self.assertEqual(to_export_profit_rule_id('pc28_fullpay_2_8'), 'pc28_high')
        self.assertEqual(to_export_profit_rule_id('pc28_fullpay_3_2'), 'pc28_high')
        self.assertEqual(to_export_profit_rule_id('pc28_high'), 'pc28_high')
        self.assertEqual(normalize_profit_rule('pc28_fullpay_2_8'), 'pc28_fullpay_2_8')
        self.assertEqual(normalize_profit_rule('not-a-rule'), 'pc28_netdisk')

    def test_catalog_exposes_four_fullpay_houses(self):
        self.assertIn('pc28_fullpay_netdisk', PC28_PROFIT_RULES)
        self.assertIn('pc28_fullpay_2_0', PC28_PROFIT_RULES)
        self.assertIn('pc28_fullpay_2_8', PC28_PROFIT_RULES)
        self.assertIn('pc28_fullpay_3_2', PC28_PROFIT_RULES)
        self.assertEqual(_profile('pc28_high', 'big_small')['odds'], 2.846)
        self.assertEqual(_profile('pc28_fullpay_2_8', 'big_small')['odds'], 2.84)
        self.assertEqual(_profile('pc28_fullpay_2_8', 'combo')['group_odds']['小单 / 大双'], 6.79)

    def test_wraparound_straight_only_when_enabled(self):
        self.assertTrue(is_pc28_straight((8, 9, 0), wraparound=True))
        self.assertFalse(is_pc28_straight((8, 9, 0), wraparound=False))
        self.assertTrue(is_pc28_straight((1, 2, 3), wraparound=False))

    def test_cai28_refunds_0_27_on_hit_only_not_combo(self):
        miss_type, miss_reason, odds = _settle(
            'pc28_fullpay_netdisk', 'big_small', hit=False, result_number=0, number_text='0,0,0'
        )
        self.assertEqual(miss_type, 'miss')
        self.assertIsNone(miss_reason)
        self.assertEqual(odds, 1.99)

        hit_type, hit_reason, _hit_odds = _settle(
            'pc28_fullpay_netdisk', 'big_small', hit=True, result_number=0, number_text='0,0,0'
        )
        self.assertEqual(hit_type, 'refund')
        self.assertEqual(hit_reason, '0/27 退本金')

        combo_type, combo_reason, _combo_odds = _settle(
            'pc28_fullpay_netdisk', 'combo', hit=True, result_number=0, number_text='0,0,0'
        )
        self.assertEqual(combo_type, 'hit')
        self.assertIsNone(combo_reason)

    def test_fullpay_2_0_combo_refunds_13_and_size_drops_odds_over_2001(self):
        combo_type, combo_reason, _odds = _settle(
            'pc28_fullpay_2_0', 'combo', hit=True, result_number=13, number_text='4,4,5'
        )
        self.assertEqual(combo_type, 'refund')
        self.assertEqual(combo_reason, '13/14 退本金')

        low = _settle(
            'pc28_fullpay_2_0', 'big_small', hit=True, result_number=13, number_text='4,4,5', stake=2001
        )
        high = _settle(
            'pc28_fullpay_2_0', 'big_small', hit=True, result_number=13, number_text='4,4,5', stake=2002
        )
        self.assertEqual(low[0], 'hit')
        self.assertEqual(low[2], 2.0)
        self.assertEqual(high[0], 'hit')
        self.assertEqual(high[2], 1.98)

    def test_fullpay_2_8_keeps_high_style_refund_with_new_odds(self):
        clean_hit = _settle('pc28_fullpay_2_8', 'big_small', hit=True, result_number=12, number_text='2,3,7')
        self.assertEqual(clean_hit[0], 'hit')
        self.assertEqual(clean_hit[2], 2.84)

        pair = _settle('pc28_fullpay_2_8', 'big_small', hit=True, result_number=20, number_text='6,7,7')
        self.assertEqual(pair[0], 'refund')
        self.assertEqual(pair[1], '对子退本金')

        wrap = _settle('pc28_fullpay_2_8', 'combo', hit=True, result_number=17, number_text='8,9,0')
        self.assertEqual(wrap[0], 'refund')
        self.assertEqual(wrap[1], '顺子退本金')

        no_wrap = _settle('pc28_fullpay_2_0', 'combo', hit=True, result_number=17, number_text='8,9,0')
        self.assertEqual(no_wrap[0], 'hit')

        legacy = _settle('pc28_high', 'big_small', hit=True, result_number=12, number_text='2,3,7')
        self.assertEqual(legacy[0], 'hit')
        self.assertEqual(legacy[2], 2.846)

    def test_fullpay_3_2_refunds_abc_0_9_on_hit_only(self):
        pair = _settle('pc28_fullpay_3_2', 'combo', hit=True, result_number=4, number_text='1,1,2')
        self.assertEqual(pair[0], 'hit')

        edge_hit = _settle('pc28_fullpay_3_2', 'big_small', hit=True, result_number=2, number_text='1,1,0')
        self.assertEqual(edge_hit[0], 'refund')
        self.assertEqual(edge_hit[1], 'ABC 含 0/9 退本金')

        edge_miss = _settle('pc28_fullpay_3_2', 'big_small', hit=False, result_number=0, number_text='0,0,0')
        self.assertEqual(edge_miss[0], 'miss')

    def test_simulation_uses_cai28_refund_when_small_hits_zero(self):
        with fresh_app_harness() as harness:
            _, user_id = harness.make_client()
            predictor_id = create_predictor(
                harness,
                user_id,
                'pc28',
                primary_metric='big_small',
                profit_default_metric='big_small',
                prediction_targets=['big_small'],
                profit_rule_id='pc28_fullpay_netdisk'
            )
            period = get_pc28_day_window(get_current_beijing_time())
            start = period['start']
            harness.db.upsert_draws('pc28', [
                {
                    'issue_no': '9001',
                    'draw_date': start.strftime('%Y-%m-%d'),
                    'draw_time': '00:01:00',
                    'open_time': (start + timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S'),
                    'result_number': 0,
                    'result_number_text': '00',
                    'big_small': '小',
                    'odd_even': '双',
                    'combo': '小双',
                    'source_payload': json.dumps({'number': '0,0,0'}, ensure_ascii=False)
                }
            ])
            harness.db.upsert_prediction({
                'predictor_id': predictor_id,
                'lottery_type': 'pc28',
                'issue_no': '9001',
                'requested_targets': ['big_small'],
                'prediction_big_small': '小',
                'status': 'settled'
            })
            simulation = harness.module.profit_simulator.build_profit_simulation(
                predictor_id,
                requested_metric='big_small',
                profit_rule_id='pc28_fullpay_netdisk',
                include_records=True
            )
            self.assertEqual(simulation['profit_rule_id'], 'pc28_fullpay_netdisk')
            self.assertEqual(simulation['records'][0]['result_type'], 'refund')
            self.assertEqual(simulation['records'][0]['net_profit'], 0.0)


if __name__ == '__main__':
    unittest.main()
