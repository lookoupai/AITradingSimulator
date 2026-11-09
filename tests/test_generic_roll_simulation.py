from __future__ import annotations

import unittest
from unittest.mock import patch
from tests.test_pc28_roll_simulation import ReplayDB, NOW
from tests import test_pc28_roll_simulation as roll_fixture
from tests.support import fresh_app_harness


class GenericRollSimulationTests(unittest.TestCase):
    def simulate(self, totals, metric='big_small', mode='win_streak_roll', **selection):
        from services.profit_simulator import ProfitSimulator
        db = ReplayDB(totals)
        db.predictor['prediction_targets'] = ['big_small', 'odd_even', 'combo', 'number']
        db.predictor['profit_rule_id'] = 'pc28_fullpay_2_0'
        for prediction in db.predictions:
            prediction.update(prediction_big_small='大', prediction_odd_even='单', prediction_number=0)
        with patch('services.profit_simulator.get_current_beijing_time', return_value=NOW):
            return ProfitSimulator(db).build_profit_simulation(1, requested_metric=metric, bet_mode=mode,
                roll_config={'structure':'single_play', 'play_type':metric, **selection},
                selection_config={k:v for k,v in selection.items() if k in ('selection_mode','fixed_value')})

    def test_size_three_gates(self):
        result=self.simulate([20,20,20,20])
        self.assertEqual([r['stake_amount'] for r in result['records']], [100,200,400])
        self.assertEqual(result['summary']['ending_balance'],800)

    def test_fixed_big_changes_prediction(self):
        row=self.simulate([20],selection_mode='fixed',fixed_value='小')['records'][0]
        self.assertEqual(row['stake_amount'],100)
        self.assertEqual(row['net_profit'],-100)

    def test_kill_five_partial_refund_does_not_add_gate(self):
        result=self.simulate([13],metric='kill_group',target_streak=5,selection_mode='fixed',fixed_value='小双')
        row=result['records'][0]
        self.assertEqual(row['stake_amount'],99)
        self.assertEqual(row['net_profit'],-66)
        self.assertEqual(row['win_streak'],0)
        self.assertTrue(row['prediction_hit'])

    def test_double_ten_gates_configuration(self):
        row=self.simulate([15],metric='double_group',target_streak=10,selection_mode='fixed',fixed_value='大单+小双')['records'][0]
        self.assertEqual(row['target_streak'],10)
        self.assertEqual(len(row['legs']),2)

    def test_fixed_and_filter_supported_by_flat_and_martingale(self):
        for mode in ('flat','martingale'):
            result=self.simulate([20], mode=mode, selection_mode='fixed',fixed_value='小')
            self.assertLess(result['summary']['net_profit'],0)
            skipped=self.simulate([20], mode=mode, selection_mode='filter',fixed_value='小')
            self.assertEqual(skipped['summary']['bet_count'],0)

    def test_number_zero_can_win(self):
        result=self.simulate([0],metric='number')
        self.assertEqual(result['records'][0]['payout_amount'],100000)

    def test_sum_is_authoritative_over_stale_derived_fields(self):
        from services.profit_simulator import ProfitSimulator
        db=ReplayDB([20]); db.draws['1000']['big_small']='小'
        with patch('services.profit_simulator.get_current_beijing_time',return_value=NOW):
            result=ProfitSimulator(db).build_profit_simulation(1,bet_mode='win_streak_roll',
                roll_config={'structure':'single_play','play_type':'big_small','selection_mode':'fixed','fixed_value':'大'})
        self.assertGreater(result['summary']['net_profit'],0)

    def test_follow_groups_use_predicted_combo_derivation(self):
        for metric,expected in [('double_group',2),('kill_group',3)]:
            result=self.simulate([5],metric=metric)
            self.assertEqual(len(result['records'][0]['legs']),expected)
            self.assertTrue(result['records'][0]['prediction_hit'])

    def test_reverse_double_group_input_is_canonicalized(self):
        result=self.simulate([15],metric='double_group',selection_mode='fixed',fixed_value='小双+大单')
        self.assertEqual(result['roll_config']['fixed_value'],'大单+小双')

    def test_invalid_selection_rejected_in_ordinary_modes(self):
        for selection in ({'selection_mode':'unknown'}, {'selection_mode':'fixed','fixed_value':'单'}):
            with self.assertRaises(ValueError):
                self.simulate([20],mode='flat',**selection)


class GenericSelectionApiTests(unittest.TestCase):
    def test_private_and_public_generic_selection(self):
        with fresh_app_harness() as h:
            client,uid=h.make_client()
            pid=roll_fixture.RollApiTests().seed(h,uid)
            with patch('services.profit_simulator.get_current_beijing_time',return_value=NOW):
                for prefix in ('/api/predictors','/api/public/predictors'):
                    response=client.get(f'{prefix}/{pid}/simulation',query_string={
                        'bet_mode':'win_streak_roll','roll_structure':'single_play','roll_play_type':'kill_group',
                        'roll_selection_mode':'fixed','roll_fixed_value':'小双','roll_target_streak':'5'})
                    self.assertEqual(response.status_code,200,response.json)
                    row=response.json['simulation']['records'][0]
                    self.assertEqual((len(row['legs']),row['stake_amount'],row['target_streak']),(3,99,5))
                    flat=client.get(f'{prefix}/{pid}/simulation',query_string={
                        'bet_mode':'flat','metric':'big_small','play_selection_mode':'fixed','play_fixed_value':'大'})
                    self.assertEqual(flat.status_code,200,flat.json)
                    self.assertEqual(flat.json['simulation']['records'][0]['predicted_value'],'大')
                    invalid=client.get(f'{prefix}/{pid}/simulation',query_string={
                        'bet_mode':'win_streak_roll','roll_structure':'single_play','roll_play_type':'big_small',
                        'roll_selection_mode':'fixed','roll_fixed_value':'单'})
                    self.assertEqual(invalid.status_code,400)
