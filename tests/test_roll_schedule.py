from __future__ import annotations

import json
import unittest
from datetime import datetime,timedelta
from unittest.mock import patch
from tests.test_pc28_roll_simulation import ReplayDB

class ScheduledReplayTests(unittest.TestCase):
    def data(self):
        times=[datetime(2026,11,11,0,2,30),datetime(2026,11,11,0,6),datetime(2026,11,11,0,9,30),datetime(2026,11,11,0,13)]
        db=ReplayDB([18,16,9,19],times=times,combos=['小双']*4)
        db.predictor['profit_rule_id']='pc28_fullpay_2_8'
        published=['2026-11-10 15:59:50','2026-11-10 16:03:00','2026-11-10 16:06:11','2026-11-10 16:10:23']
        created=['2026-11-10 16:00:13','2026-11-10 16:04:06','2026-11-10 16:06:36','2026-11-10 16:11:13']
        for i,(row,triplet) in enumerate(zip(db.predictions,[(3,7,8),(5,4,7),(0,5,4),(6,6,7)])):
            row.update(created_at=created[i],external_fetched_at=published[i])
            db.draws[row['issue_no']]['source_payload']=json.dumps({'number':'+'.join(map(str,triplet))})
        return db

    def schedule(self, **kwargs):
        return {'windows':[{'id':'midnight','start':'00:00','end':'00:59'}],
                'allow_pre_window_signal':True,'signal_max_age_seconds':300,'min_remaining_seconds':40,**kwargs}

    def run_replay(self, db=None, **kwargs):
        from services.profit_simulator import ProfitSimulator
        config={'structure':'single_play','play_type':'kill_group','selection_mode':'follow','base_amount':10,
                'target_streak':3,'schedule':self.schedule(),**kwargs}
        with patch('services.profit_simulator.get_current_beijing_time',return_value=datetime(2026,11,11,23,59,59)):
            return ProfitSimulator(db or self.data()).build_profit_simulation(1,bet_mode='win_streak_roll',roll_config=config)

    def test_midnight_opt_in_matches_three_wins_and_old_mode_matches_live_loss(self):
        enabled=self.run_replay()
        self.assertEqual([r['stake_amount'] for r in enabled['records']],[9,18,39])
        self.assertEqual(enabled['summary']['net_profit'],83.38)
        strict=self.run_replay(schedule=self.schedule(allow_pre_window_signal=False))
        self.assertEqual([r['issue_no'] for r in strict['records']],['1001','1002','1003'])
        self.assertEqual(strict['summary']['net_profit'],-4.89)
        self.assertEqual(strict['skip_reasons']['schedule_signal_before_window'],1)

    def test_publication_and_availability_are_distinct_and_buffer_is_enforced(self):
        db=self.data();db.predictions[0]['created_at']='2026-11-10 16:02:15'
        result=self.run_replay(db)
        self.assertEqual(result['records'][0]['issue_no'],'1001')
        self.assertEqual(result['skip_reasons']['window_too_short'],1)
        db=self.data();db.predictions[0]['external_fetched_at']='2026-11-10 15:50:00'
        self.assertEqual(self.run_replay(db)['skip_reasons']['schedule_signal_stale'],1)

    def test_receive_delay_can_reproduce_missed_deadline(self):
        result=self.run_replay(schedule=self.schedule(receive_delay_seconds=120))
        self.assertIn('window_too_short',result['skip_reasons'])

    def test_window_starts_are_capped_while_existing_cycle_continues_after_window(self):
        window={'id':'short','start':'00:00','end':'00:01'}
        result=self.run_replay(schedule=self.schedule(windows=[window]))
        self.assertEqual(result['summary']['target_cycle_count'],1)
        result=self.run_replay(target_streak=1,max_cycles_per_day=3,on_target='new_cycle',
                               schedule=self.schedule(max_starts_per_window=1))
        self.assertEqual(result['summary']['cycle_count'],1)
        self.assertEqual(result['skip_reasons']['schedule_window_limit'],3)
        result=self.run_replay(target_streak=1,max_cycles_per_day=3,on_target='new_cycle',
                               schedule=self.schedule(max_starts_per_window=2))
        self.assertEqual(result['summary']['cycle_count'],2)

    def test_weekdays_empty_and_unlimited_window(self):
        self.assertEqual(self.run_replay(schedule=self.schedule(weekdays=[]))['summary']['bet_count'],0)
        result=self.run_replay(target_streak=1,on_target='new_cycle',max_cycles_per_day=3,
                              schedule=self.schedule(max_starts_per_window=0))
        self.assertEqual(result['summary']['cycle_count'],3)

    def test_wait_applies_cooldown_but_immediate_restart_bypasses_it(self):
        scheduled=self.schedule(max_starts_per_window=0,cooldown_issues=2)
        result=self.run_replay(target_streak=1,on_target='wait_condition',max_cycles_per_day=3,schedule=scheduled)
        self.assertEqual([r['issue_no'] for r in result['records']],['1000','1002'])
        result=self.run_replay(target_streak=1,on_target='new_cycle',max_cycles_per_day=3,schedule=scheduled)
        self.assertEqual([r['issue_no'] for r in result['records']],['1000','1001','1002'])

    def test_current_cycle_keeps_running_over_midnight_and_daily_slots_use_start_day(self):
        db=self.data()
        for i,row in enumerate(db.predictions):
            draw=db.draws[row['issue_no']]
            moment=datetime(2026,11,10,23,59)+timedelta(minutes=3.5*i)
            draw['open_time']=moment.isoformat();row['created_at']=(moment-timedelta(hours=8,minutes=2)).isoformat();row['external_fetched_at']=row['created_at']
        result=self.run_replay(db,days=7,schedule=self.schedule(windows=[{'id':'late','start':'23:55','end':'24:00'}]))
        self.assertEqual(result['summary']['cycle_count'],1)
        self.assertEqual(result['summary']['target_cycle_count'],1)
        self.assertEqual({r['stat_date'] for r in result['records']},{'2026-11-10'})

    def test_invalid_schedule_rejected_and_json_supported(self):
        for value in ('bad', [], {'windows':[]}, self.schedule(allow_pre_window_signal='false'),
                      self.schedule(min_remaining_seconds=-1),self.schedule(max_starts_per_window=1.5),
                      self.schedule(windows=[{'start':'23:00','end':'01:00'}])):
            with self.subTest(value=value),self.assertRaises(ValueError):self.run_replay(schedule=value)
        self.assertEqual(self.run_replay(schedule=json.dumps(self.schedule()))['summary']['net_profit'],83.38)
        with self.assertRaises(ValueError):self.run_replay(day_boundary='pc28')

class ScheduledReplayApiTests(unittest.TestCase):
    def test_schedule_roundtrip_private_public_and_stats_privacy(self):
        from tests.support import fresh_app_harness,create_predictor
        with fresh_app_harness() as h:
            client,uid=h.make_client()
            pid=create_predictor(h,uid,'pc28',share_level='stats_only')
            schedule={'windows':[{'start':'00:00','end':'00:59'}], 'allow_pre_window_signal':True}
            params={'bet_mode':'win_streak_roll','roll_schedule':json.dumps(schedule)}
            for url in (f'/api/predictors/{pid}/simulation',f'/api/public/predictors/{pid}/simulation'):
                response=client.get(url,query_string=params)
                self.assertEqual(response.status_code,200,response.json)
                self.assertTrue(response.json['simulation']['roll_config']['schedule']['allow_pre_window_signal'])
                if '/public/' in url:self.assertEqual(response.json['simulation']['records'],[])
                bad=client.get(url,query_string={**params,'roll_schedule':'{"allow_pre_window_signal":"false"}'})
                self.assertEqual(bad.status_code,400)
            for url in (f'/api/predictors/{pid}/dashboard',f'/api/public/predictors/{pid}'):
                self.assertTrue(client.get(url).json['predictor']['simulation_roll_schedule'])
