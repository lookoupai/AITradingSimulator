"""
外部预测方案：创建校验、采用规则、结算与回溯资格测试。
"""
from __future__ import annotations

import json
import unittest
from datetime import datetime, timedelta
from unittest import mock

from tests.support import create_predictor, fresh_app_harness
from tests.test_external_sources import load_fixture, make_sample_response


def seed_source(harness, fixture=None, enabled=True):
    fixture = fixture or load_fixture()
    source_id = harness.db.create_external_source(
        plugin_key='jnd28',
        name='JND 公共来源',
        base_url='https://jnd-28.vip',
        interval_seconds=60,
        enabled=enabled
    )
    with mock.patch('services.external_sources.base.requests.get', return_value=make_sample_response(fixture)):
        harness.module.external_prediction_service.refresh_catalog(harness.db, harness.db.get_external_source(source_id))
    harness.db.set_external_models_enabled(source_id, ['quantum'], True)
    return harness.db.get_external_source(source_id)


def seed_draw(harness, issue_no, result_number=16, open_time='2026-09-18 23:31:00'):
    from utils.pc28 import derive_pc28_attributes

    attributes = derive_pc28_attributes(result_number)
    harness.db.upsert_draws('pc28', [{
        'issue_no': str(issue_no),
        'draw_date': open_time.split(' ')[0],
        'draw_time': open_time.split(' ')[1],
        'open_time': open_time,
        'result_number': attributes['result_number'],
        'result_number_text': attributes['result_number_text'],
        'big_small': attributes['big_small'],
        'odd_even': attributes['odd_even'],
        'combo': attributes['combo']
    }])


class ExternalPredictorValidationTests(unittest.TestCase):
    def setUp(self):
        self.harness_context = fresh_app_harness()
        self.harness = self.harness_context.__enter__()
        self.addCleanup(self.harness_context.__exit__, None, None, None)
        self.source = seed_source(self.harness)

    def test_create_external_predictor_success(self):
        client, user_id = self.harness.make_client(username='alice')
        response = client.post('/api/predictors', json={
            'name': '量子模型-大小',
            'engine_type': 'external',
            'lottery_type': 'pc28',
            'external_source_id': self.source['id'],
            'external_model_key': 'quantum',
            'prediction_targets': ['big_small'],
            'primary_metric': 'big_small',
            'profit_default_metric': 'big_small'
        })
        self.assertEqual(response.status_code, 200, response.get_data(as_text=True))
        predictor = response.get_json()['predictor']
        self.assertEqual(predictor['engine_type'], 'external')
        self.assertEqual(predictor['external_binding']['external_model_key'], 'quantum')
        self.assertEqual(predictor['external_binding']['status'], 'ok')
        self.assertNotIn('number', predictor['prediction_targets'])

    def test_external_targets_must_exclude_number(self):
        client, _ = self.harness.make_client(username='alice')
        response = client.post('/api/predictors', json={
            'name': '量子模型',
            'engine_type': 'external',
            'lottery_type': 'pc28',
            'external_source_id': self.source['id'],
            'external_model_key': 'quantum',
            'prediction_targets': ['number'],
            'primary_metric': 'big_small',
            'profit_default_metric': 'big_small'
        })
        self.assertEqual(response.status_code, 400)
        self.assertIn('至少选择一个目标', response.get_json()['error'])

    def test_external_requires_enabled_model(self):
        client, _ = self.harness.make_client(username='alice')
        response = client.post('/api/predictors', json={
            'name': '未开放模型',
            'engine_type': 'external',
            'lottery_type': 'pc28',
            'external_source_id': self.source['id'],
            'external_model_key': 'lstm',
            'prediction_targets': ['big_small'],
            'primary_metric': 'big_small',
            'profit_default_metric': 'big_small'
        })
        self.assertEqual(response.status_code, 400)
        self.assertIn('未开放', response.get_json()['error'])

    def test_disabled_source_rejects_binding(self):
        client, _ = self.harness.make_client(username='alice')
        self.harness.db.update_external_source(self.source['id'], {'enabled': False})
        response = client.post('/api/predictors', json={
            'name': '停用来源',
            'engine_type': 'external',
            'lottery_type': 'pc28',
            'external_source_id': self.source['id'],
            'external_model_key': 'quantum',
            'prediction_targets': ['big_small'],
            'primary_metric': 'big_small',
            'profit_default_metric': 'big_small'
        })
        self.assertEqual(response.status_code, 400)
        self.assertIn('已停用', response.get_json()['error'])

    def test_external_options_lists_only_enabled_models(self):
        client, _ = self.harness.make_client(username='alice')
        response = client.get('/api/external/options')
        self.assertEqual(response.status_code, 200)
        sources = response.get_json()['sources']
        self.assertEqual(len(sources), 1)
        model_keys = [model['model_key'] for model in sources[0]['models']]
        self.assertEqual(model_keys, ['quantum'])

    def test_predictor_test_endpoint_reports_binding(self):
        client, user_id = self.harness.make_client(username='alice')
        predictor_id = client.post('/api/predictors', json={
            'name': '量子模型-组合',
            'engine_type': 'external',
            'lottery_type': 'pc28',
            'external_source_id': self.source['id'],
            'external_model_key': 'quantum',
            'prediction_targets': ['combo'],
            'primary_metric': 'combo',
            'profit_default_metric': 'combo'
        }).get_json()['id']
        response = client.post('/api/predictors/test', json={
            'predictor_id': predictor_id,
            'engine_type': 'external',
            'external_source_id': self.source['id'],
            'external_model_key': 'quantum'
        })
        self.assertEqual(response.status_code, 200)
        self.assertIn('已开放', response.get_json()['message'])


class ExternalAdoptionTests(unittest.TestCase):
    def setUp(self):
        self.harness_context = fresh_app_harness()
        self.harness = self.harness_context.__enter__()
        self.addCleanup(self.harness_context.__exit__, None, None, None)
        self.fixture = load_fixture()
        self.source = seed_source(self.harness, self.fixture)
        self.target_issue = str(self.fixture['draw_number'])

    def _make_predictor(self, targets=('big_small', 'odd_even', 'combo')):
        predictor_id = create_predictor(
            self.harness,
            user_id=1,
            lottery_type='pc28',
            engine_type='external',
            algorithm_key='',
            api_key='',
            api_url='',
            model_name='',
            system_prompt='',
            prediction_targets=list(targets),
            primary_metric='big_small',
            profit_default_metric='big_small',
            external_source_id=self.source['id'],
            external_model_key='quantum'
        )
        return self.harness.db.get_predictor(predictor_id, include_secret=True)

    def test_adopt_creates_pending_prediction_for_next_issue(self):
        predictor = self._make_predictor()
        result = self.harness.module.external_prediction_service.adopt_prediction_for_predictor(
            self.harness.db, predictor, self.target_issue
        )
        self.assertEqual(result['status'], 'created')
        prediction = self.harness.db.get_prediction_by_issue(predictor['id'], self.target_issue)
        self.assertEqual(prediction['status'], 'pending')
        self.assertEqual(prediction['prediction_big_small'], '大')
        self.assertEqual(prediction['prediction_odd_even'], '单')
        self.assertEqual(prediction['prediction_combo'], '大单')
        self.assertEqual(prediction['external_source_id'], self.source['id'])
        self.assertEqual(prediction['external_model_key'], 'quantum')
        snapshot = self.harness.db.get_external_prediction(self.source['id'], 'quantum', self.target_issue)
        self.assertIsNotNone(snapshot)
        payload = json.loads(snapshot['model_payload'])
        self.assertEqual(payload['model_type'], 'quantum')

    def test_adopt_respects_target_subset(self):
        predictor = self._make_predictor(targets=('odd_even',))
        result = self.harness.module.external_prediction_service.adopt_prediction_for_predictor(
            self.harness.db, predictor, self.target_issue
        )
        self.assertEqual(result['status'], 'created')
        prediction = self.harness.db.get_prediction_by_issue(predictor['id'], self.target_issue)
        self.assertEqual(prediction['requested_targets'], ['odd_even'])
        self.assertIsNone(prediction['prediction_big_small'])

    def test_adopt_is_idempotent_first_version_wins(self):
        predictor = self._make_predictor()
        first = self.harness.module.external_prediction_service.adopt_prediction_for_predictor(
            self.harness.db, predictor, self.target_issue
        )
        modified = json.loads(json.dumps(self.fixture))
        for item in modified['models'][0]['items']:
            if item.get('scope') == 'sum' and item.get('category') == 'big':
                item['value'] = '小'
        with mock.patch('services.external_sources.base.requests.get', return_value=make_sample_response(modified)):
            self.harness.module.external_prediction_service.collect_source(self.harness.db, self.source)
        second = self.harness.module.external_prediction_service.adopt_prediction_for_predictor(
            self.harness.db, predictor, self.target_issue
        )
        self.assertEqual(first['status'], 'created')
        self.assertEqual(second['status'], 'exists')
        prediction = self.harness.db.get_prediction_by_issue(predictor['id'], self.target_issue)
        # 首个有效版本不被上游修订覆盖
        self.assertEqual(prediction['prediction_big_small'], '大')

    def test_adopt_skips_drawn_issue(self):
        predictor = self._make_predictor()
        seed_draw(self.harness, self.target_issue, result_number=16, open_time='2026-09-18 23:31:00')
        result = self.harness.module.external_prediction_service.adopt_prediction_for_predictor(
            self.harness.db, predictor, self.target_issue
        )
        self.assertEqual(result['status'], 'skipped')

    def test_adopt_waiting_when_no_batch(self):
        predictor = self._make_predictor()
        result = self.harness.module.external_prediction_service.adopt_prediction_for_predictor(
            self.harness.db, predictor, '3483599'
        )
        self.assertEqual(result['status'], 'waiting')

    def test_engine_cycle_adopts_via_generate_prediction(self):
        predictor = self._make_predictor()
        harness_module = self.harness.module
        with mock.patch.object(harness_module.pc28_service, 'sync_recent_draws', return_value=[]), \
             mock.patch.object(harness_module.pc28_service, 'fetch_keno_snapshot', return_value={'next_issue_no': self.target_issue, 'countdown': '00:01:00'}), \
             mock.patch.object(harness_module.pc28_service, 'fetch_omission_stats', return_value={}), \
             mock.patch.object(harness_module.pc28_service, 'fetch_today_stats', return_value={}), \
             mock.patch.object(harness_module.pc28_service, 'fetch_preview', return_value={}):
            result = harness_module.prediction_engine.generate_prediction(predictor['id'], auto_mode=True)
        self.assertEqual(result['status'], 'pending')
        self.assertEqual(result['issue_no'], self.target_issue)


class ExternalSettlementTests(unittest.TestCase):
    def setUp(self):
        self.harness_context = fresh_app_harness()
        self.harness = self.harness_context.__enter__()
        self.addCleanup(self.harness_context.__exit__, None, None, None)
        self.fixture = load_fixture()
        self.source = seed_source(self.harness, self.fixture)
        self.target_issue = str(self.fixture['draw_number'])

    @staticmethod
    def _beijing_open_time(minutes_from_now: float) -> str:
        # 本地开奖时间以北京时间存储；这里构造相对当前 UTC 的开奖时刻
        open_utc = datetime.utcnow() + timedelta(minutes=minutes_from_now)
        return (open_utc + timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S')

    def _adopt(self):
        predictor_id = create_predictor(
            self.harness,
            user_id=1,
            lottery_type='pc28',
            engine_type='external',
            algorithm_key='',
            api_key='',
            api_url='',
            model_name='',
            system_prompt='',
            prediction_targets=['big_small', 'odd_even', 'combo'],
            primary_metric='big_small',
            profit_default_metric='big_small',
            external_source_id=self.source['id'],
            external_model_key='quantum'
        )
        predictor = self.harness.db.get_predictor(predictor_id, include_secret=True)
        result = self.harness.module.external_prediction_service.adopt_prediction_for_predictor(
            self.harness.db, predictor, self.target_issue
        )
        self.assertEqual(result['status'], 'created')
        return predictor

    def _settle_offline(self):
        """离线结算：屏蔽真实开奖同步，使用测试种子开奖。"""
        harness_module = self.harness.module
        with mock.patch.object(harness_module.pc28_service, 'sync_recent_draws', return_value=[]), \
             mock.patch.object(harness_module.pc28_service, 'fetch_recent_draws', return_value=[]):
            return harness_module.prediction_engine.settle_pending_predictions()

    def test_settle_hit_when_fetched_before_draw(self):
        # 开奖发生在采集之后（采集留档时间早于开奖时间）→ 正常结算
        predictor = self._adopt()
        seed_draw(
            self.harness,
            self.target_issue,
            result_number=16,
            open_time=self._beijing_open_time(minutes_from_now=3)
        )
        settled = self._settle_offline()
        prediction = self.harness.db.get_prediction_by_issue(predictor['id'], self.target_issue)
        self.assertEqual(prediction['status'], 'settled')
        self.assertEqual(prediction['actual_number'], 16)
        self.assertEqual(prediction['hit_big_small'], 1)
        self.assertEqual(prediction['hit_odd_even'], 0)
        self.assertEqual(prediction['hit_combo'], 0)
        self.assertEqual(len(settled), 1)

    def test_settle_marks_fetched_after_draw_as_expired(self):
        predictor = self._adopt()
        # 采集发生在开奖之后：batch fetched_at（采集时写库）晚于开奖时间
        # 采集时间晚于开奖时间（开奖后 10 分钟才抓到）→ 回溯判定 expired
        fetched_after_draw_utc = datetime.utcnow() + timedelta(minutes=13)
        self.harness.db.upsert_prediction({
            'predictor_id': predictor['id'],
            'lottery_type': 'pc28',
            'issue_no': self.target_issue,
            'requested_targets': ['big_small'],
            'prediction_big_small': '大',
            'status': 'pending',
            'external_source_id': self.source['id'],
            'external_model_key': 'quantum',
            'external_fetched_at': fetched_after_draw_utc.strftime('%Y-%m-%d %H:%M:%S')
        })
        seed_draw(
            self.harness,
            self.target_issue,
            result_number=16,
            open_time=self._beijing_open_time(minutes_from_now=3)
        )
        self._settle_offline()
        prediction = self.harness.db.get_prediction_by_issue(predictor['id'], self.target_issue)
        self.assertEqual(prediction['status'], 'expired')
        self.assertIn('不计入实时统计', prediction['error_message'] or '')
        self.assertIsNone(prediction['hit_big_small'])


if __name__ == '__main__':
    unittest.main()
