"""
外部预测源：插件解析、管理端 API、采集批次与目录测试。
"""
from __future__ import annotations

import json
import os
import tempfile
import unittest
from unittest import mock

from tests.support import create_predictor, fresh_app_harness

FIXTURE_PATH = os.path.join(os.path.dirname(__file__), 'fixtures', 'jnd_ai_predict_sample.json')


def load_fixture() -> dict:
    with open(FIXTURE_PATH, 'r', encoding='utf-8') as handle:
        return json.load(handle)


def make_sample_response(fixture: dict):
    import requests

    response = mock.Mock()
    response.status_code = 200
    response.headers = {'Content-Type': 'application/json'}
    response.iter_content = lambda chunk_size=65536: [json.dumps(fixture, ensure_ascii=False).encode('utf-8')]
    response.__enter__ = lambda self: self
    response.__exit__ = lambda self, *args: None
    _ = requests
    return response


class ExternalSourcePluginTests(unittest.TestCase):
    def test_jnd28_parses_real_sample(self):
        fixture = load_fixture()
        from services.external_sources import get_plugin

        plugin = get_plugin('jnd28')
        with mock.patch('services.external_sources.base.requests.get', return_value=make_sample_response(fixture)):
            snapshot = plugin.fetch_snapshot('https://jnd-28.vip')

        self.assertEqual(snapshot.target_issue_no, str(fixture['draw_number']))
        self.assertEqual(snapshot.upstream_published_at, fixture['predict_time'])
        self.assertEqual(snapshot.invalid_model_count, 0)
        model_keys = {model.model_key for model in snapshot.models}
        self.assertIn('quantum', model_keys)
        quantum = next(model for model in snapshot.models if model.model_key == 'quantum')
        self.assertEqual(quantum.display_name, '理论概率偏差')
        self.assertEqual(quantum.targets['big_small'], '大')
        self.assertEqual(quantum.targets['odd_even'], '单')
        self.assertEqual(quantum.targets['combo'], '大单')
        self.assertEqual(quantum.supported_targets, ['big_small', 'odd_even', 'combo'])

    def test_jnd28_isolates_invalid_models(self):
        fixture = load_fixture()
        broken = dict(fixture['models'][0])
        broken['model_type'] = 'broken_model'
        broken['items'] = [{'scope': 'sum', 'category': 'big', 'value': '超大'}]
        another = dict(fixture['models'][0])
        another['model_type'] = 'no_name_model'
        another['model_name'] = ''
        fixture['models'] = fixture['models'] + [broken, another]

        from services.external_sources import get_plugin

        plugin = get_plugin('jnd28')
        with mock.patch('services.external_sources.base.requests.get', return_value=make_sample_response(fixture)):
            snapshot = plugin.fetch_snapshot('https://jnd-28.vip')

        self.assertEqual(snapshot.invalid_model_count, 2)
        self.assertNotIn('broken_model', {model.model_key for model in snapshot.models})
        self.assertNotIn('no_name_model', {model.model_key for model in snapshot.models})

    def test_jnd28_rejects_missing_issue(self):
        fixture = load_fixture()
        fixture.pop('draw_number')
        from services.external_sources.base import ExternalSourceParseError
        from services.external_sources import get_plugin

        plugin = get_plugin('jnd28')
        with mock.patch('services.external_sources.base.requests.get', return_value=make_sample_response(fixture)):
            with self.assertRaises(ExternalSourceParseError):
                plugin.fetch_snapshot('https://jnd-28.vip')

    def test_validate_base_url_requires_https(self):
        from services.external_sources import get_plugin

        plugin = get_plugin('jnd28')
        self.assertTrue(plugin.validate_base_url('https://jnd-28.vip'))
        self.assertFalse(plugin.validate_base_url('http://jnd-28.vip'))
        self.assertTrue(plugin.validate_base_url('http://127.0.0.1:5000'))


class ExternalCollectorStartupTests(unittest.TestCase):
    def test_default_enabled_initializes_application(self):
        """回归：EXTERNAL_COLLECTOR_ENABLED 默认开启（生产条件）时，initialize_application
        不得因缺少 global 声明抛 UnboundLocalError。"""
        tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(tempdir.cleanup)
        env_keys = ('AUTO_PREDICTION', 'DATABASE_PATH', 'NOTIFICATION_WORKER_ENABLED', 'EXTERNAL_COLLECTOR_ENABLED')
        old_env = {key: os.environ.get(key) for key in env_keys}
        os.environ['AUTO_PREDICTION'] = 'false'
        os.environ['DATABASE_PATH'] = os.path.join(tempdir.name, 'collector-startup.db')
        os.environ['NOTIFICATION_WORKER_ENABLED'] = 'false'
        os.environ.pop('EXTERNAL_COLLECTOR_ENABLED', None)
        try:
            import importlib

            from tests.support import _purge_repo_modules

            _purge_repo_modules()
            module = importlib.import_module('app')
            self.assertTrue(module._external_collector_started)
            # 采集线程持有临时目录中的 SQLite 连接，先通知退出并等待结束再清理
            module.stop_external_source_collector()
            if module._external_collector_thread is not None:
                module._external_collector_thread.join(timeout=15)
                self.assertFalse(module._external_collector_thread.is_alive())
        finally:
            _purge_repo_modules()
            for key, value in old_env.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value


class ExternalSourceAdminApiTests(unittest.TestCase):
    def setUp(self):
        self.harness_context = fresh_app_harness()
        self.harness = self.harness_context.__enter__()
        self.addCleanup(self.harness_context.__exit__, None, None, None)

    def _create_source(self, client, **overrides):
        payload = {
            'plugin_key': 'jnd28',
            'name': 'JND 公共来源',
            'base_url': 'https://jnd-28.vip',
            'interval_seconds': 60
        }
        payload.update(overrides)
        response = client.post('/api/admin/external-sources', json=payload)
        self.assertEqual(response.status_code, 201, response.get_data(as_text=True))
        return response.get_json()['source']['id']

    def test_non_admin_write_is_rejected(self):
        client, _ = self.harness.make_client(username='plain')
        response = client.post('/api/admin/external-sources', json={'plugin_key': 'jnd28'})
        self.assertEqual(response.status_code, 403)

    def test_create_source_defaults_disabled(self):
        client, _ = self.harness.make_client(username='admin', is_admin=True)
        source_id = self._create_source(client)
        source = self.harness.db.get_external_source(source_id)
        self.assertFalse(source['enabled'])
        self.assertEqual(source['plugin_key'], 'jnd28')

    def test_create_rejects_unknown_plugin_and_http_base_url(self):
        client, _ = self.harness.make_client(username='admin', is_admin=True)
        response = client.post('/api/admin/external-sources', json={'plugin_key': 'other'})
        self.assertEqual(response.status_code, 400)
        response = client.post('/api/admin/external-sources', json={
            'plugin_key': 'jnd28',
            'base_url': 'http://jnd-28.vip'
        })
        self.assertEqual(response.status_code, 400)

    def test_test_connection_and_refresh_catalog(self):
        fixture = load_fixture()
        client, _ = self.harness.make_client(username='admin', is_admin=True)
        source_id = self._create_source(client)

        with mock.patch('services.external_sources.base.requests.get', return_value=make_sample_response(fixture)):
            response = client.post(f'/api/admin/external-sources/{source_id}/test')
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.get_json()['ok'])
        self.assertEqual(response.get_json()['model_count'], len(fixture['models']))

        with mock.patch('services.external_sources.base.requests.get', return_value=make_sample_response(fixture)):
            response = client.post(f'/api/admin/external-sources/{source_id}/refresh-catalog')
        self.assertEqual(response.status_code, 200, response.get_data(as_text=True))
        catalog = response.get_json()['catalog']
        self.assertEqual(catalog['added'], len(fixture['models']))

        response = client.get(f'/api/admin/external-sources/{source_id}/models')
        models = response.get_json()['models']
        self.assertEqual(len(models), len(fixture['models']))
        self.assertTrue(all(not model['enabled'] for model in models))

    def test_model_enable_toggle(self):
        fixture = load_fixture()
        client, _ = self.harness.make_client(username='admin', is_admin=True)
        source_id = self._create_source(client)
        with mock.patch('services.external_sources.base.requests.get', return_value=make_sample_response(fixture)):
            client.post(f'/api/admin/external-sources/{source_id}/refresh-catalog')

        response = client.put(f'/api/admin/external-sources/{source_id}/models', json={
            'model_keys': ['quantum'],
            'enabled': True
        })
        self.assertEqual(response.status_code, 200)
        models = client.get(f'/api/admin/external-sources/{source_id}/models').get_json()['models']
        by_key = {model['model_key']: model for model in models}
        self.assertTrue(by_key['quantum']['enabled'])
        self.assertFalse(by_key['markov']['enabled'])

    def test_delete_source_cascades_and_keeps_user_history(self):
        fixture = load_fixture()
        client, _ = self.harness.make_client(username='admin', is_admin=True)
        source_id = self._create_source(client)
        with mock.patch('services.external_sources.base.requests.get', return_value=make_sample_response(fixture)):
            client.post(f'/api/admin/external-sources/{source_id}/refresh-catalog')
        self.harness.db.set_external_models_enabled(source_id, ['quantum'], True)

        # 绑定一个用户方案并产生一次采用
        from tests.support import create_predictor

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
            prediction_targets=['big_small'],
            primary_metric='big_small',
            profit_default_metric='big_small',
            external_source_id=source_id,
            external_model_key='quantum'
        )
        predictor = self.harness.db.get_predictor(predictor_id, include_secret=True)
        # 启用来源后才能采用预测
        client.put(f'/api/admin/external-sources/{source_id}', json={'enabled': True})
        target_issue = str(fixture['draw_number'])
        adoption = self.harness.module.external_prediction_service.adopt_prediction_for_predictor(
            self.harness.db, predictor, target_issue
        )
        self.assertEqual(adoption['status'], 'created')

        response = client.delete(f'/api/admin/external-sources/{source_id}')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()['affected_predictors'], 1)

        # 来源、目录、批次、共享快照全部清除
        self.assertIsNone(self.harness.db.get_external_source(source_id))
        self.assertEqual(self.harness.db.list_external_models(source_id), [])
        self.assertEqual(self.harness.db.get_external_batches(source_id, limit=10), [])
        self.assertIsNone(self.harness.db.get_external_prediction(source_id, 'quantum', target_issue))

        # 用户方案的历史预测行保留（含来源标识），此后不再产出新预测、导出为空
        prediction = self.harness.db.get_prediction_by_issue(predictor_id, target_issue)
        self.assertIsNotNone(prediction)
        self.assertEqual(prediction['external_source_id'], source_id)
        adoption_after = self.harness.module.external_prediction_service.adopt_prediction_for_predictor(
            self.harness.db, predictor, str(int(target_issue) + 1)
        )
        self.assertEqual(adoption_after['status'], 'skipped')
        export = client.get(f'/api/export/predictors/{predictor_id}/signals?view=execution')
        self.assertEqual(export.get_json()['items'], [])

    def test_delete_unknown_source_returns_404(self):
        client, _ = self.harness.make_client(username='admin', is_admin=True)
        response = client.delete('/api/admin/external-sources/99999')
        self.assertEqual(response.status_code, 404)

    def test_non_admin_cannot_delete_source(self):
        client, _ = self.harness.make_client(username='plain')
        response = client.delete('/api/admin/external-sources/1')
        self.assertEqual(response.status_code, 403)

    def test_source_enabled_toggle_updates_state(self):
        client, _ = self.harness.make_client(username='admin', is_admin=True)
        source_id = self._create_source(client)
        response = client.put(f'/api/admin/external-sources/{source_id}', json={'enabled': True})
        self.assertEqual(response.status_code, 200)
        self.assertTrue(self.harness.db.get_external_source(source_id)['enabled'])


class ExternalSourceCollectionTests(unittest.TestCase):
    def setUp(self):
        self.harness_context = fresh_app_harness()
        self.harness = self.harness_context.__enter__()
        self.addCleanup(self.harness_context.__exit__, None, None, None)
        self.fixture = load_fixture()

    def _make_source(self, enabled=True):
        source_id = self.harness.db.create_external_source(
            plugin_key='jnd28',
            name='JND',
            base_url='https://jnd-28.vip',
            interval_seconds=60,
            enabled=enabled
        )
        self.harness.db.upsert_external_models(
            source_id,
            [
                {'model_key': 'quantum', 'display_name': '理论概率偏差', 'supported_targets': ['big_small', 'odd_even', 'combo']},
                {'model_key': 'markov', 'display_name': '二阶转移模型', 'supported_targets': ['big_small', 'odd_even', 'combo']}
            ],
            '2026-09-18 15:00:00'
        )
        self.harness.db.set_external_models_enabled(source_id, ['quantum', 'markov'], True)
        return self.harness.db.get_external_source(source_id)

    def test_collect_dedupes_same_content_batch(self):
        source = self._make_source()
        with mock.patch('services.external_sources.base.requests.get', return_value=make_sample_response(self.fixture)):
            first = self.harness.module.external_prediction_service.collect_source(self.harness.db, source)
            second = self.harness.module.external_prediction_service.collect_source(self.harness.db, source)

        self.assertTrue(first['ok'])
        self.assertTrue(second['ok'])
        self.assertEqual(first['batch_id'], second['batch_id'])
        self.assertFalse(second['batch_created'])
        batches = self.harness.db.get_external_batches(source['id'])
        self.assertEqual(len(batches), 1)

    def test_collect_creates_new_version_when_decisions_change(self):
        source = self._make_source()
        with mock.patch('services.external_sources.base.requests.get', return_value=make_sample_response(self.fixture)):
            first = self.harness.module.external_prediction_service.collect_source(self.harness.db, source)

        modified = json.loads(json.dumps(self.fixture))
        modified['models'][0]['items'] = [
            item for item in modified['models'][0]['items'] if not (item.get('scope') == 'sum' and item.get('category') == 'big')
        ]
        modified['models'][0]['items'].append({'scope': 'sum', 'category': 'big', 'value': '小'})
        with mock.patch('services.external_sources.base.requests.get', return_value=make_sample_response(modified)):
            second = self.harness.module.external_prediction_service.collect_source(self.harness.db, source)

        self.assertTrue(first['ok'])
        self.assertTrue(second['ok'])
        batches = self.harness.db.get_external_batches(source['id'], limit=10)
        versions = sorted(batch['batch_version'] for batch in batches)
        fingerprints = {batch['fingerprint'] for batch in batches}
        self.assertEqual(versions, [1, 2])
        self.assertEqual(len(fingerprints), 2)

    def test_collect_failure_records_error_and_backoff_counter(self):
        source = self._make_source()

        import requests as requests_lib

        def fail_request(*args, **kwargs):
            raise requests_lib.exceptions.ConnectionError('network down')

        with mock.patch('services.external_sources.base.requests.get', side_effect=fail_request):
            result = self.harness.module.external_prediction_service.collect_source(self.harness.db, source)

        self.assertFalse(result['ok'])
        updated = self.harness.db.get_external_source(source['id'])
        self.assertEqual(updated['consecutive_failures'], 1)
        self.assertIn('network down', updated['last_error'] or '')

    def test_collect_cycle_runs_repeatedly_across_intervals(self):
        """回归：第二次及以后周期不得因时间比较类型错误而中断采集。"""
        source = self._make_source()
        with mock.patch('services.external_sources.base.requests.get', return_value=make_sample_response(self.fixture)) as mocked_get:
            first = self.harness.module._run_external_source_collect_cycle()
            self.assertEqual(first['collected'], 1)
            self.assertEqual(first['failed'], 0)

            # 间隔未到：不重复请求，也不允许报错
            second = self.harness.module._run_external_source_collect_cycle()
            self.assertEqual(second['collected'], 0)
            self.assertEqual(second['failed'], 0)
            self.assertEqual(mocked_get.call_count, 1)

            # 间隔已过：恢复采集
            self.harness.db.update_external_source(source['id'], {'last_attempt_at': '2026-09-18 10:00:00'})
            third = self.harness.module._run_external_source_collect_cycle()
            self.assertEqual(third['collected'], 1)
            self.assertEqual(third['failed'], 0)
            self.assertEqual(mocked_get.call_count, 2)

    def test_disabled_source_is_skipped_by_cycle(self):
        self._make_source(enabled=False)
        summary = self.harness.module._run_external_source_collect_cycle()
        self.assertEqual(summary['checked'], 0)


if __name__ == '__main__':
    unittest.main()
_ = create_predictor
