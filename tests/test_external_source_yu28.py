"""
与28开放平台（yu28）插件、按需采集与来源凭据测试。
"""
from __future__ import annotations

import json
import os
import unittest
from unittest import mock

from tests.support import create_predictor, fresh_app_harness

FIXTURES = os.path.join(os.path.dirname(__file__), 'fixtures')


def load_yu28_samples() -> dict:
    with open(os.path.join(FIXTURES, 'yu28_samples.json'), 'r', encoding='utf-8') as handle:
        return json.load(handle)


def make_yu28_fetch_mock(samples: dict, issue_override: str | None = None):
    """按 URL 分发假响应：dt → 大厅页；sf?id=X → 算法详情；dx/ds → AI 预测。"""
    import services.external_sources.base as base_module
    import services.external_sources.yu28 as yu28_module

    sf_by_id = {
        f"sf_{samples['sf_combo']['id']}": samples['sf_combo'],
        f"sf_{samples['sf_big_small']['id']}": samples['sf_big_small'],
        f"sf_{samples['sf_odd_even']['id']}": samples['sf_odd_even']
    }

    def fake_fetch_json(url, timeout, max_bytes, headers=None):
        captured['url'] = url
        captured['headers'] = headers or {}
        if '/api/dt.json' in url:
            # 目录接口按分类分发：测试样本只有组合分类页，其余分类返回空页
            if 'category=combo_predict' in url:
                return json.loads(json.dumps(samples['dt_combo']))
            return {'items': [], 'page': 1, 'pageSize': 50, 'total': 0}
        if '/api/sf.json' in url:
            algorithm_id = url.split('id=')[1].split('&')[0]
            sample = sf_by_id.get(f'sf_{algorithm_id}')
            if sample is None:
                raise base_module.ExternalSourceRequestError('HTTP 404 NOT_FOUND: 算法不存在')
            payload = json.loads(json.dumps(sample))
            if issue_override:
                payload['predictNbr'] = issue_override
            return payload
        if '/api/dx.json' in url:
            payload = json.loads(json.dumps(samples['ai_dx']))
            if issue_override:
                payload['data'][0]['nbr'] = issue_override
            return payload
        if '/api/ds.json' in url:
            payload = json.loads(json.dumps(samples['ai_ds']))
            if issue_override:
                payload['data'][0]['nbr'] = issue_override
            return payload
        raise AssertionError(f' unexpected url {url}')

    captured = {}
    return mock.patch.object(yu28_module, 'fetch_json', side_effect=fake_fetch_json), captured


class Yu28PluginTests(unittest.TestCase):
    SAMPLES = load_yu28_samples()
    COMBO_ID = str(SAMPLES['sf_combo']['id'])
    BIG_SMALL_ID = str(SAMPLES['sf_big_small']['id'])
    ODD_EVEN_ID = str(SAMPLES['sf_odd_even']['id'])
    HALL_IDS = {str(item['id']) for item in SAMPLES['dt_combo']['items']}

    def setUp(self):
        self.samples = self.SAMPLES
        self.plugin = self._plugin()

    def _plugin(self):
        from services.external_sources import get_plugin

        return get_plugin('yu28')

    def test_catalog_includes_hall_and_fixed_ai_models(self):
        patcher, _ = make_yu28_fetch_mock(self.samples)
        with patcher:
            records = self.plugin.fetch_catalog('https://yu28.top', api_key='yu28_test')
        keys = [record['model_key'] for record in records]
        self.assertIn('ai_dx', keys)
        self.assertIn('ai_ds', keys)
        self.assertTrue(self.HALL_IDS & set(keys), '大厅算法应进入目录')
        first_hall_id = str(self.samples['dt_combo']['items'][0]['id'])
        combo_record = next(record for record in records if record['model_key'] == first_hall_id)
        # 目录统一存本地目标键（combo），而不是大厅分类名（combo_predict）
        self.assertEqual(combo_record['supported_targets'], ['combo'])

    def test_snapshot_normalizes_combo_and_ai_targets(self):
        patcher, captured = make_yu28_fetch_mock(self.samples)
        with patcher:
            snapshot = self.plugin.fetch_snapshot(
                'https://yu28.top',
                api_key='yu28_test',
                model_keys=[self.COMBO_ID, self.BIG_SMALL_ID, 'ai_dx', 'ai_ds']
            )
        # 每个模型各发一次请求，且 Key 走请求头
        self.assertIn('X-Api-Key', captured['headers'])
        by_key = {model.model_key: model for model in snapshot.models}
        self.assertEqual(by_key[self.COMBO_ID].targets, {'combo': self.samples['sf_combo']['predict'].replace('+', '')})
        self.assertEqual(by_key['ai_dx'].targets, {'big_small': self.samples['ai_dx']['data'][0]['predict']})
        # 大小/单双类算法为精确口径，保留上游评分
        big_small_model = by_key[self.BIG_SMALL_ID]
        numerator, _, denominator = str(self.samples['sf_big_small']['hit20']).partition('/')
        self.assertEqual(big_small_model.confidence, round(int(numerator) / int(denominator) * 100, 2))
        self.assertEqual(by_key['ai_ds'].targets, {'odd_even': self.samples['ai_ds']['data'][0]['predict']})
        combo = by_key[self.COMBO_ID]
        # 上游组合命中是"分项任一命中"宽松口径（实测 100 期零偏差），与本平台精确组合
        # 结算不可比，因此不给评分，避免与本地命中率混淆
        self.assertIsNone(combo.confidence)
        self.assertEqual(snapshot.invalid_model_count, 0)
        self.assertEqual(snapshot.target_issue_no, self.samples['sf_combo']['predictNbr'])

    def test_snapshot_rejects_missing_api_key(self):
        from services.external_sources.base import ExternalSourceRequestError

        with self.assertRaises(ExternalSourceRequestError):
            self.plugin.fetch_snapshot('https://yu28.top', model_keys=[self.COMBO_ID])

    def test_snapshot_isolates_unknown_algorithms_and_issue_mismatch(self):
        patcher, _ = make_yu28_fetch_mock(self.samples)
        with patcher:
            snapshot = self.plugin.fetch_snapshot(
                'https://yu28.top',
                api_key='yu28_test',
                model_keys=[self.COMBO_ID, '99999999', 'ai_dx']
            )
        # 未知算法被隔离；期号一致的模型正常返回
        self.assertEqual({model.model_key for model in snapshot.models}, {self.COMBO_ID, 'ai_dx'})
        self.assertEqual(snapshot.invalid_model_count, 1)

    def test_kill_group_prediction_is_skipped(self):
        samples = json.loads(json.dumps(self.samples))
        samples['sf_combo'] = {
            **samples['sf_combo'],
            'category': 'kill_group',
            'predict': '杀大单'
        }
        patcher, _ = make_yu28_fetch_mock(samples)
        with patcher:
            with self.assertRaises(Exception):
                self.plugin.fetch_snapshot('https://yu28.top', api_key='yu28_test', model_keys=[self.COMBO_ID])


class Yu28CollectionTests(unittest.TestCase):
    SAMPLES = load_yu28_samples()
    COMBO_ID = str(SAMPLES['sf_combo']['id'])
    BIG_SMALL_ID = str(SAMPLES['sf_big_small']['id'])
    ODD_EVEN_ID = str(SAMPLES['sf_odd_even']['id'])

    def setUp(self):
        self.harness_context = fresh_app_harness()
        self.harness = self.harness_context.__enter__()
        self.addCleanup(self.harness_context.__exit__, None, None, None)
        self.samples = load_yu28_samples()

    def _make_source(self, api_key='yu28_test'):
        source_id = self.harness.db.create_external_source(
            plugin_key='yu28',
            name='与28平台',
            base_url='https://yu28.top',
            interval_seconds=60,
            enabled=True,
            api_key=api_key
        )
        return self.harness.db.get_external_source(source_id)

    def _bind_model(self, source_id, model_key, model_enabled=True, predictor_enabled=True, user_id=1, targets=('combo',)):
        self.harness.db.upsert_external_models(
            source_id,
            [{'model_key': model_key, 'display_name': f'算法 {model_key}', 'supported_targets': list(targets)}],
            '2026-09-19 10:00:00'
        )
        self.harness.db.set_external_models_enabled(source_id, [model_key], model_enabled)
        predictor_id = create_predictor(
            self.harness,
            user_id=user_id,
            lottery_type='pc28',
            name=f'yu28-{model_key}',
            engine_type='external',
            algorithm_key='',
            api_key='',
            api_url='',
            model_name='',
            system_prompt='',
            prediction_targets=list(targets),
            primary_metric=targets[0],
            profit_default_metric=targets[0],
            external_source_id=source_id,
            external_model_key=model_key,
            enabled=predictor_enabled
        )
        return self.harness.db.get_predictor(predictor_id, include_secret=True)

    def test_collect_fetches_only_bound_models_and_uses_key_header(self):
        source = self._make_source()
        self._bind_model(source['id'], self.COMBO_ID)
        patcher, captured = make_yu28_fetch_mock(self.samples)
        with patcher:
            result = self.harness.module.external_prediction_service.collect_source(self.harness.db, source)
        self.assertTrue(result['ok'])
        self.assertFalse(result.get('skipped'))
        self.assertIn(f'id={self.COMBO_ID}', captured['url'])
        self.assertEqual(captured['headers'].get('X-Api-Key'), 'yu28_test')
        batches = self.harness.db.get_external_batches(source['id'])
        self.assertEqual(len(batches), 1)
        self.assertEqual(batches[0]['target_issue_no'], self.samples['sf_combo']['predictNbr'])

    def test_collect_skips_when_no_bound_models(self):
        source = self._make_source()
        patcher, captured = make_yu28_fetch_mock(self.samples)
        with patcher:
            result = self.harness.module.external_prediction_service.collect_source(self.harness.db, source)
        self.assertTrue(result.get('skipped'))
        self.assertEqual(captured, {})

    def test_collect_uses_catalog_endpoint_for_refresh(self):
        source = self._make_source()
        patcher, captured = make_yu28_fetch_mock(self.samples)
        with patcher:
            result = self.harness.module.external_prediction_service.refresh_catalog(self.harness.db, source)
        self.assertTrue(result['ok'])
        self.assertIn('/api/dt.json', captured['url'])
        self.assertGreater(result['catalog']['added'], 0)

    def test_adopt_finds_model_in_later_batch_version(self):
        source = self._make_source()
        predictor_a = self._bind_model(source['id'], self.COMBO_ID)

        issue_no = self.samples['sf_combo']['predictNbr']
        patcher, _ = make_yu28_fetch_mock(self.samples)
        with patcher:
            first = self.harness.module.external_prediction_service.collect_source(self.harness.db, source)
            self.assertTrue(first['ok'])
            adoption_a = self.harness.module.external_prediction_service.adopt_prediction_for_predictor(
                self.harness.db, predictor_a, issue_no
            )
            self.assertEqual(adoption_a['status'], 'created')

            # 新增绑定另一个算法（不在 v1 批次中）→ 重新采集产生 v2 → 新方案可从 v2 采用
            predictor_b = self._bind_model(source['id'], self.BIG_SMALL_ID, targets=('big_small',))
            second = self.harness.module.external_prediction_service.collect_source(self.harness.db, source)
            self.assertTrue(second['ok'])
            self.assertTrue(second['batch_created'])
            adoption_b = self.harness.module.external_prediction_service.adopt_prediction_for_predictor(
                self.harness.db, predictor_b, issue_no
            )
            self.assertEqual(adoption_b['status'], 'created')

        prediction_b = self.harness.db.get_prediction_by_issue(predictor_b['id'], issue_no)
        self.assertEqual(prediction_b['status'], 'pending')

    def test_issue_mismatch_model_isolated(self):
        source = self._make_source()
        self._bind_model(source['id'], self.COMBO_ID)
        # ai_dx 返回不同期号 → 被隔离，不影响主算法
        samples = json.loads(json.dumps(self.samples))
        samples['ai_dx']['data'][0]['nbr'] = '0000001'
        patcher, _ = make_yu28_fetch_mock(samples)
        with patcher:
            result = self.harness.module.external_prediction_service.collect_source(self.harness.db, source)
        self.assertTrue(result['ok'])
        self.assertEqual(result['invalid_model_count'], 0)


class Yu28TargetValidationTests(unittest.TestCase):
    """FR-003：模型不支持所选目标时，创建/编辑返回具体错误。"""

    def setUp(self):
        self.harness_context = fresh_app_harness()
        self.harness = self.harness_context.__enter__()
        self.addCleanup(self.harness_context.__exit__, None, None, None)
        self.samples = load_yu28_samples()

    def test_create_with_mismatched_target_returns_specific_error(self):
        admin_client, _ = self.harness.make_client(username='admin', is_admin=True)
        source_id = admin_client.post('/api/admin/external-sources', json={
            'plugin_key': 'yu28',
            'base_url': 'https://yu28.top',
            'api_key': 'yu28_abcd1234567890ef'
        }).get_json()['source']['id']
        big_id = str(self.samples['sf_big_small']['id'])
        self.harness.db.upsert_external_models(
            source_id,
            [{'model_key': big_id, 'display_name': '大小算法', 'supported_targets': ['big_small']}],
            '2026-09-19 10:00:00'
        )
        self.harness.db.set_external_models_enabled(source_id, [big_id], True)

        client, _ = self.harness.make_client(username='alice')
        response = client.post('/api/predictors', json={
            'name': '目标错配',
            'engine_type': 'external',
            'lottery_type': 'pc28',
            'external_source_id': source_id,
            'external_model_key': big_id,
            'prediction_targets': ['combo'],
            'primary_metric': 'combo',
            'profit_default_metric': 'combo'
        })
        self.assertEqual(response.status_code, 400)
        self.assertIn('仅支持大/小预测', response.get_json()['error'])

    def test_create_with_matching_target_succeeds(self):
        admin_client, _ = self.harness.make_client(username='admin', is_admin=True)
        source_id = admin_client.post('/api/admin/external-sources', json={
            'plugin_key': 'yu28',
            'base_url': 'https://yu28.top',
            'api_key': 'yu28_abcd1234567890ef'
        }).get_json()['source']['id']
        big_id = str(self.samples['sf_big_small']['id'])
        self.harness.db.upsert_external_models(
            source_id,
            [{'model_key': big_id, 'display_name': '大小算法', 'supported_targets': ['big_small']}],
            '2026-09-19 10:00:00'
        )
        self.harness.db.set_external_models_enabled(source_id, [big_id], True)
        admin_client.put(f'/api/admin/external-sources/{source_id}', json={'enabled': True})

        client, _ = self.harness.make_client(username='alice2')
        response = client.post('/api/predictors', json={
            'name': '目标匹配',
            'engine_type': 'external',
            'lottery_type': 'pc28',
            'external_source_id': source_id,
            'external_model_key': big_id,
            'prediction_targets': ['big_small'],
            'primary_metric': 'big_small',
            'profit_default_metric': 'big_small'
        })
        self.assertEqual(response.status_code, 200, response.get_data(as_text=True))


class Yu28LegacyTargetNormalizationTests(unittest.TestCase):
    """历史遗留行（supported_targets 存了分类名 combo_predict）在读取时归一为目标键。"""

    def test_legacy_supported_targets_normalized_on_read(self):
        with fresh_app_harness() as harness:
            source_id = harness.db.create_external_source('yu28', '与28', 'https://yu28.top', 60, False)
            harness.db.upsert_external_models(
                source_id,
                [{'model_key': 'legacy', 'display_name': '旧行', 'supported_targets': ['combo_predict']}],
                '2026-09-19 10:00:00'
            )
            # 模拟旧版写入的原始值
            import sqlite3 as sqlite3_lib
            conn = sqlite3_lib.connect(harness.db.db_path)
            conn.execute("UPDATE external_models SET supported_targets = '[\"combo_predict\"]' WHERE model_key = 'legacy'")
            conn.commit()
            conn.close()

            model = harness.db.get_external_model(source_id, 'legacy')
            self.assertEqual(model['supported_targets'], ['combo'])
            models = harness.db.list_external_models(source_id)
            self.assertEqual(models[0]['supported_targets'], ['combo'])


class Yu28AdminApiTests(unittest.TestCase):
    def setUp(self):
        self.harness_context = fresh_app_harness()
        self.harness = self.harness_context.__enter__()
        self.addCleanup(self.harness_context.__exit__, None, None, None)

    def test_create_yu28_source_requires_api_key(self):
        client, _ = self.harness.make_client(username='admin', is_admin=True)
        response = client.post('/api/admin/external-sources', json={
            'plugin_key': 'yu28',
            'base_url': 'https://yu28.top'
        })
        self.assertEqual(response.status_code, 400)
        self.assertIn('API Key', response.get_json()['error'])

        response = client.post('/api/admin/external-sources', json={
            'plugin_key': 'yu28',
            'base_url': 'https://yu28.top',
            'api_key': 'yu28_abcd1234567890ef'
        })
        self.assertEqual(response.status_code, 201, response.get_data(as_text=True))
        source = response.get_json()['source']
        self.assertTrue(source['has_api_key'])
        self.assertTrue(source['requires_api_key'])
        # 响应不回显完整 Key
        self.assertNotIn('yu28_abcd1234567890ef', json.dumps(response.get_json()))
        self.assertTrue(str(source['masked_api_key']).startswith('yu28'))

    def test_probe_reports_catalog_and_ai_target(self):
        """回归：测试连接不得依赖已绑定算法（此前直接走采集路径导致“必须指定算法列表”报错）。"""
        client, _ = self.harness.make_client(username='admin', is_admin=True)
        source_id = client.post('/api/admin/external-sources', json={
            'plugin_key': 'yu28',
            'base_url': 'https://yu28.top',
            'api_key': 'yu28_abcd1234567890ef'
        }).get_json()['source']['id']

        samples = load_yu28_samples()
        patcher, _ = make_yu28_fetch_mock(samples)
        with patcher:
            response = client.post(f'/api/admin/external-sources/{source_id}/test')
        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        self.assertTrue(data['ok'], data)
        # 目录规模来自大厅分页；目标期号来自试取的 AI 模型
        self.assertEqual(data['model_count'], len(samples['dt_combo']['items']) + 2)
        self.assertEqual(data['target_issue_no'], samples['ai_dx']['data'][0]['nbr'])
        self.assertEqual(data['sample_model_count'], 2)

    def test_probe_reports_missing_key_error(self):
        client, _ = self.harness.make_client(username='admin', is_admin=True)
        # 绕过创建校验直接造一个无 Key 的 yu28 来源，探测应报明确错误
        source_id = self.harness.db.create_external_source(
            plugin_key='yu28', name='no-key', base_url='https://yu28.top',
            interval_seconds=60, enabled=False, api_key=None
        )
        samples = load_yu28_samples()
        patcher, _ = make_yu28_fetch_mock(samples)
        with patcher:
            response = client.post(f'/api/admin/external-sources/{source_id}/test')
        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        self.assertFalse(data['ok'])
        self.assertIn('API Key', data['error'])

    def test_update_keeps_key_when_blank(self):
        client, _ = self.harness.make_client(username='admin', is_admin=True)
        source_id = client.post('/api/admin/external-sources', json={
            'plugin_key': 'yu28',
            'base_url': 'https://yu28.top',
            'api_key': 'yu28_abcd1234567890ef'
        }).get_json()['source']['id']
        response = client.put(f'/api/admin/external-sources/{source_id}', json={'name': '改名', 'api_key': ''})
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.get_json()['source']['has_api_key'])

    def test_plugin_options_listed_dynamically(self):
        client, _ = self.harness.make_client(username='admin', is_admin=True)
        plugins = client.get('/api/admin/external-sources').get_json()['plugins']
        keys = {plugin['plugin_key'] for plugin in plugins}
        self.assertEqual(keys, {'jnd28', 'yu28'})

    def test_admin_dashboard_aggregation_includes_plugins(self):
        """回归：管理页 15 秒轮询的 /api/admin/dashboard 必须携带 plugins，
        否则“添加来源”下拉框永远停留在写死的选项。"""
        client, _ = self.harness.make_client(username='admin', is_admin=True)
        payload = client.get('/api/admin/dashboard').get_json()
        plugins = (payload.get('external_sources') or {}).get('plugins')
        self.assertIsNotNone(plugins)
        keys = {plugin['plugin_key'] for plugin in plugins}
        self.assertEqual(keys, {'jnd28', 'yu28'})
        yu28 = next(plugin for plugin in plugins if plugin['plugin_key'] == 'yu28')
        self.assertTrue(yu28['requires_api_key'])
        jnd = next(plugin for plugin in plugins if plugin['plugin_key'] == 'jnd28')
        self.assertFalse(jnd['requires_api_key'])


if __name__ == '__main__':
    unittest.main()
