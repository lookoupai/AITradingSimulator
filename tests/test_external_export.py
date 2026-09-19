"""
外部方案信号导出：有效性门槛、分享等级访问控制、Token 授权测试。
"""
from __future__ import annotations

import json
import unittest
from datetime import datetime, timedelta
from unittest import mock

from tests.support import create_predictor, fresh_app_harness
from tests.test_external_sources import load_fixture, make_sample_response
from tests.test_external_predictors import seed_draw, seed_source


class ExternalExportHelper:
    def setUp(self):
        self.harness_context = fresh_app_harness()
        self.harness = self.harness_context.__enter__()
        self.addCleanup(self.harness_context.__exit__, None, None, None)
        self.fixture = load_fixture()
        self.source = seed_source(self.harness, self.fixture)
        self.target_issue = str(self.fixture['draw_number'])

    def _make_predictor(self, share_level='records', targets=('big_small',), enabled=True, external=True, user_id=None):
        # 本地最新开奖 = 目标期号-1 → 本地下一期恰为目标期号（导出有效性判定依赖该关系）
        seed_draw(
            self.harness,
            int(self.target_issue) - 1,
            result_number=9,
            open_time='2026-09-18 23:20:00'
        )
        overrides = {
            'name': '导出测试方案',
            'lottery_type': 'pc28',
            'prediction_targets': list(targets),
            'primary_metric': 'big_small',
            'profit_default_metric': 'big_small',
            'share_level': share_level,
            'enabled': enabled
        }
        if external:
            overrides.update({
                'engine_type': 'external',
                'algorithm_key': '',
                'api_key': '',
                'api_url': '',
                'model_name': '',
                'system_prompt': '',
                'external_source_id': self.source['id'],
                'external_model_key': 'quantum'
            })
        overrides.setdefault('lottery_type', 'pc28')
        predictor_id = create_predictor(self.harness, user_id=user_id or 1, **overrides)
        predictor = self.harness.db.get_predictor(predictor_id, include_secret=True)
        if external:
            result = self.harness.module.external_prediction_service.adopt_prediction_for_predictor(
                self.harness.db, predictor, self.target_issue
            )
            self.assertEqual(result['status'], 'created')
        return predictor


class ExternalExecutionExportTests(ExternalExportHelper, unittest.TestCase):
    def test_execution_returns_current_adopted_signal(self):
        predictor = self._make_predictor()
        client, _ = self.harness.make_client(username='owner')
        response = client.get(f"/api/export/predictors/{predictor['id']}/signals?view=execution")
        self.assertEqual(response.status_code, 200)
        items = response.get_json()['items']
        self.assertEqual(len(items), 1)
        item = items[0]
        self.assertEqual(item['schema_version'], '1.0')
        self.assertEqual(item['signal_id'], f"pc28-predictor-{predictor['id']}-{self.target_issue}")
        self.assertEqual(item['issue_no'], self.target_issue)
        self.assertEqual(item['published_at'], self.harness.db.get_prediction_by_issue(predictor['id'], self.target_issue)['external_fetched_at'])
        big_small_signals = [signal for signal in item['signals'] if signal['bet_type'] == 'big_small']
        self.assertEqual(len(big_small_signals), 1)
        self.assertEqual(big_small_signals[0]['bet_value'], '大')
        self.assertEqual(big_small_signals[0]['message_text'], '大10')
        self.assertEqual(item['source_ref']['engine_type'], 'external')
        self.assertEqual(item['source_ref']['model_key'], 'quantum')
        self.assertNotIn('base_url', json.dumps(item))

    def test_execution_items_empty_after_draw_settled(self):
        predictor = self._make_predictor()
        seed_draw(
            self.harness,
            self.target_issue,
            result_number=16,
            open_time=(datetime.utcnow() + timedelta(minutes=3) + timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S')
        )
        self.harness.module.prediction_engine.settle_pending_predictions()
        client, _ = self.harness.make_client(username='owner')
        response = client.get(f"/api/export/predictors/{predictor['id']}/signals?view=execution")
        self.assertEqual(response.get_json()['items'], [])

    def test_execution_items_empty_when_source_disabled(self):
        predictor = self._make_predictor()
        self.harness.db.update_external_source(self.source['id'], {'enabled': False})
        client, _ = self.harness.make_client(username='owner')
        response = client.get(f"/api/export/predictors/{predictor['id']}/signals?view=execution")
        self.assertEqual(response.get_json()['items'], [])

    def test_execution_items_empty_for_stale_issue(self):
        predictor = self._make_predictor()
        # 本地开奖推进到 target+1 → 下一期变成 target+2，目标期号已过期
        seed_draw(
            self.harness,
            self.target_issue,
            result_number=16,
            open_time=(datetime.utcnow() + timedelta(minutes=3) + timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S')
        )
        seed_draw(
            self.harness,
            int(self.target_issue) + 1,
            result_number=9,
            open_time=(datetime.utcnow() + timedelta(minutes=8) + timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S')
        )
        client, _ = self.harness.make_client(username='owner')
        response = client.get(f"/api/export/predictors/{predictor['id']}/signals?view=execution")
        self.assertEqual(response.get_json()['items'], [])

    def test_signal_id_and_published_at_stable_across_polls(self):
        predictor = self._make_predictor()
        client, _ = self.harness.make_client(username='owner')
        first = client.get(f"/api/export/predictors/{predictor['id']}/signals?view=execution").get_json()['items'][0]
        second = client.get(f"/api/export/predictors/{predictor['id']}/signals?view=execution").get_json()['items'][0]
        self.assertEqual(first['signal_id'], second['signal_id'])
        self.assertEqual(first['published_at'], second['published_at'])


class ExportAccessControlTests(ExternalExportHelper, unittest.TestCase):
    def test_stats_only_anonymous_is_rejected(self):
        predictor = self._make_predictor(share_level='stats_only')
        client, _ = self.harness.make_client(username='owner')
        anonymous = self.harness.app.test_client()
        response = anonymous.get(f"/api/export/predictors/{predictor['id']}/signals?view=execution")
        self.assertEqual(response.status_code, 403)

    def test_records_level_anonymous_execution_allowed_without_raw(self):
        predictor = self._make_predictor(share_level='records', targets=('big_small', 'odd_even', 'combo'))
        anonymous = self.harness.app.test_client()
        response = anonymous.get(f"/api/export/predictors/{predictor['id']}/signals?view=execution")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.get_json()['items'][0]['signals']), 3)

    def test_owner_session_reads_stats_only(self):
        client, owner_id = self.harness.make_client(username='owner')
        predictor = self._make_predictor(share_level='stats_only', user_id=owner_id)
        response = client.get(f"/api/export/predictors/{predictor['id']}/signals?view=execution")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.get_json()['items']), 1)

    def test_other_user_session_rejected_for_stats_only(self):
        _, owner_id = self.harness.make_client(username='owner')
        predictor = self._make_predictor(share_level='stats_only', user_id=owner_id)
        client, _ = self.harness.make_client(username='mallory')
        response = client.get(f"/api/export/predictors/{predictor['id']}/signals?view=execution")
        self.assertEqual(response.status_code, 403)

    def test_admin_session_reads_stats_only(self):
        _, owner_id = self.harness.make_client(username='owner')
        predictor = self._make_predictor(share_level='stats_only', user_id=owner_id)
        client, _ = self.harness.make_client(username='admin', is_admin=True)
        response = client.get(f"/api/export/predictors/{predictor['id']}/signals?view=execution")
        self.assertEqual(response.status_code, 200)

    def test_token_grants_access_to_stats_only(self):
        client, owner_id = self.harness.make_client(username='owner')
        predictor = self._make_predictor(share_level='stats_only', user_id=owner_id)
        token_response = client.post(f"/api/predictors/{predictor['id']}/export-tokens", json={'label': 'pc28touzhu'})
        self.assertEqual(token_response.status_code, 201)
        token = token_response.get_json()['token']

        anonymous = self.harness.app.test_client()
        bearer = anonymous.get(
            f"/api/export/predictors/{predictor['id']}/signals?view=execution",
            headers={'Authorization': f'Bearer {token}'}
        )
        self.assertEqual(bearer.status_code, 200)
        self.assertEqual(len(bearer.get_json()['items']), 1)

        header_token = anonymous.get(
            f"/api/export/predictors/{predictor['id']}/signals?view=execution",
            headers={'X-Export-Token': token}
        )
        self.assertEqual(header_token.status_code, 200)

        records = client.get(f"/api/predictors/{predictor['id']}/export-tokens").get_json()['tokens']
        self.assertTrue(records[0]['last_used_at'])

    def test_token_not_valid_for_other_predictor(self):
        client, owner_id = self.harness.make_client(username='owner')
        predictor = self._make_predictor(share_level='stats_only', user_id=owner_id)
        other = self._make_predictor(share_level='stats_only', targets=('odd_even',), user_id=owner_id)
        token = client.post(f"/api/predictors/{predictor['id']}/export-tokens", json={}).get_json()['token']

        anonymous = self.harness.app.test_client()
        response = anonymous.get(
            f"/api/export/predictors/{other['id']}/signals?view=execution",
            headers={'Authorization': f'Bearer {token}'}
        )
        self.assertEqual(response.status_code, 403)

    def test_revoked_token_rejected(self):
        client, owner_id = self.harness.make_client(username='owner')
        predictor = self._make_predictor(share_level='stats_only', user_id=owner_id)
        created = client.post(f"/api/predictors/{predictor['id']}/export-tokens", json={}).get_json()
        token = created['token']
        token_id = created['token_record']['id']
        self.assertTrue(client.delete(f"/api/predictors/{predictor['id']}/export-tokens/{token_id}").status_code == 200)

        anonymous = self.harness.app.test_client()
        response = anonymous.get(
            f"/api/export/predictors/{predictor['id']}/signals?view=execution",
            headers={'Authorization': f'Bearer {token}'}
        )
        self.assertEqual(response.status_code, 403)

    def test_anonymous_analysis_strips_raw(self):
        predictor = self._make_predictor(share_level='analysis')
        anonymous = self.harness.app.test_client()
        response = anonymous.get(f"/api/export/predictors/{predictor['id']}/signals?view=analysis")
        self.assertEqual(response.status_code, 200)
        item = response.get_json()['items'][0]
        self.assertEqual(item['raw']['raw_response'], '')
        self.assertEqual(item['raw']['prompt_snapshot'], '')

    def test_owner_analysis_keeps_model_payload(self):
        predictor = self._make_predictor(share_level='analysis')
        client, _ = self.harness.make_client(username='owner')
        response = client.get(f"/api/export/predictors/{predictor['id']}/signals?view=analysis")
        self.assertEqual(response.status_code, 200)
        item = response.get_json()['items'][0]
        raw_response = item['raw']['raw_response']
        self.assertIn('quantum', raw_response)
        payload = json.loads(raw_response)
        self.assertEqual(payload.get('model_type'), 'quantum')

    def test_token_created_once_and_prefix_masked(self):
        client, owner_id = self.harness.make_client(username='owner')
        predictor = self._make_predictor(share_level='stats_only', user_id=owner_id)
        created = client.post(f"/api/predictors/{predictor['id']}/export-tokens", json={'label': '下游'}).get_json()
        self.assertTrue(created['token'].startswith('pts_'))
        records = client.get(f"/api/predictors/{predictor['id']}/export-tokens").get_json()['tokens']
        self.assertEqual(records[0]['token_prefix'], created['token'][:8])
        self.assertNotEqual(records[0]['token_prefix'], created['token'])


if __name__ == '__main__':
    unittest.main()
