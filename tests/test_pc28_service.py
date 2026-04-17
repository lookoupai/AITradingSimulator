from __future__ import annotations

import unittest
from unittest import mock

import requests

from services.pc28_service import PC28Service


class DummyResponse:
    def __init__(self, payload=None, status_code: int = 200):
        self.payload = payload
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f'HTTP {self.status_code}')

    def json(self):
        return self.payload


class PC28ServiceTests(unittest.TestCase):
    def setUp(self):
        self.service = PC28Service(
            base_url='https://pc28.help',
            timeout=3,
            recent_source_order=('official', 'jnd', 'feiji'),
            jnd_recent_url='https://jnd-28.vip/api/recent',
            feiji_recent_url='https://feiji28.com/api/keno/latest'
        )

    def test_fetch_recent_draws_falls_back_to_jnd(self):
        def fake_get(url, params=None, timeout=None):
            if url == 'https://pc28.help/api/kj.json':
                return DummyResponse(status_code=404)
            if url == 'https://jnd-28.vip/api/recent':
                self.assertEqual(params, {'limit': 1})
                return DummyResponse([
                    {
                        'draw_number': 3421529,
                        'draw_date': '2026-04-17T13:38:00+08:00',
                        'canada28_result': 10
                    }
                ])
            raise AssertionError(f'未预期的请求: {url}')

        with mock.patch('services.pc28_service.requests.get', side_effect=fake_get):
            draws = self.service.fetch_recent_draws(limit=1)

        self.assertEqual(len(draws), 1)
        self.assertEqual(draws[0]['issue_no'], '3421529')
        self.assertEqual(draws[0]['result_number'], 10)
        self.assertEqual(draws[0]['draw_date'], '2026-04-17')
        self.assertEqual(draws[0]['draw_time'], '13:38:00')
        self.assertEqual(draws[0]['combo'], '小双')

    def test_fetch_recent_draws_falls_back_to_feiji(self):
        def fake_get(url, params=None, timeout=None):
            if url == 'https://pc28.help/api/kj.json':
                return DummyResponse(status_code=404)
            if url == 'https://jnd-28.vip/api/recent':
                return DummyResponse(status_code=503)
            if url == 'https://feiji28.com/api/keno/latest':
                self.assertEqual(params, {'limit': 1, 'offset': 0})
                return DummyResponse({
                    'data': [
                        {
                            'draw_nbr': 3421529,
                            'final_sum': 10,
                            'created_at': '2026-04-17T05:38:04.178Z'
                        }
                    ]
                })
            raise AssertionError(f'未预期的请求: {url}')

        with mock.patch('services.pc28_service.requests.get', side_effect=fake_get):
            draws = self.service.fetch_recent_draws(limit=1)

        self.assertEqual(len(draws), 1)
        self.assertEqual(draws[0]['issue_no'], '3421529')
        self.assertEqual(draws[0]['result_number'], 10)
        self.assertEqual(draws[0]['draw_date'], '2026-04-17')
        self.assertEqual(draws[0]['draw_time'], '13:38:04')
        self.assertEqual(draws[0]['odd_even'], '双')

    def test_fetch_keno_snapshot_uses_recent_draws_when_official_snapshot_unavailable(self):
        def fake_get(url, params=None, timeout=None):
            if url == 'https://pc28.help/api/keno.json':
                return DummyResponse(status_code=404)
            raise AssertionError(f'未预期的请求: {url}')

        with mock.patch('services.pc28_service.requests.get', side_effect=fake_get):
            snapshot = self.service.fetch_keno_snapshot(recent_draws=[{'issue_no': '3421529'}])

        self.assertEqual(snapshot['latest_issue_no'], '3421529')
        self.assertEqual(snapshot['next_issue_no'], '3421530')
        self.assertEqual(snapshot['countdown'], '--:--:--')
        self.assertIn('推导下一期期号', snapshot['warning'])

    def test_build_overview_returns_warning_when_recent_source_is_backup(self):
        official_calls = {'kj': 0, 'keno': 0, 'yl': 0, 'yk': 0, 'preview': 0}

        def fake_get(url, params=None, timeout=None):
            if url == 'https://pc28.help/api/kj.json':
                official_calls['kj'] += 1
                return DummyResponse(status_code=404)
            if url == 'https://pc28.help/api/keno.json':
                official_calls['keno'] += 1
                return DummyResponse(status_code=404)
            if url == 'https://pc28.help/api/yl.json':
                official_calls['yl'] += 1
                return DummyResponse(status_code=404)
            if url == 'https://pc28.help/api/yk.json':
                official_calls['yk'] += 1
                return DummyResponse(status_code=404)
            if url == 'https://pc28.help/api/preview.json':
                official_calls['preview'] += 1
                return DummyResponse(status_code=404)
            if url == 'https://jnd-28.vip/api/recent':
                return DummyResponse([
                    {
                        'draw_number': 3421529,
                        'draw_date': '2026-04-17T13:38:00+08:00',
                        'canada28_result': 10
                    }
                ])
            raise AssertionError(f'未预期的请求: {url}')

        with mock.patch('services.pc28_service.requests.get', side_effect=fake_get):
            overview = self.service.build_overview(history_limit=1)

        self.assertEqual(overview['latest_draw']['issue_no'], '3421529')
        self.assertEqual(overview['next_issue_no'], '3421530')
        self.assertEqual(overview['countdown'], '--:--:--')
        self.assertEqual(overview['omission_preview'], {'top_numbers': [], 'groups': {}})
        self.assertEqual(overview['today_preview'], {'summary': {}, 'hot_numbers': []})
        self.assertIn('JND28', overview['warning'])
        self.assertEqual(official_calls['kj'], 1)
        self.assertEqual(official_calls['keno'], 1)


if __name__ == '__main__':
    unittest.main()
