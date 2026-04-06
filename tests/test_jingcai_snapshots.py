from __future__ import annotations

import unittest

from utils import jingcai_football as football_utils


class JingcaiSnapshotTests(unittest.TestCase):
    def test_extract_odds_snapshots_returns_initial_and_current_views(self):
        snapshots = football_utils.extract_odds_snapshots(
            odds_euro=[
                {
                    'companyName': '竞彩官方',
                    'o1Ini': '1.80',
                    'o2Ini': '3.30',
                    'o3Ini': '4.20',
                    'o1New': '1.75',
                    'o2New': '3.40',
                    'o3New': '4.40',
                    'updateTime': '2026-04-06 17:55:00'
                }
            ],
            odds_asia=[
                {
                    'companyName': '竞彩官方',
                    'o1Ini': '1.92',
                    'o2Ini': '1.88',
                    'o3IniCn': '-0.5',
                    'o1New': '1.85',
                    'o2New': '1.95',
                    'o3NewCn': '-0.75',
                    'updateTime': '2026-04-06 17:56:00'
                }
            ],
            odds_totals=[
                {
                    'companyName': '竞彩官方',
                    'o1Ini': '1.90',
                    'o2Ini': '1.90',
                    'o3IniCn': '2.5',
                    'o1New': '1.84',
                    'o2New': '1.96',
                    'o3NewCn': '2.75',
                    'updateTime': '2026-04-06 17:57:00'
                }
            ]
        )

        self.assertEqual(snapshots['euro']['company'], '竞彩官方')
        self.assertEqual(snapshots['euro']['initial']['win'], 1.8)
        self.assertEqual(snapshots['euro']['current']['lose'], 4.4)
        self.assertEqual(snapshots['asia']['initial']['line'], '-0.5')
        self.assertEqual(snapshots['asia']['current']['line'], '-0.75')
        self.assertEqual(snapshots['totals']['initial']['line'], '2.5')
        self.assertEqual(snapshots['totals']['current']['line'], '2.75')


if __name__ == '__main__':
    unittest.main()
