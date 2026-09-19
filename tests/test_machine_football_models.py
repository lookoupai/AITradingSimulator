import json
import os
import random
import sqlite3
import tempfile
import unittest

try:
    import penaltyblog  # noqa: F401
    HAS_PENALTYBLOG = True
except ImportError:
    HAS_PENALTYBLOG = False

from services.machine_prediction import (
    FOOTBALL_MODEL_MIN_HISTORY,
    _predict_jingcai_dixon_coles_v1,
    _predict_jingcai_pirating_v1,
    predict_jingcai
)
from utils.predictor_engine import (
    list_machine_algorithms,
    normalize_algorithm_key
)


LOTTERY_EVENTS_DDL = """
CREATE TABLE IF NOT EXISTS lottery_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    lottery_type TEXT NOT NULL,
    event_key TEXT NOT NULL,
    batch_key TEXT NOT NULL DEFAULT '',
    event_date TEXT,
    event_time TEXT,
    event_name TEXT,
    league TEXT,
    home_team TEXT,
    away_team TEXT,
    status TEXT,
    status_label TEXT,
    source_provider TEXT NOT NULL DEFAULT 'sina',
    result_payload TEXT,
    meta_payload TEXT,
    source_payload TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(lottery_type, event_key, source_provider)
)
"""


class _StubDB:
    """按需新建连接的临时文件库，模拟 Database 的 get_connection() 接口。"""

    def __init__(self):
        self._path = os.path.join(tempfile.mkdtemp(), 'stub.db')
        conn = self.get_connection()
        try:
            conn.execute(LOTTERY_EVENTS_DDL)
            conn.commit()
        finally:
            conn.close()

    def get_connection(self):
        conn = sqlite3.connect(self._path)
        conn.row_factory = sqlite3.Row
        return conn

    def seed_event(self, event_key, event_date, home, away, score1, score2, league='西甲'):
        conn = self.get_connection()
        try:
            conn.execute(
                """
                INSERT INTO lottery_events (
                    lottery_type, event_key, event_date, event_time, league,
                    home_team, away_team, source_provider, result_payload
                ) VALUES ('jingcai_football', ?, ?, ?, ?, ?, ?, 'sina', ?)
                """,
                (
                    event_key, event_date, f'{event_date} 20:00:00', league, home, away,
                    json.dumps({'score1': score1, 'score2': score2})
                )
            )
            conn.commit()
        finally:
            conn.close()


def _seed_synthetic_history(db, days=16, teams=12, start_day=1):
    """泊松形态的合成比分：足够接近真实足球分布，DC 的 rho 修正不会失稳。"""
    import numpy as np

    rng = np.random.default_rng(42)
    team_names = [f'测试队{index}' for index in range(teams)]
    strength = {name: float(value) for name, value in zip(team_names, rng.uniform(-0.6, 0.6, teams))}
    count = 0
    for day in range(start_day, start_day + days):
        date = f'2026-07-{day:02d}'
        pairs = set()
        while len(pairs) < 8:
            home, away = rng.choice(team_names, size=2, replace=False).tolist()
            if (home, away) in pairs:
                continue
            pairs.add((home, away))
            edge = strength[home] - strength[away]
            score1 = int(np.clip(rng.poisson(max(0.3, 1.45 + edge / 2)), 0, 8))
            score2 = int(np.clip(rng.poisson(max(0.3, 1.15 - edge / 2)), 0, 8))
            db.seed_event(f'hist{count}', date, home, away, score1, score2)
            count += 1
    return team_names


def _sample_match(home, away, league='西甲'):
    return {
        'event_key': 'tk1',
        'match_no': '周六001',
        'league': league,
        'home_team': home,
        'away_team': away,
        'spf_odds': {'胜': 1.80, '平': 3.40, '负': 4.20},
        'rqspf': {'handicap': -1, 'handicap_text': '-1', 'odds': {'胜': 2.80, '平': 3.50, '负': 2.20}}
    }


@unittest.skipUnless(HAS_PENALTYBLOG, 'penaltyblog 未安装')
class FootballModelAlgorithmTests(unittest.TestCase):
    def setUp(self):
        # 清空模型参数缓存，避免跨测试读到旧拟合结果
        import shutil
        from services import machine_prediction as mp
        cache_root = mp.Path(mp.__file__).resolve().parents[1] / 'data' / 'football_models'
        shutil.rmtree(cache_root, ignore_errors=True)
        self.db = _StubDB()
        self.teams = _seed_synthetic_history(self.db)

    def test_catalog_registers_new_algorithms(self):
        keys = [item['key'] for item in list_machine_algorithms('jingcai_football')]
        self.assertIn('football_pirating_v1', keys)
        self.assertIn('football_dixon_coles_v1', keys)
        pc28_keys = [item['key'] for item in list_machine_algorithms('pc28')]
        self.assertNotIn('football_pirating_v1', pc28_keys)
        self.assertNotIn('football_dixon_coles_v1', pc28_keys)
        self.assertEqual(
            normalize_algorithm_key('jingcai_football', 'machine', 'football_pirating_v1'),
            'football_pirating_v1'
        )

    def test_pirating_predicts_known_teams(self):
        items, debug = _predict_jingcai_pirating_v1(
            '2026-08-01', [_sample_match(self.teams[0], self.teams[1])], db=self.db
        )
        self.assertEqual(debug['algorithm'], 'football_pirating_v1')
        self.assertGreater(debug['trained_matches'], 0)
        row = debug['items'][0]
        self.assertEqual(row['prob_source'], 'pi_rating')
        self.assertIn(row['predicted_spf'], ('胜', '平', '负'))
        self.assertIn(row['predicted_rqspf'], ('胜', '平', '负'))
        self.assertAlmostEqual(sum(row['spf_probabilities'].values()), 1.0, places=2)
        self.assertEqual(items[0]['event_key'], 'tk1')

    def test_pirating_falls_back_to_odds_for_unknown_team(self):
        items, debug = _predict_jingcai_pirating_v1(
            '2026-08-01', [_sample_match('从未出现的主队', self.teams[1])], db=self.db
        )
        row = debug['items'][0]
        self.assertEqual(row['prob_source'], 'odds_fallback')
        # 退回赔率隐含概率时应选择最低赔率方向（胜 1.80）
        self.assertEqual(row['predicted_spf'], '胜')
        self.assertEqual(items[0]['predicted_spf'], '胜')

    def test_pirating_ignores_same_day_results(self):
        """训练集必须严格早于 run_key，防止用当日比分预测当日比赛。"""
        self.db.seed_event('today1', '2026-08-01', '当日队A', '当日队B', 3, 0)
        _, debug = _predict_jingcai_pirating_v1(
            '2026-08-01', [_sample_match('当日队A', '当日队B')], db=self.db
        )
        self.assertEqual(debug['items'][0]['prob_source'], 'odds_fallback')

    @unittest.skipUnless(HAS_PENALTYBLOG, 'penaltyblog 未安装')
    def test_dixon_coles_predicts_and_validates_rqspf(self):
        match = _sample_match(self.teams[0], self.teams[1])
        items, debug = _predict_jingcai_dixon_coles_v1('2026-08-01', [match], db=self.db)
        self.assertEqual(debug['algorithm'], 'football_dixon_coles_v1')
        self.assertGreaterEqual(debug['trained_matches'], FOOTBALL_MODEL_MIN_HISTORY)
        row = debug['items'][0]
        self.assertEqual(row['prob_source'], 'dixon_coles')
        self.assertIn(row['predicted_spf'], ('胜', '平', '负'))
        # 让球-1 时主队让胜概率不应高于未让球的胜概率
        self.assertLessEqual(
            row['rqspf_probabilities']['胜'],
            row['spf_probabilities']['胜'] + 1e-6
        )
        self.assertAlmostEqual(sum(row['rqspf_probabilities'].values()), 1.0, places=2)
        self.assertEqual(items[0]['predicted_spf'], row['predicted_spf'])

    def test_dixon_coles_requires_history(self):
        empty_db = _StubDB()
        with self.assertRaises(ValueError):
            _predict_jingcai_dixon_coles_v1('2026-08-01', [_sample_match('甲', '乙')], db=empty_db)

    def test_dixon_coles_falls_back_for_small_league(self):
        """样本量不足的联赛（杯赛等）应退回赔率隐含概率。"""
        match = _sample_match(self.teams[0], self.teams[1], league='某杯赛')
        _, debug = _predict_jingcai_dixon_coles_v1('2026-08-01', [match], db=self.db)
        row = debug['items'][0]
        self.assertEqual(row['prob_source'], 'odds_fallback')
        self.assertEqual(row['league_model'], '')
        self.assertEqual(row['predicted_spf'], '胜')  # 最低赔率方向

    def test_algorithms_require_db(self):
        with self.assertRaises(ValueError):
            _predict_jingcai_pirating_v1('2026-08-01', [], db=None)
        with self.assertRaises(ValueError):
            _predict_jingcai_dixon_coles_v1('2026-08-01', [], db=None)

    def test_dispatcher_routes_new_algorithms(self):
        predictor = {'engine_type': 'machine', 'algorithm_key': 'football_pirating_v1'}
        items, raw_response, label = predict_jingcai(
            '2026-08-01', [_sample_match(self.teams[0], self.teams[1])], predictor, db=self.db
        )
        self.assertEqual(len(items), 1)
        self.assertIn('Pi评级', label)
        debug = json.loads(raw_response)
        self.assertEqual(debug['algorithm'], 'football_pirating_v1')


if __name__ == '__main__':
    unittest.main()
