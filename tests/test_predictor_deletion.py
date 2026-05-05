from __future__ import annotations

import unittest

from tests.support import create_predictor, fresh_app_harness


class PredictorDeletionCascadeTests(unittest.TestCase):
    """验证 delete_predictor 级联清理相关表、但保留归档表"""

    def test_delete_predictor_cleans_execution_logs_and_predictions(self):
        with fresh_app_harness() as harness:
            client, user_id = harness.make_client()

            # 两个方案：一个要删、一个保留作为对照组
            target_predictor_id = create_predictor(harness, user_id, 'pc28', name='target')
            other_predictor_id = create_predictor(harness, user_id, 'pc28', name='other')

            # 给两个方案都插入 prediction、execution_log
            for pid in (target_predictor_id, other_predictor_id):
                harness.db.upsert_prediction({
                    'predictor_id': pid,
                    'lottery_type': 'pc28',
                    'issue_no': f'2026040800{pid}',
                    'requested_targets': ['big_small'],
                    'prediction_number': None,
                    'prediction_big_small': '大',
                    'prediction_odd_even': '单',
                    'prediction_combo': '大单',
                    'confidence': 0.5,
                    'reasoning_summary': '',
                    'raw_response': '',
                    'prompt_snapshot': '',
                    'status': 'pending',
                    'error_message': None,
                    'actual_number': None,
                    'actual_big_small': None,
                    'actual_odd_even': None,
                    'actual_combo': None,
                    'hit_number': None,
                    'hit_big_small': None,
                    'hit_odd_even': None,
                    'hit_combo': None,
                    'settled_at': None
                })
                harness.db.create_user_algorithm_execution_log({
                    'user_id': user_id,
                    'algorithm_id': None,
                    'algorithm_version': 1,
                    'predictor_id': pid,
                    'run_key': 'test',
                    'status': 'succeeded',
                    'match_count': 1,
                    'prediction_count': 1,
                    'skip_count': 0,
                    'duration_ms': 5,
                    'fallback_strategy': 'fail',
                    'fallback_used': 0,
                    'error_message': None,
                    'debug_json': '{}'
                })

            # 归档表：给 target 造一条
            conn = harness.db.get_connection()
            cur = conn.cursor()
            cur.execute(
                '''INSERT INTO pc28_prediction_daily_summary
                   (predictor_id, summary_date, total_predictions, settled_predictions)
                   VALUES (?, '2026-04-01', 10, 10)''',
                (target_predictor_id,)
            )
            conn.commit()
            conn.close()

            # ========= Act: 删掉 target =========
            harness.db.delete_predictor(target_predictor_id)

            # ========= Assert =========
            conn = harness.db.get_connection()
            cur = conn.cursor()

            # 原始预测和 log 应被级联清空
            cur.execute('SELECT COUNT(*) FROM predictions WHERE predictor_id = ?', (target_predictor_id,))
            self.assertEqual(cur.fetchone()[0], 0, 'predictions 应级联删除')

            cur.execute('SELECT COUNT(*) FROM user_algorithm_execution_logs WHERE predictor_id = ?', (target_predictor_id,))
            self.assertEqual(cur.fetchone()[0], 0, 'user_algorithm_execution_logs 应级联删除')

            cur.execute('SELECT COUNT(*) FROM predictors WHERE id = ?', (target_predictor_id,))
            self.assertEqual(cur.fetchone()[0], 0, 'predictors 自身应被删除')

            # 归档表应保留（共识分析需要用来回溯历史）
            cur.execute('SELECT COUNT(*) FROM pc28_prediction_daily_summary WHERE predictor_id = ?', (target_predictor_id,))
            self.assertEqual(cur.fetchone()[0], 1, 'pc28_prediction_daily_summary 应保留用于历史回溯')

            # 另一个方案的数据应原封不动
            cur.execute('SELECT COUNT(*) FROM predictions WHERE predictor_id = ?', (other_predictor_id,))
            self.assertEqual(cur.fetchone()[0], 1, '其它方案的 predictions 不应被影响')
            cur.execute('SELECT COUNT(*) FROM user_algorithm_execution_logs WHERE predictor_id = ?', (other_predictor_id,))
            self.assertEqual(cur.fetchone()[0], 1, '其它方案的 execution_logs 不应被影响')
            cur.execute('SELECT COUNT(*) FROM predictors WHERE id = ?', (other_predictor_id,))
            self.assertEqual(cur.fetchone()[0], 1, '其它方案自身不应被删除')

            conn.close()


if __name__ == '__main__':
    unittest.main()
