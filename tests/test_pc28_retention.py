from __future__ import annotations

import unittest
from datetime import datetime, timedelta
from tempfile import TemporaryDirectory

from database import Database


def _format_utc_timestamp(value: datetime) -> str:
    return value.strftime('%Y-%m-%d %H:%M:%S')


def _format_beijing_date(value: datetime) -> str:
    return (value + timedelta(hours=8)).strftime('%Y-%m-%d')


class PC28RetentionTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = TemporaryDirectory()
        self.db = Database(f'{self.tempdir.name}/test.db')
        self.db.init_db()
        self.user_id = self.db.create_user('tester', 'hashed-password')
        self.predictor_id = self.db.create_predictor(
            user_id=self.user_id,
            name='pc28-retention',
            api_key='test-key',
            api_url='https://example.com/v1',
            model_name='test-model',
            api_mode='auto',
            primary_metric='big_small',
            profit_default_metric='big_small',
            profit_rule_id='pc28_netdisk',
            share_level='records',
            prediction_method='test',
            system_prompt='test',
            data_injection_mode='summary',
            prediction_targets=['big_small'],
            history_window=20,
            temperature=0.3,
            enabled=True,
            lottery_type='pc28',
            engine_type='ai',
            algorithm_key=''
        )

    def tearDown(self):
        self.tempdir.cleanup()

    def test_retention_archives_old_pc28_rows_and_preserves_stats(self):
        now_utc = datetime.utcnow()
        old_base = now_utc - timedelta(days=90)
        recent_time = now_utc - timedelta(days=2)

        old_settled = [
            ('100', 0),
            ('101', 0),
            ('102', 1),
            ('103', 1)
        ]
        for offset, (issue_no, hit_big_small) in enumerate(old_settled):
            created_at = old_base + timedelta(minutes=offset)
            self.db.upsert_prediction({
                'predictor_id': self.predictor_id,
                'lottery_type': 'pc28',
                'issue_no': issue_no,
                'requested_targets': ['big_small'],
                'prediction_big_small': '大' if hit_big_small else '小',
                'prediction_combo': '大单' if hit_big_small else '小双',
                'actual_big_small': '大' if hit_big_small else '小',
                'actual_combo': '大单' if hit_big_small else '小双',
                'hit_big_small': hit_big_small,
                'status': 'settled',
                'settled_at': _format_utc_timestamp(created_at + timedelta(minutes=3))
            })
            conn = self.db.get_connection()
            try:
                conn.execute(
                    '''
                    UPDATE predictions
                    SET created_at = ?, updated_at = ?, settled_at = ?
                    WHERE predictor_id = ? AND issue_no = ?
                    ''',
                    (
                        _format_utc_timestamp(created_at),
                        _format_utc_timestamp(created_at + timedelta(minutes=4)),
                        _format_utc_timestamp(created_at + timedelta(minutes=3)),
                        self.predictor_id,
                        issue_no
                    )
                )
                conn.commit()
            finally:
                conn.close()

        failed_created_at = old_base - timedelta(minutes=10)
        self.db.upsert_prediction({
            'predictor_id': self.predictor_id,
            'lottery_type': 'pc28',
            'issue_no': '099',
            'requested_targets': ['big_small'],
            'status': 'failed',
            'error_message': 'timeout'
        })
        conn = self.db.get_connection()
        try:
            conn.execute(
                '''
                UPDATE predictions
                SET created_at = ?, updated_at = ?
                WHERE predictor_id = ? AND issue_no = ?
                ''',
                (
                    _format_utc_timestamp(failed_created_at),
                    _format_utc_timestamp(failed_created_at + timedelta(minutes=2)),
                    self.predictor_id,
                    '099'
                )
            )
            conn.commit()
        finally:
            conn.close()

        self.db.upsert_prediction({
            'predictor_id': self.predictor_id,
            'lottery_type': 'pc28',
            'issue_no': '104',
            'requested_targets': ['big_small'],
            'prediction_big_small': '大',
            'prediction_combo': '大单',
            'actual_big_small': '大',
            'actual_combo': '大单',
            'hit_big_small': 1,
            'status': 'settled',
            'settled_at': _format_utc_timestamp(recent_time + timedelta(minutes=3))
        })
        conn = self.db.get_connection()
        try:
            conn.execute(
                '''
                UPDATE predictions
                SET created_at = ?, updated_at = ?, settled_at = ?
                WHERE predictor_id = ? AND issue_no = ?
                ''',
                (
                    _format_utc_timestamp(recent_time),
                    _format_utc_timestamp(recent_time + timedelta(minutes=4)),
                    _format_utc_timestamp(recent_time + timedelta(minutes=3)),
                    self.predictor_id,
                    '104'
                )
            )
            conn.commit()
        finally:
            conn.close()

        draw_rows = [
            ('100', old_base + timedelta(minutes=1)),
            ('101', old_base + timedelta(minutes=2)),
            ('102', old_base + timedelta(minutes=3)),
            ('103', old_base + timedelta(minutes=4)),
            ('104', recent_time)
        ]
        self.db.upsert_draws(
            'pc28',
            [
                {
                    'issue_no': issue_no,
                    'draw_date': _format_beijing_date(draw_time),
                    'draw_time': '12:00:00',
                    'open_time': f"{_format_beijing_date(draw_time)} 12:00:00",
                    'result_number': 14,
                    'result_number_text': '14',
                    'big_small': '大',
                    'odd_even': '双',
                    'combo': '大双',
                    'source_payload': '{}'
                }
                for issue_no, draw_time in draw_rows
            ]
        )

        maintenance_result = self.db.run_pc28_data_retention_maintenance(60, 60)
        self.assertEqual(maintenance_result['archived_prediction_rows'], 5)
        self.assertEqual(maintenance_result['deleted_prediction_rows'], 5)
        self.assertEqual(maintenance_result['archived_draw_rows'], 4)
        self.assertEqual(maintenance_result['deleted_draw_rows'], 4)

        remaining_predictions = self.db.get_recent_predictions(self.predictor_id, limit=None)
        self.assertEqual([item['issue_no'] for item in remaining_predictions], ['104'])

        remaining_draws = self.db.get_recent_draws('pc28', limit=None)
        self.assertEqual([item['issue_no'] for item in remaining_draws], ['104'])

        stats = self.db.get_predictor_stats(self.predictor_id)
        self.assertEqual(stats['total_predictions'], 6)
        self.assertEqual(stats['settled_predictions'], 5)
        self.assertEqual(stats['failed_predictions'], 1)
        self.assertEqual(stats['pending_predictions'], 0)
        self.assertEqual(stats['latest_settled_issue'], '104')
        self.assertEqual(stats['big_small_hit_rate'], 60.0)
        self.assertEqual(stats['recent_big_small_hit_rate'], 100.0)
        self.assertEqual(stats['streaks']['current_hit_streak'], 3)
        self.assertEqual(stats['streaks']['historical_max_hit_streak'], 3)
        self.assertEqual(stats['streaks']['historical_max_miss_streak'], 2)

        archive_summary = self.db.get_pc28_prediction_archive_summary(self.predictor_id)
        self.assertEqual(archive_summary['total_predictions'], 5)
        self.assertEqual(archive_summary['settled_predictions'], 4)
        self.assertEqual(archive_summary['failed_predictions'], 1)
        self.assertEqual(archive_summary['latest_issue_no'], '103')
        self.assertEqual(archive_summary['latest_settled_issue_no'], '103')

        admin_summary = self.db.get_admin_summary_counts()
        self.assertEqual(int(admin_summary['total_predictions']), 6)
        self.assertEqual(int(admin_summary['failed_predictions']), 1)
        self.assertEqual(int(admin_summary['settled_predictions']), 5)
        self.assertEqual(int(admin_summary['total_draws']), 5)

    def test_retention_keeps_old_pending_predictions_and_linked_draws(self):
        old_time = datetime.utcnow() - timedelta(days=90)
        self.db.upsert_prediction({
            'predictor_id': self.predictor_id,
            'lottery_type': 'pc28',
            'issue_no': '200',
            'requested_targets': ['big_small'],
            'prediction_big_small': '小',
            'status': 'pending'
        })
        conn = self.db.get_connection()
        try:
            conn.execute(
                '''
                UPDATE predictions
                SET created_at = ?, updated_at = ?
                WHERE predictor_id = ? AND issue_no = ?
                ''',
                (
                    _format_utc_timestamp(old_time),
                    _format_utc_timestamp(old_time + timedelta(minutes=1)),
                    self.predictor_id,
                    '200'
                )
            )
            conn.commit()
        finally:
            conn.close()

        self.db.upsert_draws(
            'pc28',
            [{
                'issue_no': '200',
                'draw_date': _format_beijing_date(old_time),
                'draw_time': '08:00:00',
                'open_time': f"{_format_beijing_date(old_time)} 08:00:00",
                'result_number': 9,
                'result_number_text': '09',
                'big_small': '小',
                'odd_even': '单',
                'combo': '小单',
                'source_payload': '{}'
            }]
        )

        maintenance_result = self.db.run_pc28_data_retention_maintenance(60, 60)
        self.assertEqual(maintenance_result['archived_prediction_rows'], 0)
        self.assertEqual(maintenance_result['deleted_prediction_rows'], 0)
        self.assertEqual(maintenance_result['archived_draw_rows'], 0)
        self.assertEqual(maintenance_result['deleted_draw_rows'], 0)

        remaining_predictions = self.db.get_recent_predictions(self.predictor_id, limit=None)
        self.assertEqual(len(remaining_predictions), 1)
        self.assertEqual(remaining_predictions[0]['issue_no'], '200')
        self.assertEqual(remaining_predictions[0]['status'], 'pending')

        remaining_draws = self.db.get_recent_draws('pc28', limit=None)
        self.assertEqual(len(remaining_draws), 1)
        self.assertEqual(remaining_draws[0]['issue_no'], '200')


if __name__ == '__main__':
    unittest.main()
