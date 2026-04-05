"""
数据库管理模块
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta
from typing import Any, Optional

from utils.pc28 import derive_double_group, derive_kill_group, normalize_primary_metric, normalize_profit_metric, normalize_profit_rule


class Database:
    def __init__(self, db_path: str = 'pc28_predictor.db'):
        self.db_path = db_path

    def get_connection(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def init_db(self):
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                email TEXT UNIQUE,
                is_admin INTEGER NOT NULL DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            '''
        )

        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS predictors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                lottery_type TEXT NOT NULL DEFAULT 'pc28',
                api_key TEXT NOT NULL,
                api_url TEXT NOT NULL,
                model_name TEXT NOT NULL,
                api_mode TEXT NOT NULL DEFAULT 'auto',
                primary_metric TEXT NOT NULL DEFAULT 'big_small',
                profit_default_metric TEXT NOT NULL DEFAULT '',
                profit_rule_id TEXT NOT NULL DEFAULT 'pc28_netdisk',
                share_predictions INTEGER NOT NULL DEFAULT 0,
                share_level TEXT NOT NULL DEFAULT 'stats_only',
                prediction_method TEXT DEFAULT '',
                system_prompt TEXT DEFAULT '',
                data_injection_mode TEXT NOT NULL DEFAULT 'summary',
                prediction_targets TEXT NOT NULL DEFAULT '[]',
                history_window INTEGER NOT NULL DEFAULT 60,
                temperature REAL NOT NULL DEFAULT 0.7,
                enabled INTEGER NOT NULL DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(id)
            )
            '''
        )

        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS lottery_draws (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lottery_type TEXT NOT NULL,
                issue_no TEXT NOT NULL,
                draw_date TEXT,
                draw_time TEXT,
                open_time TEXT,
                result_number INTEGER NOT NULL,
                result_number_text TEXT NOT NULL,
                big_small TEXT NOT NULL,
                odd_even TEXT NOT NULL,
                combo TEXT NOT NULL,
                source_payload TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(lottery_type, issue_no)
            )
            '''
        )

        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS predictions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                predictor_id INTEGER NOT NULL,
                lottery_type TEXT NOT NULL DEFAULT 'pc28',
                issue_no TEXT NOT NULL,
                requested_targets TEXT NOT NULL DEFAULT '[]',
                prediction_number INTEGER,
                prediction_big_small TEXT,
                prediction_odd_even TEXT,
                prediction_combo TEXT,
                confidence REAL,
                reasoning_summary TEXT,
                raw_response TEXT,
                prompt_snapshot TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                error_message TEXT,
                actual_number INTEGER,
                actual_big_small TEXT,
                actual_odd_even TEXT,
                actual_combo TEXT,
                hit_number INTEGER,
                hit_big_small INTEGER,
                hit_odd_even INTEGER,
                hit_combo INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                settled_at TIMESTAMP,
                FOREIGN KEY (predictor_id) REFERENCES predictors(id),
                UNIQUE(predictor_id, issue_no)
            )
            '''
        )

        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS scheduler_state (
                name TEXT PRIMARY KEY,
                owner_id TEXT NOT NULL,
                heartbeat_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            '''
        )

        cursor.execute('CREATE INDEX IF NOT EXISTS idx_predictors_user ON predictors(user_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_draws_issue ON lottery_draws(lottery_type, issue_no)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_predictions_predictor ON predictions(predictor_id, issue_no)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_predictions_status ON predictions(status, issue_no)')

        try:
            cursor.execute("ALTER TABLE users ADD COLUMN is_admin INTEGER NOT NULL DEFAULT 0")
        except Exception:
            pass
        try:
            cursor.execute("ALTER TABLE predictors ADD COLUMN data_injection_mode TEXT NOT NULL DEFAULT 'summary'")
        except Exception:
            pass
        try:
            cursor.execute("ALTER TABLE predictors ADD COLUMN api_mode TEXT NOT NULL DEFAULT 'auto'")
        except Exception:
            pass
        try:
            cursor.execute("ALTER TABLE predictors ADD COLUMN primary_metric TEXT NOT NULL DEFAULT 'big_small'")
        except Exception:
            pass
        try:
            cursor.execute("ALTER TABLE predictors ADD COLUMN profit_default_metric TEXT NOT NULL DEFAULT ''")
        except Exception:
            pass
        try:
            cursor.execute("ALTER TABLE predictors ADD COLUMN profit_rule_id TEXT NOT NULL DEFAULT 'pc28_high'")
        except Exception:
            pass
        try:
            cursor.execute("ALTER TABLE predictors ADD COLUMN share_predictions INTEGER NOT NULL DEFAULT 0")
        except Exception:
            pass
        try:
            cursor.execute("ALTER TABLE predictors ADD COLUMN share_level TEXT NOT NULL DEFAULT 'stats_only'")
        except Exception:
            pass

        cursor.execute(
            '''
            UPDATE users
            SET is_admin = 1
            WHERE id = (
                SELECT id FROM users
                ORDER BY created_at ASC, id ASC
                LIMIT 1
            )
            AND NOT EXISTS (
                SELECT 1 FROM users WHERE is_admin = 1
            )
            '''
        )

        cursor.execute(
            '''
            UPDATE predictors
            SET profit_default_metric = primary_metric
            WHERE profit_default_metric IS NULL OR profit_default_metric = ''
            '''
        )

        cursor.execute(
            '''
            UPDATE predictors
            SET profit_rule_id = 'pc28_high'
            WHERE profit_rule_id IS NULL OR profit_rule_id = ''
            '''
        )

        cursor.execute(
            '''
            UPDATE predictors
            SET share_level = CASE
                WHEN share_predictions = 1 AND (share_level IS NULL OR share_level = 'stats_only') THEN 'records'
                WHEN share_level IS NULL OR share_level = '' THEN 'stats_only'
                ELSE share_level
            END
            '''
        )

        conn.commit()
        conn.close()

    # ============ Predictor Management ============

    def create_predictor(
        self,
        user_id: int,
        name: str,
        api_key: str,
        api_url: str,
        model_name: str,
        api_mode: str,
        primary_metric: str,
        profit_default_metric: str,
        profit_rule_id: str,
        share_level: str,
        prediction_method: str,
        system_prompt: str,
        data_injection_mode: str,
        prediction_targets: list[str],
        history_window: int,
        temperature: float,
        enabled: bool,
        lottery_type: str = 'pc28'
    ) -> int:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            INSERT INTO predictors (
                user_id, name, lottery_type, api_key, api_url, model_name, api_mode, primary_metric, profit_default_metric, profit_rule_id, share_predictions, share_level,
                prediction_method, system_prompt, data_injection_mode,
                prediction_targets, history_window, temperature, enabled
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                user_id,
                name,
                lottery_type,
                api_key,
                api_url,
                model_name,
                api_mode,
                primary_metric,
                profit_default_metric,
                profit_rule_id,
                1 if share_level != 'stats_only' else 0,
                share_level,
                prediction_method,
                system_prompt,
                data_injection_mode,
                json.dumps(prediction_targets, ensure_ascii=False),
                history_window,
                temperature,
                1 if enabled else 0
            )
        )
        predictor_id = cursor.lastrowid
        conn.commit()
        conn.close()
        return predictor_id

    def update_predictor(self, predictor_id: int, fields: dict):
        if not fields:
            return

        updates = []
        values = []
        for key, value in fields.items():
            if key == 'prediction_targets':
                value = json.dumps(value, ensure_ascii=False)
            if key == 'primary_metric':
                value = normalize_primary_metric(value)
            if key == 'profit_default_metric':
                value = normalize_profit_metric(value)
            if key == 'profit_rule_id':
                value = normalize_profit_rule(value)
            if key == 'enabled':
                value = 1 if value else 0
            updates.append(f'{key} = ?')
            values.append(value)

        updates.append('updated_at = CURRENT_TIMESTAMP')
        values.append(predictor_id)

        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            f'''
            UPDATE predictors
            SET {', '.join(updates)}
            WHERE id = ?
            ''',
            values
        )
        conn.commit()
        conn.close()

    def delete_predictor(self, predictor_id: int):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('DELETE FROM predictions WHERE predictor_id = ?', (predictor_id,))
        cursor.execute('DELETE FROM predictors WHERE id = ?', (predictor_id,))
        conn.commit()
        conn.close()

    def get_predictor(self, predictor_id: int, include_secret: bool = False) -> Optional[dict]:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM predictors WHERE id = ?', (predictor_id,))
        row = cursor.fetchone()
        conn.close()
        return self._prepare_predictor(row, include_secret=include_secret)

    def get_predictors_by_user(self, user_id: int, include_secret: bool = False) -> list[dict]:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT * FROM predictors
            WHERE user_id = ?
            ORDER BY created_at DESC
            ''',
            (user_id,)
        )
        rows = cursor.fetchall()
        conn.close()
        return [item for item in (self._prepare_predictor(row, include_secret=include_secret) for row in rows) if item]

    def get_all_predictors(self, include_secret: bool = False) -> list[dict]:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT * FROM predictors
            ORDER BY created_at DESC
            '''
        )
        rows = cursor.fetchall()
        conn.close()
        return [item for item in (self._prepare_predictor(row, include_secret=include_secret) for row in rows) if item]

    def get_enabled_predictors(self, lottery_type: str = 'pc28', include_secret: bool = True) -> list[dict]:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT * FROM predictors
            WHERE lottery_type = ? AND enabled = 1
            ORDER BY created_at ASC
            ''',
            (lottery_type,)
        )
        rows = cursor.fetchall()
        conn.close()
        return [item for item in (self._prepare_predictor(row, include_secret=include_secret) for row in rows) if item]

    def predictor_exists_for_user(self, predictor_id: int, user_id: int) -> bool:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            'SELECT 1 FROM predictors WHERE id = ? AND user_id = ?',
            (predictor_id, user_id)
        )
        row = cursor.fetchone()
        conn.close()
        return row is not None

    # ============ Draw Sync ============

    def upsert_draws(self, lottery_type: str, draws: list[dict]):
        if not draws:
            return

        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.executemany(
            '''
            INSERT INTO lottery_draws (
                lottery_type, issue_no, draw_date, draw_time, open_time,
                result_number, result_number_text, big_small, odd_even, combo,
                source_payload
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(lottery_type, issue_no) DO UPDATE SET
                draw_date = excluded.draw_date,
                draw_time = excluded.draw_time,
                open_time = excluded.open_time,
                result_number = excluded.result_number,
                result_number_text = excluded.result_number_text,
                big_small = excluded.big_small,
                odd_even = excluded.odd_even,
                combo = excluded.combo,
                source_payload = excluded.source_payload,
                updated_at = CURRENT_TIMESTAMP
            ''',
            [
                (
                    lottery_type,
                    draw['issue_no'],
                    draw.get('draw_date'),
                    draw.get('draw_time'),
                    draw.get('open_time'),
                    draw['result_number'],
                    draw['result_number_text'],
                    draw['big_small'],
                    draw['odd_even'],
                    draw['combo'],
                    draw.get('source_payload')
                )
                for draw in draws
            ]
        )
        conn.commit()
        conn.close()

    def get_recent_draws(self, lottery_type: str = 'pc28', limit: int = 20) -> list[dict]:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT * FROM lottery_draws
            WHERE lottery_type = ?
            ORDER BY CAST(issue_no AS INTEGER) DESC
            LIMIT ?
            ''',
            (lottery_type, limit)
        )
        rows = cursor.fetchall()
        conn.close()
        return [self._prepare_draw(row) for row in rows]

    def get_draw_by_issue(self, lottery_type: str, issue_no: str) -> Optional[dict]:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT * FROM lottery_draws
            WHERE lottery_type = ? AND issue_no = ?
            LIMIT 1
            ''',
            (lottery_type, issue_no)
        )
        row = cursor.fetchone()
        conn.close()
        return self._prepare_draw(row) if row else None

    def get_draws_by_issues(self, lottery_type: str, issue_nos: list[str]) -> dict[str, dict]:
        if not issue_nos:
            return {}

        conn = self.get_connection()
        cursor = conn.cursor()
        rows = []
        unique_issue_nos = list(dict.fromkeys(issue_nos))

        for start in range(0, len(unique_issue_nos), 500):
            batch = unique_issue_nos[start:start + 500]
            placeholders = ','.join('?' for _ in batch)
            cursor.execute(
                f'''
                SELECT * FROM lottery_draws
                WHERE lottery_type = ? AND issue_no IN ({placeholders})
                ''',
                (lottery_type, *batch)
            )
            rows.extend(cursor.fetchall())

        conn.close()
        return {
            row['issue_no']: self._prepare_draw(row)
            for row in rows
        }

    def get_oldest_pending_issue(self, lottery_type: str = 'pc28') -> Optional[str]:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT issue_no
            FROM predictions
            WHERE lottery_type = ? AND status = 'pending'
            ORDER BY CAST(issue_no AS INTEGER) ASC
            LIMIT 1
            ''',
            (lottery_type,)
        )
        row = cursor.fetchone()
        conn.close()
        return row['issue_no'] if row else None

    # ============ Predictions ============

    def upsert_prediction(self, payload: dict):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            INSERT INTO predictions (
                predictor_id, lottery_type, issue_no, requested_targets,
                prediction_number, prediction_big_small, prediction_odd_even,
                prediction_combo, confidence, reasoning_summary, raw_response,
                prompt_snapshot, status, error_message, actual_number,
                actual_big_small, actual_odd_even, actual_combo, hit_number,
                hit_big_small, hit_odd_even, hit_combo, settled_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(predictor_id, issue_no) DO UPDATE SET
                requested_targets = excluded.requested_targets,
                prediction_number = excluded.prediction_number,
                prediction_big_small = excluded.prediction_big_small,
                prediction_odd_even = excluded.prediction_odd_even,
                prediction_combo = excluded.prediction_combo,
                confidence = excluded.confidence,
                reasoning_summary = excluded.reasoning_summary,
                raw_response = excluded.raw_response,
                prompt_snapshot = excluded.prompt_snapshot,
                status = excluded.status,
                error_message = excluded.error_message,
                actual_number = excluded.actual_number,
                actual_big_small = excluded.actual_big_small,
                actual_odd_even = excluded.actual_odd_even,
                actual_combo = excluded.actual_combo,
                hit_number = excluded.hit_number,
                hit_big_small = excluded.hit_big_small,
                hit_odd_even = excluded.hit_odd_even,
                hit_combo = excluded.hit_combo,
                settled_at = excluded.settled_at,
                updated_at = CURRENT_TIMESTAMP
            ''',
            (
                payload['predictor_id'],
                payload.get('lottery_type', 'pc28'),
                payload['issue_no'],
                json.dumps(payload.get('requested_targets') or [], ensure_ascii=False),
                payload.get('prediction_number'),
                payload.get('prediction_big_small'),
                payload.get('prediction_odd_even'),
                payload.get('prediction_combo'),
                payload.get('confidence'),
                payload.get('reasoning_summary'),
                payload.get('raw_response'),
                payload.get('prompt_snapshot'),
                payload.get('status', 'pending'),
                payload.get('error_message'),
                payload.get('actual_number'),
                payload.get('actual_big_small'),
                payload.get('actual_odd_even'),
                payload.get('actual_combo'),
                payload.get('hit_number'),
                payload.get('hit_big_small'),
                payload.get('hit_odd_even'),
                payload.get('hit_combo'),
                payload.get('settled_at')
            )
        )
        conn.commit()
        conn.close()

    def get_prediction_by_issue(self, predictor_id: int, issue_no: str) -> Optional[dict]:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT * FROM predictions
            WHERE predictor_id = ? AND issue_no = ?
            LIMIT 1
            ''',
            (predictor_id, issue_no)
        )
        row = cursor.fetchone()
        conn.close()
        return self._prepare_prediction(row) if row else None

    def get_latest_prediction(self, predictor_id: int) -> Optional[dict]:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT * FROM predictions
            WHERE predictor_id = ?
            ORDER BY CAST(issue_no AS INTEGER) DESC
            LIMIT 1
            ''',
            (predictor_id,)
        )
        row = cursor.fetchone()
        conn.close()
        return self._prepare_prediction(row) if row else None

    def get_recent_predictions(self, predictor_id: int, limit: Optional[int] = 20) -> list[dict]:
        conn = self.get_connection()
        cursor = conn.cursor()
        if limit is None:
            cursor.execute(
                '''
                SELECT * FROM predictions
                WHERE predictor_id = ?
                ORDER BY CAST(issue_no AS INTEGER) DESC
                ''',
                (predictor_id,)
            )
        else:
            cursor.execute(
                '''
                SELECT * FROM predictions
                WHERE predictor_id = ?
                ORDER BY CAST(issue_no AS INTEGER) DESC
                LIMIT ?
                ''',
                (predictor_id, limit)
            )
        rows = cursor.fetchall()
        conn.close()
        return [self._prepare_prediction(row) for row in rows]

    def get_pending_predictions(self, lottery_type: str = 'pc28') -> list[dict]:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT p.*
            FROM predictions p
            JOIN predictors r ON r.id = p.predictor_id
            WHERE p.lottery_type = ? AND p.status = 'pending'
            ORDER BY CAST(p.issue_no AS INTEGER) ASC
            ''',
            (lottery_type,)
        )
        rows = cursor.fetchall()
        conn.close()
        return [self._prepare_prediction(row) for row in rows]

    def get_predictor_stats(self, predictor_id: int) -> dict:
        predictor = self.get_predictor(predictor_id, include_secret=True) or {}
        rows = self.get_recent_predictions(predictor_id, limit=None)
        settled_rows = [row for row in rows if row['status'] == 'settled']
        latest_settled = settled_rows[0] if settled_rows else None

        metric_keys = ['number', 'big_small', 'odd_even', 'combo', 'double_group', 'kill_group']
        windows = {
            'recent_20': settled_rows[:20],
            'recent_100': settled_rows[:100],
            'overall': settled_rows
        }

        metrics = {}
        metric_streaks = {}
        for metric_key in metric_keys:
            metrics[metric_key] = {
                'label': self._metric_label(metric_key),
                'recent_20': self._build_metric_stats(windows['recent_20'], metric_key),
                'recent_100': self._build_metric_stats(windows['recent_100'], metric_key),
                'overall': self._build_metric_stats(windows['overall'], metric_key)
            }
            metric_streaks[metric_key] = self._build_streak_stats(settled_rows, metric_key)

        primary_metric = normalize_primary_metric(predictor.get('primary_metric'))
        streaks = metric_streaks[primary_metric]

        return {
            'total_predictions': len(rows),
            'settled_predictions': len(settled_rows),
            'pending_predictions': len([row for row in rows if row['status'] == 'pending']),
            'failed_predictions': len([row for row in rows if row['status'] == 'failed']),
            'expired_predictions': len([row for row in rows if row['status'] == 'expired']),
            'latest_settled_issue': latest_settled['issue_no'] if latest_settled else None,
            'primary_metric': primary_metric,
            'primary_metric_label': self._metric_label(primary_metric),
            'metrics': metrics,
            'metric_streaks': metric_streaks,
            'streaks': streaks,
            'number_hit_rate': metrics['number']['overall']['hit_rate'],
            'big_small_hit_rate': metrics['big_small']['overall']['hit_rate'],
            'odd_even_hit_rate': metrics['odd_even']['overall']['hit_rate'],
            'combo_hit_rate': metrics['combo']['overall']['hit_rate'],
            'recent_number_hit_rate': metrics['number']['recent_20']['hit_rate'],
            'recent_big_small_hit_rate': metrics['big_small']['recent_20']['hit_rate'],
            'recent_odd_even_hit_rate': metrics['odd_even']['recent_20']['hit_rate'],
            'recent_combo_hit_rate': metrics['combo']['recent_20']['hit_rate']
        }

    # ============ Scheduler Lease ============

    def try_acquire_scheduler(self, name: str, owner_id: str, stale_after_seconds: int = 60) -> bool:
        conn = self.get_connection()
        conn.isolation_level = None
        cursor = conn.cursor()
        acquired = False

        try:
            cursor.execute('BEGIN IMMEDIATE')
            cursor.execute('SELECT owner_id, heartbeat_at FROM scheduler_state WHERE name = ?', (name,))
            row = cursor.fetchone()
            now = datetime.utcnow()

            if row is None:
                cursor.execute(
                    '''
                    INSERT INTO scheduler_state (name, owner_id, heartbeat_at)
                    VALUES (?, ?, CURRENT_TIMESTAMP)
                    ''',
                    (name, owner_id)
                )
                acquired = True
            else:
                last_heartbeat = self._parse_timestamp(row['heartbeat_at'])
                is_stale = last_heartbeat is None or (now - last_heartbeat) > timedelta(seconds=stale_after_seconds)
                if row['owner_id'] == owner_id or is_stale:
                    cursor.execute(
                        '''
                        UPDATE scheduler_state
                        SET owner_id = ?, heartbeat_at = CURRENT_TIMESTAMP
                        WHERE name = ?
                        ''',
                        (owner_id, name)
                    )
                    acquired = True

            cursor.execute('COMMIT')
        except Exception:
            try:
                cursor.execute('ROLLBACK')
            except Exception:
                pass
            acquired = False
        finally:
            conn.close()

        return acquired

    def heartbeat_scheduler(self, name: str, owner_id: str):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            UPDATE scheduler_state
            SET heartbeat_at = CURRENT_TIMESTAMP
            WHERE name = ? AND owner_id = ?
            ''',
            (name, owner_id)
        )
        conn.commit()
        conn.close()

    # ============ User Management ============

    def create_user(self, username: str, password_hash: str, email: str = None, is_admin: bool = False) -> int:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            INSERT INTO users (username, password_hash, email, is_admin)
            VALUES (?, ?, ?, ?)
            ''',
            (username, password_hash, email, 1 if is_admin else 0)
        )
        user_id = cursor.lastrowid
        conn.commit()
        conn.close()
        return user_id

    def get_user_by_username(self, username: str) -> Optional[dict]:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM users WHERE username = ?', (username,))
        row = cursor.fetchone()
        conn.close()
        return dict(row) if row else None

    def get_user_by_id(self, user_id: int) -> Optional[dict]:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM users WHERE id = ?', (user_id,))
        row = cursor.fetchone()
        conn.close()
        return dict(row) if row else None

    def get_all_users(self) -> list[dict]:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT id, username, email, is_admin, created_at FROM users')
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]

    def count_users(self) -> int:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) AS total FROM users')
        row = cursor.fetchone()
        conn.close()
        return int(row['total']) if row else 0

    def count_admin_users(self) -> int:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) AS total FROM users WHERE is_admin = 1')
        row = cursor.fetchone()
        conn.close()
        return int(row['total']) if row else 0

    def update_user_admin(self, user_id: int, is_admin: bool):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            UPDATE users
            SET is_admin = ?
            WHERE id = ?
            ''',
            (1 if is_admin else 0, user_id)
        )
        conn.commit()
        conn.close()

    def get_admin_users_overview(self) -> list[dict]:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT
                u.id,
                u.username,
                u.email,
                u.is_admin,
                u.created_at,
                COUNT(p.id) AS predictor_count,
                SUM(CASE WHEN p.enabled = 1 THEN 1 ELSE 0 END) AS enabled_predictor_count,
                MAX(p.updated_at) AS latest_predictor_update
            FROM users u
            LEFT JOIN predictors p ON p.user_id = u.id
            GROUP BY u.id
            ORDER BY u.created_at ASC, u.id ASC
            '''
        )
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]

    def get_admin_predictors_overview(self) -> list[dict]:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT
                p.id,
                p.user_id,
                p.name,
                p.model_name,
                p.primary_metric,
                COALESCE(NULLIF(p.profit_default_metric, ''), p.primary_metric) AS profit_default_metric,
                COALESCE(NULLIF(p.profit_rule_id, ''), 'pc28_high') AS profit_rule_id,
                p.share_level,
                p.enabled,
                p.created_at,
                p.updated_at,
                u.username,
                COUNT(pr.id) AS prediction_count,
                MAX(pr.issue_no) AS latest_issue_no,
                MAX(pr.updated_at) AS latest_prediction_update,
                SUM(CASE WHEN pr.status = 'failed' THEN 1 ELSE 0 END) AS failed_prediction_count
            FROM predictors p
            JOIN users u ON u.id = p.user_id
            LEFT JOIN predictions pr ON pr.predictor_id = p.id
            GROUP BY p.id
            ORDER BY p.updated_at DESC, p.id DESC
            '''
        )
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]

    def get_scheduler_snapshot(self, name: str) -> Optional[dict]:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT name, owner_id, heartbeat_at
            FROM scheduler_state
            WHERE name = ?
            LIMIT 1
            ''',
            (name,)
        )
        row = cursor.fetchone()
        conn.close()
        return dict(row) if row else None

    def get_admin_summary_counts(self) -> dict:
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute(
            '''
            SELECT
                (SELECT COUNT(*) FROM users) AS total_users,
                (SELECT COUNT(*) FROM users WHERE is_admin = 1) AS admin_users,
                (SELECT COUNT(*) FROM predictors) AS total_predictors,
                (SELECT COUNT(*) FROM predictors WHERE enabled = 1) AS enabled_predictors,
                (SELECT COUNT(*) FROM predictors WHERE share_level != 'stats_only') AS shared_predictors,
                (SELECT COUNT(*) FROM predictions) AS total_predictions,
                (SELECT COUNT(*) FROM predictions WHERE status = 'pending') AS pending_predictions,
                (SELECT COUNT(*) FROM predictions WHERE status = 'failed') AS failed_predictions,
                (SELECT COUNT(*) FROM predictions WHERE status = 'settled') AS settled_predictions,
                (SELECT COUNT(*) FROM lottery_draws WHERE lottery_type = 'pc28') AS total_draws
            '''
        )
        row = cursor.fetchone()
        conn.close()
        return dict(row) if row else {}

    def get_recent_failed_predictions(self, limit: int = 20) -> list[dict]:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT
                p.issue_no,
                p.status,
                p.error_message,
                p.updated_at,
                pr.id AS predictor_id,
                pr.name AS predictor_name,
                u.username
            FROM predictions p
            JOIN predictors pr ON pr.id = p.predictor_id
            JOIN users u ON u.id = pr.user_id
            WHERE p.status = 'failed'
            ORDER BY p.updated_at DESC
            LIMIT ?
            ''',
            (limit,)
        )
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]

    # ============ Serializers ============

    def _prepare_predictor(self, row, include_secret: bool = False) -> Optional[dict]:
        if row is None:
            return None

        data = dict(row)
        data['prediction_targets'] = self._decode_json_list(data.get('prediction_targets'))
        data['enabled'] = bool(data.get('enabled'))
        data['api_mode'] = data.get('api_mode') or 'auto'
        data['primary_metric'] = normalize_primary_metric(data.get('primary_metric'))
        data['profit_default_metric'] = normalize_profit_metric(data.get('profit_default_metric') or data.get('primary_metric'))
        data['profit_rule_id'] = normalize_profit_rule(data.get('profit_rule_id') or 'pc28_high', default='pc28_high')
        data['share_level'] = data.get('share_level') or ('records' if data.get('share_predictions') else 'stats_only')
        data['share_predictions'] = bool(data.get('share_predictions'))
        data['data_injection_mode'] = data.get('data_injection_mode') or 'summary'
        if not include_secret:
            data.pop('api_key', None)
        return data

    def _prepare_draw(self, row) -> dict:
        return dict(row)

    def _prepare_prediction(self, row) -> dict:
        data = dict(row)
        data['requested_targets'] = self._decode_json_list(data.get('requested_targets'))
        return data

    def _decode_json_list(self, value: Any) -> list[str]:
        if value is None:
            return []
        if isinstance(value, list):
            return value
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else []
        except (TypeError, json.JSONDecodeError):
            return []

    def _metric_label(self, metric_key: str) -> str:
        labels = {
            'number': '单点',
            'big_small': '大/小',
            'odd_even': '单/双',
            'combo': '组合投注',
            'double_group': '组合分组统计',
            'kill_group': '排除统计'
        }
        return labels.get(metric_key, metric_key)

    def _extract_metric_hit(self, row: dict, metric_key: str) -> Optional[int]:
        if metric_key == 'number':
            return row.get('hit_number')
        if metric_key == 'big_small':
            return row.get('hit_big_small')
        if metric_key == 'odd_even':
            return row.get('hit_odd_even')
        if metric_key == 'combo':
            return row.get('hit_combo')
        if metric_key == 'double_group':
            predicted_group = derive_double_group(row.get('prediction_combo'))
            actual_group = derive_double_group(row.get('actual_combo'))
            if predicted_group is None or actual_group is None:
                return None
            return 1 if predicted_group == actual_group else 0
        if metric_key == 'kill_group':
            kill_group = derive_kill_group(row.get('prediction_combo'))
            actual_combo = row.get('actual_combo')
            if kill_group is None or actual_combo is None:
                return None
            return 1 if actual_combo != kill_group else 0
        return None

    def _build_metric_stats(self, rows: list[dict], metric_key: str) -> dict:
        outcomes = [self._extract_metric_hit(row, metric_key) for row in rows]
        attempted = [item for item in outcomes if item is not None]
        hit_count = sum(attempted) if attempted else 0
        sample_count = len(attempted)
        hit_rate = round(hit_count / sample_count * 100, 2) if sample_count else None

        return {
            'hit_count': hit_count,
            'sample_count': sample_count,
            'hit_rate': hit_rate,
            'ratio_text': f'{hit_count}/{sample_count}' if sample_count else '--'
        }

    def _build_streak_stats(self, rows: list[dict], metric_key: str) -> dict:
        outcomes = [self._extract_metric_hit(row, metric_key) for row in rows]
        attempted = [item for item in outcomes if item is not None]
        recent_100 = attempted[:100]

        current_hit_streak = 0
        current_miss_streak = 0
        for outcome in attempted:
            if outcome == 1:
                if current_miss_streak == 0:
                    current_hit_streak += 1
                else:
                    break
            else:
                if current_hit_streak == 0:
                    current_miss_streak += 1
                else:
                    break

        return {
            'current_hit_streak': current_hit_streak,
            'current_miss_streak': current_miss_streak,
            'recent_100_max_hit_streak': self._max_streak(recent_100, 1),
            'recent_100_max_miss_streak': self._max_streak(recent_100, 0),
            'historical_max_hit_streak': self._max_streak(attempted, 1),
            'historical_max_miss_streak': self._max_streak(attempted, 0)
        }

    def _max_streak(self, outcomes: list[int], expected: int) -> int:
        best = 0
        current = 0
        for outcome in outcomes:
            if outcome == expected:
                current += 1
                if current > best:
                    best = current
            else:
                current = 0
        return best

    def _parse_timestamp(self, value: Any) -> Optional[datetime]:
        if not value:
            return None
        try:
            return datetime.strptime(str(value), '%Y-%m-%d %H:%M:%S')
        except ValueError:
            return None
