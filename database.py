"""
数据库管理模块
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta
from typing import Any, Optional


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
            cursor.execute("ALTER TABLE predictors ADD COLUMN data_injection_mode TEXT NOT NULL DEFAULT 'summary'")
        except Exception:
            pass

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
                user_id, name, lottery_type, api_key, api_url, model_name,
                prediction_method, system_prompt, data_injection_mode,
                prediction_targets, history_window, temperature, enabled
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                user_id,
                name,
                lottery_type,
                api_key,
                api_url,
                model_name,
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

    def get_recent_predictions(self, predictor_id: int, limit: int = 20) -> list[dict]:
        conn = self.get_connection()
        cursor = conn.cursor()
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
        rows = self.get_recent_predictions(predictor_id, limit=500)
        settled_rows = [row for row in rows if row['status'] == 'settled']
        recent_rows = settled_rows[:20]

        def _rate(items: list[dict], key: str) -> Optional[float]:
            attempted = [item for item in items if item[key] is not None]
            if not attempted:
                return None
            return round(sum(item[key] for item in attempted) / len(attempted) * 100, 2)

        latest_settled = settled_rows[0] if settled_rows else None

        return {
            'total_predictions': len(rows),
            'settled_predictions': len(settled_rows),
            'pending_predictions': len([row for row in rows if row['status'] == 'pending']),
            'failed_predictions': len([row for row in rows if row['status'] == 'failed']),
            'number_hit_rate': _rate(settled_rows, 'hit_number'),
            'big_small_hit_rate': _rate(settled_rows, 'hit_big_small'),
            'odd_even_hit_rate': _rate(settled_rows, 'hit_odd_even'),
            'combo_hit_rate': _rate(settled_rows, 'hit_combo'),
            'recent_number_hit_rate': _rate(recent_rows, 'hit_number'),
            'recent_big_small_hit_rate': _rate(recent_rows, 'hit_big_small'),
            'recent_odd_even_hit_rate': _rate(recent_rows, 'hit_odd_even'),
            'recent_combo_hit_rate': _rate(recent_rows, 'hit_combo'),
            'latest_settled_issue': latest_settled['issue_no'] if latest_settled else None
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

    def create_user(self, username: str, password_hash: str, email: str = None) -> int:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            INSERT INTO users (username, password_hash, email)
            VALUES (?, ?, ?)
            ''',
            (username, password_hash, email)
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
        cursor.execute('SELECT id, username, email, created_at FROM users')
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

    def _parse_timestamp(self, value: Any) -> Optional[datetime]:
        if not value:
            return None
        try:
            return datetime.strptime(str(value), '%Y-%m-%d %H:%M:%S')
        except ValueError:
            return None
