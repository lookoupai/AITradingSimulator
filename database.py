"""
数据库管理模块
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta
from typing import Any, Optional

from lotteries.registry import (
    get_target_label,
    normalize_lottery_type,
    normalize_prediction_targets,
    normalize_primary_metric,
    normalize_profit_metric,
    normalize_profit_rule
)
from utils.pc28 import derive_double_group, derive_kill_group


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
            CREATE TABLE IF NOT EXISTS predictor_runtime_state (
                predictor_id INTEGER PRIMARY KEY,
                consecutive_ai_failures INTEGER NOT NULL DEFAULT 0,
                auto_paused INTEGER NOT NULL DEFAULT 0,
                auto_paused_at TIMESTAMP,
                auto_pause_reason TEXT,
                last_ai_error_category TEXT,
                last_ai_error_message TEXT,
                last_ai_error_at TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (predictor_id) REFERENCES predictors(id)
            )
            '''
        )

        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS system_settings (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            '''
        )

        cursor.execute(
            '''
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
                source_provider TEXT NOT NULL DEFAULT '',
                result_payload TEXT,
                meta_payload TEXT,
                source_payload TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(lottery_type, event_key, source_provider)
            )
            '''
        )

        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS lottery_event_details (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lottery_type TEXT NOT NULL,
                event_key TEXT NOT NULL,
                detail_type TEXT NOT NULL,
                source_provider TEXT NOT NULL DEFAULT '',
                payload TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(lottery_type, event_key, detail_type, source_provider)
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
            CREATE TABLE IF NOT EXISTS prediction_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                predictor_id INTEGER NOT NULL,
                lottery_type TEXT NOT NULL,
                run_key TEXT NOT NULL,
                title TEXT DEFAULT '',
                requested_targets TEXT NOT NULL DEFAULT '[]',
                status TEXT NOT NULL DEFAULT 'pending',
                total_items INTEGER NOT NULL DEFAULT 0,
                settled_items INTEGER NOT NULL DEFAULT 0,
                hit_items INTEGER NOT NULL DEFAULT 0,
                confidence REAL,
                reasoning_summary TEXT,
                raw_response TEXT,
                prompt_snapshot TEXT,
                error_message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                settled_at TIMESTAMP,
                FOREIGN KEY (predictor_id) REFERENCES predictors(id),
                UNIQUE(predictor_id, run_key)
            )
            '''
        )

        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS prediction_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER NOT NULL,
                predictor_id INTEGER NOT NULL,
                lottery_type TEXT NOT NULL,
                run_key TEXT NOT NULL,
                event_key TEXT NOT NULL,
                item_order INTEGER NOT NULL DEFAULT 0,
                issue_no TEXT NOT NULL DEFAULT '',
                title TEXT NOT NULL DEFAULT '',
                requested_targets TEXT NOT NULL DEFAULT '[]',
                prediction_payload TEXT NOT NULL DEFAULT '{}',
                actual_payload TEXT NOT NULL DEFAULT '{}',
                hit_payload TEXT NOT NULL DEFAULT '{}',
                confidence REAL,
                reasoning_summary TEXT,
                raw_response TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                error_message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                settled_at TIMESTAMP,
                FOREIGN KEY (run_id) REFERENCES prediction_runs(id),
                FOREIGN KEY (predictor_id) REFERENCES predictors(id),
                UNIQUE(run_id, event_key)
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
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_lottery_events_lookup ON lottery_events(lottery_type, batch_key, event_time)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_lottery_event_details_lookup ON lottery_event_details(lottery_type, event_key, detail_type)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_draws_issue ON lottery_draws(lottery_type, issue_no)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_prediction_runs_predictor ON prediction_runs(predictor_id, run_key)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_prediction_runs_status ON prediction_runs(lottery_type, status, run_key)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_prediction_items_predictor ON prediction_items(predictor_id, created_at)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_prediction_items_run ON prediction_items(run_id, event_key)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_predictions_predictor ON predictions(predictor_id, issue_no)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_predictions_status ON predictions(status, issue_no)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_predictor_runtime_state_paused ON predictor_runtime_state(auto_paused, predictor_id)')

        try:
            cursor.execute("ALTER TABLE users ADD COLUMN is_admin INTEGER NOT NULL DEFAULT 0")
        except Exception:
            pass
        try:
            cursor.execute("ALTER TABLE predictors ADD COLUMN lottery_type TEXT NOT NULL DEFAULT 'pc28'")
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
            WHERE lottery_type = 'pc28' AND (profit_default_metric IS NULL OR profit_default_metric = '')
            '''
        )

        cursor.execute(
            '''
            UPDATE predictors
            SET profit_rule_id = 'pc28_high'
            WHERE lottery_type = 'pc28' AND (profit_rule_id IS NULL OR profit_rule_id = '')
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

        cursor.execute(
            '''
            UPDATE predictors
            SET lottery_type = 'pc28'
            WHERE lottery_type IS NULL OR lottery_type = ''
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
        normalized_lottery_type = normalize_lottery_type(lottery_type)
        normalized_targets = normalize_prediction_targets(normalized_lottery_type, prediction_targets)
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
                normalized_lottery_type,
                api_key,
                api_url,
                model_name,
                api_mode,
                normalize_primary_metric(normalized_lottery_type, primary_metric),
                normalize_profit_metric(normalized_lottery_type, profit_default_metric),
                normalize_profit_rule(normalized_lottery_type, profit_rule_id),
                1 if share_level != 'stats_only' else 0,
                share_level,
                prediction_method,
                system_prompt,
                data_injection_mode,
                json.dumps(normalized_targets, ensure_ascii=False),
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

        existing = self.get_predictor(predictor_id, include_secret=True) or {}
        lottery_type = normalize_lottery_type(fields.get('lottery_type') or existing.get('lottery_type'))
        updates = []
        values = []
        for key, value in fields.items():
            if key == 'prediction_targets':
                value = json.dumps(normalize_prediction_targets(lottery_type, value), ensure_ascii=False)
            if key == 'lottery_type':
                value = normalize_lottery_type(value)
            if key == 'primary_metric':
                value = normalize_primary_metric(lottery_type, value)
            if key == 'profit_default_metric':
                value = normalize_profit_metric(lottery_type, value)
            if key == 'profit_rule_id':
                value = normalize_profit_rule(lottery_type, value)
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
        cursor.execute('DELETE FROM predictor_runtime_state WHERE predictor_id = ?', (predictor_id,))
        cursor.execute('DELETE FROM prediction_items WHERE predictor_id = ?', (predictor_id,))
        cursor.execute('DELETE FROM prediction_runs WHERE predictor_id = ?', (predictor_id,))
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
        predictor = self._prepare_predictor(row, include_secret=include_secret)
        return self._attach_predictor_runtime_state(predictor)

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
        predictors = [item for item in (self._prepare_predictor(row, include_secret=include_secret) for row in rows) if item]
        return self._attach_predictor_runtime_state_batch(predictors)

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
        predictors = [item for item in (self._prepare_predictor(row, include_secret=include_secret) for row in rows) if item]
        return self._attach_predictor_runtime_state_batch(predictors)

    def get_enabled_predictors(
        self,
        lottery_type: str = 'pc28',
        include_secret: bool = True,
        exclude_auto_paused: bool = False
    ) -> list[dict]:
        normalized_lottery_type = normalize_lottery_type(lottery_type)
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT * FROM predictors
            WHERE lottery_type = ? AND enabled = 1
            ORDER BY created_at ASC
            ''',
            (normalized_lottery_type,)
        )
        rows = cursor.fetchall()
        conn.close()
        predictors = [item for item in (self._prepare_predictor(row, include_secret=include_secret) for row in rows) if item]
        predictors = self._attach_predictor_runtime_state_batch(predictors)
        if exclude_auto_paused:
            predictors = [item for item in predictors if not item.get('auto_paused')]
        return predictors

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

    def get_system_settings(self, keys: Optional[list[str]] = None) -> dict[str, str]:
        conn = self.get_connection()
        cursor = conn.cursor()
        if keys:
            unique_keys = list(dict.fromkeys([str(item) for item in keys if str(item).strip()]))
            if not unique_keys:
                conn.close()
                return {}
            placeholders = ','.join('?' for _ in unique_keys)
            cursor.execute(
                f'''
                SELECT key, value FROM system_settings
                WHERE key IN ({placeholders})
                ''',
                unique_keys
            )
        else:
            cursor.execute('SELECT key, value FROM system_settings')
        rows = cursor.fetchall()
        conn.close()
        return {str(row['key']): row['value'] for row in rows}

    def set_system_settings(self, values: dict[str, Any]):
        if not values:
            return

        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.executemany(
            '''
            INSERT INTO system_settings (key, value)
            VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET
                value = excluded.value,
                updated_at = CURRENT_TIMESTAMP
            ''',
            [
                (str(key), None if value is None else str(value))
                for key, value in values.items()
            ]
        )
        conn.commit()
        conn.close()

    def get_predictor_runtime_state(self, predictor_id: int) -> dict:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT * FROM predictor_runtime_state
            WHERE predictor_id = ?
            LIMIT 1
            ''',
            (predictor_id,)
        )
        row = cursor.fetchone()
        conn.close()
        if row is None:
            return self._default_predictor_runtime_state(predictor_id)
        return self._prepare_predictor_runtime_state(row)

    def get_predictor_runtime_state_map(self, predictor_ids: list[int]) -> dict[int, dict]:
        unique_ids = [int(item) for item in dict.fromkeys(predictor_ids or []) if item is not None]
        if not unique_ids:
            return {}

        conn = self.get_connection()
        cursor = conn.cursor()
        placeholders = ','.join('?' for _ in unique_ids)
        cursor.execute(
            f'''
            SELECT * FROM predictor_runtime_state
            WHERE predictor_id IN ({placeholders})
            ''',
            unique_ids
        )
        rows = cursor.fetchall()
        conn.close()

        state_map = {
            int(row['predictor_id']): self._prepare_predictor_runtime_state(row)
            for row in rows
        }
        for predictor_id in unique_ids:
            state_map.setdefault(int(predictor_id), self._default_predictor_runtime_state(int(predictor_id)))
        return state_map

    def update_predictor_runtime_state(self, predictor_id: int, fields: dict):
        existing = self.get_predictor_runtime_state(predictor_id)
        payload = {
            **existing,
            **(fields or {}),
            'predictor_id': predictor_id
        }

        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            INSERT INTO predictor_runtime_state (
                predictor_id,
                consecutive_ai_failures,
                auto_paused,
                auto_paused_at,
                auto_pause_reason,
                last_ai_error_category,
                last_ai_error_message,
                last_ai_error_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(predictor_id) DO UPDATE SET
                consecutive_ai_failures = excluded.consecutive_ai_failures,
                auto_paused = excluded.auto_paused,
                auto_paused_at = excluded.auto_paused_at,
                auto_pause_reason = excluded.auto_pause_reason,
                last_ai_error_category = excluded.last_ai_error_category,
                last_ai_error_message = excluded.last_ai_error_message,
                last_ai_error_at = excluded.last_ai_error_at,
                updated_at = CURRENT_TIMESTAMP
            ''',
            (
                payload['predictor_id'],
                int(payload.get('consecutive_ai_failures') or 0),
                1 if payload.get('auto_paused') else 0,
                payload.get('auto_paused_at'),
                payload.get('auto_pause_reason'),
                payload.get('last_ai_error_category'),
                payload.get('last_ai_error_message'),
                payload.get('last_ai_error_at')
            )
        )
        conn.commit()
        conn.close()

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

    # ============ Generic Lottery Events ============

    def upsert_lottery_events(self, events: list[dict]):
        if not events:
            return

        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.executemany(
            '''
            INSERT INTO lottery_events (
                lottery_type, event_key, batch_key, event_date, event_time,
                event_name, league, home_team, away_team, status, status_label,
                source_provider, result_payload, meta_payload, source_payload
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(lottery_type, event_key, source_provider) DO UPDATE SET
                batch_key = excluded.batch_key,
                event_date = excluded.event_date,
                event_time = excluded.event_time,
                event_name = excluded.event_name,
                league = excluded.league,
                home_team = excluded.home_team,
                away_team = excluded.away_team,
                status = excluded.status,
                status_label = excluded.status_label,
                result_payload = excluded.result_payload,
                meta_payload = excluded.meta_payload,
                source_payload = excluded.source_payload,
                updated_at = CURRENT_TIMESTAMP
            ''',
            [
                (
                    normalize_lottery_type(item.get('lottery_type')),
                    item['event_key'],
                    item.get('batch_key', ''),
                    item.get('event_date'),
                    item.get('event_time'),
                    item.get('event_name'),
                    item.get('league'),
                    item.get('home_team'),
                    item.get('away_team'),
                    item.get('status'),
                    item.get('status_label'),
                    item.get('source_provider', ''),
                    item.get('result_payload'),
                    item.get('meta_payload'),
                    item.get('source_payload')
                )
                for item in events
            ]
        )
        conn.commit()
        conn.close()

    def get_recent_lottery_events(
        self,
        lottery_type: str,
        limit: int = 20,
        batch_key: str | None = None,
        source_provider: str | None = None
    ) -> list[dict]:
        conn = self.get_connection()
        cursor = conn.cursor()
        query = '''
            SELECT * FROM lottery_events
            WHERE lottery_type = ?
        '''
        values: list[object] = [normalize_lottery_type(lottery_type)]
        if batch_key:
            query += ' AND batch_key = ?'
            values.append(batch_key)
        if source_provider:
            query += ' AND source_provider = ?'
            values.append(source_provider)
        query += ' ORDER BY event_time DESC, updated_at DESC LIMIT ?'
        values.append(limit)
        cursor.execute(query, values)
        rows = cursor.fetchall()
        conn.close()
        return [self._prepare_lottery_event(row) for row in rows]

    def get_lottery_event_by_key(
        self,
        lottery_type: str,
        event_key: str,
        source_provider: str | None = None
    ) -> Optional[dict]:
        conn = self.get_connection()
        cursor = conn.cursor()
        query = '''
            SELECT * FROM lottery_events
            WHERE lottery_type = ? AND event_key = ?
        '''
        values: list[object] = [normalize_lottery_type(lottery_type), event_key]
        if source_provider:
            query += ' AND source_provider = ?'
            values.append(source_provider)
        query += ' LIMIT 1'
        cursor.execute(query, values)
        row = cursor.fetchone()
        conn.close()
        return self._prepare_lottery_event(row) if row else None

    def get_lottery_event_map(
        self,
        lottery_type: str,
        event_keys: list[str],
        source_provider: str | None = None
    ) -> dict[str, dict]:
        if not event_keys:
            return {}

        conn = self.get_connection()
        cursor = conn.cursor()
        rows = []
        unique_event_keys = list(dict.fromkeys(event_keys))
        normalized_lottery_type = normalize_lottery_type(lottery_type)

        for start in range(0, len(unique_event_keys), 500):
            batch = unique_event_keys[start:start + 500]
            placeholders = ','.join('?' for _ in batch)
            query = f'''
                SELECT * FROM lottery_events
                WHERE lottery_type = ? AND event_key IN ({placeholders})
            '''
            values: list[object] = [normalized_lottery_type, *batch]
            if source_provider:
                query += ' AND source_provider = ?'
                values.append(source_provider)
            cursor.execute(query, values)
            rows.extend(cursor.fetchall())

        conn.close()
        return {
            row['event_key']: self._prepare_lottery_event(row)
            for row in rows
        }

    def upsert_lottery_event_details(self, details: list[dict]):
        if not details:
            return

        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.executemany(
            '''
            INSERT INTO lottery_event_details (
                lottery_type, event_key, detail_type, source_provider, payload
            )
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(lottery_type, event_key, detail_type, source_provider) DO UPDATE SET
                payload = excluded.payload,
                updated_at = CURRENT_TIMESTAMP
            ''',
            [
                (
                    normalize_lottery_type(item.get('lottery_type')),
                    item['event_key'],
                    item['detail_type'],
                    item.get('source_provider', ''),
                    json.dumps(item.get('payload') or {}, ensure_ascii=False)
                )
                for item in details
            ]
        )
        conn.commit()
        conn.close()

    def get_lottery_event_details(
        self,
        lottery_type: str,
        event_key: str,
        source_provider: str | None = None
    ) -> dict[str, dict]:
        conn = self.get_connection()
        cursor = conn.cursor()
        query = '''
            SELECT * FROM lottery_event_details
            WHERE lottery_type = ? AND event_key = ?
        '''
        values: list[object] = [normalize_lottery_type(lottery_type), event_key]
        if source_provider:
            query += ' AND source_provider = ?'
            values.append(source_provider)
        cursor.execute(query, values)
        rows = cursor.fetchall()
        conn.close()
        return {
            row['detail_type']: self._prepare_lottery_event_detail(row)
            for row in rows
        }

    # ============ Predictions ============

    def upsert_prediction_run(self, payload: dict) -> int:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            INSERT INTO prediction_runs (
                predictor_id, lottery_type, run_key, title, requested_targets,
                status, total_items, settled_items, hit_items, confidence,
                reasoning_summary, raw_response, prompt_snapshot, error_message, settled_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(predictor_id, run_key) DO UPDATE SET
                requested_targets = excluded.requested_targets,
                title = excluded.title,
                status = excluded.status,
                total_items = excluded.total_items,
                settled_items = excluded.settled_items,
                hit_items = excluded.hit_items,
                confidence = excluded.confidence,
                reasoning_summary = excluded.reasoning_summary,
                raw_response = excluded.raw_response,
                prompt_snapshot = excluded.prompt_snapshot,
                error_message = excluded.error_message,
                settled_at = excluded.settled_at,
                updated_at = CURRENT_TIMESTAMP
            ''',
            (
                payload['predictor_id'],
                normalize_lottery_type(payload.get('lottery_type')),
                payload['run_key'],
                payload.get('title', ''),
                json.dumps(payload.get('requested_targets') or [], ensure_ascii=False),
                payload.get('status', 'pending'),
                payload.get('total_items', 0),
                payload.get('settled_items', 0),
                payload.get('hit_items', 0),
                payload.get('confidence'),
                payload.get('reasoning_summary'),
                payload.get('raw_response'),
                payload.get('prompt_snapshot'),
                payload.get('error_message'),
                payload.get('settled_at')
            )
        )
        conn.commit()
        run_id = cursor.lastrowid or 0
        conn.close()
        if run_id:
            return run_id
        existing = self.get_prediction_run_by_key(payload['predictor_id'], payload['run_key'])
        return int(existing['id']) if existing else 0

    def get_prediction_run_by_key(self, predictor_id: int, run_key: str) -> Optional[dict]:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT * FROM prediction_runs
            WHERE predictor_id = ? AND run_key = ?
            LIMIT 1
            ''',
            (predictor_id, run_key)
        )
        row = cursor.fetchone()
        conn.close()
        return self._prepare_prediction_run(row) if row else None

    def get_prediction_run(self, run_id: int) -> Optional[dict]:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT * FROM prediction_runs
            WHERE id = ?
            LIMIT 1
            ''',
            (run_id,)
        )
        row = cursor.fetchone()
        conn.close()
        return self._prepare_prediction_run(row) if row else None

    def get_recent_prediction_runs(self, predictor_id: int, lottery_type: str | None = None, limit: int = 20) -> list[dict]:
        conn = self.get_connection()
        cursor = conn.cursor()
        if lottery_type:
            cursor.execute(
                '''
                SELECT * FROM prediction_runs
                WHERE predictor_id = ? AND lottery_type = ?
                ORDER BY created_at DESC, id DESC
                LIMIT ?
                ''',
                (predictor_id, normalize_lottery_type(lottery_type), limit)
            )
        else:
            cursor.execute(
                '''
                SELECT * FROM prediction_runs
                WHERE predictor_id = ?
                ORDER BY created_at DESC, id DESC
                LIMIT ?
                ''',
                (predictor_id, limit)
            )
        rows = cursor.fetchall()
        conn.close()
        return [self._prepare_prediction_run(row) for row in rows]

    def get_pending_prediction_runs(self, lottery_type: str) -> list[dict]:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT * FROM prediction_runs
            WHERE lottery_type = ? AND status = 'pending'
            ORDER BY created_at ASC, id ASC
            ''',
            (normalize_lottery_type(lottery_type),)
        )
        rows = cursor.fetchall()
        conn.close()
        return [self._prepare_prediction_run(row) for row in rows]

    def upsert_prediction_items(self, items: list[dict]):
        if not items:
            return

        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.executemany(
            '''
            INSERT INTO prediction_items (
                run_id, predictor_id, lottery_type, run_key, event_key, item_order,
                issue_no, title, requested_targets, prediction_payload, actual_payload,
                hit_payload, confidence, reasoning_summary, raw_response, status,
                error_message, settled_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(run_id, event_key) DO UPDATE SET
                item_order = excluded.item_order,
                issue_no = excluded.issue_no,
                title = excluded.title,
                requested_targets = excluded.requested_targets,
                prediction_payload = excluded.prediction_payload,
                actual_payload = excluded.actual_payload,
                hit_payload = excluded.hit_payload,
                confidence = excluded.confidence,
                reasoning_summary = excluded.reasoning_summary,
                raw_response = excluded.raw_response,
                status = excluded.status,
                error_message = excluded.error_message,
                settled_at = excluded.settled_at,
                updated_at = CURRENT_TIMESTAMP
            ''',
            [
                (
                    item['run_id'],
                    item['predictor_id'],
                    normalize_lottery_type(item.get('lottery_type')),
                    item['run_key'],
                    item['event_key'],
                    item.get('item_order', 0),
                    item.get('issue_no', ''),
                    item.get('title', ''),
                    json.dumps(item.get('requested_targets') or [], ensure_ascii=False),
                    json.dumps(item.get('prediction_payload') or {}, ensure_ascii=False),
                    json.dumps(item.get('actual_payload') or {}, ensure_ascii=False),
                    json.dumps(item.get('hit_payload') or {}, ensure_ascii=False),
                    item.get('confidence'),
                    item.get('reasoning_summary'),
                    item.get('raw_response'),
                    item.get('status', 'pending'),
                    item.get('error_message'),
                    item.get('settled_at')
                )
                for item in items
            ]
        )
        conn.commit()
        conn.close()

    def get_prediction_run_items(self, run_id: int) -> list[dict]:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT * FROM prediction_items
            WHERE run_id = ?
            ORDER BY item_order ASC, id ASC
            ''',
            (run_id,)
        )
        rows = cursor.fetchall()
        conn.close()
        return [self._prepare_prediction_item(row) for row in rows]

    def get_recent_prediction_items(self, predictor_id: int, lottery_type: str | None = None, limit: int = 100) -> list[dict]:
        conn = self.get_connection()
        cursor = conn.cursor()
        if lottery_type:
            cursor.execute(
                '''
                SELECT
                    i.*,
                    r.created_at AS run_created_at,
                    r.updated_at AS run_updated_at,
                    r.raw_response AS run_raw_response,
                    r.error_message AS run_error_message
                FROM prediction_items i
                JOIN prediction_runs r ON r.id = i.run_id
                WHERE i.predictor_id = ? AND i.lottery_type = ?
                ORDER BY i.created_at DESC, i.id DESC
                LIMIT ?
                ''',
                (predictor_id, normalize_lottery_type(lottery_type), limit)
            )
        else:
            cursor.execute(
                '''
                SELECT
                    i.*,
                    r.created_at AS run_created_at,
                    r.updated_at AS run_updated_at,
                    r.raw_response AS run_raw_response,
                    r.error_message AS run_error_message
                FROM prediction_items i
                JOIN prediction_runs r ON r.id = i.run_id
                WHERE i.predictor_id = ?
                ORDER BY i.created_at DESC, i.id DESC
                LIMIT ?
                ''',
                (predictor_id, limit)
            )
        rows = cursor.fetchall()
        conn.close()
        return [self._prepare_prediction_item(row) for row in rows]

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
            (normalize_lottery_type(lottery_type),)
        )
        rows = cursor.fetchall()
        conn.close()
        return [self._prepare_prediction(row) for row in rows]

    def get_predictor_stats(self, predictor_id: int) -> dict:
        predictor = self.get_predictor(predictor_id, include_secret=True) or {}
        lottery_type = normalize_lottery_type(predictor.get('lottery_type'))
        if lottery_type == 'jingcai_football':
            return self._build_football_predictor_stats(predictor)

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

        primary_metric = normalize_primary_metric(lottery_type, predictor.get('primary_metric'))
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

    def _build_football_predictor_stats(self, predictor: dict) -> dict:
        predictor_id = predictor.get('id')
        rows = self.get_recent_prediction_items(predictor_id, lottery_type='jingcai_football', limit=1000)
        settled_rows = [row for row in rows if row.get('status') == 'settled']
        primary_metric = normalize_primary_metric('jingcai_football', predictor.get('primary_metric'))
        metric_keys = ['spf', 'rqspf']
        metrics = {}

        for metric_key in metric_keys:
            metric_rows = []
            for row in settled_rows:
                if metric_key not in (row.get('requested_targets') or []):
                    continue
                hit_payload = row.get('hit_payload') or {}
                if metric_key not in hit_payload:
                    continue
                metric_rows.append(int(hit_payload[metric_key]))

            metrics[metric_key] = {
                'label': get_target_label('jingcai_football', metric_key),
                'recent_20': self._build_binary_metric_stats(metric_rows[:20]),
                'recent_100': self._build_binary_metric_stats(metric_rows[:100]),
                'overall': self._build_binary_metric_stats(metric_rows)
            }

        latest_settled = settled_rows[0] if settled_rows else None
        run_rows = self.get_recent_prediction_runs(predictor_id, lottery_type='jingcai_football', limit=1000)

        return {
            'total_predictions': len(run_rows),
            'settled_predictions': len([row for row in run_rows if row.get('status') == 'settled']),
            'pending_predictions': len([row for row in run_rows if row.get('status') == 'pending']),
            'failed_predictions': len([row for row in run_rows if row.get('status') == 'failed']),
            'expired_predictions': 0,
            'latest_settled_issue': latest_settled.get('issue_no') if latest_settled else None,
            'primary_metric': primary_metric,
            'primary_metric_label': get_target_label('jingcai_football', primary_metric),
            'metrics': metrics,
            'metric_streaks': {},
            'streaks': {}
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
                p.lottery_type,
                p.model_name,
                p.primary_metric,
                COALESCE(NULLIF(p.profit_default_metric, ''), p.primary_metric) AS profit_default_metric,
                COALESCE(NULLIF(p.profit_rule_id, ''), 'pc28_high') AS profit_rule_id,
                p.share_level,
                p.enabled,
                p.created_at,
                p.updated_at,
                u.username,
                COALESCE(prediction_stats.prediction_count, 0) + COALESCE(run_stats.prediction_count, 0) AS prediction_count,
                COALESCE(prediction_stats.failed_prediction_count, 0) + COALESCE(run_stats.failed_prediction_count, 0) AS failed_prediction_count,
                CASE
                    WHEN COALESCE(run_stats.latest_prediction_update, '') > COALESCE(prediction_stats.latest_prediction_update, '')
                        THEN run_stats.latest_issue_no
                    ELSE prediction_stats.latest_issue_no
                END AS latest_issue_no,
                CASE
                    WHEN COALESCE(run_stats.latest_prediction_update, '') > COALESCE(prediction_stats.latest_prediction_update, '')
                        THEN run_stats.latest_prediction_update
                    ELSE prediction_stats.latest_prediction_update
                END AS latest_prediction_update
            FROM predictors p
            JOIN users u ON u.id = p.user_id
            LEFT JOIN (
                SELECT
                    predictor_id,
                    COUNT(*) AS prediction_count,
                    MAX(issue_no) AS latest_issue_no,
                    MAX(updated_at) AS latest_prediction_update,
                    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed_prediction_count
                FROM predictions
                GROUP BY predictor_id
            ) AS prediction_stats ON prediction_stats.predictor_id = p.id
            LEFT JOIN (
                SELECT
                    predictor_id,
                    COUNT(*) AS prediction_count,
                    MAX(run_key) AS latest_issue_no,
                    MAX(updated_at) AS latest_prediction_update,
                    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed_prediction_count
                FROM prediction_runs
                GROUP BY predictor_id
            ) AS run_stats ON run_stats.predictor_id = p.id
            ORDER BY p.updated_at DESC, p.id DESC
            '''
        )
        rows = cursor.fetchall()
        conn.close()
        return self._attach_predictor_runtime_state_batch([dict(row) for row in rows])

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
                (
                    SELECT COUNT(*)
                    FROM predictors p
                    LEFT JOIN predictor_runtime_state prs ON prs.predictor_id = p.id
                    WHERE p.enabled = 1 AND COALESCE(prs.auto_paused, 0) = 0
                ) AS enabled_predictors,
                (
                    SELECT COUNT(*)
                    FROM predictors p
                    LEFT JOIN predictor_runtime_state prs ON prs.predictor_id = p.id
                    WHERE p.enabled = 1 AND COALESCE(prs.auto_paused, 0) = 1
                ) AS auto_paused_predictors,
                (SELECT COUNT(*) FROM predictors WHERE share_level != 'stats_only') AS shared_predictors,
                (SELECT COUNT(*) FROM predictions) + (SELECT COUNT(*) FROM prediction_runs) AS total_predictions,
                (SELECT COUNT(*) FROM predictions WHERE status = 'pending') + (SELECT COUNT(*) FROM prediction_runs WHERE status = 'pending') AS pending_predictions,
                (SELECT COUNT(*) FROM predictions WHERE status = 'failed') + (SELECT COUNT(*) FROM prediction_runs WHERE status = 'failed') AS failed_predictions,
                (SELECT COUNT(*) FROM predictions WHERE status = 'settled') + (SELECT COUNT(*) FROM prediction_runs WHERE status = 'settled') AS settled_predictions,
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
                failures.issue_no,
                failures.status,
                failures.error_message,
                failures.updated_at,
                failures.predictor_id,
                failures.predictor_name,
                failures.username,
                failures.lottery_type
            FROM (
                SELECT
                    p.issue_no AS issue_no,
                    p.status AS status,
                    p.error_message AS error_message,
                    p.updated_at AS updated_at,
                    pr.id AS predictor_id,
                    pr.name AS predictor_name,
                    u.username AS username,
                    pr.lottery_type AS lottery_type
                FROM predictions p
                JOIN predictors pr ON pr.id = p.predictor_id
                JOIN users u ON u.id = pr.user_id
                WHERE p.status = 'failed'

                UNION ALL

                SELECT
                    r.run_key AS issue_no,
                    r.status AS status,
                    r.error_message AS error_message,
                    r.updated_at AS updated_at,
                    pr.id AS predictor_id,
                    pr.name AS predictor_name,
                    u.username AS username,
                    pr.lottery_type AS lottery_type
                FROM prediction_runs r
                JOIN predictors pr ON pr.id = r.predictor_id
                JOIN users u ON u.id = pr.user_id
                WHERE r.status = 'failed'
            ) AS failures
            ORDER BY failures.updated_at DESC
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
        lottery_type = normalize_lottery_type(data.get('lottery_type'))
        data['lottery_type'] = lottery_type
        data['prediction_targets'] = normalize_prediction_targets(lottery_type, self._decode_json_list(data.get('prediction_targets')))
        data['enabled'] = bool(data.get('enabled'))
        data['api_mode'] = data.get('api_mode') or 'auto'
        data['primary_metric'] = normalize_primary_metric(lottery_type, data.get('primary_metric'))
        data['profit_default_metric'] = normalize_profit_metric(lottery_type, data.get('profit_default_metric') or data.get('primary_metric'))
        data['profit_rule_id'] = normalize_profit_rule(lottery_type, data.get('profit_rule_id') or 'pc28_high')
        data['share_level'] = data.get('share_level') or ('records' if data.get('share_predictions') else 'stats_only')
        data['share_predictions'] = bool(data.get('share_predictions'))
        data['data_injection_mode'] = data.get('data_injection_mode') or 'summary'
        if not include_secret:
            data.pop('api_key', None)
        return data

    def _default_predictor_runtime_state(self, predictor_id: int | None = None) -> dict:
        return {
            'predictor_id': predictor_id,
            'consecutive_ai_failures': 0,
            'auto_paused': False,
            'auto_paused_at': None,
            'auto_pause_reason': None,
            'last_ai_error_category': None,
            'last_ai_error_message': None,
            'last_ai_error_at': None
        }

    def _prepare_predictor_runtime_state(self, row) -> dict:
        data = dict(row)
        return {
            'predictor_id': int(data.get('predictor_id')) if data.get('predictor_id') is not None else None,
            'consecutive_ai_failures': int(data.get('consecutive_ai_failures') or 0),
            'auto_paused': bool(data.get('auto_paused')),
            'auto_paused_at': data.get('auto_paused_at'),
            'auto_pause_reason': data.get('auto_pause_reason'),
            'last_ai_error_category': data.get('last_ai_error_category'),
            'last_ai_error_message': data.get('last_ai_error_message'),
            'last_ai_error_at': data.get('last_ai_error_at')
        }

    def _attach_predictor_runtime_state(self, predictor: Optional[dict]) -> Optional[dict]:
        if predictor is None:
            return None
        runtime_state = self.get_predictor_runtime_state(predictor['id'])
        return {
            **predictor,
            **runtime_state
        }

    def _attach_predictor_runtime_state_batch(self, predictors: list[dict]) -> list[dict]:
        if not predictors:
            return []

        state_map = self.get_predictor_runtime_state_map([item['id'] for item in predictors if item.get('id') is not None])
        enriched = []
        for predictor in predictors:
            runtime_state = state_map.get(int(predictor['id']), self._default_predictor_runtime_state(predictor['id']))
            enriched.append({
                **predictor,
                **runtime_state
            })
        return enriched

    def _prepare_lottery_event(self, row) -> dict:
        data = dict(row)
        data['result_payload'] = self._decode_json_object(data.get('result_payload'))
        data['meta_payload'] = self._decode_json_object(data.get('meta_payload'))
        return data

    def _prepare_lottery_event_detail(self, row) -> dict:
        data = dict(row)
        data['payload'] = self._decode_json_object(data.get('payload'))
        return data

    def _prepare_draw(self, row) -> dict:
        return dict(row)

    def _prepare_prediction_run(self, row) -> Optional[dict]:
        if row is None:
            return None
        data = dict(row)
        data['requested_targets'] = self._decode_json_list(data.get('requested_targets'))
        return data

    def _prepare_prediction_item(self, row) -> dict:
        data = dict(row)
        data['requested_targets'] = self._decode_json_list(data.get('requested_targets'))
        data['prediction_payload'] = self._decode_json_object(data.get('prediction_payload'))
        data['actual_payload'] = self._decode_json_object(data.get('actual_payload'))
        data['hit_payload'] = self._decode_json_object(data.get('hit_payload'))
        if not data.get('raw_response') and data.get('run_raw_response'):
            data['raw_response'] = data.get('run_raw_response')
        if not data.get('error_message') and data.get('run_error_message'):
            data['error_message'] = data.get('run_error_message')
        return data

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

    def _decode_json_object(self, value: Any) -> dict:
        if value is None:
            return {}
        if isinstance(value, dict):
            return value
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except (TypeError, json.JSONDecodeError):
            return {}

    def _metric_label(self, metric_key: str, lottery_type: str = 'pc28') -> str:
        return get_target_label(lottery_type, metric_key)

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

    def _build_binary_metric_stats(self, outcomes: list[int]) -> dict:
        hit_count = sum(outcomes) if outcomes else 0
        sample_count = len(outcomes)
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
