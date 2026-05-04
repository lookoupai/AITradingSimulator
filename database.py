"""
数据库管理模块
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta
from typing import Any, Optional

import config
from lotteries.registry import (
    get_target_label,
    normalize_lottery_type,
    normalize_prediction_targets,
    normalize_primary_metric,
    normalize_profit_metric,
    normalize_profit_rule
)
from utils.pc28 import derive_double_group, derive_kill_group
from utils.predictor_engine import (
    get_algorithm_label,
    get_default_machine_algorithm,
    get_engine_type_label,
    get_user_algorithm_id,
    is_user_algorithm_key,
    normalize_algorithm_key,
    normalize_engine_type,
    resolve_execution_label
)


class Database:
    def __init__(self, db_path: str = 'pc28_predictor.db'):
        self.db_path = db_path

    def get_connection(self):
        timeout_seconds = max(
            1.0,
            float(getattr(config, 'SQLITE_BUSY_TIMEOUT_MS', 5000)) / 1000.0
        )
        conn = sqlite3.connect(self.db_path, timeout=timeout_seconds)
        conn.row_factory = sqlite3.Row
        if bool(getattr(config, 'SQLITE_WAL_ENABLED', True)):
            conn.execute('PRAGMA journal_mode = WAL')
        conn.execute(f'PRAGMA busy_timeout = {int(getattr(config, "SQLITE_BUSY_TIMEOUT_MS", 5000))}')
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
                engine_type TEXT NOT NULL DEFAULT 'ai',
                algorithm_key TEXT NOT NULL DEFAULT '',
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
                user_algorithm_fallback_strategy TEXT NOT NULL DEFAULT 'fail',
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
            CREATE TABLE IF NOT EXISTS user_algorithm_execution_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                algorithm_id INTEGER,
                algorithm_version INTEGER,
                predictor_id INTEGER,
                run_key TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'succeeded',
                match_count INTEGER NOT NULL DEFAULT 0,
                prediction_count INTEGER NOT NULL DEFAULT 0,
                skip_count INTEGER NOT NULL DEFAULT 0,
                duration_ms INTEGER NOT NULL DEFAULT 0,
                fallback_strategy TEXT NOT NULL DEFAULT 'fail',
                fallback_used INTEGER NOT NULL DEFAULT 0,
                error_message TEXT,
                debug_json TEXT NOT NULL DEFAULT '{}',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(id),
                FOREIGN KEY (algorithm_id) REFERENCES user_algorithms(id),
                FOREIGN KEY (predictor_id) REFERENCES predictors(id)
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
                last_counted_failure_key TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (predictor_id) REFERENCES predictors(id)
            )
            '''
        )

        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS user_algorithms (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                lottery_type TEXT NOT NULL,
                name TEXT NOT NULL,
                description TEXT NOT NULL DEFAULT '',
                algorithm_type TEXT NOT NULL DEFAULT 'dsl',
                definition_json TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'draft',
                active_version INTEGER NOT NULL DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(id)
            )
            '''
        )

        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS user_algorithm_versions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                algorithm_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                version INTEGER NOT NULL,
                change_summary TEXT NOT NULL DEFAULT '',
                definition_json TEXT NOT NULL,
                validation_json TEXT NOT NULL DEFAULT '{}',
                backtest_json TEXT NOT NULL DEFAULT '{}',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (algorithm_id) REFERENCES user_algorithms(id),
                FOREIGN KEY (user_id) REFERENCES users(id),
                UNIQUE(algorithm_id, version)
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
            CREATE TABLE IF NOT EXISTS notification_sender_accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                channel_type TEXT NOT NULL DEFAULT 'telegram',
                sender_name TEXT NOT NULL DEFAULT '',
                bot_name TEXT NOT NULL DEFAULT '',
                bot_token TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'active',
                is_default INTEGER NOT NULL DEFAULT 0,
                last_verified_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(id)
            )
            '''
        )

        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS bet_profiles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                lottery_type TEXT NOT NULL DEFAULT 'pc28',
                mode TEXT NOT NULL DEFAULT 'flat',
                base_stake REAL NOT NULL DEFAULT 10,
                multiplier REAL NOT NULL DEFAULT 2,
                max_steps INTEGER NOT NULL DEFAULT 6,
                refund_action TEXT NOT NULL DEFAULT 'hold',
                cap_action TEXT NOT NULL DEFAULT 'reset',
                enabled INTEGER NOT NULL DEFAULT 1,
                is_default INTEGER NOT NULL DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(id)
            )
            '''
        )

        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS notification_endpoints (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                channel_type TEXT NOT NULL DEFAULT 'telegram',
                endpoint_key TEXT NOT NULL,
                endpoint_label TEXT NOT NULL DEFAULT '',
                config_json TEXT NOT NULL DEFAULT '{}',
                status TEXT NOT NULL DEFAULT 'active',
                is_default INTEGER NOT NULL DEFAULT 0,
                last_verified_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(id),
                UNIQUE(user_id, channel_type, endpoint_key)
            )
            '''
        )

        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS notification_subscriptions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                predictor_id INTEGER NOT NULL,
                endpoint_id INTEGER NOT NULL,
                sender_mode TEXT NOT NULL DEFAULT 'platform',
                sender_account_id INTEGER,
                bet_profile_id INTEGER,
                event_type TEXT NOT NULL DEFAULT 'prediction_created',
                delivery_mode TEXT NOT NULL DEFAULT 'notify_only',
                filter_json TEXT NOT NULL DEFAULT '{}',
                enabled INTEGER NOT NULL DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(id),
                FOREIGN KEY (predictor_id) REFERENCES predictors(id),
                FOREIGN KEY (endpoint_id) REFERENCES notification_endpoints(id),
                FOREIGN KEY (sender_account_id) REFERENCES notification_sender_accounts(id),
                FOREIGN KEY (bet_profile_id) REFERENCES bet_profiles(id),
                UNIQUE(user_id, predictor_id, endpoint_id, event_type)
            )
            '''
        )

        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS notification_deliveries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                subscription_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                predictor_id INTEGER NOT NULL,
                endpoint_id INTEGER NOT NULL,
                event_type TEXT NOT NULL,
                record_key TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                payload_json TEXT NOT NULL DEFAULT '{}',
                error_message TEXT,
                sent_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (subscription_id) REFERENCES notification_subscriptions(id),
                FOREIGN KEY (user_id) REFERENCES users(id),
                FOREIGN KEY (predictor_id) REFERENCES predictors(id),
                FOREIGN KEY (endpoint_id) REFERENCES notification_endpoints(id),
                UNIQUE(subscription_id, event_type, record_key)
            )
            '''
        )

        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS notification_delivery_jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                delivery_id INTEGER NOT NULL UNIQUE,
                subscription_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                predictor_id INTEGER NOT NULL,
                endpoint_id INTEGER NOT NULL,
                sender_mode TEXT NOT NULL DEFAULT 'platform',
                sender_account_id INTEGER,
                channel_type TEXT NOT NULL DEFAULT 'telegram',
                status TEXT NOT NULL DEFAULT 'queued',
                attempt_count INTEGER NOT NULL DEFAULT 0,
                available_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                locked_at TIMESTAMP,
                last_error_message TEXT,
                last_response_json TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (delivery_id) REFERENCES notification_deliveries(id),
                FOREIGN KEY (subscription_id) REFERENCES notification_subscriptions(id),
                FOREIGN KEY (user_id) REFERENCES users(id),
                FOREIGN KEY (predictor_id) REFERENCES predictors(id),
                FOREIGN KEY (endpoint_id) REFERENCES notification_endpoints(id),
                FOREIGN KEY (sender_account_id) REFERENCES notification_sender_accounts(id)
            )
            '''
        )

        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS notification_rule_states (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                subscription_id INTEGER NOT NULL,
                rule_id TEXT NOT NULL,
                last_evaluated_issue TEXT,
                last_triggered_issue TEXT,
                last_triggered_at TIMESTAMP,
                last_status TEXT,
                last_payload_json TEXT NOT NULL DEFAULT '{}',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (subscription_id) REFERENCES notification_subscriptions(id),
                UNIQUE(subscription_id, rule_id)
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
            CREATE TABLE IF NOT EXISTS jingcai_history_backfill_jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trigger_source TEXT NOT NULL DEFAULT 'manual',
                status TEXT NOT NULL DEFAULT 'pending',
                start_date TEXT NOT NULL,
                end_date TEXT NOT NULL,
                include_details INTEGER NOT NULL DEFAULT 1,
                requested_days INTEGER NOT NULL DEFAULT 0,
                match_count INTEGER NOT NULL DEFAULT 0,
                detail_count INTEGER NOT NULL DEFAULT 0,
                error_message TEXT,
                result_json TEXT NOT NULL DEFAULT '{}',
                started_at TIMESTAMP,
                finished_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
                algorithm_key TEXT NOT NULL DEFAULT '',
                algorithm_version INTEGER,
                algorithm_snapshot_json TEXT NOT NULL DEFAULT '{}',
                execution_log_json TEXT NOT NULL DEFAULT '{}',
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
                retry_count INTEGER NOT NULL DEFAULT 0,
                last_retry_at TIMESTAMP,
                last_retry_error TEXT,
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
            CREATE TABLE IF NOT EXISTS pc28_prediction_daily_summary (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                predictor_id INTEGER NOT NULL,
                summary_date TEXT NOT NULL,
                total_predictions INTEGER NOT NULL DEFAULT 0,
                settled_predictions INTEGER NOT NULL DEFAULT 0,
                failed_predictions INTEGER NOT NULL DEFAULT 0,
                expired_predictions INTEGER NOT NULL DEFAULT 0,
                latest_issue_no TEXT,
                latest_settled_issue_no TEXT,
                metric_segments_json TEXT NOT NULL DEFAULT '{}',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (predictor_id) REFERENCES predictors(id),
                UNIQUE(predictor_id, summary_date)
            )
            '''
        )

        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS pc28_draw_daily_summary (
                summary_date TEXT PRIMARY KEY,
                draw_count INTEGER NOT NULL DEFAULT 0,
                latest_issue_no TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            '''
        )

        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS jingcai_prediction_daily_summary (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                predictor_id INTEGER NOT NULL,
                summary_date TEXT NOT NULL,
                total_items INTEGER NOT NULL DEFAULT 0,
                settled_items INTEGER NOT NULL DEFAULT 0,
                failed_items INTEGER NOT NULL DEFAULT 0,
                expired_items INTEGER NOT NULL DEFAULT 0,
                hit_breakdown_json TEXT NOT NULL DEFAULT '{}',
                latest_run_key TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (predictor_id) REFERENCES predictors(id),
                UNIQUE(predictor_id, summary_date)
            )
            '''
        )

        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS user_consensus_rules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                lottery_type TEXT NOT NULL DEFAULT 'jingcai_football',
                rules_json TEXT NOT NULL,
                summary TEXT NOT NULL DEFAULT '',
                predictor_pool_snapshot TEXT NOT NULL DEFAULT '[]',
                window_days INTEGER,
                sample_count INTEGER NOT NULL DEFAULT 0,
                generated_by_model TEXT NOT NULL DEFAULT '',
                raw_prompt TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(id),
                UNIQUE(user_id, lottery_type)
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
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_user_algorithm_execution_logs_algorithm ON user_algorithm_execution_logs(algorithm_id, created_at)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_lottery_events_lookup ON lottery_events(lottery_type, batch_key, event_time)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_lottery_event_details_lookup ON lottery_event_details(lottery_type, event_key, detail_type)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_jingcai_backfill_jobs_status ON jingcai_history_backfill_jobs(status, created_at)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_draws_issue ON lottery_draws(lottery_type, issue_no)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_prediction_runs_predictor ON prediction_runs(predictor_id, run_key)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_prediction_runs_status ON prediction_runs(lottery_type, status, run_key)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_prediction_items_predictor ON prediction_items(predictor_id, created_at)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_prediction_items_run ON prediction_items(run_id, event_key)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_predictions_predictor ON predictions(predictor_id, issue_no)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_predictions_status ON predictions(status, issue_no)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_pc28_prediction_daily_summary_predictor ON pc28_prediction_daily_summary(predictor_id, summary_date)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_jingcai_prediction_daily_summary_predictor ON jingcai_prediction_daily_summary(predictor_id, summary_date)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_user_consensus_rules_user ON user_consensus_rules(user_id, lottery_type)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_predictor_runtime_state_paused ON predictor_runtime_state(auto_paused, predictor_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_user_algorithms_user ON user_algorithms(user_id, lottery_type, status)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_user_algorithm_versions_algorithm ON user_algorithm_versions(algorithm_id, version)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_notification_sender_accounts_user ON notification_sender_accounts(user_id, channel_type, status)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_bet_profiles_user ON bet_profiles(user_id, lottery_type, enabled)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_notification_endpoints_user ON notification_endpoints(user_id, channel_type, status)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_notification_subscriptions_user ON notification_subscriptions(user_id, predictor_id, enabled)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_notification_deliveries_subscription ON notification_deliveries(subscription_id, created_at)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_notification_delivery_jobs_status ON notification_delivery_jobs(status, available_at)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_notification_rule_states_subscription ON notification_rule_states(subscription_id, rule_id)')

        try:
            cursor.execute("ALTER TABLE users ADD COLUMN is_admin INTEGER NOT NULL DEFAULT 0")
        except Exception:
            pass
        try:
            cursor.execute("ALTER TABLE predictors ADD COLUMN lottery_type TEXT NOT NULL DEFAULT 'pc28'")
        except Exception:
            pass
        try:
            cursor.execute("ALTER TABLE predictors ADD COLUMN engine_type TEXT NOT NULL DEFAULT 'ai'")
        except Exception:
            pass
        try:
            cursor.execute("ALTER TABLE predictors ADD COLUMN algorithm_key TEXT NOT NULL DEFAULT ''")
        except Exception:
            pass
        try:
            cursor.execute("ALTER TABLE predictors ADD COLUMN data_injection_mode TEXT NOT NULL DEFAULT 'summary'")
        except Exception:
            pass
        try:
            cursor.execute("ALTER TABLE predictors ADD COLUMN user_algorithm_fallback_strategy TEXT NOT NULL DEFAULT 'fail'")
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
        try:
            cursor.execute("ALTER TABLE notification_subscriptions ADD COLUMN sender_mode TEXT NOT NULL DEFAULT 'platform'")
        except Exception:
            pass
        try:
            cursor.execute("ALTER TABLE notification_subscriptions ADD COLUMN sender_account_id INTEGER")
        except Exception:
            pass
        try:
            cursor.execute("ALTER TABLE prediction_items ADD COLUMN retry_count INTEGER NOT NULL DEFAULT 0")
        except Exception:
            pass
        try:
            cursor.execute("ALTER TABLE prediction_items ADD COLUMN last_retry_at TIMESTAMP")
        except Exception:
            pass
        try:
            cursor.execute("ALTER TABLE prediction_items ADD COLUMN last_retry_error TEXT")
        except Exception:
            pass
        try:
            cursor.execute("ALTER TABLE predictor_runtime_state ADD COLUMN last_counted_failure_key TEXT")
        except Exception:
            pass
        try:
            cursor.execute("ALTER TABLE prediction_runs ADD COLUMN algorithm_key TEXT NOT NULL DEFAULT ''")
        except Exception:
            pass
        try:
            cursor.execute("ALTER TABLE prediction_runs ADD COLUMN algorithm_version INTEGER")
        except Exception:
            pass
        try:
            cursor.execute("ALTER TABLE prediction_runs ADD COLUMN algorithm_snapshot_json TEXT NOT NULL DEFAULT '{}'")
        except Exception:
            pass
        try:
            cursor.execute("ALTER TABLE prediction_runs ADD COLUMN execution_log_json TEXT NOT NULL DEFAULT '{}'")
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
        lottery_type: str = 'pc28',
        engine_type: str = 'ai',
        algorithm_key: str = '',
        user_algorithm_fallback_strategy: str = 'fail'
    ) -> int:
        normalized_lottery_type = normalize_lottery_type(lottery_type)
        normalized_engine_type = normalize_engine_type(engine_type)
        normalized_targets = normalize_prediction_targets(normalized_lottery_type, prediction_targets)
        normalized_algorithm_key = normalize_algorithm_key(
            normalized_lottery_type,
            normalized_engine_type,
            algorithm_key
        )
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            INSERT INTO predictors (
                user_id, name, lottery_type, engine_type, algorithm_key, api_key, api_url, model_name, api_mode, primary_metric, profit_default_metric, profit_rule_id, share_predictions, share_level,
                prediction_method, system_prompt, data_injection_mode,
                prediction_targets, user_algorithm_fallback_strategy, history_window, temperature, enabled
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                user_id,
                name,
                normalized_lottery_type,
                normalized_engine_type,
                normalized_algorithm_key,
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
                self._normalize_user_algorithm_fallback_strategy(user_algorithm_fallback_strategy),
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
        engine_type = normalize_engine_type(fields.get('engine_type') or existing.get('engine_type'))
        updates = []
        values = []
        for key, value in fields.items():
            if key == 'prediction_targets':
                value = json.dumps(normalize_prediction_targets(lottery_type, value), ensure_ascii=False)
            if key == 'lottery_type':
                value = normalize_lottery_type(value)
                lottery_type = value
            if key == 'engine_type':
                value = normalize_engine_type(value)
                engine_type = value
            if key == 'algorithm_key':
                value = normalize_algorithm_key(lottery_type, engine_type, value)
            if key == 'primary_metric':
                value = normalize_primary_metric(lottery_type, value)
            if key == 'profit_default_metric':
                value = normalize_profit_metric(lottery_type, value)
            if key == 'profit_rule_id':
                value = normalize_profit_rule(lottery_type, value)
            if key == 'enabled':
                value = 1 if value else 0
            if key == 'user_algorithm_fallback_strategy':
                value = self._normalize_user_algorithm_fallback_strategy(value)
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
        cursor.execute(
            '''
            DELETE FROM notification_rule_states
            WHERE subscription_id IN (
                SELECT id FROM notification_subscriptions WHERE predictor_id = ?
            )
            ''',
            (predictor_id,)
        )
        cursor.execute('DELETE FROM notification_deliveries WHERE predictor_id = ?', (predictor_id,))
        cursor.execute('DELETE FROM notification_subscriptions WHERE predictor_id = ?', (predictor_id,))
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

    # ============ User Algorithm Management ============

    def create_user_algorithm(
        self,
        user_id: int,
        lottery_type: str,
        name: str,
        description: str,
        definition: dict,
        validation: dict | None = None,
        status: str = 'draft',
        algorithm_type: str = 'dsl',
        change_summary: str = '初始版本'
    ) -> int:
        normalized_lottery_type = normalize_lottery_type(lottery_type)
        normalized_status = self._normalize_user_algorithm_status(status)
        definition_json = json.dumps(definition or {}, ensure_ascii=False)
        validation_json = json.dumps(validation or {}, ensure_ascii=False)

        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            INSERT INTO user_algorithms (
                user_id, lottery_type, name, description, algorithm_type,
                definition_json, status, active_version
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, 1)
            ''',
            (
                user_id,
                normalized_lottery_type,
                str(name or '').strip(),
                str(description or '').strip(),
                str(algorithm_type or 'dsl').strip() or 'dsl',
                definition_json,
                normalized_status
            )
        )
        algorithm_id = int(cursor.lastrowid or 0)
        cursor.execute(
            '''
            INSERT INTO user_algorithm_versions (
                algorithm_id, user_id, version, change_summary,
                definition_json, validation_json, backtest_json
            )
            VALUES (?, ?, 1, ?, ?, ?, '{}')
            ''',
            (
                algorithm_id,
                user_id,
                str(change_summary or '').strip() or '初始版本',
                definition_json,
                validation_json
            )
        )
        conn.commit()
        conn.close()
        return algorithm_id

    def update_user_algorithm(
        self,
        algorithm_id: int,
        user_id: int,
        fields: dict,
        create_version: bool = True,
        change_summary: str = '更新算法定义'
    ):
        if not fields:
            return

        existing = self.get_user_algorithm_for_user(algorithm_id, user_id)
        if not existing:
            return

        allowed_fields = {'name', 'description', 'definition_json', 'status'}
        updates = []
        values = []
        for key, value in fields.items():
            if key not in allowed_fields:
                continue
            if key == 'status':
                value = self._normalize_user_algorithm_status(value)
            updates.append(f'{key} = ?')
            values.append(value)

        if not updates:
            return

        next_version = int(existing.get('active_version') or 1)
        if create_version and 'definition_json' in fields:
            next_version = self._next_user_algorithm_version(algorithm_id, user_id)
            updates.append('active_version = ?')
            values.append(next_version)

        updates.append('updated_at = CURRENT_TIMESTAMP')
        values.extend([algorithm_id, user_id])

        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            f'''
            UPDATE user_algorithms
            SET {', '.join(updates)}
            WHERE id = ? AND user_id = ?
            ''',
            values
        )
        if create_version and 'definition_json' in fields:
            cursor.execute(
                '''
                INSERT INTO user_algorithm_versions (
                    algorithm_id, user_id, version, change_summary,
                    definition_json, validation_json, backtest_json
                )
                VALUES (?, ?, ?, ?, ?, ?, '{}')
                ''',
                (
                    algorithm_id,
                    user_id,
                    next_version,
                    str(change_summary or '').strip() or '更新算法定义',
                    fields['definition_json'],
                    fields.get('validation_json') or '{}'
                )
            )
        conn.commit()
        conn.close()

    def get_user_algorithm(self, algorithm_id: int) -> Optional[dict]:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM user_algorithms WHERE id = ?', (algorithm_id,))
        row = cursor.fetchone()
        conn.close()
        return self._prepare_user_algorithm(row)

    def get_user_algorithm_for_user(self, algorithm_id: int, user_id: int) -> Optional[dict]:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT * FROM user_algorithms
            WHERE id = ? AND user_id = ?
            ''',
            (algorithm_id, user_id)
        )
        row = cursor.fetchone()
        conn.close()
        return self._prepare_user_algorithm(row)

    def get_user_algorithms_by_user(
        self,
        user_id: int,
        lottery_type: str | None = None,
        include_disabled: bool = False
    ) -> list[dict]:
        conditions = ['user_id = ?']
        values: list[Any] = [user_id]
        if lottery_type:
            conditions.append('lottery_type = ?')
            values.append(normalize_lottery_type(lottery_type))
        if not include_disabled:
            conditions.append("status != 'disabled'")

        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            f'''
            SELECT * FROM user_algorithms
            WHERE {' AND '.join(conditions)}
            ORDER BY updated_at DESC, id DESC
            ''',
            values
        )
        rows = cursor.fetchall()
        conn.close()
        return [self._prepare_user_algorithm(row) for row in rows]

    def count_user_algorithms_by_user(self, user_id: int, include_disabled: bool = False) -> int:
        conditions = ['user_id = ?']
        values: list[Any] = [user_id]
        if not include_disabled:
            conditions.append("status != 'disabled'")
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            f'''
            SELECT COUNT(*) AS total
            FROM user_algorithms
            WHERE {' AND '.join(conditions)}
            ''',
            values
        )
        row = cursor.fetchone()
        conn.close()
        return int(row['total'] or 0) if row else 0

    def get_predictors_using_user_algorithm(self, user_id: int, algorithm_id: int) -> list[dict]:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT *
            FROM predictors
            WHERE user_id = ? AND engine_type = 'machine' AND algorithm_key = ?
            ORDER BY updated_at DESC, id DESC
            ''',
            (user_id, f'user:{int(algorithm_id or 0)}')
        )
        rows = cursor.fetchall()
        conn.close()
        return [
            item
            for item in (self._prepare_predictor(row, include_secret=False) for row in rows)
            if item
        ]

    def get_user_algorithm_versions_for_user(self, algorithm_id: int, user_id: int) -> list[dict]:
        if not self.user_algorithm_exists_for_user(algorithm_id, user_id):
            return []

        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT * FROM user_algorithm_versions
            WHERE algorithm_id = ? AND user_id = ?
            ORDER BY version DESC
            ''',
            (algorithm_id, user_id)
        )
        rows = cursor.fetchall()
        conn.close()
        return [self._prepare_user_algorithm_version(row) for row in rows]

    def create_user_algorithm_execution_log(self, payload: dict) -> int:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            INSERT INTO user_algorithm_execution_logs (
                user_id, algorithm_id, algorithm_version, predictor_id, run_key, status,
                match_count, prediction_count, skip_count, duration_ms,
                fallback_strategy, fallback_used, error_message, debug_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                payload.get('user_id'),
                payload.get('algorithm_id'),
                payload.get('algorithm_version'),
                payload.get('predictor_id'),
                str(payload.get('run_key') or ''),
                str(payload.get('status') or 'succeeded'),
                int(payload.get('match_count') or 0),
                int(payload.get('prediction_count') or 0),
                int(payload.get('skip_count') or 0),
                int(payload.get('duration_ms') or 0),
                self._normalize_user_algorithm_fallback_strategy(payload.get('fallback_strategy')),
                1 if payload.get('fallback_used') else 0,
                payload.get('error_message'),
                json.dumps(payload.get('debug') or {}, ensure_ascii=False)
            )
        )
        log_id = int(cursor.lastrowid or 0)
        conn.commit()
        conn.close()
        return log_id

    def get_user_algorithm_execution_logs_for_user(self, algorithm_id: int, user_id: int, limit: int = 50) -> list[dict]:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT *
            FROM user_algorithm_execution_logs
            WHERE algorithm_id = ? AND user_id = ?
            ORDER BY created_at DESC, id DESC
            LIMIT ?
            ''',
            (algorithm_id, user_id, max(1, min(int(limit or 50), 200)))
        )
        rows = cursor.fetchall()
        conn.close()
        return [self._prepare_user_algorithm_execution_log(row) for row in rows]

    def get_user_algorithm_version_for_user(self, algorithm_id: int, user_id: int, version: int) -> Optional[dict]:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT * FROM user_algorithm_versions
            WHERE algorithm_id = ? AND user_id = ? AND version = ?
            LIMIT 1
            ''',
            (algorithm_id, user_id, int(version or 0))
        )
        row = cursor.fetchone()
        conn.close()
        return self._prepare_user_algorithm_version(row)

    def update_user_algorithm_version_backtest(
        self,
        algorithm_id: int,
        user_id: int,
        version: int,
        backtest: dict
    ) -> bool:
        backtest_json = json.dumps(backtest or {}, ensure_ascii=False)
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            UPDATE user_algorithm_versions
            SET backtest_json = ?
            WHERE algorithm_id = ? AND user_id = ? AND version = ?
            ''',
            (backtest_json, algorithm_id, user_id, int(version or 0))
        )
        changed = cursor.rowcount > 0
        conn.commit()
        conn.close()
        return changed

    def activate_user_algorithm_version(self, algorithm_id: int, user_id: int, version: int) -> bool:
        version_row = self.get_user_algorithm_version_for_user(algorithm_id, user_id, version)
        if not version_row:
            return False

        status = 'validated' if (version_row.get('validation') or {}).get('valid') else 'draft'
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            UPDATE user_algorithms
            SET definition_json = ?, status = ?, active_version = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ? AND user_id = ?
            ''',
            (
                json.dumps(version_row.get('definition') or {}, ensure_ascii=False),
                status,
                int(version_row.get('version') or version),
                algorithm_id,
                user_id
            )
        )
        changed = cursor.rowcount > 0
        conn.commit()
        conn.close()
        return changed

    def _next_user_algorithm_version(self, algorithm_id: int, user_id: int) -> int:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT MAX(version) AS max_version
            FROM user_algorithm_versions
            WHERE algorithm_id = ? AND user_id = ?
            ''',
            (algorithm_id, user_id)
        )
        row = cursor.fetchone()
        conn.close()
        return int(row['max_version'] or 0) + 1 if row else 1

    def user_algorithm_exists_for_user(self, algorithm_id: int, user_id: int) -> bool:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            'SELECT 1 FROM user_algorithms WHERE id = ? AND user_id = ?',
            (algorithm_id, user_id)
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
                last_ai_error_at,
                last_counted_failure_key
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(predictor_id) DO UPDATE SET
                consecutive_ai_failures = excluded.consecutive_ai_failures,
                auto_paused = excluded.auto_paused,
                auto_paused_at = excluded.auto_paused_at,
                auto_pause_reason = excluded.auto_pause_reason,
                last_ai_error_category = excluded.last_ai_error_category,
                last_ai_error_message = excluded.last_ai_error_message,
                last_ai_error_at = excluded.last_ai_error_at,
                last_counted_failure_key = excluded.last_counted_failure_key,
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
                payload.get('last_ai_error_at'),
                payload.get('last_counted_failure_key')
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

    # ============= user_consensus_rules CRUD =============

    def save_user_consensus_rules(
        self,
        *,
        user_id: int,
        lottery_type: str,
        rules: list,
        summary: str,
        predictor_pool_snapshot: list,
        window_days: int | None,
        sample_count: int,
        generated_by_model: str,
        raw_prompt: str | None = None
    ) -> int:
        """INSERT OR REPLACE：每个用户每彩种只保留一份当前规则。"""
        normalized_lottery = normalize_lottery_type(lottery_type)
        rules_json = json.dumps(rules or [], ensure_ascii=False)
        snapshot_json = json.dumps(predictor_pool_snapshot or [], ensure_ascii=False)
        # raw_prompt 截断 4KB，防止表膨胀
        prompt_text = (raw_prompt or '')[:4096]

        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                '''
                INSERT INTO user_consensus_rules (
                    user_id, lottery_type, rules_json, summary,
                    predictor_pool_snapshot, window_days, sample_count,
                    generated_by_model, raw_prompt
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(user_id, lottery_type) DO UPDATE SET
                    rules_json = excluded.rules_json,
                    summary = excluded.summary,
                    predictor_pool_snapshot = excluded.predictor_pool_snapshot,
                    window_days = excluded.window_days,
                    sample_count = excluded.sample_count,
                    generated_by_model = excluded.generated_by_model,
                    raw_prompt = excluded.raw_prompt,
                    updated_at = CURRENT_TIMESTAMP
                ''',
                (
                    int(user_id), normalized_lottery, rules_json,
                    str(summary or '').strip(),
                    snapshot_json,
                    int(window_days) if window_days is not None else None,
                    int(sample_count or 0),
                    str(generated_by_model or '').strip(),
                    prompt_text
                )
            )
            conn.commit()
            cursor.execute(
                'SELECT id FROM user_consensus_rules WHERE user_id = ? AND lottery_type = ?',
                (int(user_id), normalized_lottery)
            )
            row = cursor.fetchone()
            return int(row['id']) if row else 0
        finally:
            conn.close()

    def get_user_consensus_rules(self, user_id: int, lottery_type: str) -> Optional[dict]:
        """返回包含 rules / snapshot 已解析的 dict；不存在返回 None。"""
        normalized_lottery = normalize_lottery_type(lottery_type)
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                '''
                SELECT * FROM user_consensus_rules
                WHERE user_id = ? AND lottery_type = ?
                LIMIT 1
                ''',
                (int(user_id), normalized_lottery)
            )
            row = cursor.fetchone()
        finally:
            conn.close()

        if not row:
            return None
        try:
            rules = json.loads(row['rules_json'] or '[]')
        except (TypeError, ValueError):
            rules = []
        try:
            snapshot = json.loads(row['predictor_pool_snapshot'] or '[]')
        except (TypeError, ValueError):
            snapshot = []
        return {
            'id': row['id'],
            'user_id': row['user_id'],
            'lottery_type': row['lottery_type'],
            'rules': rules,
            'summary': row['summary'] or '',
            'predictor_pool_snapshot': snapshot,
            'window_days': row['window_days'],
            'sample_count': row['sample_count'],
            'generated_by_model': row['generated_by_model'] or '',
            'created_at': row['created_at'],
            'updated_at': row['updated_at']
        }

    def delete_user_consensus_rules(self, user_id: int, lottery_type: str) -> bool:
        normalized_lottery = normalize_lottery_type(lottery_type)
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                'DELETE FROM user_consensus_rules WHERE user_id = ? AND lottery_type = ?',
                (int(user_id), normalized_lottery)
            )
            conn.commit()
            return (cursor.rowcount or 0) > 0
        finally:
            conn.close()

    def run_pc28_data_retention_maintenance(self, prediction_retention_days: int, draw_retention_days: int) -> dict:
        prediction_retention_days = max(1, int(prediction_retention_days or 1))
        draw_retention_days = max(1, int(draw_retention_days or 1))
        prediction_cutoff_date = self._get_beijing_cutoff_date(prediction_retention_days)
        draw_cutoff_date = self._get_beijing_cutoff_date(draw_retention_days)

        conn = self.get_connection()
        cursor = conn.cursor()
        result = {
            'prediction_cutoff_date': prediction_cutoff_date,
            'draw_cutoff_date': draw_cutoff_date,
            'archived_prediction_rows': 0,
            'archived_prediction_days': 0,
            'deleted_prediction_rows': 0,
            'archived_draw_rows': 0,
            'archived_draw_days': 0,
            'deleted_draw_rows': 0
        }

        try:
            prediction_result = self._archive_pc28_predictions_before_date(cursor, prediction_cutoff_date)
            draw_result = self._archive_pc28_draws_before_date(cursor, draw_cutoff_date)
            conn.commit()
            result.update(prediction_result)
            result.update(draw_result)
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

        return result

    def run_jingcai_data_retention_maintenance(self, retention_days: int) -> dict:
        """
        竞彩足球（lottery_type='jingcai_football'）的归档+清理。
        把超过 retention_days 的已结算/失败/过期 prediction_items 聚合到
        jingcai_prediction_daily_summary，再删除原始 prediction_items 与对应的
        prediction_runs（仅当一条 run 下所有 items 都已被删除时）。
        """
        retention_days = max(1, int(retention_days or 1))
        cutoff_date = self._get_beijing_cutoff_date(retention_days)

        conn = self.get_connection()
        cursor = conn.cursor()
        result = {
            'cutoff_date': cutoff_date,
            'archived_item_rows': 0,
            'archived_item_days': 0,
            'deleted_item_rows': 0,
            'deleted_run_rows': 0
        }

        try:
            archive_result = self._archive_jingcai_predictions_before_date(cursor, cutoff_date)
            conn.commit()
            result.update(archive_result)
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

        return result

    def _archive_jingcai_predictions_before_date(self, cursor, cutoff_date: str) -> dict:
        """聚合竞彩足球过期 items 到 daily summary 后删除原始记录。"""
        cursor.execute(
            '''
            SELECT
                predictor_id,
                run_key,
                status,
                created_at,
                date(datetime(created_at, '+8 hours')) AS summary_date,
                hit_payload
            FROM prediction_items
            WHERE lottery_type = 'jingcai_football'
              AND status IN ('settled', 'failed', 'expired')
              AND date(datetime(created_at, '+8 hours')) < ?
            ORDER BY predictor_id ASC, summary_date DESC
            ''',
            (cutoff_date,)
        )
        rows = cursor.fetchall()
        if not rows:
            return {
                'archived_item_rows': 0,
                'archived_item_days': 0,
                'deleted_item_rows': 0,
                'deleted_run_rows': 0
            }

        # 聚合 (predictor_id, summary_date) -> 统计
        daily_summary_map: dict[tuple[int, str], dict] = {}
        for row in rows:
            predictor_id = int(row['predictor_id'])
            summary_date = row['summary_date']
            key = (predictor_id, summary_date)
            bucket = daily_summary_map.get(key)
            if bucket is None:
                bucket = {
                    'predictor_id': predictor_id,
                    'summary_date': summary_date,
                    'total_items': 0,
                    'settled_items': 0,
                    'failed_items': 0,
                    'expired_items': 0,
                    'latest_run_key': None,
                    'hit_breakdown': {}  # field -> {'total': n, 'hit': n}
                }
                daily_summary_map[key] = bucket

            bucket['total_items'] += 1
            status = str(row['status'] or '').strip().lower()
            if bucket['latest_run_key'] is None:
                bucket['latest_run_key'] = row['run_key']

            if status == 'settled':
                bucket['settled_items'] += 1
                # 拆解 hit_payload，按字段累计 total/hit
                try:
                    hit_payload = json.loads(row['hit_payload'] or '{}')
                except (TypeError, ValueError):
                    hit_payload = {}
                if isinstance(hit_payload, dict):
                    for field_key, hit_value in hit_payload.items():
                        if hit_value is None:
                            continue
                        breakdown = bucket['hit_breakdown'].setdefault(
                            field_key, {'total': 0, 'hit': 0}
                        )
                        breakdown['total'] += 1
                        breakdown['hit'] += int(bool(hit_value))
            elif status == 'failed':
                bucket['failed_items'] += 1
            elif status == 'expired':
                bucket['expired_items'] += 1

        for summary in daily_summary_map.values():
            self._upsert_jingcai_prediction_daily_summary(cursor, summary)

        # 删除已归档的 prediction_items
        cursor.execute(
            '''
            DELETE FROM prediction_items
            WHERE lottery_type = 'jingcai_football'
              AND status IN ('settled', 'failed', 'expired')
              AND date(datetime(created_at, '+8 hours')) < ?
            ''',
            (cutoff_date,)
        )
        deleted_item_rows = cursor.rowcount if cursor.rowcount is not None else len(rows)

        # 清理"已没有任何 items 关联"的 prediction_runs（仅竞彩足球，且 run 自身也旧）
        cursor.execute(
            '''
            DELETE FROM prediction_runs
            WHERE lottery_type = 'jingcai_football'
              AND date(datetime(created_at, '+8 hours')) < ?
              AND id NOT IN (SELECT DISTINCT run_id FROM prediction_items WHERE run_id IS NOT NULL)
            ''',
            (cutoff_date,)
        )
        deleted_run_rows = cursor.rowcount if cursor.rowcount is not None else 0

        return {
            'archived_item_rows': len(rows),
            'archived_item_days': len(daily_summary_map),
            'deleted_item_rows': deleted_item_rows,
            'deleted_run_rows': deleted_run_rows
        }

    def _upsert_jingcai_prediction_daily_summary(self, cursor, summary: dict):
        """合并写入：如果同一 (predictor_id, summary_date) 已存在记录，把计数与字段统计相加。"""
        cursor.execute(
            '''
            SELECT total_items, settled_items, failed_items, expired_items,
                   hit_breakdown_json, latest_run_key
            FROM jingcai_prediction_daily_summary
            WHERE predictor_id = ? AND summary_date = ?
            ''',
            (summary['predictor_id'], summary['summary_date'])
        )
        existing = cursor.fetchone()

        new_breakdown = dict(summary.get('hit_breakdown') or {})
        if existing:
            try:
                old_breakdown = json.loads(existing['hit_breakdown_json'] or '{}')
            except (TypeError, ValueError):
                old_breakdown = {}
            if isinstance(old_breakdown, dict):
                for field_key, stat in old_breakdown.items():
                    if not isinstance(stat, dict):
                        continue
                    merged = new_breakdown.setdefault(field_key, {'total': 0, 'hit': 0})
                    merged['total'] += int(stat.get('total') or 0)
                    merged['hit'] += int(stat.get('hit') or 0)

            total_items = int(existing['total_items'] or 0) + summary['total_items']
            settled_items = int(existing['settled_items'] or 0) + summary['settled_items']
            failed_items = int(existing['failed_items'] or 0) + summary['failed_items']
            expired_items = int(existing['expired_items'] or 0) + summary['expired_items']
            latest_run_key = summary.get('latest_run_key') or existing['latest_run_key']

            cursor.execute(
                '''
                UPDATE jingcai_prediction_daily_summary
                SET total_items = ?,
                    settled_items = ?,
                    failed_items = ?,
                    expired_items = ?,
                    hit_breakdown_json = ?,
                    latest_run_key = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE predictor_id = ? AND summary_date = ?
                ''',
                (
                    total_items, settled_items, failed_items, expired_items,
                    json.dumps(new_breakdown, ensure_ascii=False),
                    latest_run_key,
                    summary['predictor_id'], summary['summary_date']
                )
            )
        else:
            cursor.execute(
                '''
                INSERT INTO jingcai_prediction_daily_summary (
                    predictor_id, summary_date, total_items, settled_items,
                    failed_items, expired_items, hit_breakdown_json, latest_run_key
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    summary['predictor_id'], summary['summary_date'],
                    summary['total_items'], summary['settled_items'],
                    summary['failed_items'], summary['expired_items'],
                    json.dumps(new_breakdown, ensure_ascii=False),
                    summary.get('latest_run_key')
                )
            )

    def vacuum(self):
        conn = self.get_connection()
        try:
            conn.execute('VACUUM')
        finally:
            conn.close()

    def _archive_pc28_predictions_before_date(self, cursor, cutoff_date: str) -> dict:
        cursor.execute(
            '''
            SELECT
                predictor_id,
                issue_no,
                status,
                created_at,
                date(datetime(created_at, '+8 hours')) AS summary_date,
                hit_number,
                hit_big_small,
                hit_odd_even,
                hit_combo,
                prediction_combo,
                actual_combo
            FROM predictions
            WHERE lottery_type = 'pc28'
              AND status IN ('settled', 'failed', 'expired')
              AND date(datetime(created_at, '+8 hours')) < ?
            ORDER BY predictor_id ASC, summary_date DESC, CAST(issue_no AS INTEGER) DESC
            ''',
            (cutoff_date,)
        )
        rows = cursor.fetchall()
        if not rows:
            return {
                'archived_prediction_rows': 0,
                'archived_prediction_days': 0,
                'deleted_prediction_rows': 0
            }

        daily_summary_map: dict[tuple[int, str], dict] = {}
        metric_keys = ['number', 'big_small', 'odd_even', 'combo', 'double_group', 'kill_group']

        for row in rows:
            predictor_id = int(row['predictor_id'])
            summary_date = row['summary_date']
            key = (predictor_id, summary_date)
            bucket = daily_summary_map.get(key)
            if bucket is None:
                bucket = {
                    'predictor_id': predictor_id,
                    'summary_date': summary_date,
                    'total_predictions': 0,
                    'settled_predictions': 0,
                    'failed_predictions': 0,
                    'expired_predictions': 0,
                    'latest_issue_no': None,
                    'latest_settled_issue_no': None,
                    'metrics': {}
                }
                daily_summary_map[key] = bucket

            bucket['total_predictions'] += 1
            status = str(row['status'] or '').strip().lower()
            if bucket['latest_issue_no'] is None:
                bucket['latest_issue_no'] = row['issue_no']
            if status == 'settled':
                bucket['settled_predictions'] += 1
                if bucket['latest_settled_issue_no'] is None:
                    bucket['latest_settled_issue_no'] = row['issue_no']
                prepared_row = self._prepare_prediction(row)
                for metric_key in metric_keys:
                    outcome = self._extract_metric_hit(prepared_row, metric_key)
                    if outcome is None:
                        continue
                    existing = bucket['metrics'].get(metric_key)
                    bucket['metrics'][metric_key] = self._merge_sequence_summaries(
                        existing,
                        self._sequence_summary_from_outcome(int(outcome))
                    )
            elif status == 'failed':
                bucket['failed_predictions'] += 1
            elif status == 'expired':
                bucket['expired_predictions'] += 1

        for summary in daily_summary_map.values():
            self._upsert_pc28_prediction_daily_summary(cursor, summary)

        cursor.execute(
            '''
            DELETE FROM predictions
            WHERE lottery_type = 'pc28'
              AND status IN ('settled', 'failed', 'expired')
              AND date(datetime(created_at, '+8 hours')) < ?
            ''',
            (cutoff_date,)
        )

        return {
            'archived_prediction_rows': len(rows),
            'archived_prediction_days': len(daily_summary_map),
            'deleted_prediction_rows': cursor.rowcount if cursor.rowcount is not None else len(rows)
        }

    def _archive_pc28_draws_before_date(self, cursor, cutoff_date: str) -> dict:
        cursor.execute(
            '''
            SELECT
                issue_no,
                COALESCE(NULLIF(draw_date, ''), date(datetime(created_at, '+8 hours'))) AS summary_date
            FROM lottery_draws d
            WHERE lottery_type = 'pc28'
              AND COALESCE(NULLIF(draw_date, ''), date(datetime(created_at, '+8 hours'))) < ?
              AND NOT EXISTS (
                  SELECT 1
                  FROM predictions p
                  WHERE p.lottery_type = 'pc28'
                    AND p.status = 'pending'
                    AND p.issue_no = d.issue_no
              )
            ORDER BY summary_date DESC, CAST(issue_no AS INTEGER) DESC
            ''',
            (cutoff_date,)
        )
        rows = cursor.fetchall()
        if not rows:
            return {
                'archived_draw_rows': 0,
                'archived_draw_days': 0,
                'deleted_draw_rows': 0
            }

        daily_summary_map: dict[str, dict] = {}
        for row in rows:
            summary_date = row['summary_date']
            bucket = daily_summary_map.get(summary_date)
            if bucket is None:
                bucket = {
                    'summary_date': summary_date,
                    'draw_count': 0,
                    'latest_issue_no': None
                }
                daily_summary_map[summary_date] = bucket
            bucket['draw_count'] += 1
            if bucket['latest_issue_no'] is None:
                bucket['latest_issue_no'] = row['issue_no']

        for summary in daily_summary_map.values():
            self._upsert_pc28_draw_daily_summary(cursor, summary)

        cursor.execute(
            '''
            DELETE FROM lottery_draws
            WHERE lottery_type = 'pc28'
              AND COALESCE(NULLIF(draw_date, ''), date(datetime(created_at, '+8 hours'))) < ?
              AND NOT EXISTS (
                  SELECT 1
                  FROM predictions p
                  WHERE p.lottery_type = 'pc28'
                    AND p.status = 'pending'
                    AND p.issue_no = lottery_draws.issue_no
              )
            ''',
            (cutoff_date,)
        )

        return {
            'archived_draw_rows': len(rows),
            'archived_draw_days': len(daily_summary_map),
            'deleted_draw_rows': cursor.rowcount if cursor.rowcount is not None else len(rows)
        }

    def _upsert_pc28_prediction_daily_summary(self, cursor, summary: dict):
        cursor.execute(
            '''
            SELECT *
            FROM pc28_prediction_daily_summary
            WHERE predictor_id = ? AND summary_date = ?
            LIMIT 1
            ''',
            (summary['predictor_id'], summary['summary_date'])
        )
        existing = cursor.fetchone()
        existing_metrics = self._decode_metric_segments(existing['metric_segments_json']) if existing else {}
        merged_metrics = dict(existing_metrics)

        for metric_key, sequence_summary in (summary.get('metrics') or {}).items():
            merged_metrics[metric_key] = self._merge_sequence_summaries(
                merged_metrics.get(metric_key),
                sequence_summary
            )

        latest_issue_no = summary.get('latest_issue_no')
        latest_settled_issue_no = summary.get('latest_settled_issue_no')
        if existing:
            latest_issue_no = self._max_issue_no(existing['latest_issue_no'], latest_issue_no)
            latest_settled_issue_no = self._max_issue_no(existing['latest_settled_issue_no'], latest_settled_issue_no)

        cursor.execute(
            '''
            INSERT INTO pc28_prediction_daily_summary (
                predictor_id,
                summary_date,
                total_predictions,
                settled_predictions,
                failed_predictions,
                expired_predictions,
                latest_issue_no,
                latest_settled_issue_no,
                metric_segments_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(predictor_id, summary_date) DO UPDATE SET
                total_predictions = excluded.total_predictions,
                settled_predictions = excluded.settled_predictions,
                failed_predictions = excluded.failed_predictions,
                expired_predictions = excluded.expired_predictions,
                latest_issue_no = excluded.latest_issue_no,
                latest_settled_issue_no = excluded.latest_settled_issue_no,
                metric_segments_json = excluded.metric_segments_json,
                updated_at = CURRENT_TIMESTAMP
            ''',
            (
                summary['predictor_id'],
                summary['summary_date'],
                int(summary.get('total_predictions') or 0) + int(existing['total_predictions'] or 0) if existing else int(summary.get('total_predictions') or 0),
                int(summary.get('settled_predictions') or 0) + int(existing['settled_predictions'] or 0) if existing else int(summary.get('settled_predictions') or 0),
                int(summary.get('failed_predictions') or 0) + int(existing['failed_predictions'] or 0) if existing else int(summary.get('failed_predictions') or 0),
                int(summary.get('expired_predictions') or 0) + int(existing['expired_predictions'] or 0) if existing else int(summary.get('expired_predictions') or 0),
                latest_issue_no,
                latest_settled_issue_no,
                json.dumps(merged_metrics, ensure_ascii=False)
            )
        )

    def _upsert_pc28_draw_daily_summary(self, cursor, summary: dict):
        cursor.execute(
            '''
            SELECT *
            FROM pc28_draw_daily_summary
            WHERE summary_date = ?
            LIMIT 1
            ''',
            (summary['summary_date'],)
        )
        existing = cursor.fetchone()
        latest_issue_no = summary.get('latest_issue_no')
        if existing:
            latest_issue_no = self._max_issue_no(existing['latest_issue_no'], latest_issue_no)

        cursor.execute(
            '''
            INSERT INTO pc28_draw_daily_summary (
                summary_date,
                draw_count,
                latest_issue_no
            )
            VALUES (?, ?, ?)
            ON CONFLICT(summary_date) DO UPDATE SET
                draw_count = excluded.draw_count,
                latest_issue_no = excluded.latest_issue_no,
                updated_at = CURRENT_TIMESTAMP
            ''',
            (
                summary['summary_date'],
                int(summary.get('draw_count') or 0) + int(existing['draw_count'] or 0) if existing else int(summary.get('draw_count') or 0),
                latest_issue_no
            )
        )

    def get_recent_draws(self, lottery_type: str = 'pc28', limit: Optional[int] = 20, offset: int = 0) -> list[dict]:
        conn = self.get_connection()
        cursor = conn.cursor()
        if limit is None:
            cursor.execute(
                '''
                SELECT * FROM lottery_draws
                WHERE lottery_type = ?
                ORDER BY CAST(issue_no AS INTEGER) DESC
                ''',
                (lottery_type,)
            )
        else:
            cursor.execute(
                '''
                SELECT * FROM lottery_draws
                WHERE lottery_type = ?
                ORDER BY CAST(issue_no AS INTEGER) DESC
                LIMIT ?
                OFFSET ?
                ''',
                (lottery_type, limit, max(0, int(offset or 0)))
            )
        rows = cursor.fetchall()
        conn.close()
        return [self._prepare_draw(row) for row in rows]

    def count_draws(self, lottery_type: str = 'pc28') -> int:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT COUNT(*) AS total FROM lottery_draws
            WHERE lottery_type = ?
            ''',
            (normalize_lottery_type(lottery_type),)
        )
        row = cursor.fetchone()
        conn.close()
        return int(row['total'] or 0) if row else 0

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
        limit: int | None = 20,
        offset: int = 0,
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
        query += ' ORDER BY event_time DESC, updated_at DESC'
        if limit is not None:
            query += ' LIMIT ? OFFSET ?'
            values.append(limit)
            values.append(max(0, int(offset or 0)))
        cursor.execute(query, values)
        rows = cursor.fetchall()
        conn.close()
        return [self._prepare_lottery_event(row) for row in rows]

    def count_lottery_events(
        self,
        lottery_type: str,
        batch_key: str | None = None,
        source_provider: str | None = None
    ) -> int:
        conn = self.get_connection()
        cursor = conn.cursor()
        query = '''
            SELECT COUNT(*) AS total FROM lottery_events
            WHERE lottery_type = ?
        '''
        values: list[object] = [normalize_lottery_type(lottery_type)]
        if batch_key:
            query += ' AND batch_key = ?'
            values.append(batch_key)
        if source_provider:
            query += ' AND source_provider = ?'
            values.append(source_provider)
        cursor.execute(query, values)
        row = cursor.fetchone()
        conn.close()
        return int(row['total'] or 0) if row else 0

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

    # ============ Jingcai History Backfill ============

    def create_jingcai_backfill_job(
        self,
        trigger_source: str,
        start_date: str,
        end_date: str,
        include_details: bool,
        requested_days: int
    ) -> int:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            INSERT INTO jingcai_history_backfill_jobs (
                trigger_source, status, start_date, end_date, include_details, requested_days
            )
            VALUES (?, 'pending', ?, ?, ?, ?)
            ''',
            (
                str(trigger_source or 'manual').strip() or 'manual',
                str(start_date or '').strip(),
                str(end_date or '').strip(),
                1 if include_details else 0,
                int(requested_days or 0)
            )
        )
        job_id = int(cursor.lastrowid or 0)
        conn.commit()
        conn.close()
        return job_id

    def update_jingcai_backfill_job(self, job_id: int, fields: dict) -> bool:
        if not fields:
            return False
        allowed_fields = {
            'status',
            'match_count',
            'detail_count',
            'error_message',
            'result_json',
            'started_at',
            'finished_at'
        }
        updates = []
        values = []
        for key, value in fields.items():
            if key not in allowed_fields:
                continue
            if key == 'result_json' and not isinstance(value, str):
                value = json.dumps(value or {}, ensure_ascii=False)
            updates.append(f'{key} = ?')
            values.append(value)
        if not updates:
            return False
        updates.append('updated_at = CURRENT_TIMESTAMP')
        values.append(int(job_id or 0))
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            f'''
            UPDATE jingcai_history_backfill_jobs
            SET {', '.join(updates)}
            WHERE id = ?
            ''',
            values
        )
        changed = cursor.rowcount > 0
        conn.commit()
        conn.close()
        return changed

    def get_recent_jingcai_backfill_jobs(self, limit: int = 10) -> list[dict]:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT *
            FROM jingcai_history_backfill_jobs
            ORDER BY created_at DESC, id DESC
            LIMIT ?
            ''',
            (max(1, min(int(limit or 10), 100)),)
        )
        rows = cursor.fetchall()
        conn.close()
        return [self._prepare_jingcai_backfill_job(row) for row in rows]

    def get_latest_jingcai_backfill_job(self, status: str | None = None, trigger_source: str | None = None) -> Optional[dict]:
        conditions = []
        values: list[object] = []
        if status:
            conditions.append('status = ?')
            values.append(str(status))
        if trigger_source:
            conditions.append('trigger_source = ?')
            values.append(str(trigger_source))
        query = 'SELECT * FROM jingcai_history_backfill_jobs'
        if conditions:
            query += f" WHERE {' AND '.join(conditions)}"
        query += ' ORDER BY created_at DESC, id DESC LIMIT 1'
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(query, values)
        row = cursor.fetchone()
        conn.close()
        return self._prepare_jingcai_backfill_job(row) if row else None

    def build_jingcai_data_health(self, sample_limit: int = 2000) -> dict:
        normalized_lottery_type = 'jingcai_football'
        sample_limit = max(1, min(int(sample_limit or 2000), 10000))
        total_events = self.count_lottery_events(normalized_lottery_type, source_provider='sina')
        events = self.get_recent_lottery_events(
            normalized_lottery_type,
            limit=sample_limit,
            source_provider='sina'
        )
        event_keys = [item.get('event_key') for item in events if item.get('event_key')]
        detail_coverage = self._build_lottery_detail_coverage(normalized_lottery_type, event_keys, 'sina')
        counters = {
            'settled': 0,
            'spf_odds': 0,
            'rqspf_odds': 0,
            'recent_form': 0,
            'injury': 0,
            'euro_odds_snapshot': 0
        }
        earliest_date = None
        latest_date = None
        for event in events:
            event_date = str(event.get('event_date') or event.get('event_time') or '').strip()[:10]
            if event_date:
                earliest_date = event_date if earliest_date is None else min(earliest_date, event_date)
                latest_date = event_date if latest_date is None else max(latest_date, event_date)
            meta_payload = event.get('meta_payload') or {}
            result_payload = event.get('result_payload') or {}
            if result_payload.get('score1') is not None and result_payload.get('score2') is not None:
                counters['settled'] += 1
            if self._has_complete_football_odds(meta_payload.get('spf_odds') or {}):
                counters['spf_odds'] += 1
            if self._has_complete_football_odds(((meta_payload.get('rqspf') or {}).get('odds') or {})):
                counters['rqspf_odds'] += 1
            if event.get('event_key') in detail_coverage.get('recent_form', set()):
                counters['recent_form'] += 1
            if event.get('event_key') in detail_coverage.get('injury', set()):
                counters['injury'] += 1
            if event.get('event_key') in detail_coverage.get('euro_odds_snapshot', set()):
                counters['euro_odds_snapshot'] += 1

        sample_count = len(events)
        return {
            'lottery_type': normalized_lottery_type,
            'source_provider': 'sina',
            'total_event_count': total_events,
            'sample_count': sample_count,
            'earliest_sample_date': earliest_date,
            'latest_sample_date': latest_date,
            'metrics': {
                key: {
                    'count': value,
                    'rate': round(value / sample_count * 100, 2) if sample_count else None
                }
                for key, value in counters.items()
            },
            'enough_for_backtest': counters['settled'] >= 50 and counters['spf_odds'] >= 50,
            'recent_jobs': self.get_recent_jingcai_backfill_jobs(limit=5)
        }

    # ============ Predictions ============

    def upsert_prediction_run(self, payload: dict) -> int:
        existing_payload = None
        if payload.get('predictor_id') and payload.get('run_key') and not any(
            key in payload
            for key in ('algorithm_key', 'algorithm_version', 'algorithm_snapshot', 'execution_log')
        ):
            existing_payload = self.get_prediction_run_by_key(payload['predictor_id'], payload['run_key'])
            if existing_payload:
                payload = {
                    **payload,
                    'algorithm_key': existing_payload.get('algorithm_key') or '',
                    'algorithm_version': existing_payload.get('algorithm_version'),
                    'algorithm_snapshot': existing_payload.get('algorithm_snapshot') or {},
                    'execution_log': existing_payload.get('execution_log') or {}
                }
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            INSERT INTO prediction_runs (
                predictor_id, lottery_type, run_key, title, requested_targets,
                status, total_items, settled_items, hit_items, confidence,
                reasoning_summary, raw_response, prompt_snapshot, error_message, settled_at,
                algorithm_key, algorithm_version, algorithm_snapshot_json, execution_log_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                algorithm_key = excluded.algorithm_key,
                algorithm_version = excluded.algorithm_version,
                algorithm_snapshot_json = excluded.algorithm_snapshot_json,
                execution_log_json = excluded.execution_log_json,
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
                payload.get('settled_at'),
                payload.get('algorithm_key', ''),
                payload.get('algorithm_version'),
                json.dumps(payload.get('algorithm_snapshot') or {}, ensure_ascii=False),
                json.dumps(payload.get('execution_log') or {}, ensure_ascii=False)
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
                error_message, retry_count, last_retry_at, last_retry_error, settled_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                retry_count = excluded.retry_count,
                last_retry_at = excluded.last_retry_at,
                last_retry_error = excluded.last_retry_error,
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
                    int(item.get('retry_count') or 0),
                    item.get('last_retry_at'),
                    item.get('last_retry_error'),
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

    def get_recent_prediction_items(self, predictor_id: int, lottery_type: str | None = None, limit: int | None = 100, offset: int = 0) -> list[dict]:
        conn = self.get_connection()
        cursor = conn.cursor()
        if lottery_type:
            if limit is None:
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
                    ''',
                    (predictor_id, normalize_lottery_type(lottery_type))
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
                    WHERE i.predictor_id = ? AND i.lottery_type = ?
                    ORDER BY i.created_at DESC, i.id DESC
                    LIMIT ?
                    OFFSET ?
                    ''',
                    (predictor_id, normalize_lottery_type(lottery_type), limit, max(0, int(offset or 0)))
                )
        else:
            if limit is None:
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
                    ''',
                    (predictor_id,)
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
                    OFFSET ?
                    ''',
                    (predictor_id, limit, max(0, int(offset or 0)))
                )
        rows = cursor.fetchall()
        conn.close()
        return [self._prepare_prediction_item(row) for row in rows]

    def count_prediction_items(self, predictor_id: int, lottery_type: str | None = None) -> int:
        conn = self.get_connection()
        cursor = conn.cursor()
        if lottery_type:
            cursor.execute(
                '''
                SELECT COUNT(*) AS total
                FROM prediction_items
                WHERE predictor_id = ? AND lottery_type = ?
                ''',
                (predictor_id, normalize_lottery_type(lottery_type))
            )
        else:
            cursor.execute(
                '''
                SELECT COUNT(*) AS total
                FROM prediction_items
                WHERE predictor_id = ?
                ''',
                (predictor_id,)
            )
        row = cursor.fetchone()
        conn.close()
        return int(row['total'] or 0) if row else 0

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

    def get_recent_predictions(self, predictor_id: int, limit: Optional[int] = 20, offset: int = 0) -> list[dict]:
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
                OFFSET ?
                ''',
                (predictor_id, limit, max(0, int(offset or 0)))
            )
        rows = cursor.fetchall()
        conn.close()
        return [self._prepare_prediction(row) for row in rows]

    def get_recent_prediction_metric_samples(self, predictor_id: int, metric_key: str, limit: int = 100) -> list[dict]:
        rows = self.get_recent_predictions(predictor_id, limit=None)
        samples = []
        for row in rows:
            if row.get('status') != 'settled':
                continue
            hit_value = self._extract_metric_hit(row, metric_key)
            if hit_value is None:
                continue
            samples.append({
                'prediction_id': row.get('id'),
                'predictor_id': row.get('predictor_id'),
                'issue_no': row.get('issue_no'),
                'metric_key': metric_key,
                'hit': int(hit_value),
                'settled_at': row.get('settled_at'),
                'created_at': row.get('created_at'),
                'updated_at': row.get('updated_at')
            })
            if len(samples) >= max(1, int(limit or 100)):
                break
        return samples

    def count_predictions(self, predictor_id: int) -> int:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT COUNT(*) AS total
            FROM predictions
            WHERE predictor_id = ?
            ''',
            (predictor_id,)
        )
        row = cursor.fetchone()
        conn.close()
        return int(row['total'] or 0) if row else 0

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

    def get_pc28_prediction_archive_summary(self, predictor_id: int) -> dict:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT *
            FROM pc28_prediction_daily_summary
            WHERE predictor_id = ?
            ORDER BY summary_date DESC
            ''',
            (predictor_id,)
        )
        rows = cursor.fetchall()
        conn.close()

        aggregated_metrics: dict[str, dict] = {}
        total_predictions = 0
        settled_predictions = 0
        failed_predictions = 0
        expired_predictions = 0
        latest_issue_no = None
        latest_settled_issue_no = None

        for row in rows:
            total_predictions += int(row['total_predictions'] or 0)
            settled_predictions += int(row['settled_predictions'] or 0)
            failed_predictions += int(row['failed_predictions'] or 0)
            expired_predictions += int(row['expired_predictions'] or 0)
            latest_issue_no = self._max_issue_no(latest_issue_no, row['latest_issue_no'])
            latest_settled_issue_no = self._max_issue_no(latest_settled_issue_no, row['latest_settled_issue_no'])
            for metric_key, sequence_summary in self._decode_metric_segments(row['metric_segments_json']).items():
                aggregated_metrics[metric_key] = self._merge_sequence_summaries(
                    aggregated_metrics.get(metric_key),
                    sequence_summary
                )

        return {
            'total_predictions': total_predictions,
            'settled_predictions': settled_predictions,
            'failed_predictions': failed_predictions,
            'expired_predictions': expired_predictions,
            'latest_issue_no': latest_issue_no,
            'latest_settled_issue_no': latest_settled_issue_no,
            'metrics': aggregated_metrics
        }

    def get_predictor_stats(self, predictor_id: int) -> dict:
        predictor = self.get_predictor(predictor_id, include_secret=True) or {}
        lottery_type = normalize_lottery_type(predictor.get('lottery_type'))
        if lottery_type == 'jingcai_football':
            return self._build_football_predictor_stats(predictor)

        rows = self.get_recent_predictions(predictor_id, limit=None)
        settled_rows = [row for row in rows if row['status'] == 'settled']
        latest_settled = settled_rows[0] if settled_rows else None
        archived_summary = self.get_pc28_prediction_archive_summary(predictor_id)

        metric_keys = ['number', 'big_small', 'odd_even', 'combo', 'double_group', 'kill_group']
        windows = {
            'recent_20': settled_rows[:20],
            'recent_50': settled_rows[:50],
            'recent_100': settled_rows[:100],
            'overall': settled_rows
        }

        metrics = {}
        metric_streaks = {}
        for metric_key in metric_keys:
            recent_outcomes = self._extract_metric_outcomes(settled_rows, metric_key)
            recent_sequence_summary = self._build_sequence_summary_from_outcomes(recent_outcomes)
            overall_sequence_summary = self._merge_sequence_summaries(
                recent_sequence_summary,
                (archived_summary.get('metrics') or {}).get(metric_key)
            )
            metrics[metric_key] = {
                'label': self._metric_label(metric_key),
                'recent_20': self._build_metric_stats(windows['recent_20'], metric_key),
                'recent_50': self._build_metric_stats(windows['recent_50'], metric_key),
                'recent_100': self._build_metric_stats(windows['recent_100'], metric_key),
                'overall': self._build_metric_stats_from_sequence(overall_sequence_summary)
            }
            metric_streaks[metric_key] = self._build_streak_stats_from_sequence(
                overall_sequence_summary,
                recent_outcomes
            )

        primary_metric = normalize_primary_metric(lottery_type, predictor.get('primary_metric'))
        streaks = metric_streaks[primary_metric]

        return {
            'total_predictions': len(rows) + int(archived_summary.get('total_predictions') or 0),
            'settled_predictions': len(settled_rows) + int(archived_summary.get('settled_predictions') or 0),
            'pending_predictions': len([row for row in rows if row['status'] == 'pending']),
            'failed_predictions': len([row for row in rows if row['status'] == 'failed']) + int(archived_summary.get('failed_predictions') or 0),
            'expired_predictions': len([row for row in rows if row['status'] == 'expired']) + int(archived_summary.get('expired_predictions') or 0),
            'latest_settled_issue': latest_settled['issue_no'] if latest_settled else archived_summary.get('latest_settled_issue_no'),
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
        result = self.try_acquire_scheduler_with_details(
            name,
            owner_id,
            stale_after_seconds=stale_after_seconds
        )
        return bool(result.get('acquired'))

    def try_acquire_scheduler_with_details(
        self,
        name: str,
        owner_id: str,
        stale_after_seconds: int = 60
    ) -> dict:
        conn = self.get_connection()
        conn.isolation_level = None
        cursor = conn.cursor()
        result = {
            'acquired': False,
            'scheduler_name': name,
            'owner_id': owner_id,
            'current_owner_id': None,
            'current_heartbeat_at': None,
            'is_stale': None,
            'error': None
        }

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
                result['acquired'] = True
            else:
                last_heartbeat = self._parse_timestamp(row['heartbeat_at'])
                is_stale = last_heartbeat is None or (now - last_heartbeat) > timedelta(seconds=stale_after_seconds)
                result['current_owner_id'] = row['owner_id']
                result['current_heartbeat_at'] = row['heartbeat_at']
                result['is_stale'] = is_stale
                if row['owner_id'] == owner_id or is_stale:
                    cursor.execute(
                        '''
                        UPDATE scheduler_state
                        SET owner_id = ?, heartbeat_at = CURRENT_TIMESTAMP
                        WHERE name = ?
                        ''',
                        (owner_id, name)
                    )
                    result['acquired'] = True

            cursor.execute('COMMIT')
        except Exception as exc:
            try:
                cursor.execute('ROLLBACK')
            except Exception:
                pass
            result['error'] = str(exc)
        finally:
            conn.close()

        return result

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

    # ============ Notification Sender Accounts ============

    def create_notification_sender_account(
        self,
        user_id: int,
        channel_type: str,
        sender_name: str,
        bot_name: str,
        bot_token: str,
        status: str,
        is_default: bool,
        last_verified_at: str | None = None
    ) -> int:
        normalized_channel_type = str(channel_type or 'telegram').strip().lower() or 'telegram'
        conn = self.get_connection()
        cursor = conn.cursor()
        if is_default:
            cursor.execute(
                '''
                UPDATE notification_sender_accounts
                SET is_default = 0, updated_at = CURRENT_TIMESTAMP
                WHERE user_id = ? AND channel_type = ?
                ''',
                (user_id, normalized_channel_type)
            )
        cursor.execute(
            '''
            INSERT INTO notification_sender_accounts (
                user_id, channel_type, sender_name, bot_name, bot_token,
                status, is_default, last_verified_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                user_id,
                normalized_channel_type,
                sender_name,
                bot_name,
                bot_token,
                status,
                1 if is_default else 0,
                last_verified_at
            )
        )
        sender_id = cursor.lastrowid
        conn.commit()
        conn.close()
        return int(sender_id)

    def get_notification_sender_account(self, sender_id: int) -> Optional[dict]:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT * FROM notification_sender_accounts
            WHERE id = ?
            LIMIT 1
            ''',
            (sender_id,)
        )
        row = cursor.fetchone()
        conn.close()
        return self._prepare_notification_sender_account(row) if row else None

    def list_notification_sender_accounts(self, user_id: int) -> list[dict]:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT * FROM notification_sender_accounts
            WHERE user_id = ?
            ORDER BY channel_type ASC, is_default DESC, created_at ASC, id ASC
            ''',
            (user_id,)
        )
        rows = cursor.fetchall()
        conn.close()
        return [self._prepare_notification_sender_account(row) for row in rows]

    def update_notification_sender_account(self, sender_id: int, user_id: int, fields: dict):
        if not fields:
            return
        existing = self.get_notification_sender_account(sender_id)
        if not existing or int(existing.get('user_id') or 0) != int(user_id):
            return
        channel_type = str(fields.get('channel_type') or existing.get('channel_type') or 'telegram').strip().lower() or 'telegram'
        updates = []
        values = []
        conn = self.get_connection()
        cursor = conn.cursor()
        if 'is_default' in fields and fields.get('is_default'):
            cursor.execute(
                '''
                UPDATE notification_sender_accounts
                SET is_default = 0, updated_at = CURRENT_TIMESTAMP
                WHERE user_id = ? AND channel_type = ? AND id != ?
                ''',
                (user_id, channel_type, sender_id)
            )
        for key, value in fields.items():
            if key == 'channel_type':
                value = channel_type
            if key == 'is_default':
                value = 1 if value else 0
            updates.append(f'{key} = ?')
            values.append(value)
        updates.append('updated_at = CURRENT_TIMESTAMP')
        values.extend([sender_id, user_id])
        cursor.execute(
            f'''
            UPDATE notification_sender_accounts
            SET {', '.join(updates)}
            WHERE id = ? AND user_id = ?
            ''',
            values
        )
        conn.commit()
        conn.close()

    def delete_notification_sender_account(self, sender_id: int, user_id: int):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            UPDATE notification_subscriptions
            SET sender_mode = 'platform', sender_account_id = NULL, updated_at = CURRENT_TIMESTAMP
            WHERE sender_account_id = ? AND user_id = ?
            ''',
            (sender_id, user_id)
        )
        cursor.execute(
            '''
            DELETE FROM notification_sender_accounts
            WHERE id = ? AND user_id = ?
            ''',
            (sender_id, user_id)
        )
        conn.commit()
        conn.close()

    # ============ Bet Profiles ============

    def create_bet_profile(
        self,
        user_id: int,
        name: str,
        lottery_type: str,
        mode: str,
        base_stake: float,
        multiplier: float,
        max_steps: int,
        refund_action: str,
        cap_action: str,
        enabled: bool,
        is_default: bool
    ) -> int:
        normalized_lottery_type = normalize_lottery_type(lottery_type)
        conn = self.get_connection()
        cursor = conn.cursor()
        if is_default:
            cursor.execute(
                '''
                UPDATE bet_profiles
                SET is_default = 0, updated_at = CURRENT_TIMESTAMP
                WHERE user_id = ? AND lottery_type = ?
                ''',
                (user_id, normalized_lottery_type)
            )
        cursor.execute(
            '''
            INSERT INTO bet_profiles (
                user_id, name, lottery_type, mode, base_stake,
                multiplier, max_steps, refund_action, cap_action,
                enabled, is_default
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                user_id,
                name,
                normalized_lottery_type,
                mode,
                base_stake,
                multiplier,
                max_steps,
                refund_action,
                cap_action,
                1 if enabled else 0,
                1 if is_default else 0
            )
        )
        profile_id = cursor.lastrowid
        conn.commit()
        conn.close()
        return int(profile_id)

    def get_bet_profile(self, profile_id: int) -> Optional[dict]:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT * FROM bet_profiles
            WHERE id = ?
            LIMIT 1
            ''',
            (profile_id,)
        )
        row = cursor.fetchone()
        conn.close()
        return self._prepare_bet_profile(row) if row else None

    def list_bet_profiles(self, user_id: int, lottery_type: str | None = None) -> list[dict]:
        conn = self.get_connection()
        cursor = conn.cursor()
        if lottery_type:
            cursor.execute(
                '''
                SELECT * FROM bet_profiles
                WHERE user_id = ? AND lottery_type = ?
                ORDER BY is_default DESC, created_at ASC, id ASC
                ''',
                (user_id, normalize_lottery_type(lottery_type))
            )
        else:
            cursor.execute(
                '''
                SELECT * FROM bet_profiles
                WHERE user_id = ?
                ORDER BY lottery_type ASC, is_default DESC, created_at ASC, id ASC
                ''',
                (user_id,)
            )
        rows = cursor.fetchall()
        conn.close()
        return [self._prepare_bet_profile(row) for row in rows]

    def update_bet_profile(self, profile_id: int, user_id: int, fields: dict):
        if not fields:
            return
        existing = self.get_bet_profile(profile_id)
        if not existing or int(existing.get('user_id') or 0) != int(user_id):
            return

        normalized_lottery_type = normalize_lottery_type(fields.get('lottery_type') or existing.get('lottery_type'))
        updates = []
        values = []
        conn = self.get_connection()
        cursor = conn.cursor()
        if 'is_default' in fields and fields.get('is_default'):
            cursor.execute(
                '''
                UPDATE bet_profiles
                SET is_default = 0, updated_at = CURRENT_TIMESTAMP
                WHERE user_id = ? AND lottery_type = ? AND id != ?
                ''',
                (user_id, normalized_lottery_type, profile_id)
            )

        for key, value in fields.items():
            if key == 'lottery_type':
                value = normalized_lottery_type
            if key in {'enabled', 'is_default'}:
                value = 1 if value else 0
            updates.append(f'{key} = ?')
            values.append(value)

        updates.append('updated_at = CURRENT_TIMESTAMP')
        values.append(profile_id)
        values.append(user_id)
        cursor.execute(
            f'''
            UPDATE bet_profiles
            SET {', '.join(updates)}
            WHERE id = ? AND user_id = ?
            ''',
            values
        )
        conn.commit()
        conn.close()

    def delete_bet_profile(self, profile_id: int, user_id: int):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            DELETE FROM notification_subscriptions
            WHERE bet_profile_id = ? AND user_id = ?
            ''',
            (profile_id, user_id)
        )
        cursor.execute(
            '''
            DELETE FROM bet_profiles
            WHERE id = ? AND user_id = ?
            ''',
            (profile_id, user_id)
        )
        conn.commit()
        conn.close()

    # ============ Notification Endpoints ============

    def create_notification_endpoint(
        self,
        user_id: int,
        channel_type: str,
        endpoint_key: str,
        endpoint_label: str,
        config: dict,
        status: str,
        is_default: bool,
        last_verified_at: str | None = None
    ) -> int:
        conn = self.get_connection()
        cursor = conn.cursor()
        if is_default:
            cursor.execute(
                '''
                UPDATE notification_endpoints
                SET is_default = 0, updated_at = CURRENT_TIMESTAMP
                WHERE user_id = ? AND channel_type = ?
                ''',
                (user_id, channel_type)
            )
        cursor.execute(
            '''
            INSERT INTO notification_endpoints (
                user_id, channel_type, endpoint_key, endpoint_label,
                config_json, status, is_default, last_verified_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                user_id,
                channel_type,
                endpoint_key,
                endpoint_label,
                json.dumps(config or {}, ensure_ascii=False),
                status,
                1 if is_default else 0,
                last_verified_at
            )
        )
        endpoint_id = cursor.lastrowid
        conn.commit()
        conn.close()
        return int(endpoint_id)

    def get_notification_endpoint(self, endpoint_id: int) -> Optional[dict]:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT * FROM notification_endpoints
            WHERE id = ?
            LIMIT 1
            ''',
            (endpoint_id,)
        )
        row = cursor.fetchone()
        conn.close()
        return self._prepare_notification_endpoint(row) if row else None

    def list_notification_endpoints(self, user_id: int) -> list[dict]:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT * FROM notification_endpoints
            WHERE user_id = ?
            ORDER BY channel_type ASC, is_default DESC, created_at ASC, id ASC
            ''',
            (user_id,)
        )
        rows = cursor.fetchall()
        conn.close()
        return [self._prepare_notification_endpoint(row) for row in rows]

    def update_notification_endpoint(self, endpoint_id: int, user_id: int, fields: dict):
        if not fields:
            return
        existing = self.get_notification_endpoint(endpoint_id)
        if not existing or int(existing.get('user_id') or 0) != int(user_id):
            return

        channel_type = str(fields.get('channel_type') or existing.get('channel_type') or 'telegram').strip().lower()
        updates = []
        values = []
        conn = self.get_connection()
        cursor = conn.cursor()
        if 'is_default' in fields and fields.get('is_default'):
            cursor.execute(
                '''
                UPDATE notification_endpoints
                SET is_default = 0, updated_at = CURRENT_TIMESTAMP
                WHERE user_id = ? AND channel_type = ? AND id != ?
                ''',
                (user_id, channel_type, endpoint_id)
            )

        for key, value in fields.items():
            if key == 'config':
                key = 'config_json'
                value = json.dumps(value or {}, ensure_ascii=False)
            if key == 'is_default':
                value = 1 if value else 0
            updates.append(f'{key} = ?')
            values.append(value)

        updates.append('updated_at = CURRENT_TIMESTAMP')
        values.append(endpoint_id)
        values.append(user_id)
        cursor.execute(
            f'''
            UPDATE notification_endpoints
            SET {', '.join(updates)}
            WHERE id = ? AND user_id = ?
            ''',
            values
        )
        conn.commit()
        conn.close()

    def delete_notification_endpoint(self, endpoint_id: int, user_id: int):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            DELETE FROM notification_rule_states
            WHERE subscription_id IN (
                SELECT id FROM notification_subscriptions
                WHERE endpoint_id = ? AND user_id = ?
            )
            ''',
            (endpoint_id, user_id)
        )
        cursor.execute(
            '''
            DELETE FROM notification_deliveries
            WHERE endpoint_id = ? AND user_id = ?
            ''',
            (endpoint_id, user_id)
        )
        cursor.execute(
            '''
            DELETE FROM notification_subscriptions
            WHERE endpoint_id = ? AND user_id = ?
            ''',
            (endpoint_id, user_id)
        )
        cursor.execute(
            '''
            DELETE FROM notification_endpoints
            WHERE id = ? AND user_id = ?
            ''',
            (endpoint_id, user_id)
        )
        conn.commit()
        conn.close()

    # ============ Notification Subscriptions ============

    def create_notification_subscription(
        self,
        user_id: int,
        predictor_id: int,
        endpoint_id: int,
        sender_mode: str,
        sender_account_id: int | None,
        bet_profile_id: int | None,
        event_type: str,
        delivery_mode: str,
        filters: dict,
        enabled: bool
    ) -> int:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            INSERT INTO notification_subscriptions (
                user_id, predictor_id, endpoint_id, sender_mode, sender_account_id, bet_profile_id,
                event_type, delivery_mode, filter_json, enabled
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                user_id,
                predictor_id,
                endpoint_id,
                sender_mode,
                sender_account_id,
                bet_profile_id,
                event_type,
                delivery_mode,
                json.dumps(filters or {}, ensure_ascii=False),
                1 if enabled else 0
            )
        )
        subscription_id = cursor.lastrowid
        conn.commit()
        conn.close()
        return int(subscription_id)

    def get_notification_subscription(self, subscription_id: int) -> Optional[dict]:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT
                s.*,
                p.name AS predictor_name,
                p.lottery_type AS predictor_lottery_type,
                e.channel_type,
                e.endpoint_key,
                e.endpoint_label,
                sa.sender_name AS sender_account_name,
                sa.bot_name AS sender_bot_name,
                sa.status AS sender_account_status,
                b.name AS bet_profile_name,
                b.mode AS bet_profile_mode,
                b.base_stake AS bet_profile_base_stake
            FROM notification_subscriptions s
            JOIN predictors p ON p.id = s.predictor_id
            JOIN notification_endpoints e ON e.id = s.endpoint_id
            LEFT JOIN notification_sender_accounts sa ON sa.id = s.sender_account_id
            LEFT JOIN bet_profiles b ON b.id = s.bet_profile_id
            WHERE s.id = ?
            LIMIT 1
            ''',
            (subscription_id,)
        )
        row = cursor.fetchone()
        conn.close()
        return self._prepare_notification_subscription(row) if row else None

    def list_notification_subscriptions(self, user_id: int) -> list[dict]:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT
                s.*,
                p.name AS predictor_name,
                p.lottery_type AS predictor_lottery_type,
                e.channel_type,
                e.endpoint_key,
                e.endpoint_label,
                sa.sender_name AS sender_account_name,
                sa.bot_name AS sender_bot_name,
                sa.status AS sender_account_status,
                b.name AS bet_profile_name,
                b.mode AS bet_profile_mode,
                b.base_stake AS bet_profile_base_stake
            FROM notification_subscriptions s
            JOIN predictors p ON p.id = s.predictor_id
            JOIN notification_endpoints e ON e.id = s.endpoint_id
            LEFT JOIN notification_sender_accounts sa ON sa.id = s.sender_account_id
            LEFT JOIN bet_profiles b ON b.id = s.bet_profile_id
            WHERE s.user_id = ?
            ORDER BY s.created_at DESC, s.id DESC
            ''',
            (user_id,)
        )
        rows = cursor.fetchall()
        conn.close()
        return [self._prepare_notification_subscription(row) for row in rows]

    def list_active_notification_subscriptions_by_predictor(
        self,
        predictor_id: int,
        event_type: str = 'prediction_created'
    ) -> list[dict]:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT
                s.*,
                p.name AS predictor_name,
                p.lottery_type AS predictor_lottery_type,
                e.channel_type,
                e.endpoint_key,
                e.endpoint_label,
                e.status AS endpoint_status,
                e.config_json AS endpoint_config_json,
                sa.sender_name AS sender_account_name,
                sa.bot_name AS sender_bot_name,
                sa.bot_token AS sender_bot_token,
                sa.status AS sender_account_status,
                b.name AS bet_profile_name,
                b.lottery_type AS bet_profile_lottery_type,
                b.mode AS bet_profile_mode,
                b.base_stake AS bet_profile_base_stake,
                b.multiplier AS bet_profile_multiplier,
                b.max_steps AS bet_profile_max_steps,
                b.refund_action AS bet_profile_refund_action,
                b.cap_action AS bet_profile_cap_action,
                b.enabled AS bet_profile_enabled
            FROM notification_subscriptions s
            JOIN predictors p ON p.id = s.predictor_id
            JOIN notification_endpoints e ON e.id = s.endpoint_id
            LEFT JOIN notification_sender_accounts sa ON sa.id = s.sender_account_id
            LEFT JOIN bet_profiles b ON b.id = s.bet_profile_id
            WHERE s.predictor_id = ?
              AND s.event_type = ?
              AND s.enabled = 1
              AND e.status = 'active'
            ORDER BY s.created_at ASC, s.id ASC
            ''',
            (predictor_id, event_type)
        )
        rows = cursor.fetchall()
        conn.close()
        return [self._prepare_notification_subscription(row) for row in rows]

    def update_notification_subscription(self, subscription_id: int, user_id: int, fields: dict):
        if not fields:
            return
        updates = []
        values = []
        for key, value in fields.items():
            if key == 'filters':
                key = 'filter_json'
                value = json.dumps(value or {}, ensure_ascii=False)
            if key == 'enabled':
                value = 1 if value else 0
            updates.append(f'{key} = ?')
            values.append(value)
        updates.append('updated_at = CURRENT_TIMESTAMP')
        values.append(subscription_id)
        values.append(user_id)

        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            f'''
            UPDATE notification_subscriptions
            SET {', '.join(updates)}
            WHERE id = ? AND user_id = ?
            ''',
            values
        )
        conn.commit()
        conn.close()

    def delete_notification_subscription(self, subscription_id: int, user_id: int):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            DELETE FROM notification_rule_states
            WHERE subscription_id = ?
            ''',
            (subscription_id,)
        )
        cursor.execute(
            '''
            DELETE FROM notification_deliveries
            WHERE subscription_id = ? AND user_id = ?
            ''',
            (subscription_id, user_id)
        )
        cursor.execute(
            '''
            DELETE FROM notification_subscriptions
            WHERE id = ? AND user_id = ?
            ''',
            (subscription_id, user_id)
        )
        conn.commit()
        conn.close()

    # ============ Notification Rule States ============

    def get_notification_rule_state(self, subscription_id: int, rule_id: str) -> Optional[dict]:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT * FROM notification_rule_states
            WHERE subscription_id = ? AND rule_id = ?
            LIMIT 1
            ''',
            (subscription_id, str(rule_id or ''))
        )
        row = cursor.fetchone()
        conn.close()
        return self._prepare_notification_rule_state(row) if row else None

    def upsert_notification_rule_state(self, payload: dict) -> int:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            INSERT INTO notification_rule_states (
                subscription_id, rule_id, last_evaluated_issue, last_triggered_issue,
                last_triggered_at, last_status, last_payload_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(subscription_id, rule_id) DO UPDATE SET
                last_evaluated_issue = excluded.last_evaluated_issue,
                last_triggered_issue = excluded.last_triggered_issue,
                last_triggered_at = excluded.last_triggered_at,
                last_status = excluded.last_status,
                last_payload_json = excluded.last_payload_json,
                updated_at = CURRENT_TIMESTAMP
            ''',
            (
                int(payload['subscription_id']),
                str(payload['rule_id']),
                payload.get('last_evaluated_issue'),
                payload.get('last_triggered_issue'),
                payload.get('last_triggered_at'),
                payload.get('last_status'),
                json.dumps(payload.get('last_payload') or {}, ensure_ascii=False)
            )
        )
        conn.commit()
        state_id = cursor.lastrowid or 0
        if not state_id:
            cursor.execute(
                '''
                SELECT id FROM notification_rule_states
                WHERE subscription_id = ? AND rule_id = ?
                LIMIT 1
                ''',
                (int(payload['subscription_id']), str(payload['rule_id']))
            )
            row = cursor.fetchone()
            state_id = int(row['id']) if row and row['id'] is not None else 0
        conn.close()
        return int(state_id)

    # ============ Notification Deliveries ============

    def upsert_notification_delivery(self, payload: dict) -> int:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            INSERT INTO notification_deliveries (
                subscription_id, user_id, predictor_id, endpoint_id, event_type,
                record_key, status, payload_json, error_message, sent_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(subscription_id, event_type, record_key) DO UPDATE SET
                status = excluded.status,
                payload_json = excluded.payload_json,
                error_message = excluded.error_message,
                sent_at = excluded.sent_at,
                updated_at = CURRENT_TIMESTAMP
            ''',
            (
                payload['subscription_id'],
                payload['user_id'],
                payload['predictor_id'],
                payload['endpoint_id'],
                payload['event_type'],
                payload['record_key'],
                payload.get('status', 'pending'),
                json.dumps(payload.get('payload') or {}, ensure_ascii=False),
                payload.get('error_message'),
                payload.get('sent_at')
            )
        )
        conn.commit()
        delivery_id = cursor.lastrowid or 0
        if not delivery_id:
            cursor.execute(
                '''
                SELECT id FROM notification_deliveries
                WHERE subscription_id = ? AND event_type = ? AND record_key = ?
                LIMIT 1
                ''',
                (payload['subscription_id'], payload['event_type'], payload['record_key'])
            )
            row = cursor.fetchone()
            delivery_id = int(row['id']) if row and row['id'] is not None else 0
        conn.close()
        return int(delivery_id)

    def upsert_notification_delivery_job(self, payload: dict) -> int:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            INSERT INTO notification_delivery_jobs (
                delivery_id, subscription_id, user_id, predictor_id, endpoint_id,
                sender_mode, sender_account_id, channel_type, status, attempt_count,
                available_at, locked_at, last_error_message, last_response_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(delivery_id) DO UPDATE SET
                subscription_id = excluded.subscription_id,
                user_id = excluded.user_id,
                predictor_id = excluded.predictor_id,
                endpoint_id = excluded.endpoint_id,
                sender_mode = excluded.sender_mode,
                sender_account_id = excluded.sender_account_id,
                channel_type = excluded.channel_type,
                status = excluded.status,
                available_at = excluded.available_at,
                locked_at = excluded.locked_at,
                last_error_message = excluded.last_error_message,
                last_response_json = excluded.last_response_json,
                updated_at = CURRENT_TIMESTAMP
            ''',
            (
                payload['delivery_id'],
                payload['subscription_id'],
                payload['user_id'],
                payload['predictor_id'],
                payload['endpoint_id'],
                payload.get('sender_mode', 'platform'),
                payload.get('sender_account_id'),
                payload.get('channel_type', 'telegram'),
                payload.get('status', 'queued'),
                int(payload.get('attempt_count') or 0),
                payload.get('available_at'),
                payload.get('locked_at'),
                payload.get('last_error_message'),
                json.dumps(payload.get('last_response') or {}, ensure_ascii=False)
            )
        )
        conn.commit()
        job_id = cursor.lastrowid or 0
        if not job_id:
            cursor.execute(
                '''
                SELECT id FROM notification_delivery_jobs
                WHERE delivery_id = ?
                LIMIT 1
                ''',
                (payload['delivery_id'],)
            )
            row = cursor.fetchone()
            job_id = int(row['id']) if row and row['id'] is not None else 0
        conn.close()
        return int(job_id)

    def claim_notification_delivery_jobs(self, limit: int = 20, stale_after_seconds: int = 120) -> list[dict]:
        conn = self.get_connection()
        conn.isolation_level = None
        cursor = conn.cursor()
        now = datetime.utcnow()
        jobs: list[dict] = []
        try:
            cursor.execute('BEGIN IMMEDIATE')
            cursor.execute(
                '''
                SELECT id
                FROM notification_delivery_jobs
                WHERE (
                    status IN ('queued', 'retrying')
                    AND datetime(COALESCE(available_at, CURRENT_TIMESTAMP)) <= CURRENT_TIMESTAMP
                )
                OR (
                    status = 'processing'
                    AND locked_at IS NOT NULL
                    AND datetime(locked_at, '+' || ? || ' seconds') <= CURRENT_TIMESTAMP
                )
                ORDER BY datetime(COALESCE(available_at, created_at)) ASC, id ASC
                LIMIT ?
                ''',
                (int(stale_after_seconds), int(limit))
            )
            job_ids = [int(row['id']) for row in cursor.fetchall()]
            if job_ids:
                placeholders = ','.join('?' for _ in job_ids)
                cursor.execute(
                    f'''
                    UPDATE notification_delivery_jobs
                    SET status = 'processing',
                        locked_at = CURRENT_TIMESTAMP,
                        attempt_count = attempt_count + 1,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id IN ({placeholders})
                    ''',
                    job_ids
                )
                cursor.execute(
                    f'''
                    SELECT * FROM notification_delivery_jobs
                    WHERE id IN ({placeholders})
                    ORDER BY id ASC
                    ''',
                    job_ids
                )
                jobs = [self._prepare_notification_delivery_job(row) for row in cursor.fetchall()]
            cursor.execute('COMMIT')
        except Exception:
            try:
                cursor.execute('ROLLBACK')
            except Exception:
                pass
            jobs = []
        finally:
            conn.close()
        return jobs

    def get_notification_delivery_job(self, job_id: int) -> Optional[dict]:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT * FROM notification_delivery_jobs
            WHERE id = ?
            LIMIT 1
            ''',
            (job_id,)
        )
        row = cursor.fetchone()
        conn.close()
        return self._prepare_notification_delivery_job(row) if row else None

    def update_notification_delivery_job(self, job_id: int, fields: dict):
        if not fields:
            return
        updates = []
        values = []
        for key, value in fields.items():
            if key == 'last_response':
                key = 'last_response_json'
                value = json.dumps(value or {}, ensure_ascii=False)
            updates.append(f'{key} = ?')
            values.append(value)
        updates.append('updated_at = CURRENT_TIMESTAMP')
        values.append(job_id)
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            f'''
            UPDATE notification_delivery_jobs
            SET {', '.join(updates)}
            WHERE id = ?
            ''',
            values
        )
        conn.commit()
        conn.close()

    def get_notification_delivery(self, subscription_id: int, event_type: str, record_key: str) -> Optional[dict]:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT
                d.*,
                p.name AS predictor_name,
                e.channel_type,
                e.endpoint_label
            FROM notification_deliveries d
            JOIN predictors p ON p.id = d.predictor_id
            JOIN notification_endpoints e ON e.id = d.endpoint_id
            WHERE d.subscription_id = ? AND d.event_type = ? AND d.record_key = ?
            LIMIT 1
            ''',
            (subscription_id, event_type, record_key)
        )
        row = cursor.fetchone()
        conn.close()
        return self._prepare_notification_delivery(row) if row else None

    def get_notification_delivery_by_id(self, delivery_id: int, user_id: int | None = None) -> Optional[dict]:
        conn = self.get_connection()
        cursor = conn.cursor()
        query = '''
            SELECT
                d.*,
                p.name AS predictor_name,
                p.lottery_type AS predictor_lottery_type,
                e.channel_type,
                e.endpoint_key,
                e.endpoint_label,
                e.status AS endpoint_status,
                e.config_json AS endpoint_config_json,
                s.delivery_mode,
                s.filter_json,
                s.enabled AS subscription_enabled,
                s.sender_mode,
                s.sender_account_id,
                s.endpoint_id AS subscription_endpoint_id,
                s.predictor_id AS subscription_predictor_id,
                s.event_type AS subscription_event_type,
                sa.sender_name AS sender_account_name,
                sa.bot_name AS sender_bot_name,
                sa.bot_token AS sender_bot_token,
                sa.status AS sender_account_status,
                b.name AS bet_profile_name,
                b.mode AS bet_profile_mode,
                b.base_stake AS bet_profile_base_stake,
                b.multiplier AS bet_profile_multiplier,
                b.max_steps AS bet_profile_max_steps
            FROM notification_deliveries d
            JOIN predictors p ON p.id = d.predictor_id
            JOIN notification_endpoints e ON e.id = d.endpoint_id
            JOIN notification_subscriptions s ON s.id = d.subscription_id
            LEFT JOIN notification_sender_accounts sa ON sa.id = s.sender_account_id
            LEFT JOIN bet_profiles b ON b.id = s.bet_profile_id
            WHERE d.id = ?
        '''
        values: list[object] = [delivery_id]
        if user_id is not None:
            query += ' AND d.user_id = ?'
            values.append(user_id)
        query += ' LIMIT 1'
        cursor.execute(query, values)
        row = cursor.fetchone()
        conn.close()
        return self._prepare_notification_delivery(row) if row else None

    def list_notification_deliveries(self, user_id: int, limit: int = 50) -> list[dict]:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT
                d.*,
                p.name AS predictor_name,
                e.channel_type,
                e.endpoint_label
            FROM notification_deliveries d
            JOIN predictors p ON p.id = d.predictor_id
            JOIN notification_endpoints e ON e.id = d.endpoint_id
            WHERE d.user_id = ?
            ORDER BY d.created_at DESC, d.id DESC
            LIMIT ?
            ''',
            (user_id, limit)
        )
        rows = cursor.fetchall()
        conn.close()
        return [self._prepare_notification_delivery(row) for row in rows]

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
                COALESCE(prediction_stats.prediction_count, 0)
                    + COALESCE(run_stats.prediction_count, 0)
                    + CASE WHEN p.lottery_type = 'pc28' THEN COALESCE(archived_prediction_stats.prediction_count, 0) ELSE 0 END AS prediction_count,
                COALESCE(prediction_stats.failed_prediction_count, 0)
                    + COALESCE(run_stats.failed_prediction_count, 0)
                    + CASE WHEN p.lottery_type = 'pc28' THEN COALESCE(archived_prediction_stats.failed_prediction_count, 0) ELSE 0 END AS failed_prediction_count,
                CASE
                    WHEN p.lottery_type = 'pc28'
                        AND COALESCE(prediction_stats.latest_issue_no_num, 0) >= COALESCE(archived_prediction_stats.latest_issue_no_num, 0)
                        THEN prediction_stats.latest_issue_no
                    WHEN p.lottery_type = 'pc28'
                        AND COALESCE(archived_prediction_stats.latest_issue_no_num, 0) > 0
                        THEN CAST(archived_prediction_stats.latest_issue_no_num AS TEXT)
                    WHEN COALESCE(run_stats.latest_prediction_update, '') > COALESCE(prediction_stats.latest_prediction_update, '')
                        THEN run_stats.latest_issue_no
                    ELSE prediction_stats.latest_issue_no
                END AS latest_issue_no,
                CASE
                    WHEN p.lottery_type = 'pc28'
                        AND COALESCE(prediction_stats.latest_prediction_update, '') != ''
                        THEN prediction_stats.latest_prediction_update
                    WHEN p.lottery_type = 'pc28'
                        AND COALESCE(archived_prediction_stats.latest_prediction_date, '') != ''
                        THEN archived_prediction_stats.latest_prediction_date || ' 00:00:00'
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
                    MAX(CAST(issue_no AS INTEGER)) AS latest_issue_no_num,
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
            LEFT JOIN (
                SELECT
                    predictor_id,
                    SUM(total_predictions) AS prediction_count,
                    SUM(failed_predictions) AS failed_prediction_count,
                    MAX(CAST(COALESCE(NULLIF(latest_issue_no, ''), '0') AS INTEGER)) AS latest_issue_no_num,
                    MAX(summary_date) AS latest_prediction_date
                FROM pc28_prediction_daily_summary
                GROUP BY predictor_id
            ) AS archived_prediction_stats ON archived_prediction_stats.predictor_id = p.id
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
                (SELECT COUNT(*) FROM predictions)
                    + (SELECT COUNT(*) FROM prediction_runs)
                    + (SELECT COALESCE(SUM(total_predictions), 0) FROM pc28_prediction_daily_summary) AS total_predictions,
                (SELECT COUNT(*) FROM predictions WHERE status = 'pending') + (SELECT COUNT(*) FROM prediction_runs WHERE status = 'pending') AS pending_predictions,
                (SELECT COUNT(*) FROM predictions WHERE status = 'failed')
                    + (SELECT COUNT(*) FROM prediction_runs WHERE status = 'failed')
                    + (SELECT COALESCE(SUM(failed_predictions), 0) FROM pc28_prediction_daily_summary) AS failed_predictions,
                (SELECT COUNT(*) FROM predictions WHERE status = 'settled')
                    + (SELECT COUNT(*) FROM prediction_runs WHERE status = 'settled')
                    + (SELECT COALESCE(SUM(settled_predictions), 0) FROM pc28_prediction_daily_summary) AS settled_predictions,
                (SELECT COUNT(*) FROM lottery_draws WHERE lottery_type = 'pc28')
                    + (SELECT COALESCE(SUM(draw_count), 0) FROM pc28_draw_daily_summary) AS total_draws
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
        data['engine_type'] = normalize_engine_type(data.get('engine_type'))
        data['algorithm_key'] = normalize_algorithm_key(
            lottery_type,
            data['engine_type'],
            data.get('algorithm_key')
        )
        data['prediction_targets'] = normalize_prediction_targets(lottery_type, self._decode_json_list(data.get('prediction_targets')))
        data['enabled'] = bool(data.get('enabled'))
        data['api_mode'] = data.get('api_mode') or 'auto'
        data['primary_metric'] = normalize_primary_metric(lottery_type, data.get('primary_metric'))
        data['profit_default_metric'] = normalize_profit_metric(lottery_type, data.get('profit_default_metric') or data.get('primary_metric'))
        data['profit_rule_id'] = normalize_profit_rule(lottery_type, data.get('profit_rule_id') or 'pc28_high')
        data['share_level'] = data.get('share_level') or ('records' if data.get('share_predictions') else 'stats_only')
        data['share_predictions'] = bool(data.get('share_predictions'))
        data['data_injection_mode'] = data.get('data_injection_mode') or 'summary'
        data['user_algorithm_fallback_strategy'] = self._normalize_user_algorithm_fallback_strategy(
            data.get('user_algorithm_fallback_strategy')
        )
        data['engine_type_label'] = get_engine_type_label(data['engine_type'])
        data['algorithm_label'] = get_algorithm_label(lottery_type, data['engine_type'], data['algorithm_key'])
        if is_user_algorithm_key(data['algorithm_key']):
            user_algorithm_id = get_user_algorithm_id(data['algorithm_key'])
            user_algorithm = self.get_user_algorithm(user_algorithm_id) if user_algorithm_id else None
            data['user_algorithm_id'] = user_algorithm_id
            data['user_algorithm'] = user_algorithm
            if user_algorithm:
                data['algorithm_label'] = user_algorithm.get('name') or data['algorithm_label']
        else:
            data['user_algorithm_id'] = None
            data['user_algorithm'] = None
        data['execution_label'] = resolve_execution_label(data)
        if data['engine_type'] == 'machine' and not data['algorithm_key']:
            data['algorithm_key'] = get_default_machine_algorithm(lottery_type)
        if not include_secret:
            data.pop('api_key', None)
        return data

    def _prepare_user_algorithm(self, row) -> Optional[dict]:
        if row is None:
            return None

        data = dict(row)
        data['lottery_type'] = normalize_lottery_type(data.get('lottery_type'))
        data['definition'] = self._decode_json_object(data.get('definition_json'))
        data['key'] = f"user:{data['id']}"
        data['status'] = self._normalize_user_algorithm_status(data.get('status'))
        return data

    def _prepare_user_algorithm_version(self, row) -> Optional[dict]:
        if row is None:
            return None

        data = dict(row)
        data['definition'] = self._decode_json_object(data.get('definition_json'))
        data['validation'] = self._decode_json_object(data.get('validation_json'))
        data['backtest'] = self._decode_json_object(data.get('backtest_json'))
        return data

    def _prepare_user_algorithm_execution_log(self, row) -> Optional[dict]:
        if row is None:
            return None
        data = dict(row)
        data['fallback_used'] = bool(data.get('fallback_used'))
        data['debug'] = self._decode_json_object(data.get('debug_json'))
        data.pop('debug_json', None)
        return data

    def _normalize_user_algorithm_status(self, value) -> str:
        text = str(value or '').strip().lower()
        if text in {'draft', 'validated', 'disabled'}:
            return text
        return 'draft'

    def _normalize_user_algorithm_fallback_strategy(self, value) -> str:
        text = str(value or '').strip().lower()
        if text in {'fail', 'builtin_baseline', 'skip'}:
            return text
        return 'fail'

    def _default_predictor_runtime_state(self, predictor_id: int | None = None) -> dict:
        return {
            'predictor_id': predictor_id,
            'consecutive_ai_failures': 0,
            'auto_paused': False,
            'auto_paused_at': None,
            'auto_pause_reason': None,
            'last_ai_error_category': None,
            'last_ai_error_message': None,
            'last_ai_error_at': None,
            'last_counted_failure_key': None
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
            'last_ai_error_at': data.get('last_ai_error_at'),
            'last_counted_failure_key': data.get('last_counted_failure_key')
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

    def _prepare_bet_profile(self, row) -> Optional[dict]:
        if row is None:
            return None
        data = dict(row)
        data['lottery_type'] = normalize_lottery_type(data.get('lottery_type'))
        data['enabled'] = bool(data.get('enabled'))
        data['is_default'] = bool(data.get('is_default'))
        data['base_stake'] = round(float(data.get('base_stake') or 0), 2)
        data['multiplier'] = round(float(data.get('multiplier') or 0), 2)
        data['max_steps'] = int(data.get('max_steps') or 1)
        return data

    def _prepare_notification_sender_account(self, row) -> Optional[dict]:
        if row is None:
            return None
        data = dict(row)
        data['channel_type'] = str(data.get('channel_type') or 'telegram').strip().lower()
        data['status'] = str(data.get('status') or 'active').strip().lower()
        data['is_default'] = bool(data.get('is_default'))
        return data

    def _prepare_notification_endpoint(self, row) -> Optional[dict]:
        if row is None:
            return None
        data = dict(row)
        data['channel_type'] = str(data.get('channel_type') or 'telegram').strip().lower()
        config_value = data.get('config_json')
        if config_value is None and data.get('endpoint_config_json') is not None:
            config_value = data.get('endpoint_config_json')
        data['config'] = self._decode_json_object(config_value)
        data['status'] = str(data.get('status') or 'active').strip().lower()
        data['is_default'] = bool(data.get('is_default'))
        data.pop('config_json', None)
        data.pop('endpoint_config_json', None)
        return data

    def _prepare_notification_subscription(self, row) -> Optional[dict]:
        if row is None:
            return None
        data = dict(row)
        data['filter'] = self._decode_json_object(data.get('filter_json'))
        data['enabled'] = bool(data.get('enabled'))
        data['predictor_lottery_type'] = normalize_lottery_type(data.get('predictor_lottery_type'))
        data['endpoint_config'] = self._decode_json_object(data.get('endpoint_config_json'))
        data['endpoint_status'] = str(data.get('endpoint_status') or '').strip().lower() if data.get('endpoint_status') is not None else None
        data['bet_profile_enabled'] = bool(data.get('bet_profile_enabled')) if data.get('bet_profile_enabled') is not None else None
        data['sender_mode'] = str(data.get('sender_mode') or 'platform').strip().lower()
        data['sender_account_status'] = str(data.get('sender_account_status') or '').strip().lower() if data.get('sender_account_status') is not None else None
        data.pop('filter_json', None)
        data.pop('endpoint_config_json', None)
        return data

    def _prepare_notification_delivery(self, row) -> Optional[dict]:
        if row is None:
            return None
        data = dict(row)
        data['payload'] = self._decode_json_object(data.get('payload_json'))
        data['predictor_lottery_type'] = normalize_lottery_type(data.get('predictor_lottery_type'))
        data['endpoint_config'] = self._decode_json_object(data.get('endpoint_config_json'))
        data['filter'] = self._decode_json_object(data.get('filter_json'))
        data['subscription_enabled'] = bool(data.get('subscription_enabled')) if data.get('subscription_enabled') is not None else None
        data['endpoint_status'] = str(data.get('endpoint_status') or '').strip().lower() if data.get('endpoint_status') is not None else None
        data['sender_mode'] = str(data.get('sender_mode') or 'platform').strip().lower()
        data['sender_account_status'] = str(data.get('sender_account_status') or '').strip().lower() if data.get('sender_account_status') is not None else None
        data.pop('payload_json', None)
        data.pop('endpoint_config_json', None)
        data.pop('filter_json', None)
        return data

    def _prepare_notification_delivery_job(self, row) -> Optional[dict]:
        if row is None:
            return None
        data = dict(row)
        data['sender_mode'] = str(data.get('sender_mode') or 'platform').strip().lower()
        data['channel_type'] = str(data.get('channel_type') or 'telegram').strip().lower()
        data['attempt_count'] = int(data.get('attempt_count') or 0)
        data['last_response'] = self._decode_json_object(data.get('last_response_json'))
        data.pop('last_response_json', None)
        return data

    def _prepare_notification_rule_state(self, row) -> Optional[dict]:
        if row is None:
            return None
        data = dict(row)
        data['last_payload'] = self._decode_json_object(data.get('last_payload_json'))
        data.pop('last_payload_json', None)
        return data

    def _prepare_lottery_event(self, row) -> dict:
        data = dict(row)
        data['result_payload'] = self._decode_json_object(data.get('result_payload'))
        data['meta_payload'] = self._decode_json_object(data.get('meta_payload'))
        return data

    def _prepare_lottery_event_detail(self, row) -> dict:
        data = dict(row)
        data['payload'] = self._decode_json_value(data.get('payload'))
        return data

    def _prepare_jingcai_backfill_job(self, row) -> Optional[dict]:
        if row is None:
            return None
        data = dict(row)
        data['include_details'] = bool(data.get('include_details'))
        data['requested_days'] = int(data.get('requested_days') or 0)
        data['match_count'] = int(data.get('match_count') or 0)
        data['detail_count'] = int(data.get('detail_count') or 0)
        data['result'] = self._decode_json_object(data.get('result_json'))
        data.pop('result_json', None)
        return data

    def _prepare_draw(self, row) -> dict:
        return dict(row)

    def _prepare_prediction_run(self, row) -> Optional[dict]:
        if row is None:
            return None
        data = dict(row)
        data['requested_targets'] = self._decode_json_list(data.get('requested_targets'))
        data['algorithm_snapshot'] = self._decode_json_object(data.get('algorithm_snapshot_json'))
        data['execution_log'] = self._decode_json_object(data.get('execution_log_json'))
        data.pop('algorithm_snapshot_json', None)
        data.pop('execution_log_json', None)
        return data

    def _prepare_prediction_item(self, row) -> dict:
        data = dict(row)
        data['requested_targets'] = self._decode_json_list(data.get('requested_targets'))
        data['prediction_payload'] = self._decode_json_object(data.get('prediction_payload'))
        data['actual_payload'] = self._decode_json_object(data.get('actual_payload'))
        data['hit_payload'] = self._decode_json_object(data.get('hit_payload'))
        data['retry_count'] = int(data.get('retry_count') or 0)
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

    def _decode_json_value(self, value: Any):
        if value is None:
            return {}
        if isinstance(value, (dict, list)):
            return value
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, (dict, list)) else {}
        except (TypeError, json.JSONDecodeError):
            return {}

    def _build_lottery_detail_coverage(self, lottery_type: str, event_keys: list[str], source_provider: str) -> dict[str, set[str]]:
        coverage = {
            'recent_form': set(),
            'injury': set(),
            'euro_odds_snapshot': set()
        }
        if not event_keys:
            return coverage
        conn = self.get_connection()
        cursor = conn.cursor()
        unique_event_keys = list(dict.fromkeys(event_keys))
        for start in range(0, len(unique_event_keys), 500):
            batch = unique_event_keys[start:start + 500]
            placeholders = ','.join('?' for _ in batch)
            cursor.execute(
                f'''
                SELECT event_key, detail_type
                FROM lottery_event_details
                WHERE lottery_type = ?
                  AND source_provider = ?
                  AND event_key IN ({placeholders})
                ''',
                [normalize_lottery_type(lottery_type), source_provider, *batch]
            )
            for row in cursor.fetchall():
                detail_type = row['detail_type']
                event_key = row['event_key']
                if detail_type in {'recent_form_team1', 'recent_form_team2'}:
                    coverage['recent_form'].add(event_key)
                if detail_type == 'injury':
                    coverage['injury'].add(event_key)
                if detail_type in {'odds_euro', 'odds_snapshots'}:
                    coverage['euro_odds_snapshot'].add(event_key)
        conn.close()
        return coverage

    def _has_complete_football_odds(self, odds_map: dict) -> bool:
        if not isinstance(odds_map, dict):
            return False
        for outcome in ('胜', '平', '负'):
            try:
                if float(odds_map.get(outcome)) <= 0:
                    return False
            except (TypeError, ValueError):
                return False
        return True

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

    def _extract_metric_outcomes(self, rows: list[dict], metric_key: str) -> list[int]:
        outcomes = [self._extract_metric_hit(row, metric_key) for row in rows]
        return [int(item) for item in outcomes if item is not None]

    def _empty_sequence_summary(self) -> dict:
        return {
            'sample_count': 0,
            'hit_count': 0,
            'first_outcome': None,
            'first_streak_len': 0,
            'last_outcome': None,
            'last_streak_len': 0,
            'max_hit_streak': 0,
            'max_miss_streak': 0
        }

    def _sequence_summary_from_outcome(self, outcome: int) -> dict:
        normalized = 1 if int(outcome) else 0
        return {
            'sample_count': 1,
            'hit_count': normalized,
            'first_outcome': normalized,
            'first_streak_len': 1,
            'last_outcome': normalized,
            'last_streak_len': 1,
            'max_hit_streak': 1 if normalized == 1 else 0,
            'max_miss_streak': 1 if normalized == 0 else 0
        }

    def _build_sequence_summary_from_outcomes(self, outcomes: list[int]) -> dict:
        summary = self._empty_sequence_summary()
        for outcome in outcomes:
            summary = self._merge_sequence_summaries(summary, self._sequence_summary_from_outcome(int(outcome)))
        return summary

    def _merge_sequence_summaries(self, newer: Optional[dict], older: Optional[dict]) -> dict:
        newer_summary = self._normalize_sequence_summary(newer)
        older_summary = self._normalize_sequence_summary(older)
        if newer_summary['sample_count'] <= 0:
            return older_summary
        if older_summary['sample_count'] <= 0:
            return newer_summary

        merged = {
            'sample_count': newer_summary['sample_count'] + older_summary['sample_count'],
            'hit_count': newer_summary['hit_count'] + older_summary['hit_count'],
            'first_outcome': newer_summary['first_outcome'],
            'first_streak_len': newer_summary['first_streak_len'],
            'last_outcome': older_summary['last_outcome'],
            'last_streak_len': older_summary['last_streak_len'],
            'max_hit_streak': max(newer_summary['max_hit_streak'], older_summary['max_hit_streak']),
            'max_miss_streak': max(newer_summary['max_miss_streak'], older_summary['max_miss_streak'])
        }

        if (
            newer_summary['first_streak_len'] == newer_summary['sample_count']
            and newer_summary['first_outcome'] == older_summary['first_outcome']
        ):
            merged['first_streak_len'] = newer_summary['sample_count'] + older_summary['first_streak_len']

        if (
            older_summary['last_streak_len'] == older_summary['sample_count']
            and older_summary['last_outcome'] == newer_summary['last_outcome']
        ):
            merged['last_streak_len'] = older_summary['sample_count'] + newer_summary['last_streak_len']

        if newer_summary['last_outcome'] == older_summary['first_outcome'] == 1:
            merged['max_hit_streak'] = max(
                merged['max_hit_streak'],
                newer_summary['last_streak_len'] + older_summary['first_streak_len']
            )
        if newer_summary['last_outcome'] == older_summary['first_outcome'] == 0:
            merged['max_miss_streak'] = max(
                merged['max_miss_streak'],
                newer_summary['last_streak_len'] + older_summary['first_streak_len']
            )

        return merged

    def _normalize_sequence_summary(self, value: Optional[dict]) -> dict:
        base = self._empty_sequence_summary()
        if not isinstance(value, dict):
            return base
        normalized = dict(base)
        normalized['sample_count'] = max(0, int(value.get('sample_count') or 0))
        normalized['hit_count'] = max(0, int(value.get('hit_count') or 0))
        normalized['first_streak_len'] = max(0, int(value.get('first_streak_len') or 0))
        normalized['last_streak_len'] = max(0, int(value.get('last_streak_len') or 0))
        normalized['max_hit_streak'] = max(0, int(value.get('max_hit_streak') or 0))
        normalized['max_miss_streak'] = max(0, int(value.get('max_miss_streak') or 0))
        first_outcome = value.get('first_outcome')
        last_outcome = value.get('last_outcome')
        normalized['first_outcome'] = int(first_outcome) if first_outcome in {0, 1, '0', '1'} else None
        normalized['last_outcome'] = int(last_outcome) if last_outcome in {0, 1, '0', '1'} else None
        if normalized['sample_count'] <= 0:
            return base
        return normalized

    def _decode_metric_segments(self, value: Any) -> dict[str, dict]:
        payload = self._decode_json_object(value)
        decoded = {}
        for metric_key, sequence_summary in payload.items():
            decoded[str(metric_key)] = self._normalize_sequence_summary(sequence_summary)
        return decoded

    def _build_metric_stats_from_sequence(self, summary: Optional[dict]) -> dict:
        sequence_summary = self._normalize_sequence_summary(summary)
        sample_count = sequence_summary['sample_count']
        hit_count = sequence_summary['hit_count']
        hit_rate = round(hit_count / sample_count * 100, 2) if sample_count else None
        return {
            'hit_count': hit_count,
            'sample_count': sample_count,
            'hit_rate': hit_rate,
            'ratio_text': f'{hit_count}/{sample_count}' if sample_count else '--'
        }

    def _build_streak_stats_from_sequence(self, summary: Optional[dict], recent_outcomes: list[int]) -> dict:
        sequence_summary = self._normalize_sequence_summary(summary)
        recent_100 = recent_outcomes[:100]
        current_hit_streak = sequence_summary['first_streak_len'] if sequence_summary['first_outcome'] == 1 else 0
        current_miss_streak = sequence_summary['first_streak_len'] if sequence_summary['first_outcome'] == 0 else 0
        return {
            'current_hit_streak': current_hit_streak,
            'current_miss_streak': current_miss_streak,
            'recent_100_max_hit_streak': self._max_streak(recent_100, 1),
            'recent_100_max_miss_streak': self._max_streak(recent_100, 0),
            'historical_max_hit_streak': sequence_summary['max_hit_streak'],
            'historical_max_miss_streak': sequence_summary['max_miss_streak']
        }

    def _build_metric_stats(self, rows: list[dict], metric_key: str) -> dict:
        return self._build_metric_stats_from_sequence(
            self._build_sequence_summary_from_outcomes(self._extract_metric_outcomes(rows, metric_key))
        )

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
        outcomes = self._extract_metric_outcomes(rows, metric_key)
        return self._build_streak_stats_from_sequence(
            self._build_sequence_summary_from_outcomes(outcomes),
            outcomes
        )

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

    def _max_issue_no(self, left: Any, right: Any) -> Optional[str]:
        left_value = self._parse_issue_no(left)
        right_value = self._parse_issue_no(right)
        if left_value is None:
            return str(right) if right_value is not None else None
        if right_value is None:
            return str(left) if left_value is not None else None
        return str(left) if left_value >= right_value else str(right)

    def _parse_issue_no(self, value: Any) -> Optional[int]:
        if value is None:
            return None
        try:
            return int(str(value).strip())
        except (TypeError, ValueError):
            return None

    def _get_beijing_cutoff_date(self, retention_days: int) -> str:
        retention_days = max(1, int(retention_days or 1))
        beijing_now = datetime.utcnow() + timedelta(hours=8)
        cutoff_date = (beijing_now - timedelta(days=retention_days)).date()
        return cutoff_date.isoformat()

    def _parse_timestamp(self, value: Any) -> Optional[datetime]:
        if not value:
            return None
        try:
            return datetime.strptime(str(value), '%Y-%m-%d %H:%M:%S')
        except ValueError:
            return None
