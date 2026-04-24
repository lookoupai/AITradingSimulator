from __future__ import annotations

import importlib
import os
import sys
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass


MODULE_PREFIXES = (
    'app',
    'config',
    'database',
    'services',
    'utils',
    'lotteries',
    'ai_trader'
)


def _purge_repo_modules():
    for name in list(sys.modules):
        if any(name == prefix or name.startswith(f'{prefix}.') for prefix in MODULE_PREFIXES):
            sys.modules.pop(name, None)


@dataclass
class AppHarness:
    module: object
    tempdir: tempfile.TemporaryDirectory

    @property
    def db(self):
        return self.module.db

    @property
    def app(self):
        return self.module.app

    def make_client(self, username: str = 'tester', is_admin: bool = False):
        user_id = self.db.create_user(
            username=username,
            password_hash=self.module.hash_password('password'),
            is_admin=is_admin
        )
        client = self.app.test_client()
        with client.session_transaction() as session:
            session['user_id'] = user_id
            session['username'] = username
            session['is_admin'] = 1 if is_admin else 0
        return client, user_id

    def close(self):
        self.tempdir.cleanup()


@contextmanager
def fresh_app_harness():
    tempdir = tempfile.TemporaryDirectory()
    db_path = os.path.join(tempdir.name, 'test.db')
    old_env = {
        'AUTO_PREDICTION': os.environ.get('AUTO_PREDICTION'),
        'DATABASE_PATH': os.environ.get('DATABASE_PATH'),
        'NOTIFICATION_WORKER_ENABLED': os.environ.get('NOTIFICATION_WORKER_ENABLED')
    }
    os.environ['AUTO_PREDICTION'] = 'false'
    os.environ['DATABASE_PATH'] = db_path
    os.environ['NOTIFICATION_WORKER_ENABLED'] = 'false'

    try:
        _purge_repo_modules()
        module = importlib.import_module('app')
        yield AppHarness(module=module, tempdir=tempdir)
    finally:
        _purge_repo_modules()
        for key, value in old_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        tempdir.cleanup()


def create_predictor(
    harness: AppHarness,
    user_id: int,
    lottery_type: str,
    **overrides
) -> int:
    defaults = {
        'name': f'{lottery_type}-predictor',
        'api_key': 'test-key',
        'api_url': 'https://example.com/v1',
        'model_name': 'test-model',
        'api_mode': 'auto',
        'primary_metric': 'big_small' if lottery_type == 'pc28' else 'spf',
        'profit_default_metric': 'big_small' if lottery_type == 'pc28' else 'spf',
        'profit_rule_id': 'pc28_netdisk' if lottery_type == 'pc28' else 'jingcai_snapshot',
        'share_level': 'records',
        'prediction_method': 'test',
        'system_prompt': 'test',
        'data_injection_mode': 'summary',
        'prediction_targets': ['number', 'big_small', 'odd_even', 'combo'] if lottery_type == 'pc28' else ['spf', 'rqspf'],
        'history_window': 20,
        'temperature': 0.3,
        'enabled': True,
        'lottery_type': lottery_type,
        'engine_type': 'ai',
        'algorithm_key': ''
    }
    defaults.update(overrides)
    return harness.db.create_predictor(user_id=user_id, **defaults)
