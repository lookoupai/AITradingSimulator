"""
统一日志管理模块
"""
from __future__ import annotations

import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

import config

BASE_LOGGER_NAME = 'ai_trading_simulator'


def setup_logger(name: str = BASE_LOGGER_NAME) -> logging.Logger:
    """
    设置日志记录器
    
    Args:
        name: 日志记录器名称
        
    Returns:
        配置好的Logger实例
    """
    logger = logging.getLogger(name)

    # 避免重复添加handler
    if logger.handlers:
        return logger

    logger.setLevel(getattr(logging, config.LOG_LEVEL))
    logger.propagate = False

    # 控制台输出
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(console_formatter)

    # 文件输出
    log_file = Path(config.LOG_FILE)
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=int(getattr(config, 'LOG_MAX_BYTES', 20 * 1024 * 1024)),
        backupCount=int(getattr(config, 'LOG_BACKUP_COUNT', 7)),
        encoding='utf-8'
    )
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(config.LOG_FORMAT)
    file_handler.setFormatter(file_formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger


def get_logger(name: str | None = None) -> logging.Logger:
    if not name:
        return setup_logger(BASE_LOGGER_NAME)
    return setup_logger(f'{BASE_LOGGER_NAME}.{name}')


# 创建全局logger实例
logger = setup_logger()
