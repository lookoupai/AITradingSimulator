"""
PC28 预测平台统一配置
"""
import os

from dotenv import load_dotenv

load_dotenv()

# ============ 服务器配置 ============
HOST = os.getenv('HOST', '0.0.0.0')
PORT = int(os.getenv('PORT', 35008))
DEBUG = os.getenv('DEBUG', 'False').lower() == 'true'

# ============ 数据库配置 ============
DATABASE_PATH = os.getenv('DATABASE_PATH', 'pc28_predictor.db')

# ============ PC28 数据配置 ============
PC28_API_BASE_URL = os.getenv('PC28_API_BASE_URL', 'https://www.pc28.ai')
PC28_REQUEST_TIMEOUT = int(os.getenv('PC28_REQUEST_TIMEOUT', 10))
PC28_SYNC_HISTORY = int(os.getenv('PC28_SYNC_HISTORY', 120))

# ============ 预测任务配置 ============
AUTO_PREDICTION = os.getenv(
    'AUTO_PREDICTION',
    os.getenv('AUTO_TRADING', 'True')
).lower() == 'true'
PREDICTION_POLL_INTERVAL = int(
    os.getenv(
        'PREDICTION_POLL_INTERVAL',
        os.getenv('TRADING_INTERVAL', 20)
    )
)
DEFAULT_HISTORY_WINDOW = int(os.getenv('DEFAULT_HISTORY_WINDOW', 60))
DEFAULT_PREDICTION_TEMPERATURE = float(os.getenv('DEFAULT_PREDICTION_TEMPERATURE', 0.7))

# ============ 竞彩足球调度配置 ============
JINGCAI_IDLE_POLL_INTERVAL_DAY = int(os.getenv('JINGCAI_IDLE_POLL_INTERVAL_DAY', 3600))
JINGCAI_IDLE_POLL_INTERVAL_NIGHT = int(os.getenv('JINGCAI_IDLE_POLL_INTERVAL_NIGHT', 1800))
JINGCAI_SETTLEMENT_POLL_INTERVAL = int(os.getenv('JINGCAI_SETTLEMENT_POLL_INTERVAL', 600))

# ============ 竞彩足球调度配置 ============
JINGCAI_REQUEST_TIMEOUT = int(os.getenv('JINGCAI_REQUEST_TIMEOUT', 15))
JINGCAI_IDLE_INTERVAL = int(os.getenv('JINGCAI_IDLE_INTERVAL', 1800))
JINGCAI_DAYTIME_IDLE_INTERVAL = int(os.getenv('JINGCAI_DAYTIME_IDLE_INTERVAL', 3600))
JINGCAI_DAYTIME_IDLE_START_HOUR = int(os.getenv('JINGCAI_DAYTIME_IDLE_START_HOUR', 9))
JINGCAI_DAYTIME_IDLE_END_HOUR = int(os.getenv('JINGCAI_DAYTIME_IDLE_END_HOUR', 17))
JINGCAI_DISCOVERY_INTERVAL = int(os.getenv('JINGCAI_DISCOVERY_INTERVAL', 1800))
JINGCAI_PREMATCH_LOOKAHEAD_HOURS = int(os.getenv('JINGCAI_PREMATCH_LOOKAHEAD_HOURS', 6))
JINGCAI_PREMATCH_INTERVAL = int(os.getenv('JINGCAI_PREMATCH_INTERVAL', 600))
JINGCAI_NEAR_MATCH_LOOKAHEAD_HOURS = int(os.getenv('JINGCAI_NEAR_MATCH_LOOKAHEAD_HOURS', 2))
JINGCAI_NEAR_MATCH_INTERVAL = int(os.getenv('JINGCAI_NEAR_MATCH_INTERVAL', 120))
JINGCAI_SETTLEMENT_INTERVAL = int(os.getenv('JINGCAI_SETTLEMENT_INTERVAL', 300))
JINGCAI_DETAIL_CACHE_SECONDS = int(os.getenv('JINGCAI_DETAIL_CACHE_SECONDS', 21600))

# ============ 竞彩足球调度配置 ============
JINGCAI_IDLE_POLL_INTERVAL = int(os.getenv('JINGCAI_IDLE_POLL_INTERVAL', 3600))
JINGCAI_ACTIVE_POLL_INTERVAL = int(os.getenv('JINGCAI_ACTIVE_POLL_INTERVAL', 600))
JINGCAI_PREMATCH_POLL_INTERVAL = int(os.getenv('JINGCAI_PREMATCH_POLL_INTERVAL', 120))
JINGCAI_SETTLEMENT_POLL_INTERVAL = int(os.getenv('JINGCAI_SETTLEMENT_POLL_INTERVAL', 300))
JINGCAI_PREMATCH_WINDOW_MINUTES = int(os.getenv('JINGCAI_PREMATCH_WINDOW_MINUTES', 120))

# 兼容旧变量名
AUTO_TRADING = AUTO_PREDICTION
TRADING_INTERVAL = PREDICTION_POLL_INTERVAL

# ============ Linux DO OAuth配置 ============
LINUXDO_CLIENT_ID = os.getenv('LINUXDO_CLIENT_ID', '你的Client ID')
LINUXDO_CLIENT_SECRET = os.getenv('LINUXDO_CLIENT_SECRET', '你的Client Secret')
LINUXDO_REDIRECT_URI = os.getenv('LINUXDO_REDIRECT_URI', 'https://trade.easy2ai.com/api/auth/callback')
LINUXDO_AUTHORIZE_URL = 'https://connect.linux.do/oauth2/authorize'
LINUXDO_TOKEN_URL = 'https://connect.linux.do/oauth2/token'
LINUXDO_USERINFO_URL = 'https://connect.linux.do/api/user'
LINUXDO_MIN_TRUST_LEVEL = int(os.getenv('LINUXDO_MIN_TRUST_LEVEL', 1))

# ============ 日志配置 ============
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
LOG_FILE = 'pc28_predictor.log'
