"""
时区工具模块 - 数据库存UTC，展示用东八区（UTC+8）
"""
from __future__ import annotations

import calendar
from datetime import datetime, timedelta, timezone

# 东八区偏移量
TIMEZONE_OFFSET = timedelta(hours=8)
BEIJING_FIXED_TZ = timezone(TIMEZONE_OFFSET)

def get_current_utc_time() -> datetime:
    """
    获取当前UTC时间（用于数据库存储）

    Returns:
        datetime: UTC时间
    """
    return datetime.utcnow()

def get_current_utc_time_str(format: str = '%Y-%m-%d %H:%M:%S') -> str:
    """
    获取当前UTC时间字符串（用于数据库存储）

    Args:
        format: 时间格式，默认 '%Y-%m-%d %H:%M:%S'

    Returns:
        str: 格式化的UTC时间字符串
    """
    return get_current_utc_time().strftime(format)

def get_current_beijing_time() -> datetime:
    """
    获取当前东八区时间（用于日志显示）

    Returns:
        datetime: 东八区当前时间
    """
    return datetime.utcnow() + TIMEZONE_OFFSET


def parse_beijing_time(value: str, format: str = '%Y-%m-%d %H:%M:%S') -> datetime | None:
    """
    解析东八区时间字符串

    Args:
        value: 时间字符串
        format: 输入格式

    Returns:
        datetime | None: 解析后的东八区时间（naive）
    """
    text = str(value or '').strip()
    if not text:
        return None

    try:
        if 'T' in text:
            parsed = datetime.fromisoformat(text.replace('Z', '+00:00'))
            if parsed.tzinfo:
                return parsed.astimezone(BEIJING_FIXED_TZ).replace(tzinfo=None)
            return parsed
        return datetime.strptime(text, format)
    except Exception:
        return None


def format_beijing_time(value: datetime, format: str = '%Y-%m-%d %H:%M:%S') -> str:
    """格式化东八区时间（naive）"""
    return value.strftime(format)


def is_vancouver_dst(beijing_time: datetime) -> bool:
    """
    判断给定东八区时间对应的温哥华是否处于夏令时
    """
    dst_start, dst_end = _get_vancouver_dst_window_in_beijing(beijing_time.year)
    return dst_start <= beijing_time < dst_end


def get_pc28_day_window(reference_time: datetime | None = None) -> dict:
    """
    获取当前 PC28 盘日窗口（东八区）

    Returns:
        dict: {
            'start': datetime,
            'end_exclusive': datetime,
            'is_dst': bool,
            'boundary_hour': int
        }
    """
    current_time = reference_time or get_current_beijing_time()
    is_dst = is_vancouver_dst(current_time)
    boundary_hour = 20 if is_dst else 21
    start_time = current_time.replace(hour=boundary_hour, minute=0, second=0, microsecond=0)

    if current_time < start_time:
        start_time -= timedelta(days=1)

    return {
        'start': start_time,
        'end_exclusive': start_time + timedelta(days=1),
        'is_dst': is_dst,
        'boundary_hour': boundary_hour
    }


def _get_vancouver_dst_window_in_beijing(year: int) -> tuple[datetime, datetime]:
    """
    温哥华夏令时边界换算到北京时间

    温哥华：
    - 夏令时开始：3月第二个周日 02:00 PST
    - 夏令时结束：11月第一个周日 02:00 PDT
    换算到北京时间：
    - 开始：同日 18:00
    - 结束：同日 17:00
    """
    march_second_sunday = _nth_weekday_of_month(year, 3, calendar.SUNDAY, 2)
    november_first_sunday = _nth_weekday_of_month(year, 11, calendar.SUNDAY, 1)
    return (
        datetime(year, 3, march_second_sunday, 18, 0, 0),
        datetime(year, 11, november_first_sunday, 17, 0, 0)
    )


def _nth_weekday_of_month(year: int, month: int, weekday: int, occurrence: int) -> int:
    count = 0
    for day in range(1, calendar.monthrange(year, month)[1] + 1):
        if datetime(year, month, day).weekday() == weekday:
            count += 1
            if count == occurrence:
                return day
    raise ValueError('无法计算指定月份中的星期序号')

def get_current_beijing_time_str(format: str = '%Y-%m-%d %H:%M:%S') -> str:
    """
    获取当前东八区时间字符串（用于日志显示）

    Args:
        format: 时间格式，默认 '%Y-%m-%d %H:%M:%S'

    Returns:
        str: 格式化的东八区时间字符串
    """
    return get_current_beijing_time().strftime(format)

def utc_to_beijing(utc_time_str: str, format: str = '%Y-%m-%d %H:%M:%S', iso_format: bool = True) -> str:
    """
    将UTC时间字符串转换为东八区时间字符串（用于API返回）

    Args:
        utc_time_str: UTC时间字符串
        format: 输入时间格式，默认 '%Y-%m-%d %H:%M:%S'
        iso_format: 是否返回ISO 8601格式（带时区标记），默认True

    Returns:
        str: 东八区时间字符串（ISO 8601格式：2025-10-21T18:20:20+08:00）
    """
    try:
        utc_time = datetime.strptime(utc_time_str, format)
        beijing_time = utc_time + TIMEZONE_OFFSET

        if iso_format:
            # 返回ISO 8601格式，明确标记东八区时区
            return beijing_time.strftime('%Y-%m-%dT%H:%M:%S') + '+08:00'
        else:
            # 返回普通格式
            return beijing_time.strftime(format)
    except:
        return utc_time_str  # 解析失败返回原值

def beijing_to_utc(beijing_time: datetime) -> datetime:
    """
    将东八区时间转换为UTC时间

    Args:
        beijing_time: 东八区时间

    Returns:
        datetime: UTC时间
    """
    return beijing_time - TIMEZONE_OFFSET
