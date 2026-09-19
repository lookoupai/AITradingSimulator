"""
外部预测源插件基类与共享工具。
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone

import requests

# 大小/单双/组合的目标值白名单（与本地开奖属性口径一致：derive_pc28_attributes/build_combo）
SUPPORTED_TARGET_KEYS = ('big_small', 'odd_even', 'combo')
EXTERNAL_TARGET_VALUES = {
    'big_small': ('大', '小'),
    'odd_even': ('单', '双'),
    'combo': ('大单', '大双', '小单', '小双')
}


class ExternalSourceError(Exception):
    """外部来源异常基类。"""


class ExternalSourceRequestError(ExternalSourceError):
    """网络请求失败、超时或响应超限。"""


class ExternalSourceParseError(ExternalSourceError):
    """响应无法解析或缺少必要字段。"""


@dataclass
class ExternalModelSnapshot:
    model_key: str
    display_name: str
    targets: dict = field(default_factory=dict)  # {'big_small': '大', ...}
    confidence: float | None = None
    raw: dict = field(default_factory=dict)

    @property
    def supported_targets(self) -> list[str]:
        return [key for key in SUPPORTED_TARGET_KEYS if self.targets.get(key)]


@dataclass
class ExternalSnapshot:
    target_issue_no: str
    upstream_published_at: str | None
    models: list[ExternalModelSnapshot]
    invalid_model_count: int
    raw: bytes = b''


def _describe_error_response(response) -> str:
    """从错误响应中提取上游错误信息（如 {"error":{"code","message"}}），不泄露请求凭据。"""
    try:
        body = json.loads(response.text or '')
    except (AttributeError, json.JSONDecodeError, ValueError):
        return f'HTTP {response.status_code}'
    if isinstance(body, dict):
        error = body.get('error')
        if isinstance(error, dict):
            code = str(error.get('code') or '').strip()
            message = str(error.get('message') or '').strip()
            if code or message:
                return f'HTTP {response.status_code} {code}: {message}'.strip()
    return f'HTTP {response.status_code}'


def parse_iso_timestamp(value) -> datetime | None:
    """解析带时区的 ISO 时间字符串；解析失败返回 None，不伪造时间。"""
    text = str(value or '').strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace('Z', '+00:00'))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        # 无时区的时间按 UTC 处理（数据库口径），避免混用本地时区
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def fetch_json(url: str, timeout: float, max_bytes: int, headers: dict | None = None) -> dict:
    """带超时与响应体上限的 GET，返回解析后的 JSON 对象。"""
    request_headers = {'Accept': 'application/json', 'User-Agent': 'AITradingSimulator/1.0'}
    if headers:
        request_headers.update(headers)
    try:
        response = requests.get(
            url,
            timeout=(min(10.0, timeout), timeout),
            headers=request_headers,
            stream=True
        )
    except requests.RequestException as exc:
        raise ExternalSourceRequestError(f'请求失败：{exc}') from exc
    try:
        with response:
            if response.status_code != 200:
                raise ExternalSourceRequestError(_describe_error_response(response))
            content_type = str(response.headers.get('Content-Type') or '')
            if 'json' not in content_type.lower() and 'text' not in content_type.lower():
                raise ExternalSourceParseError(f'响应类型异常：{content_type or "未知"}')
            chunks = []
            total = 0
            for chunk in response.iter_content(chunk_size=65536):
                total += len(chunk)
                if total > max_bytes:
                    raise ExternalSourceRequestError(f'响应体超过上限 {max_bytes} 字节')
                chunks.append(chunk)
            payload_bytes = b''.join(chunks)
    except ExternalSourceError:
        raise
    except requests.RequestException as exc:
        raise ExternalSourceRequestError(f'读取响应失败：{exc}') from exc
    try:
        data = json.loads(payload_bytes.decode('utf-8'))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ExternalSourceParseError(f'响应不是合法 JSON：{exc}') from exc
    if not isinstance(data, dict):
        raise ExternalSourceParseError('响应必须是 JSON 对象')
    return data
