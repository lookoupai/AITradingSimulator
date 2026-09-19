"""
外部预测源接入插件包。

插件负责"请求与解析"：把来源的原始响应解析为标准化快照
（目标期号 + 模型列表 + 大小/单双/组合目标值），不负责存储与调度。
"""
from services.external_sources.base import (
    ExternalModelSnapshot,
    ExternalSnapshot,
    ExternalSourceError,
    ExternalSourceParseError,
    ExternalSourceRequestError,
    parse_iso_timestamp,
)
from services.external_sources.registry import (
    PLUGIN_REGISTRY,
    get_plugin,
    list_plugins,
)

__all__ = [
    'ExternalModelSnapshot',
    'ExternalSnapshot',
    'ExternalSourceError',
    'ExternalSourceParseError',
    'ExternalSourceRequestError',
    'parse_iso_timestamp',
    'PLUGIN_REGISTRY',
    'get_plugin',
    'list_plugins',
]
