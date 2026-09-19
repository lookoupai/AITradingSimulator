"""
外部预测源插件注册表。
"""
from __future__ import annotations

from services.external_sources.base import ExternalSourceError
from services.external_sources.jnd28 import JND28_PLUGIN

PLUGIN_REGISTRY = {
    JND28_PLUGIN.plugin_key: JND28_PLUGIN
}


def get_plugin(plugin_key: str):
    """按 key 取插件；未知 key 抛 ExternalSourceError。"""
    plugin = PLUGIN_REGISTRY.get(str(plugin_key or '').strip())
    if plugin is None:
        raise ExternalSourceError(f'未知的接入插件：{plugin_key}')
    return plugin


def list_plugins() -> list[dict]:
    return [
        {'plugin_key': plugin.plugin_key, 'display_name': plugin.display_name}
        for plugin in PLUGIN_REGISTRY.values()
    ]
