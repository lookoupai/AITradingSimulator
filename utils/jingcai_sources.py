"""
竞彩足球数据源端点与请求参数
"""
from __future__ import annotations


SINA_JINGCAI_URL = 'https://alpha.lottery.sina.com.cn/gateway/index/entry'
SINA_JINGCAI_DETAIL_URL = 'https://mix.lottery.sina.com.cn/gateway/index/entry'
SPORTTERY_GATEWAY_URL = 'https://webapi.sporttery.cn/gateway'

SINA_LIST_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1',
    'Referer': 'https://alpha.lottery.sina.com.cn/lottery/jczq/'
}

SINA_DETAIL_HEADERS = {
    'User-Agent': SINA_LIST_HEADERS['User-Agent'],
    'Referer': 'https://mix.lottery.sina.com.cn/'
}

SPORTTERY_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1',
    'Referer': 'https://www.sporttery.cn/',
    'Accept': 'application/json, text/plain, */*'
}

_SINA_BASE_PARAMS = {
    'format': 'json',
    '__caller__': 'wap',
    '__version__': '1.0.0',
    '__verno__': '10000'
}


def build_sina_match_list_params(
    date: str = '',
    is_prized: str = '',
    game_types: str = 'spf',
    cache_bust: int | str | None = None
) -> dict:
    params = {
        **_SINA_BASE_PARAMS,
        'cat1': 'jczqMatches',
        'gameTypes': game_types,
        'date': date,
        'isPrized': is_prized,
        'isAll': 1,
        'dpc': 1
    }
    if cache_bust not in {None, ''}:
        params['t'] = cache_bust
    return params


def build_sina_detail_params(cat1: str, extra_params: dict | None = None) -> dict:
    return {
        **_SINA_BASE_PARAMS,
        'cat1': cat1,
        'dpc': 1,
        **(extra_params or {})
    }
