#!/usr/bin/env python3
"""
竞彩足球数据源探测脚本
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
import sys
from typing import Any

import requests


PROJECT_ROOT = Path(__file__).resolve().parents[4]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import config
from utils.jingcai_sources import (
    SINA_DETAIL_HEADERS,
    SINA_JINGCAI_DETAIL_URL,
    SINA_JINGCAI_URL,
    SINA_LIST_HEADERS,
    SPORTTERY_GATEWAY_URL,
    SPORTTERY_HEADERS,
    build_sina_detail_params,
    build_sina_match_list_params
)


DEFAULT_DETAIL_CATEGORIES = [
    'footballMatchDetail',
    'footballMatchOddsEuro',
    'footballMatchOddsAsia',
    'footballMatchTeamTable',
    'footballMatchTeamBattleHistory',
    'footballMatchTeamInjury',
    'FootballMatchIntelligence'
]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='探测竞彩足球数据源可用性与响应形态')
    parser.add_argument('--provider', choices=['sina', 'sporttery'], default='sina', help='选择要探测的数据源')
    parser.add_argument('--date', default='', help='探测的自然日，格式 YYYY-MM-DD')
    parser.add_argument('--is-prized', default='', help='新浪列表接口 isPrized 参数，常用值为空、0、1')
    parser.add_argument('--game-types', default='spf', help='新浪列表接口 gameTypes 参数，默认 spf')
    parser.add_argument('--timeout', type=int, default=getattr(config, 'JINGCAI_REQUEST_TIMEOUT', 15), help='请求超时秒数')
    parser.add_argument('--include-details', action='store_true', help='对新浪额外探测详情接口')
    parser.add_argument('--match-id', default='', help='指定新浪详情接口使用的 matchId；未提供时优先使用列表首场比赛')
    parser.add_argument('--detail-cat', dest='detail_categories', action='append', help='限制详情探测类别，可重复传入')
    parser.add_argument('--sporttery-match-issue-no', default='', help='探测竞彩网固定奖金接口时使用的 matchIssueNo')
    parser.add_argument('--indent', type=int, default=2, help='JSON 输出缩进，默认 2')
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        if args.provider == 'sporttery':
            payload = probe_sporttery(args)
        else:
            payload = probe_sina(args)
        exit_code = 0
    except requests.RequestException as exc:
        payload = build_error_payload(args.provider, exc, 'request_error')
        exit_code = 1
    except Exception as exc:
        payload = build_error_payload(args.provider, exc, 'unexpected_error')
        exit_code = 1

    print(json.dumps(payload, ensure_ascii=False, indent=args.indent))
    return exit_code


def probe_sina(args) -> dict:
    list_response = requests.get(
        SINA_JINGCAI_URL,
        params=build_sina_match_list_params(
            date=args.date,
            is_prized=args.is_prized,
            game_types=args.game_types
        ),
        headers=SINA_LIST_HEADERS,
        timeout=args.timeout
    )

    result = {
        'provider': 'sina',
        'checked_at': utc_now_iso(),
        'timeout_seconds': args.timeout,
        'request': {
            'url': SINA_JINGCAI_URL,
            'params': build_sina_match_list_params(
                date=args.date,
                is_prized=args.is_prized,
                game_types=args.game_types
            ),
            'headers': SINA_LIST_HEADERS
        },
        'list_probe': summarize_http_response(list_response)
    }

    payload = parse_json_safely(list_response)
    if isinstance(payload, dict):
        raw_result = payload.get('result') or {}
        status_code = (raw_result.get('status') or {}).get('code')
        matches = raw_result.get('data') or []
        first_match = matches[0] if isinstance(matches, list) and matches else {}
        result['list_probe'].update({
            'result_status': status_code,
            'batch_key': str(raw_result.get('date') or args.date or '').strip(),
            'available_dates': raw_result.get('dates') or [],
            'match_count': len(matches) if isinstance(matches, list) else 0,
            'first_match': summarize_sina_match(first_match)
        })

        if args.include_details:
            match_id = str(args.match_id or first_match.get('matchId') or '').strip()
            result['detail_probe'] = probe_sina_details(
                match_id=match_id,
                timeout=args.timeout,
                categories=args.detail_categories or DEFAULT_DETAIL_CATEGORIES
            )
    elif args.include_details:
        result['detail_probe'] = {
            'ok': False,
            'match_id': str(args.match_id or '').strip(),
            'error': '列表接口未返回可解析 JSON，跳过详情探测'
        }

    return result


def probe_sina_details(match_id: str, timeout: int, categories: list[str]) -> dict:
    if not match_id:
        return {
            'ok': False,
            'match_id': '',
            'error': '未提供 matchId，且列表接口没有可用首场比赛'
        }

    results = {}
    timestamp = int(datetime.now(tz=timezone.utc).timestamp() * 1000)

    for category in categories:
        params = build_detail_probe_params(category, match_id, timestamp)
        host = 'alpha' if category == 'FootballMatchIntelligence' else 'mix'
        base_url = SINA_JINGCAI_URL if host == 'alpha' else SINA_JINGCAI_DETAIL_URL
        headers = SINA_LIST_HEADERS if host == 'alpha' else SINA_DETAIL_HEADERS

        try:
            response = requests.get(
                base_url,
                params=build_sina_detail_params(category, params),
                headers=headers,
                timeout=timeout
            )
            summary = summarize_http_response(response)
            payload = parse_json_safely(response)
            raw_result = payload.get('result') if isinstance(payload, dict) else None
            data = raw_result.get('data') if isinstance(raw_result, dict) else None
            summary.update({
                'result_status': (raw_result.get('status') or {}).get('code') if isinstance(raw_result, dict) else None,
                'data_shape': detect_shape(data),
                'data_preview': build_data_preview(data)
            })
            results[category] = summary
        except requests.RequestException as exc:
            results[category] = {
                'ok': False,
                'error': str(exc),
                'request': {
                    'url': base_url,
                    'params': build_sina_detail_params(category, params)
                }
            }

    return {
        'ok': any(item.get('ok') for item in results.values()),
        'match_id': match_id,
        'categories': results
    }


def build_detail_probe_params(category: str, match_id: str, timestamp: int) -> dict:
    if category == 'footballMatchDetail':
        return {'matchId': match_id, 't': timestamp}
    if category == 'footballMatchTeamBattleHistory':
        return {'matchId': match_id, 'limit': 10, 'isSameHostAway': 0, 'isSameLeague': 0}
    if category in {'footballMatchOddsEuro', 'footballMatchOddsAsia', 'footballMatchOddsTotals', 'footballMatchTeamTable', 'footballMatchTeamRecentMatches', 'footballMatchTeamInjury'}:
        return {'matchId': match_id}
    if category == 'FootballMatchIntelligence':
        return {'matchId': match_id, 't': timestamp}
    return {'matchId': match_id}


def probe_sporttery(args) -> dict:
    request_url = f'{SPORTTERY_GATEWAY_URL}/uniform/football/getMatchListV1.qry'
    response = requests.get(
        request_url,
        params={
            'clientCode': 3001,
            'showHistory': 1,
            'sort': 0
        },
        headers=SPORTTERY_HEADERS,
        timeout=args.timeout
    )

    result = {
        'provider': 'sporttery',
        'checked_at': utc_now_iso(),
        'timeout_seconds': args.timeout,
        'match_list_probe': summarize_http_response(response)
    }

    payload = parse_json_safely(response)
    if isinstance(payload, dict):
        result['match_list_probe']['top_level_keys'] = sorted(payload.keys())[:20]

    if args.sporttery_match_issue_no:
        fixed_bonus_url = f'{SPORTTERY_GATEWAY_URL}/uniform/football/getFixedBonusV1.qry'
        fixed_bonus_response = requests.get(
            fixed_bonus_url,
            params={
                'clientCode': 3001,
                'matchIssueNo': args.sporttery_match_issue_no
            },
            headers=SPORTTERY_HEADERS,
            timeout=args.timeout
        )
        fixed_bonus_summary = summarize_http_response(fixed_bonus_response)
        fixed_bonus_payload = parse_json_safely(fixed_bonus_response)
        if isinstance(fixed_bonus_payload, dict):
            fixed_bonus_summary['top_level_keys'] = sorted(fixed_bonus_payload.keys())[:20]
        result['fixed_bonus_probe'] = fixed_bonus_summary

    return result


def summarize_http_response(response: requests.Response) -> dict:
    return {
        'ok': response.ok,
        'status_code': response.status_code,
        'reason': response.reason,
        'content_type': response.headers.get('Content-Type', ''),
        'is_json': looks_like_json(response),
        'body_preview': (response.text or '')[:500]
    }


def looks_like_json(response: requests.Response) -> bool:
    content_type = (response.headers.get('Content-Type') or '').lower()
    if 'json' in content_type:
        return True
    body = (response.text or '').lstrip()
    return body.startswith('{') or body.startswith('[')


def parse_json_safely(response: requests.Response) -> dict[str, Any] | list[Any] | None:
    try:
        return response.json()
    except ValueError:
        return None


def summarize_sina_match(match: dict) -> dict:
    if not isinstance(match, dict):
        return {}
    return {
        'matchId': match.get('matchId'),
        'tiCaiId': match.get('tiCaiId'),
        'matchNo': match.get('matchNo'),
        'league': match.get('league'),
        'team1': match.get('team1'),
        'team2': match.get('team2'),
        'matchTime': match.get('matchTime'),
        'showSellStatus': match.get('showSellStatus'),
        'showSellStatusCn': match.get('showSellStatusCn'),
        'spfSellStatus': match.get('spfSellStatus'),
        'rqspfSellStatus': match.get('rqspfSellStatus')
    }


def detect_shape(value: Any) -> str:
    if isinstance(value, dict):
        return 'object'
    if isinstance(value, list):
        return 'array'
    if value is None:
        return 'null'
    return type(value).__name__


def build_data_preview(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            'keys': sorted(value.keys())[:20]
        }
    if isinstance(value, list):
        preview = {
            'length': len(value)
        }
        if value:
            first_item = value[0]
            preview['first_item_shape'] = detect_shape(first_item)
            if isinstance(first_item, dict):
                preview['first_item_keys'] = sorted(first_item.keys())[:20]
        return preview
    return value


def utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).replace(microsecond=0).isoformat()


def build_error_payload(provider: str, exc: Exception, error_type: str) -> dict:
    return {
        'provider': provider,
        'checked_at': utc_now_iso(),
        'ok': False,
        'error_type': error_type,
        'error_class': type(exc).__name__,
        'error': str(exc)
    }


if __name__ == '__main__':
    raise SystemExit(main())
