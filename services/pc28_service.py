"""
PC28 数据服务
"""
from __future__ import annotations

import json
from typing import Any, Optional

import requests

import config
from utils.pc28 import derive_pc28_attributes, next_issue_no, parse_pc28_number
from utils.timezone import get_current_beijing_time_str


class PC28Service:
    """封装 PC28 官方公开数据接口"""

    def __init__(self, base_url: str = config.PC28_API_BASE_URL, timeout: int = config.PC28_REQUEST_TIMEOUT):
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout

    def _request_json(self, path: str, params: Optional[dict] = None) -> dict:
        response = requests.get(
            f'{self.base_url}/{path.lstrip("/")}',
            params=params or {},
            timeout=self.timeout
        )
        response.raise_for_status()
        payload = response.json()

        if payload.get('message') != 'success':
            raise ValueError(f'PC28 API 返回失败: {payload}')

        return payload

    def fetch_recent_draws(self, limit: int = 50) -> list[dict]:
        """获取官方最新开奖号"""
        limit = max(1, min(limit, 100))
        payload = self._request_json('api/kj.json', {'nbr': limit})
        data = payload.get('data') or []
        draws = []
        for item in data:
            normalized = self._normalize_draw(item)
            if normalized:
                draws.append(normalized)
        return draws

    def fetch_keno_snapshot(self) -> dict:
        """获取当前期倒计时与期号快照"""
        payload = self._request_json('api/keno.json', {'nbr': 1})
        data = payload.get('data') or []
        latest = data[0] if data else {}
        latest_issue_no = str(latest.get('nbr') or '').strip() or None

        return {
            'countdown': str(payload.get('countdown') or '00:00:00'),
            'latest_issue_no': latest_issue_no,
            'next_issue_no': next_issue_no(latest_issue_no),
            'raw_item': latest
        }

    def fetch_omission_stats(self) -> dict:
        """获取遗漏统计"""
        payload = self._request_json('api/yl.json')
        return payload.get('data') or {}

    def fetch_today_stats(self) -> dict:
        """获取今日统计"""
        payload = self._request_json('api/yk.json')
        return payload.get('data') or {}

    def fetch_preview(self) -> dict:
        """获取聚合预览接口"""
        payload = self._request_json('api/preview.json')
        return payload.get('data') or {}

    def sync_recent_draws(self, db, limit: int = 120) -> list[dict]:
        """同步最近开奖到本地数据库"""
        draws = self.fetch_recent_draws(limit=limit)
        db.upsert_draws('pc28', draws)
        return draws

    def build_overview(self, history_limit: int = 20) -> dict:
        """构建前端公开总览数据"""
        draws = self.fetch_recent_draws(limit=history_limit)
        keno_snapshot = self.fetch_keno_snapshot()
        omission_stats = self.fetch_omission_stats()
        today_stats = self.fetch_today_stats()

        preview = {}
        try:
            preview = self.fetch_preview()
        except Exception:
            preview = {}

        latest_draw = draws[0] if draws else None
        next_issue = self._resolve_next_issue_no(
            keno_snapshot.get('next_issue_no'),
            next_issue_no(latest_draw['issue_no']) if latest_draw else None
        )

        return {
            'lottery_type': 'pc28',
            'latest_draw': latest_draw,
            'next_issue_no': next_issue,
            'countdown': keno_snapshot.get('countdown', '00:00:00'),
            'recent_draws': draws,
            'omission_preview': self._build_omission_preview(omission_stats),
            'today_preview': self._build_today_preview(today_stats),
            'preview': preview,
            'generated_at': get_current_beijing_time_str()
        }

    def _resolve_next_issue_no(self, *candidates: Optional[str]) -> Optional[str]:
        valid_candidates = []
        for candidate in candidates:
            text = str(candidate or '').strip()
            if text.isdigit():
                valid_candidates.append(text)

        if not valid_candidates:
            return None

        return max(valid_candidates, key=lambda value: int(value))

    def _normalize_draw(self, item: dict) -> Optional[dict]:
        issue_no = str(item.get('nbr') or '').strip()
        result_number = parse_pc28_number(item.get('num'))
        if result_number is None:
            result_number = parse_pc28_number(item.get('number'))

        if not issue_no or result_number is None:
            return None

        normalized = derive_pc28_attributes(result_number)
        open_time = ' '.join(
            part for part in [str(item.get('date') or '').strip(), str(item.get('time') or '').strip()] if part
        ).strip()

        return {
            'issue_no': issue_no,
            'draw_date': str(item.get('date') or '').strip(),
            'draw_time': str(item.get('time') or '').strip(),
            'open_time': open_time,
            'result_number': normalized['result_number'],
            'result_number_text': normalized['result_number_text'],
            'big_small': normalized['big_small'],
            'odd_even': normalized['odd_even'],
            'combo': normalized['combo'],
            'source_payload': json.dumps(item, ensure_ascii=False)
        }

    def _build_omission_preview(self, stats: dict) -> dict:
        number_items = []
        for key, value in (stats or {}).items():
            label = str(key).strip()
            if label.isdigit():
                number_items.append({
                    'label': label.zfill(2),
                    'value': int(value)
                })

        number_items.sort(key=lambda item: item['value'], reverse=True)

        groups = {}
        for label in ['大', '小', '单', '双', '大单', '大双', '小单', '小双']:
            if label in stats:
                groups[label] = int(stats[label])

        return {
            'top_numbers': number_items[:6],
            'groups': groups
        }

    def _build_today_preview(self, stats: dict) -> dict:
        summary_keys = ['总期数', '大', '小', '单', '双', '大单', '大双', '小单', '小双']
        summary = {
            key: int(stats[key]) for key in summary_keys if key in stats and str(stats[key]).isdigit()
        }

        hot_numbers = []
        for key, value in (stats or {}).items():
            label = str(key).strip()
            if label.isdigit():
                hot_numbers.append({
                    'label': label.zfill(2),
                    'value': int(value)
                })

        hot_numbers.sort(key=lambda item: item['value'], reverse=True)

        return {
            'summary': summary,
            'hot_numbers': hot_numbers[:6]
        }
