"""
PC28 数据服务
"""
from __future__ import annotations

import json
from typing import Any, Optional

import requests

import config
from utils.pc28 import derive_pc28_attributes, next_issue_no, parse_pc28_number
from utils.timezone import get_current_beijing_time_str, parse_beijing_time


class PC28Service:
    """封装 PC28 公开数据接口，并在官方源异常时回退备用源"""

    def __init__(
        self,
        base_url: str = config.PC28_API_BASE_URL,
        timeout: int = config.PC28_REQUEST_TIMEOUT,
        recent_source_order: tuple[str, ...] = config.PC28_RECENT_SOURCE_ORDER,
        jnd_recent_url: str = config.PC28_JND_RECENT_URL,
        feiji_recent_url: str = config.PC28_FEIJI_RECENT_URL
    ):
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout
        self.recent_source_order = tuple(recent_source_order or ('official', 'jnd', 'feiji'))
        self.jnd_recent_url = jnd_recent_url
        self.feiji_recent_url = feiji_recent_url

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

    def _request_public_json(self, url: str, params: Optional[dict] = None) -> Any:
        response = requests.get(url, params=params or {}, timeout=self.timeout)
        response.raise_for_status()
        return response.json()

    def fetch_recent_draws(self, limit: int = 50) -> list[dict]:
        """获取最新开奖号，优先官方源，失败时自动回退备用源"""
        draws, _ = self._fetch_recent_draws_with_source(limit=limit)
        return draws

    def _fetch_recent_draws_with_source(self, limit: int = 50) -> tuple[list[dict], str]:
        limit = max(1, min(limit, 100))
        errors = []

        for source in self.recent_source_order:
            if source == 'official':
                fetcher = self._fetch_official_recent_draws
            elif source == 'jnd':
                fetcher = self._fetch_jnd_recent_draws
            elif source == 'feiji':
                fetcher = self._fetch_feiji_recent_draws
            else:
                errors.append(f'{source}: 未知数据源')
                continue

            try:
                draws = fetcher(limit=limit)
                if draws:
                    return draws, source
                errors.append(f'{source}: 返回空数据')
            except Exception as exc:
                errors.append(f'{source}: {exc}')

        raise RuntimeError(f'PC28 最近开奖接口全部不可用: {" | ".join(errors)}')

    def _fetch_official_recent_draws(self, limit: int) -> list[dict]:
        payload = self._request_json('api/kj.json', {'nbr': limit})
        return self._normalize_draw_list(payload.get('data') or [], source='official')

    def _fetch_jnd_recent_draws(self, limit: int) -> list[dict]:
        payload = self._request_public_json(self.jnd_recent_url, {'limit': limit})
        if not isinstance(payload, list):
            raise ValueError(f'JND28 recent 接口返回格式异常: {payload}')
        return self._normalize_draw_list(payload, source='jnd')

    def _fetch_feiji_recent_draws(self, limit: int) -> list[dict]:
        payload = self._request_public_json(self.feiji_recent_url, {'limit': limit, 'offset': 0})
        data = payload.get('data') if isinstance(payload, dict) else None
        if not isinstance(data, list):
            raise ValueError(f'Feiji28 latest 接口返回格式异常: {payload}')
        return self._normalize_draw_list(data, source='feiji')

    def fetch_keno_snapshot(self, recent_draws: Optional[list[dict]] = None) -> dict:
        """获取当前期倒计时与期号快照"""
        try:
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
        except Exception as exc:
            draws = recent_draws or self.fetch_recent_draws(limit=1)
            latest_draw = draws[0] if draws else None
            latest_issue_no = str((latest_draw or {}).get('issue_no') or '').strip() or None
            if not latest_issue_no:
                raise RuntimeError(f'PC28 快照接口不可用，且无法从最近开奖推导下一期期号: {exc}') from exc

            return {
                'countdown': '--:--:--',
                'latest_issue_no': latest_issue_no,
                'next_issue_no': next_issue_no(latest_issue_no),
                'raw_item': latest_draw,
                'warning': '官方快照接口不可用，已按最近开奖推导下一期期号'
            }

    def fetch_omission_stats(self) -> dict:
        """获取遗漏统计"""
        try:
            payload = self._request_json('api/yl.json')
            return payload.get('data') or {}
        except Exception:
            return {}

    def fetch_today_stats(self) -> dict:
        """获取今日统计"""
        try:
            payload = self._request_json('api/yk.json')
            return payload.get('data') or {}
        except Exception:
            return {}

    def fetch_preview(self) -> dict:
        """获取聚合预览接口"""
        try:
            payload = self._request_json('api/preview.json')
            return payload.get('data') or {}
        except Exception:
            return {}

    def sync_recent_draws(self, db, limit: int = 120) -> list[dict]:
        """同步最近开奖到本地数据库"""
        draws = self.fetch_recent_draws(limit=limit)
        db.upsert_draws('pc28', draws)
        return draws

    def build_overview(self, history_limit: int = 20) -> dict:
        """构建前端公开总览数据"""
        draws, recent_source = self._fetch_recent_draws_with_source(limit=history_limit)
        keno_snapshot = self.fetch_keno_snapshot(recent_draws=draws[:1])
        omission_stats = self.fetch_omission_stats()
        today_stats = self.fetch_today_stats()
        preview = self.fetch_preview()

        latest_draw = draws[0] if draws else None
        next_issue = self._resolve_next_issue_no(
            keno_snapshot.get('next_issue_no'),
            next_issue_no(latest_draw['issue_no']) if latest_draw else None
        )
        warnings = []
        if recent_source != 'official':
            warnings.append(f'官方开奖接口不可用，已回退到{self._get_source_label(recent_source)}')
        if keno_snapshot.get('warning'):
            warnings.append(keno_snapshot['warning'])

        overview = {
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
        if warnings:
            overview['warning'] = '；'.join(dict.fromkeys(warnings))
        return overview

    def _resolve_next_issue_no(self, *candidates: Optional[str]) -> Optional[str]:
        valid_candidates = []
        for candidate in candidates:
            text = str(candidate or '').strip()
            if text.isdigit():
                valid_candidates.append(text)

        if not valid_candidates:
            return None

        return max(valid_candidates, key=lambda value: int(value))

    def _normalize_draw_list(self, items: list[dict], source: str) -> list[dict]:
        draws = []
        for item in items:
            normalized = self._normalize_draw(item, source=source)
            if normalized:
                draws.append(normalized)
        return draws

    def _normalize_draw(self, item: dict, source: str = 'official') -> Optional[dict]:
        issue_no = ''
        result_number = None
        draw_date = ''
        draw_time = ''
        open_time = ''

        if source == 'official':
            issue_no = str(item.get('nbr') or '').strip()
            result_number = parse_pc28_number(item.get('num'))
            if result_number is None:
                result_number = parse_pc28_number(item.get('number'))
            draw_date = str(item.get('date') or '').strip()
            draw_time = str(item.get('time') or '').strip()
            open_time = ' '.join(part for part in [draw_date, draw_time] if part).strip()
        elif source == 'jnd':
            issue_no = str(item.get('draw_number') or '').strip()
            result_number = parse_pc28_number(item.get('canada28_result'))
            draw_date, draw_time, open_time = self._split_datetime_parts(item.get('draw_date'))
        elif source == 'feiji':
            issue_no = str(item.get('draw_nbr') or '').strip()
            result_number = parse_pc28_number(item.get('final_sum'))
            draw_date, draw_time, open_time = self._extract_feiji_open_time(item)
        else:
            raise ValueError(f'不支持的 PC28 开奖数据源: {source}')

        if not issue_no or result_number is None:
            return None

        normalized = derive_pc28_attributes(result_number)

        return {
            'issue_no': issue_no,
            'draw_date': draw_date,
            'draw_time': draw_time,
            'open_time': open_time,
            'result_number': normalized['result_number'],
            'result_number_text': normalized['result_number_text'],
            'big_small': normalized['big_small'],
            'odd_even': normalized['odd_even'],
            'combo': normalized['combo'],
            'source_payload': json.dumps(item, ensure_ascii=False)
        }

    def _split_datetime_parts(self, value: Any) -> tuple[str, str, str]:
        text = str(value or '').strip()
        if not text:
            return '', '', ''

        parsed = parse_beijing_time(text)
        if parsed:
            return (
                parsed.strftime('%Y-%m-%d'),
                parsed.strftime('%H:%M:%S'),
                parsed.strftime('%Y-%m-%d %H:%M:%S')
            )

        return '', '', text

    def _extract_feiji_open_time(self, item: dict) -> tuple[str, str, str]:
        created_at = item.get('created_at')
        if created_at:
            return self._split_datetime_parts(created_at)

        draw_date = str(item.get('draw_date') or '').strip()
        draw_time = str(item.get('draw_time') or '').strip()
        if draw_date and draw_time:
            date_text = draw_date[:10]
            parsed = parse_beijing_time(f'{date_text} {draw_time}')
            if parsed:
                return (
                    parsed.strftime('%Y-%m-%d'),
                    parsed.strftime('%H:%M:%S'),
                    parsed.strftime('%Y-%m-%d %H:%M:%S')
                )
            return date_text, draw_time, f'{date_text} {draw_time}'

        return '', '', ''

    def _get_source_label(self, source: str) -> str:
        labels = {
            'official': '官方源',
            'jnd': 'JND28',
            'feiji': 'Feiji28'
        }
        return labels.get(source, source)

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
