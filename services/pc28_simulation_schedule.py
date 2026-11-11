"""Pure scheduled replay gates; times are naive Beijing local timestamps."""
from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone
from utils.timezone import BEIJING_FIXED_TZ

WEEKDAYS = ('Mon','Tue','Wed','Thu','Fri','Sat','Sun')


def utc_to_beijing(value):
    try:
        moment = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
        if moment.tzinfo is None:
            moment = moment.replace(tzinfo=timezone.utc)
        return moment.astimezone(BEIJING_FIXED_TZ).replace(tzinfo=None)
    except (ValueError, TypeError, OverflowError):
        return None


def normalize_schedule(value):
    if value is None:
        return None
    if isinstance(value, str):
        try: value = json.loads(value)
        except (ValueError, TypeError): raise ValueError('定时回放设置必须为 JSON 对象') from None
    if not isinstance(value, dict):
        raise ValueError('定时回放设置必须为对象')
    allowed = {'windows','weekdays','allow_pre_window_signal','signal_max_age_seconds',
               'min_remaining_seconds','receive_delay_seconds','max_starts_per_window','cooldown_issues'}
    if set(value)-allowed:
        raise ValueError('定时回放存在未知参数')
    config = {'weekdays':list(WEEKDAYS), 'allow_pre_window_signal':False,
              'signal_max_age_seconds':300,'min_remaining_seconds':40,'receive_delay_seconds':0,
              'max_starts_per_window':1,'cooldown_issues':0,**value}
    if not isinstance(config['allow_pre_window_signal'], bool):
        raise ValueError('允许时段前信号必须为布尔值')
    for key,minimum,maximum in [('signal_max_age_seconds',1,86400),('min_remaining_seconds',0,3600),
                              ('receive_delay_seconds',0,3600),('max_starts_per_window',0,1000),('cooldown_issues',0,10000)]:
        v=config[key]
        if isinstance(v,bool) or not isinstance(v,int) or not minimum<=v<=maximum:
            raise ValueError(f'定时回放 {key} 必须为 {minimum}–{maximum} 的整数')
    if not isinstance(config['weekdays'],list) or any(day not in WEEKDAYS for day in config['weekdays']):
        raise ValueError('定时回放星期无效')
    windows=config.get('windows')
    if not isinstance(windows,list) or not 1<=len(windows)<=24:
        raise ValueError('定时回放需要 1–24 个时段')
    normalized=[];seen=set()
    for i,w in enumerate(windows):
        if not isinstance(w,dict):raise ValueError('定时回放时段无效')
        start,end=w.get('start'),w.get('end');identity=w.get('id',f'w{i+1}')
        if not isinstance(identity,str) or not identity or identity in seen:raise ValueError('定时回放时段 ID 无效或重复')
        if not isinstance(start,str) or not re.fullmatch(r'(?:[01]\d|2[0-3]):[0-5]\d',start):raise ValueError('时段开始格式必须为 HH:MM')
        if not isinstance(end,str) or not (end=='24:00' or re.fullmatch(r'(?:[01]\d|2[0-3]):[0-5]\d',end)) or start>=end:
            raise ValueError('时段结束必须晚于开始，跨日请拆为两个时段')
        seen.add(identity);normalized.append({'id':identity,'start':start,'end':end})
    config['windows']=sorted(normalized,key=lambda w:(w['start'],w['end']))
    return config


def schedule_candidate(config, prediction, draw_time, available_at, *, starting, previous_draw=None, earliest=None):
    """Earliest possible dispatch, subject to known publication/availability and previous settlement.

    receive_delay models additional polling/synchronization delay, not a claim of
    recovering actual historical Telegram receipt or trigger checks.
    """
    delay=timedelta(seconds=config['receive_delay_seconds'])
    candidate=available_at+delay
    if previous_draw: candidate=max(candidate,previous_draw+delay)
    if earliest: candidate=max(candidate,earliest)
    published=utc_to_beijing(prediction.get('external_fetched_at') or prediction.get('created_at'))
    if published is None:return None,'unknown_prediction_time'
    if not starting:
        remaining=(draw_time-candidate).total_seconds()
        return (candidate,'') if remaining>=config['min_remaining_seconds'] and remaining>0 else (None,'window_too_short')
    # A signal may wait for a later window, including one across local midnight.
    day=candidate.replace(hour=0,minute=0,second=0,microsecond=0)
    reason='outside_schedule_window'
    while day<=draw_time:
        if WEEKDAYS[day.weekday()] in config['weekdays']:
            for window in config['windows']:
                def at(hm):
                    h,m=map(int,hm.split(':'));return day+timedelta(hours=h,minutes=m)
                begin,finish=at(window['start']),at(window['end'])
                when=max(candidate,begin)
                if when>=finish or when>=draw_time:continue
                if not config['allow_pre_window_signal'] and published<begin:
                    reason='schedule_signal_before_window';continue
                if (when-published).total_seconds()>config['signal_max_age_seconds']:
                    reason='schedule_signal_stale';continue
                if (draw_time-when).total_seconds()<config['min_remaining_seconds']:
                    reason='window_too_short';continue
                return (when,window['id']),''
        day+=timedelta(days=1)
    return None,reason
