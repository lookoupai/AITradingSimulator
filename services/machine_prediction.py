"""
内置机器算法预测服务
"""
from __future__ import annotations

import json

from services.algorithm_executor import predict_jingcai_with_user_algorithm
from utils import jingcai_football as football_utils
from utils.pc28 import derive_pc28_attributes
from utils.predictor_engine import get_algorithm_label, is_user_algorithm_key, normalize_algorithm_key


def predict_pc28(context: dict, predictor: dict) -> tuple[dict, str, str]:
    algorithm_key = normalize_algorithm_key('pc28', predictor.get('engine_type'), predictor.get('algorithm_key'))
    if is_user_algorithm_key(algorithm_key):
        raise ValueError('PC28 用户自定义算法执行器暂未接入')
    if algorithm_key == 'pc28_frequency_v1':
        prediction, debug_payload = _predict_pc28_frequency_v1(context, predictor)
    elif algorithm_key == 'pc28_omission_reversion_v1':
        prediction, debug_payload = _predict_pc28_omission_reversion_v1(context, predictor)
    elif algorithm_key == 'pc28_combo_markov_v1':
        prediction, debug_payload = _predict_pc28_combo_markov_v1(context, predictor)
    else:
        raise ValueError(f'暂不支持的 PC28 机器算法: {algorithm_key}')
    algorithm_label = get_algorithm_label('pc28', 'machine', algorithm_key)
    return prediction, json.dumps(debug_payload, ensure_ascii=False), f'机器算法：{algorithm_label}'


def predict_jingcai(run_key: str, matches: list[dict], predictor: dict) -> tuple[list[dict], str, str]:
    algorithm_key = normalize_algorithm_key(
        'jingcai_football',
        predictor.get('engine_type'),
        predictor.get('algorithm_key')
    )
    if is_user_algorithm_key(algorithm_key):
        items_payload, debug_payload = predict_jingcai_with_user_algorithm(run_key, matches, predictor)
    elif algorithm_key == 'football_odds_baseline_v1':
        items_payload, debug_payload = _predict_jingcai_odds_baseline_v1(run_key, matches)
    elif algorithm_key == 'football_odds_form_weighted_v1':
        items_payload, debug_payload = _predict_jingcai_odds_form_weighted_v1(run_key, matches)
    elif algorithm_key == 'football_handicap_consistency_v1':
        items_payload, debug_payload = _predict_jingcai_handicap_consistency_v1(run_key, matches)
    elif algorithm_key == 'football_value_edge_v1':
        items_payload, debug_payload = _predict_jingcai_value_edge_v1(run_key, matches)
    else:
        raise ValueError(f'暂不支持的 竞彩足球机器算法: {algorithm_key}')
    algorithm_label = (predictor.get('user_algorithm') or {}).get('name') or get_algorithm_label('jingcai_football', 'machine', algorithm_key)
    return items_payload, json.dumps(debug_payload, ensure_ascii=False), f'机器算法：{algorithm_label}'


def _predict_pc28_frequency_v1(context: dict, predictor: dict) -> tuple[dict, dict]:
    recent_draws = list(context.get('recent_draws') or [])
    if not recent_draws:
        raise ValueError('机器算法缺少可用历史开奖数据')

    history_window = max(10, min(int(predictor.get('history_window') or len(recent_draws) or 60), len(recent_draws)))
    window = recent_draws[:history_window]

    number_scores = {number: 0.0 for number in range(28)}
    big_small_scores = {'大': 0.0, '小': 0.0}
    odd_even_scores = {'单': 0.0, '双': 0.0}
    combo_scores = {'大单': 0.0, '大双': 0.0, '小单': 0.0, '小双': 0.0}
    total_weight = 0.0

    for index, draw in enumerate(window):
        try:
            number = int(draw.get('result_number'))
        except (TypeError, ValueError):
            continue
        weight = max(0.25, 1.0 - (index / max(len(window), 1)))
        total_weight += weight
        number_scores[number] += weight * 1.8
        big_small = str(draw.get('big_small') or '').strip()
        odd_even = str(draw.get('odd_even') or '').strip()
        combo = str(draw.get('combo') or '').strip()
        if big_small in big_small_scores:
            big_small_scores[big_small] += weight
        if odd_even in odd_even_scores:
            odd_even_scores[odd_even] += weight
        if combo in combo_scores:
            combo_scores[combo] += weight

    ranked_candidates: list[dict] = []
    for number in range(28):
        attrs = derive_pc28_attributes(number)
        omission = len(window)
        for index, draw in enumerate(window):
            try:
                if int(draw.get('result_number')) == number:
                    omission = index
                    break
            except (TypeError, ValueError):
                continue

        omission_ratio = omission / max(len(window), 1)
        attr_score = (
            combo_scores.get(attrs['combo'], 0.0) * 0.55
            + big_small_scores.get(attrs['big_small'], 0.0) * 0.25
            + odd_even_scores.get(attrs['odd_even'], 0.0) * 0.20
        )
        score = number_scores[number] + attr_score * 0.32 + omission_ratio * 0.45
        ranked_candidates.append({
            'number': number,
            'score': round(score, 6),
            'omission': omission,
            'attrs': attrs
        })

    ranked_candidates.sort(key=lambda item: (item['score'], -item['number']), reverse=True)
    winner = ranked_candidates[0]
    runner_up = ranked_candidates[1] if len(ranked_candidates) > 1 else ranked_candidates[0]
    winner_attrs = winner['attrs']
    combo_focus = combo_scores.get(winner_attrs['combo'], 0.0) / max(total_weight, 1.0)
    gap_ratio = max(0.0, winner['score'] - runner_up['score']) / max(winner['score'], 0.01)
    confidence = round(max(0.38, min(0.86, 0.44 + combo_focus * 0.22 + gap_ratio * 0.36)), 2)

    prediction = {
        'issue_no': context.get('next_issue_no'),
        'prediction_number': winner['number'],
        'prediction_big_small': winner_attrs['big_small'],
        'prediction_odd_even': winner_attrs['odd_even'],
        'prediction_combo': winner_attrs['combo'],
        'confidence': confidence,
        'reasoning_summary': (
            f"近{len(window)}期{winner_attrs['combo']}权重更强，和值{winner['number']}的频次与遗漏综合得分最高"
        )
    }
    debug_payload = {
        'algorithm': 'pc28_frequency_v1',
        'next_issue_no': context.get('next_issue_no'),
        'history_window': len(window),
        'preferred_big_small': max(big_small_scores, key=big_small_scores.get),
        'preferred_odd_even': max(odd_even_scores, key=odd_even_scores.get),
        'preferred_combo': max(combo_scores, key=combo_scores.get),
        'top_candidates': [
            {
                'number': item['number'],
                'score': round(item['score'], 4),
                'omission': item['omission'],
                'combo': item['attrs']['combo']
            }
            for item in ranked_candidates[:5]
        ]
    }
    return prediction, debug_payload


def _predict_pc28_omission_reversion_v1(context: dict, predictor: dict) -> tuple[dict, dict]:
    recent_draws = list(context.get('recent_draws') or [])
    if not recent_draws:
        raise ValueError('机器算法缺少可用历史开奖数据')

    history_window = max(10, min(int(predictor.get('history_window') or len(recent_draws) or 60), len(recent_draws)))
    window = recent_draws[:history_window]
    recent_window = window[:min(12, len(window))]
    latest_draw = window[0] if window else {}

    ranked_candidates: list[dict] = []
    for number in range(28):
        attrs = derive_pc28_attributes(number)
        omission = _pc28_value_omission(window, lambda draw: draw.get('result_number'), number)
        combo_omission = _pc28_value_omission(window, lambda draw: draw.get('combo'), attrs['combo'])
        big_small_omission = _pc28_value_omission(window, lambda draw: draw.get('big_small'), attrs['big_small'])
        odd_even_omission = _pc28_value_omission(window, lambda draw: draw.get('odd_even'), attrs['odd_even'])
        recent_hits = sum(
            1
            for draw in recent_window
            if int(draw.get('result_number') if draw.get('result_number') is not None else -1) == number
        )
        overall_hits = sum(
            1
            for draw in window
            if int(draw.get('result_number') if draw.get('result_number') is not None else -1) == number
        )
        number_ratio = omission / max(len(window), 1)
        combo_ratio = combo_omission / max(len(window), 1)
        big_small_ratio = big_small_omission / max(len(window), 1)
        odd_even_ratio = odd_even_omission / max(len(window), 1)
        center_bonus = 1 - abs(number - 13.5) / 13.5
        score = (
            number_ratio * 0.58
            + combo_ratio * 0.20
            + big_small_ratio * 0.08
            + odd_even_ratio * 0.08
            + center_bonus * 0.06
            - (recent_hits / max(len(recent_window), 1)) * 0.26
            - (overall_hits / max(len(window), 1)) * 0.10
        )
        if str(latest_draw.get('combo') or '').strip() == attrs['combo']:
            score -= 0.04
        ranked_candidates.append({
            'number': number,
            'score': round(score, 6),
            'omission': omission,
            'combo_omission': combo_omission,
            'attrs': attrs,
            'recent_hits': recent_hits
        })

    ranked_candidates.sort(
        key=lambda item: (
            item['score'],
            item['combo_omission'],
            item['omission'],
            -item['recent_hits'],
            -abs(item['number'] - 13.5)
        ),
        reverse=True
    )
    winner = ranked_candidates[0]
    runner_up = ranked_candidates[1] if len(ranked_candidates) > 1 else ranked_candidates[0]
    winner_attrs = winner['attrs']
    omission_focus = (
        winner['omission'] * 0.58
        + winner['combo_omission'] * 0.24
        + _pc28_value_omission(window, lambda draw: draw.get('big_small'), winner_attrs['big_small']) * 0.10
        + _pc28_value_omission(window, lambda draw: draw.get('odd_even'), winner_attrs['odd_even']) * 0.08
    ) / max(len(window), 1)
    gap_ratio = max(0.0, winner['score'] - runner_up['score']) / max(abs(winner['score']), 0.01)
    confidence = round(max(0.37, min(0.81, 0.39 + omission_focus * 0.28 + gap_ratio * 0.18)), 2)

    prediction = {
        'issue_no': context.get('next_issue_no'),
        'prediction_number': winner['number'],
        'prediction_big_small': winner_attrs['big_small'],
        'prediction_odd_even': winner_attrs['odd_even'],
        'prediction_combo': winner_attrs['combo'],
        'confidence': confidence,
        'reasoning_summary': (
            f"近{len(window)}期{winner_attrs['combo']}遗漏偏高，和值{winner['number']}在回补评分中领先"
        )
    }
    debug_payload = {
        'algorithm': 'pc28_omission_reversion_v1',
        'next_issue_no': context.get('next_issue_no'),
        'history_window': len(window),
        'latest_combo': latest_draw.get('combo'),
        'preferred_combo': winner_attrs['combo'],
        'top_candidates': [
            {
                'number': item['number'],
                'score': round(item['score'], 4),
                'omission': item['omission'],
                'combo_omission': item['combo_omission'],
                'combo': item['attrs']['combo']
            }
            for item in ranked_candidates[:5]
        ]
    }
    return prediction, debug_payload


def _predict_pc28_combo_markov_v1(context: dict, predictor: dict) -> tuple[dict, dict]:
    recent_draws = list(context.get('recent_draws') or [])
    if not recent_draws:
        raise ValueError('机器算法缺少可用历史开奖数据')

    history_window = max(10, min(int(predictor.get('history_window') or len(recent_draws) or 60), len(recent_draws)))
    window = recent_draws[:history_window]
    ordered_draws = list(reversed(window))
    combo_states = ('大单', '大双', '小单', '小双')
    transitions = {
        combo: {target: 0.0 for target in combo_states}
        for combo in combo_states
    }
    transition_counts = {
        combo: {target: 0 for target in combo_states}
        for combo in combo_states
    }

    for index in range(len(ordered_draws) - 1):
        current_combo = str(ordered_draws[index].get('combo') or '').strip()
        next_combo = str(ordered_draws[index + 1].get('combo') or '').strip()
        if current_combo not in transitions or next_combo not in transitions[current_combo]:
            continue
        weight = 1.0 + (index / max(len(ordered_draws) - 1, 1)) * 0.8
        transitions[current_combo][next_combo] += weight
        transition_counts[current_combo][next_combo] += 1

    latest_combo = str(window[0].get('combo') or '').strip()
    if latest_combo not in transitions:
        latest_combo = combo_states[0]

    combo_omissions = {
        combo: _pc28_value_omission(window, lambda draw: draw.get('combo'), combo)
        for combo in combo_states
    }
    global_combo_scores = {
        combo: (
            sum(transitions[source][combo] for source in combo_states) * 0.28
            + combo_omissions[combo] * 0.12
        )
        for combo in combo_states
    }
    next_combo_scores = {
        combo: transitions[latest_combo][combo] + global_combo_scores[combo]
        for combo in combo_states
    }
    ranked_combos = sorted(next_combo_scores.items(), key=lambda item: item[1], reverse=True)
    predicted_combo = ranked_combos[0][0]

    candidates = []
    for number in range(28):
        attrs = derive_pc28_attributes(number)
        if attrs['combo'] != predicted_combo:
            continue
        omission = _pc28_value_omission(window, lambda draw: draw.get('result_number'), number)
        combo_omission = combo_omissions[predicted_combo]
        recent_hits = sum(
            1
            for draw in window[:min(15, len(window))]
            if int(draw.get('result_number') if draw.get('result_number') is not None else -1) == number
        )
        weighted_hits = 0.0
        for index, draw in enumerate(window):
            try:
                draw_number = int(draw.get('result_number'))
            except (TypeError, ValueError):
                continue
            if draw_number != number:
                continue
            weighted_hits += max(0.15, 1.0 - (index / max(len(window), 1)))
        center_bonus = 1 - abs(number - 13.5) / 13.5
        score = (
            combo_omission / max(len(window), 1) * 0.44
            + omission / max(len(window), 1) * 0.24
            + weighted_hits * 0.20
            + center_bonus * 0.12
            - recent_hits * 0.05
        )
        candidates.append({
            'number': number,
            'score': round(score, 6),
            'omission': omission,
            'recent_hits': recent_hits,
            'weighted_hits': round(weighted_hits, 4),
            'attrs': attrs
        })

    candidates.sort(
        key=lambda item: (
            item['score'],
            item['omission'],
            item['weighted_hits'],
            -item['recent_hits'],
            -abs(item['number'] - 13.5)
        ),
        reverse=True
    )
    winner = candidates[0]
    runner_up = candidates[1] if len(candidates) > 1 else candidates[0]
    winner_attrs = winner['attrs']
    combo_confidence = next_combo_scores[predicted_combo] / max(sum(next_combo_scores.values()), 0.01)
    gap_ratio = max(0.0, winner['score'] - runner_up['score']) / max(abs(winner['score']), 0.01)
    confidence = round(max(0.39, min(0.83, 0.41 + combo_confidence * 0.24 + gap_ratio * 0.18)), 2)

    prediction = {
        'issue_no': context.get('next_issue_no'),
        'prediction_number': winner['number'],
        'prediction_big_small': winner_attrs['big_small'],
        'prediction_odd_even': winner_attrs['odd_even'],
        'prediction_combo': winner_attrs['combo'],
        'confidence': confidence,
        'reasoning_summary': (
            f"最新{latest_combo}后的历史转移更偏向{predicted_combo}，和值{winner['number']}在该组合内评分最高"
        )
    }
    debug_payload = {
        'algorithm': 'pc28_combo_markov_v1',
        'next_issue_no': context.get('next_issue_no'),
        'history_window': len(window),
        'latest_combo': latest_combo,
        'predicted_combo': predicted_combo,
        'combo_transition_scores': {
            combo: round(score, 4)
            for combo, score in ranked_combos
        },
        'combo_transition_counts': transition_counts.get(latest_combo) or {},
        'top_candidates': [
            {
                'number': item['number'],
                'score': round(item['score'], 4),
                'omission': item['omission'],
                'recent_hits': item['recent_hits'],
                'combo': item['attrs']['combo']
            }
            for item in candidates[:5]
        ]
    }
    return prediction, debug_payload


def _predict_jingcai_odds_baseline_v1(run_key: str, matches: list[dict]) -> tuple[list[dict], dict]:
    items_payload = []
    debug_rows = []
    for match in matches:
        spf_prediction = _pick_odds_outcome(match.get('spf_odds') or {})
        rq_prediction = _pick_odds_outcome(((match.get('rqspf') or {}).get('odds') or {}))
        requested_confidences = [item['confidence'] for item in (spf_prediction, rq_prediction) if item['confidence'] is not None]
        confidence = round(sum(requested_confidences) / len(requested_confidences), 2) if requested_confidences else None
        handicap_text = str(((match.get('rqspf') or {}).get('handicap_text') or '')).strip() or '--'
        reasoning_parts = []
        if spf_prediction['outcome']:
            reasoning_parts.append(
                f"SPF主向{spf_prediction['outcome']}，主赔{_format_odds_value(spf_prediction['odds'])}"
            )
        if rq_prediction['outcome']:
            reasoning_parts.append(
                f"让{handicap_text}主向{rq_prediction['outcome']}，主赔{_format_odds_value(rq_prediction['odds'])}"
            )
        reasoning_summary = '；'.join(reasoning_parts) or '按赔率基线生成默认预测'

        items_payload.append({
            'event_key': match.get('event_key') or '',
            'match_no': match.get('match_no') or '',
            'predicted_spf': spf_prediction['outcome'],
            'predicted_rqspf': rq_prediction['outcome'],
            'confidence': confidence,
            'reasoning_summary': reasoning_summary
        })
        debug_rows.append({
            'event_key': match.get('event_key') or '',
            'match_no': match.get('match_no') or '',
            'predicted_spf': spf_prediction['outcome'],
            'predicted_rqspf': rq_prediction['outcome'],
            'confidence': confidence,
            'spf_probabilities': spf_prediction['probabilities'],
            'rqspf_probabilities': rq_prediction['probabilities']
        })

    return items_payload, {
        'algorithm': 'football_odds_baseline_v1',
        'run_key': run_key,
        'items': debug_rows
    }


def _predict_jingcai_odds_form_weighted_v1(run_key: str, matches: list[dict]) -> tuple[list[dict], dict]:
    items_payload = []
    debug_rows = []
    for match in matches:
        detail_bundle = match.get('detail_bundle') or {}
        spf_prediction = _pick_odds_outcome(match.get('spf_odds') or {})
        rq_prediction = _pick_odds_outcome(((match.get('rqspf') or {}).get('odds') or {}))
        form_delta = _football_form_delta(match, detail_bundle)
        table_delta = _football_table_delta(detail_bundle)
        injury_delta = _football_injury_delta(detail_bundle)
        market_delta = _football_market_delta(detail_bundle)

        home_bias = max(
            -0.55,
            min(
                0.55,
                form_delta * 0.40
                + table_delta * 0.34
                + injury_delta * 0.12
                + market_delta * 0.14
            )
        )
        adjusted_spf = _apply_spf_bias(spf_prediction['probabilities'], home_bias)
        predicted_spf, spf_confidence = _select_spf_outcome(adjusted_spf)

        expected_margin = _estimate_expected_margin(
            adjusted_spf,
            form_delta=form_delta,
            table_delta=table_delta,
            injury_delta=injury_delta,
            market_delta=market_delta
        )
        predicted_rqspf, rq_confidence = _select_rqspf_outcome(
            expected_margin=expected_margin,
            handicap=(match.get('rqspf') or {}).get('handicap')
        )

        confidence_values = [value for value in (spf_confidence, rq_confidence) if value is not None]
        confidence = round(sum(confidence_values) / len(confidence_values), 2) if confidence_values else None
        handicap_text = str(((match.get('rqspf') or {}).get('handicap_text') or '')).strip() or '--'
        reasoning_bits = [
            f"赔率主向{predicted_spf or '--'}",
            f"近况差{form_delta:+.2f}",
            f"排名差{table_delta:+.2f}"
        ]
        if injury_delta:
            reasoning_bits.append(f"伤停差{injury_delta:+.2f}")
        if market_delta:
            reasoning_bits.append(f"欧赔变动{market_delta:+.2f}")
        reasoning_bits.append(f"让{handicap_text}主向{predicted_rqspf or '--'}")
        reasoning_summary = '；'.join(reasoning_bits)

        items_payload.append({
            'event_key': match.get('event_key') or '',
            'match_no': match.get('match_no') or '',
            'predicted_spf': predicted_spf,
            'predicted_rqspf': predicted_rqspf,
            'confidence': confidence,
            'reasoning_summary': reasoning_summary
        })
        debug_rows.append({
            'event_key': match.get('event_key') or '',
            'match_no': match.get('match_no') or '',
            'predicted_spf': predicted_spf,
            'predicted_rqspf': predicted_rqspf,
            'confidence': confidence,
            'base_spf_probabilities': spf_prediction['probabilities'],
            'base_rqspf_probabilities': rq_prediction['probabilities'],
            'adjusted_spf_probabilities': adjusted_spf,
            'expected_margin': round(expected_margin, 4),
            'form_delta': round(form_delta, 4),
            'table_delta': round(table_delta, 4),
            'injury_delta': round(injury_delta, 4),
            'market_delta': round(market_delta, 4)
        })

    return items_payload, {
        'algorithm': 'football_odds_form_weighted_v1',
        'run_key': run_key,
        'items': debug_rows
    }


def _predict_jingcai_handicap_consistency_v1(run_key: str, matches: list[dict]) -> tuple[list[dict], dict]:
    items_payload = []
    debug_rows = []
    for match in matches:
        detail_bundle = match.get('detail_bundle') or {}
        handicap = (match.get('rqspf') or {}).get('handicap')
        handicap_text = str(((match.get('rqspf') or {}).get('handicap_text') or '')).strip() or '--'
        spf_market = _pick_odds_outcome(match.get('spf_odds') or {})
        rq_market = _pick_odds_outcome(((match.get('rqspf') or {}).get('odds') or {}))
        euro_support = _football_market_delta(detail_bundle)
        asia_support = _football_asia_market_delta(detail_bundle)
        expected_margin = _estimate_handicap_consistency_margin(
            spf_probabilities=spf_market.get('probabilities') or {},
            euro_support=euro_support,
            asia_support=asia_support
        )
        implied_rqspf, _ = _select_rqspf_outcome(expected_margin, handicap)

        rq_scores = {
            key: float((rq_market.get('probabilities') or {}).get(key) or 0.0)
            for key in ('胜', '平', '负')
        }
        rq_scores[implied_rqspf] += 0.18
        if rq_market.get('outcome') == implied_rqspf:
            rq_scores[implied_rqspf] += 0.08
        if asia_support > 0.08:
            rq_scores['胜'] += 0.04
        elif asia_support < -0.08:
            rq_scores['负'] += 0.04
        if euro_support > 0.08:
            rq_scores['胜'] += 0.03
        elif euro_support < -0.08:
            rq_scores['负'] += 0.03
        predicted_rqspf, rq_confidence = _pick_probability_outcome(rq_scores, min_confidence=0.39, max_confidence=0.86)

        spf_scores = {
            key: float((spf_market.get('probabilities') or {}).get(key) or 0.0)
            for key in ('胜', '平', '负')
        }
        handicap_value = football_utils.parse_int(handicap)
        if (handicap_value or 0) < 0:
            if predicted_rqspf == '胜':
                spf_scores['胜'] += 0.05
            elif predicted_rqspf == '平':
                spf_scores['胜'] += 0.02
                spf_scores['平'] += 0.03
            else:
                spf_scores['平'] += 0.04
                spf_scores['负'] += 0.02
        elif (handicap_value or 0) > 0:
            if predicted_rqspf == '胜':
                spf_scores['胜'] += 0.03
                spf_scores['平'] += 0.02
            elif predicted_rqspf == '平':
                spf_scores['平'] += 0.04
            else:
                spf_scores['负'] += 0.04
        predicted_spf, spf_confidence = _pick_probability_outcome(spf_scores, min_confidence=0.40, max_confidence=0.86)

        confidence_values = [value for value in (spf_confidence, rq_confidence) if value is not None]
        confidence = round(sum(confidence_values) / len(confidence_values), 2) if confidence_values else None
        reasoning_summary = (
            f"SPF主向{predicted_spf or '--'}；让{handicap_text}主向{predicted_rqspf or '--'}；"
            f"盘路一致性{implied_rqspf}，欧赔偏移{euro_support:+.2f}，亚盘偏移{asia_support:+.2f}"
        )
        items_payload.append({
            'event_key': match.get('event_key') or '',
            'match_no': match.get('match_no') or '',
            'predicted_spf': predicted_spf,
            'predicted_rqspf': predicted_rqspf,
            'confidence': confidence,
            'reasoning_summary': reasoning_summary
        })
        debug_rows.append({
            'event_key': match.get('event_key') or '',
            'match_no': match.get('match_no') or '',
            'predicted_spf': predicted_spf,
            'predicted_rqspf': predicted_rqspf,
            'confidence': confidence,
            'spf_probabilities': spf_market.get('probabilities') or {},
            'rqspf_probabilities': rq_market.get('probabilities') or {},
            'expected_margin': round(expected_margin, 4),
            'implied_rqspf': implied_rqspf,
            'euro_support': round(euro_support, 4),
            'asia_support': round(asia_support, 4)
        })

    return items_payload, {
        'algorithm': 'football_handicap_consistency_v1',
        'run_key': run_key,
        'items': debug_rows
    }


def _predict_jingcai_value_edge_v1(run_key: str, matches: list[dict]) -> tuple[list[dict], dict]:
    items_payload = []
    debug_rows = []
    for match in matches:
        detail_bundle = match.get('detail_bundle') or {}
        spf_market = _pick_odds_outcome(match.get('spf_odds') or {})
        rq_market = _pick_odds_outcome(((match.get('rqspf') or {}).get('odds') or {}))
        form_delta = _football_form_delta(match, detail_bundle)
        table_delta = _football_table_delta(detail_bundle)
        injury_delta = _football_injury_delta(detail_bundle)
        market_delta = _football_market_delta(detail_bundle)
        asia_support = _football_asia_market_delta(detail_bundle)
        home_bias = max(
            -0.55,
            min(
                0.55,
                form_delta * 0.40
                + table_delta * 0.32
                + injury_delta * 0.10
                + market_delta * 0.10
            )
        )
        spf_model_probabilities = _apply_spf_bias(spf_market.get('probabilities') or {}, home_bias)
        expected_margin = _estimate_expected_margin(
            spf_model_probabilities,
            form_delta=form_delta,
            table_delta=table_delta,
            injury_delta=injury_delta,
            market_delta=market_delta + asia_support * 0.4
        )
        rq_model_probabilities = _build_rqspf_model_probabilities(
            market_probabilities=rq_market.get('probabilities') or {},
            expected_margin=expected_margin,
            handicap=(match.get('rqspf') or {}).get('handicap'),
            asia_support=asia_support
        )

        spf_value = _select_value_candidate(
            probabilities=spf_model_probabilities,
            odds_map=match.get('spf_odds') or {},
            min_odds=1.55,
            min_edge=0.045,
            min_ev=0.03
        )
        rq_value = _select_value_candidate(
            probabilities=rq_model_probabilities,
            odds_map=((match.get('rqspf') or {}).get('odds') or {}),
            min_odds=1.75,
            min_edge=0.04,
            min_ev=0.04
        )

        predicted_spf = spf_value['outcome']
        predicted_rqspf = rq_value['outcome']
        confidence_values = [value for value in (spf_value['confidence'], rq_value['confidence']) if value is not None]
        confidence = round(sum(confidence_values) / len(confidence_values), 2) if confidence_values else None
        handicap_text = str(((match.get('rqspf') or {}).get('handicap_text') or '')).strip() or '--'
        reasoning_parts = []
        if predicted_spf:
            reasoning_parts.append(
                f"SPF {predicted_spf} edge {spf_value['edge']:+.3f} EV {spf_value['ev']:+.3f}"
            )
        else:
            reasoning_parts.append('SPF 无价值，热门赔率未达阈值或 edge 不足')
        if predicted_rqspf:
            reasoning_parts.append(
                f"让{handicap_text} {predicted_rqspf} edge {rq_value['edge']:+.3f} EV {rq_value['ev']:+.3f}"
            )
        else:
            reasoning_parts.append(f"让{handicap_text} 无价值，盘口优势不足")

        items_payload.append({
            'event_key': match.get('event_key') or '',
            'match_no': match.get('match_no') or '',
            'predicted_spf': predicted_spf,
            'predicted_rqspf': predicted_rqspf,
            'confidence': confidence,
            'reasoning_summary': '；'.join(reasoning_parts)
        })
        debug_rows.append({
            'event_key': match.get('event_key') or '',
            'match_no': match.get('match_no') or '',
            'predicted_spf': predicted_spf,
            'predicted_rqspf': predicted_rqspf,
            'confidence': confidence,
            'spf_model_probabilities': spf_model_probabilities,
            'rq_model_probabilities': rq_model_probabilities,
            'spf_value': spf_value,
            'rq_value': rq_value,
            'expected_margin': round(expected_margin, 4),
            'form_delta': round(form_delta, 4),
            'table_delta': round(table_delta, 4),
            'injury_delta': round(injury_delta, 4),
            'market_delta': round(market_delta, 4),
            'asia_support': round(asia_support, 4)
        })

    return items_payload, {
        'algorithm': 'football_value_edge_v1',
        'run_key': run_key,
        'items': debug_rows
    }


def _pick_odds_outcome(odds_map: dict) -> dict:
    normalized_odds = []
    for outcome in ('胜', '平', '负'):
        odds = football_utils.parse_float((odds_map or {}).get(outcome))
        if odds is not None and odds > 0:
            normalized_odds.append((outcome, odds))

    if not normalized_odds:
        return {
            'outcome': None,
            'odds': None,
            'confidence': None,
            'probabilities': {}
        }

    inverse_sum = sum(1 / odds for _, odds in normalized_odds)
    probabilities = {
        outcome: round((1 / odds) / inverse_sum, 4)
        for outcome, odds in normalized_odds
    }
    ranked = sorted(
        normalized_odds,
        key=lambda item: (probabilities.get(item[0], 0.0), -item[1]),
        reverse=True
    )
    winner_outcome, winner_odds = ranked[0]
    winner_probability = probabilities.get(winner_outcome, 0.0)
    runner_up_probability = probabilities.get(ranked[1][0], 0.0) if len(ranked) > 1 else 0.0
    gap = max(0.0, winner_probability - runner_up_probability)
    confidence = round(max(0.36, min(0.84, 0.34 + winner_probability * 0.28 + gap * 1.2)), 2)
    return {
        'outcome': winner_outcome,
        'odds': winner_odds,
        'confidence': confidence,
        'probabilities': probabilities
    }


def _pc28_value_omission(draws: list[dict], resolver, target) -> int:
    for index, draw in enumerate(draws):
        value = resolver(draw)
        if value is None:
            continue
        if resolver is not None and str(value) == str(target):
            return index
    return len(draws)


def _apply_spf_bias(probabilities: dict, home_bias: float) -> dict[str, float]:
    adjusted = {
        '胜': max(0.02, float(probabilities.get('胜') or 0.0) + home_bias * 0.55),
        '平': max(0.02, float(probabilities.get('平') or 0.0) - abs(home_bias) * 0.28),
        '负': max(0.02, float(probabilities.get('负') or 0.0) - home_bias * 0.55)
    }
    total = sum(adjusted.values()) or 1.0
    return {
        key: round(value / total, 4)
        for key, value in adjusted.items()
    }


def _select_spf_outcome(probabilities: dict[str, float]) -> tuple[str | None, float | None]:
    if not probabilities:
        return None, None
    ranked = sorted(probabilities.items(), key=lambda item: item[1], reverse=True)
    winner_outcome, winner_probability = ranked[0]
    runner_up_probability = ranked[1][1] if len(ranked) > 1 else 0.0
    confidence = round(max(0.38, min(0.87, 0.32 + winner_probability * 0.35 + (winner_probability - runner_up_probability) * 0.9)), 2)
    return winner_outcome, confidence


def _pick_probability_outcome(scores: dict[str, float], min_confidence: float = 0.38, max_confidence: float = 0.87) -> tuple[str | None, float | None]:
    numbers = {
        key: max(0.0, float(value or 0.0))
        for key, value in (scores or {}).items()
    }
    if not any(numbers.values()):
        return None, None
    total = sum(numbers.values()) or 1.0
    normalized = {
        key: value / total
        for key, value in numbers.items()
    }
    ranked = sorted(normalized.items(), key=lambda item: item[1], reverse=True)
    winner_outcome, winner_probability = ranked[0]
    runner_up_probability = ranked[1][1] if len(ranked) > 1 else 0.0
    confidence = round(max(min_confidence, min(max_confidence, min_confidence + winner_probability * 0.32 + (winner_probability - runner_up_probability) * 0.76)), 2)
    return winner_outcome, confidence


def _build_rqspf_model_probabilities(
    market_probabilities: dict[str, float],
    expected_margin: float,
    handicap,
    asia_support: float
) -> dict[str, float]:
    implied_outcome, _ = _select_rqspf_outcome(expected_margin, handicap)
    scores = {
        key: float(market_probabilities.get(key) or 0.0)
        for key in ('胜', '平', '负')
    }
    scores[implied_outcome] += 0.16
    if asia_support > 0.08:
        scores['胜'] += 0.05
    elif asia_support < -0.08:
        scores['负'] += 0.05
    scores['平'] += max(0.0, 0.08 - abs(expected_margin + (football_utils.parse_int(handicap) or 0)) * 0.03)
    total = sum(max(0.01, value) for value in scores.values()) or 1.0
    return {
        key: round(max(0.01, value) / total, 4)
        for key, value in scores.items()
    }


def _select_value_candidate(
    probabilities: dict[str, float],
    odds_map: dict,
    min_odds: float,
    min_edge: float,
    min_ev: float
) -> dict:
    implied_probabilities = _normalized_probability_map(
        (odds_map or {}).get('胜'),
        (odds_map or {}).get('平'),
        (odds_map or {}).get('负')
    )
    best = {
        'outcome': None,
        'model_probability': None,
        'implied_probability': None,
        'odds': None,
        'edge': None,
        'ev': None,
        'confidence': None
    }
    for outcome in ('胜', '平', '负'):
        model_probability = float(probabilities.get(outcome) or 0.0)
        odds = football_utils.parse_float((odds_map or {}).get(outcome))
        if odds is None or odds <= 0:
            continue
        implied_probability = float(implied_probabilities.get(outcome) or 0.0)
        edge = model_probability - implied_probability
        ev = model_probability * odds - 1
        if odds < min_odds:
            continue
        if edge < min_edge or ev < min_ev:
            continue
        confidence = round(max(0.38, min(0.86, 0.36 + model_probability * 0.24 + edge * 1.2 + ev * 0.85)), 2)
        candidate = {
            'outcome': outcome,
            'model_probability': round(model_probability, 4),
            'implied_probability': round(implied_probability, 4),
            'odds': round(odds, 2),
            'edge': round(edge, 4),
            'ev': round(ev, 4),
            'confidence': confidence
        }
        if best['outcome'] is None or candidate['ev'] > best['ev'] or (
            candidate['ev'] == best['ev'] and candidate['edge'] > best['edge']
        ):
            best = candidate
    return best


def _estimate_expected_margin(
    probabilities: dict[str, float],
    form_delta: float,
    table_delta: float,
    injury_delta: float,
    market_delta: float
) -> float:
    if not probabilities:
        return 0.0
    draw_probability = float(probabilities.get('平') or 0.0)
    return (
        (float(probabilities.get('胜') or 0.0) - float(probabilities.get('负') or 0.0)) * 2.6
        + form_delta * 0.95
        + table_delta * 0.78
        + injury_delta * 0.36
        + market_delta * 0.52
        - draw_probability * 0.22
    )


def _select_rqspf_outcome(expected_margin: float, handicap) -> tuple[str, float]:
    handicap_value = football_utils.parse_int(handicap)
    adjusted_margin = expected_margin + (handicap_value or 0)
    if adjusted_margin > 0.38:
        outcome = '胜'
    elif adjusted_margin < -0.38:
        outcome = '负'
    else:
        outcome = '平'
    confidence = round(max(0.36, min(0.84, 0.42 + min(abs(adjusted_margin), 1.6) * 0.18)), 2)
    return outcome, confidence


def _football_form_delta(match: dict, detail_bundle: dict) -> float:
    home_snapshot = _football_recent_form_snapshot(
        items=detail_bundle.get('recent_form_team1') or [],
        team_name=str(match.get('home_team') or ''),
        team_id=match.get('team1_id')
    )
    away_snapshot = _football_recent_form_snapshot(
        items=detail_bundle.get('recent_form_team2') or [],
        team_name=str(match.get('away_team') or ''),
        team_id=match.get('team2_id')
    )
    if not home_snapshot['matches'] or not away_snapshot['matches']:
        return 0.0

    ppm_delta = home_snapshot['points_per_match'] - away_snapshot['points_per_match']
    gd_delta = home_snapshot['goal_diff_per_match'] - away_snapshot['goal_diff_per_match']
    return max(-0.8, min(0.8, ppm_delta * 0.28 + gd_delta * 0.12))


def _football_recent_form_snapshot(items: list[dict], team_name: str, team_id) -> dict:
    team_id_text = str(team_id or '').strip()
    win = draw = loss = goals_for = goals_against = matches = 0
    for item in items[:5]:
        score1 = football_utils.parse_int(item.get('score1'))
        score2 = football_utils.parse_int(item.get('score2'))
        team1 = str(item.get('team1') or '')
        team2 = str(item.get('team2') or '')
        team1_id = str(item.get('team1Id') or '').strip()
        team2_id = str(item.get('team2Id') or '').strip()
        if score1 is None or score2 is None:
            continue
        if team_id_text and team1_id == team_id_text:
            gf, ga = score1, score2
        elif team_id_text and team2_id == team_id_text:
            gf, ga = score2, score1
        elif team1 == team_name:
            gf, ga = score1, score2
        elif team2 == team_name:
            gf, ga = score2, score1
        else:
            continue
        matches += 1
        goals_for += gf
        goals_against += ga
        if gf > ga:
            win += 1
        elif gf == ga:
            draw += 1
        else:
            loss += 1

    if not matches:
        return {
            'matches': 0,
            'points_per_match': 0.0,
            'goal_diff_per_match': 0.0
        }
    points = win * 3 + draw
    return {
        'matches': matches,
        'points_per_match': points / matches,
        'goal_diff_per_match': (goals_for - goals_against) / matches
    }


def _football_table_delta(detail_bundle: dict) -> float:
    team_table = detail_bundle.get('team_table') or {}
    team1_items = ((team_table.get('team1') or {}).get('items') or {})
    team2_items = ((team_table.get('team2') or {}).get('items') or {})
    team1_all = (team1_items.get('all') or [{}])[0]
    team2_all = (team2_items.get('all') or [{}])[0]
    team1_home = (team1_items.get('home') or [{}])[0]
    team2_away = (team2_items.get('away') or [{}])[0]

    home_points = football_utils.parse_float(team1_all.get('points')) or 0.0
    away_points = football_utils.parse_float(team2_all.get('points')) or 0.0
    home_position = football_utils.parse_float(team1_all.get('position')) or 0.0
    away_position = football_utils.parse_float(team2_all.get('position')) or 0.0
    home_home_ppm = _record_points_per_match(team1_home)
    away_away_ppm = _record_points_per_match(team2_away)

    points_delta = (home_points - away_points) / 18 if (home_points or away_points) else 0.0
    position_delta = (away_position - home_position) / 12 if (home_position and away_position) else 0.0
    venue_delta = home_home_ppm - away_away_ppm
    return max(-0.7, min(0.7, points_delta * 0.42 + position_delta * 0.38 + venue_delta * 0.20))


def _record_points_per_match(item: dict) -> float:
    if not item:
        return 0.0
    won = football_utils.parse_float(item.get('won')) or 0.0
    draw = football_utils.parse_float(item.get('draw')) or 0.0
    loss = football_utils.parse_float(item.get('loss')) or 0.0
    matches = won + draw + loss
    if not matches:
        return 0.0
    return (won * 3 + draw) / matches


def _football_injury_delta(detail_bundle: dict) -> float:
    injury = detail_bundle.get('injury') or {}
    home_count = len(injury.get('team1') or [])
    away_count = len(injury.get('team2') or [])
    return max(-0.24, min(0.24, (away_count - home_count) * 0.06))


def _football_market_delta(detail_bundle: dict) -> float:
    snapshots = detail_bundle.get('odds_snapshots') or {}
    euro = snapshots.get('euro') or {}
    initial = euro.get('initial') or {}
    current = euro.get('current') or {}
    home_initial = football_utils.parse_float(initial.get('win'))
    draw_initial = football_utils.parse_float(initial.get('draw'))
    away_initial = football_utils.parse_float(initial.get('lose'))
    home_current = football_utils.parse_float(current.get('win'))
    draw_current = football_utils.parse_float(current.get('draw'))
    away_current = football_utils.parse_float(current.get('lose'))
    initial_prob = _normalized_probability_map(home_initial, draw_initial, away_initial)
    current_prob = _normalized_probability_map(home_current, draw_current, away_current)
    if not initial_prob or not current_prob:
        return 0.0
    return max(
        -0.35,
        min(
            0.35,
            (current_prob.get('胜', 0.0) - initial_prob.get('胜', 0.0))
            - (current_prob.get('负', 0.0) - initial_prob.get('负', 0.0))
        )
    )


def _football_asia_market_delta(detail_bundle: dict) -> float:
    snapshots = detail_bundle.get('odds_snapshots') or {}
    asia = snapshots.get('asia') or {}
    initial = asia.get('initial') or {}
    current = asia.get('current') or {}
    initial_line = _parse_handicap_line(initial.get('line'))
    current_line = _parse_handicap_line(current.get('line'))
    initial_home = football_utils.parse_float(initial.get('home'))
    initial_away = football_utils.parse_float(initial.get('away'))
    current_home = football_utils.parse_float(current.get('home'))
    current_away = football_utils.parse_float(current.get('away'))

    delta = 0.0
    if initial_line is not None and current_line is not None:
        delta += (current_line - initial_line) * 0.22
    if current_home is not None and current_away is not None:
        delta += (current_away - current_home) * 0.10
    elif initial_home is not None and initial_away is not None:
        delta += (initial_away - initial_home) * 0.06
    return max(-0.3, min(0.3, delta))


def _estimate_handicap_consistency_margin(
    spf_probabilities: dict[str, float],
    euro_support: float,
    asia_support: float
) -> float:
    return (
        (float(spf_probabilities.get('胜') or 0.0) - float(spf_probabilities.get('负') or 0.0)) * 1.65
        + euro_support * 0.95
        + asia_support * 0.85
    )


def _parse_handicap_line(value) -> float | None:
    text = str(value or '').strip()
    if not text or text == '--':
        return None
    normalized = text.replace(' ', '').replace('/', '').replace('球', '')
    negative = normalized.startswith('受')
    if negative:
        normalized = normalized[1:]
    mapping = {
        '平手': 0.0,
        '平': 0.0,
        '平半': 0.25,
        '半': 0.5,
        '半一': 0.75,
        '一': 1.0,
        '一球': 1.0,
        '一半': 1.25,
        '一球半': 1.5,
        '球半': 1.5,
        '半两': 1.75,
        '球半两': 1.75,
        '两球': 2.0,
        '两球半': 2.5
    }
    if normalized in mapping:
        number = mapping[normalized]
        return -number if negative else number
    parsed = football_utils.parse_float(normalized)
    if parsed is None:
        return None
    return -parsed if negative else parsed


def _normalized_probability_map(home_odds, draw_odds, away_odds) -> dict[str, float]:
    candidates = [
        ('胜', football_utils.parse_float(home_odds)),
        ('平', football_utils.parse_float(draw_odds)),
        ('负', football_utils.parse_float(away_odds))
    ]
    filtered = [(key, value) for key, value in candidates if value is not None and value > 0]
    if len(filtered) < 2:
        return {}
    inverse_sum = sum(1 / value for _, value in filtered) or 1.0
    return {
        key: (1 / value) / inverse_sum
        for key, value in filtered
    }


def _format_odds_value(value) -> str:
    odds = football_utils.parse_float(value)
    if odds is None:
        return '--'
    return f'{odds:.2f}'
