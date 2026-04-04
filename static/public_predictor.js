class PublicPredictorPage {
    constructor() {
        this.predictorId = window.PUBLIC_PREDICTOR_ID;
        this.init();
    }

    async init() {
        if (!this.predictorId) {
            return;
        }
        await this.loadDetail();
    }

    async loadDetail() {
        try {
            const response = await fetch(`/api/public/predictors/${this.predictorId}`);
            const data = await response.json();
            if (!response.ok) {
                throw new Error(data.error || '加载公开方案失败');
            }

            this.renderHero(data);
            this.renderStats(data.stats);
            this.renderMetricStats(data.stats);
            this.renderStreakStats(data.stats);
            this.renderPredictions(data.recent_predictions || []);
        } catch (error) {
            const hero = document.getElementById('publicPredictorHero');
            if (hero) {
                hero.innerHTML = `<div class="warning-banner">${this.escapeHtml(error.message)}</div>`;
            }
        }
    }

    renderHero(data) {
        const hero = document.getElementById('publicPredictorHero');
        const predictor = data.predictor || {};
        const stats = data.stats || {};
        const primaryMetric = this.targetLabel(stats.primary_metric || predictor.primary_metric || 'combo');
        const currentPrediction = data.current_prediction || data.latest_prediction;
        const primaryStats = ((stats.metrics || {})[stats.primary_metric || 'big_small']) || {};
        const currentSnapshot = this.buildPlaySnapshot(
            currentPrediction?.prediction_number,
            currentPrediction?.prediction_big_small,
            currentPrediction?.prediction_odd_even,
            currentPrediction?.prediction_combo
        );

        hero.innerHTML = `
            <div class="hero-metric">
                <span class="mini-label">发布者</span>
                <strong>${this.escapeHtml(predictor.username || '--')}</strong>
            </div>
            <div class="hero-metric">
                <span class="mini-label">主玩法 / 公开层级</span>
                <strong>${this.escapeHtml(primaryMetric)} · ${this.escapeHtml(predictor.share_level_label || '--')}</strong>
            </div>
            <div class="hero-metric">
                <span class="mini-label">最近100期表现</span>
                <strong>${this.formatRatioRate(primaryStats.recent_100)}</strong>
            </div>
            <div class="hero-metric">
                <span class="mini-label">最新公开预测</span>
                <div class="badge-row">
                    <span class="tag">${this.escapeHtml(currentSnapshot.numberText || '--')}</span>
                    <span class="tag">${this.escapeHtml(currentSnapshot.bigSmall || '--')}</span>
                    <span class="tag">${this.escapeHtml(currentSnapshot.oddEven || '--')}</span>
                    <span class="tag">${this.escapeHtml(currentSnapshot.combo || '--')}</span>
                </div>
            </div>
        `;
    }

    renderStats(stats) {
        const container = document.getElementById('publicStatsGrid');
        const metricKey = stats.primary_metric || 'combo';
        const primaryMetric = (stats.metrics || {})[metricKey] || {};
        const recent20 = primaryMetric.recent_20 || {};
        const recent100 = primaryMetric.recent_100 || {};
        const streaks = stats.streaks || {};

        container.innerHTML = `
            <article class="stat-card">
                <span class="stat-label">主玩法</span>
                <strong class="stat-value">${this.escapeHtml(this.targetLabel(metricKey))}</strong>
            </article>
            <article class="stat-card">
                <span class="stat-label">20期命中率</span>
                <strong class="stat-value">${this.formatRatioRate(recent20)}</strong>
            </article>
            <article class="stat-card">
                <span class="stat-label">100期命中率</span>
                <strong class="stat-value">${this.formatRatioRate(recent100)}</strong>
            </article>
            <article class="stat-card">
                <span class="stat-label">当前连中</span>
                <strong class="stat-value">${streaks.current_hit_streak || 0}</strong>
            </article>
            <article class="stat-card">
                <span class="stat-label">历史最大连中</span>
                <strong class="stat-value">${streaks.historical_max_hit_streak || 0}</strong>
            </article>
            <article class="stat-card">
                <span class="stat-label">已结算期数</span>
                <strong class="stat-value">${stats.settled_predictions || 0}</strong>
            </article>
        `;

        this.renderStatsMetricHint(metricKey);
    }

    renderStatsMetricHint(metricKey) {
        const container = document.getElementById('publicMetricHint');
        if (!container) {
            return;
        }

        const meta = this.metricMeta(metricKey);
        const aliasText = meta.alias ? `<span class="metric-hint-alias">${this.escapeHtml(meta.alias)}</span>` : '';

        container.className = 'metric-hint';
        container.innerHTML = `
            <div class="metric-hint-head">
                <div>
                    <strong>${this.escapeHtml(meta.label)}</strong>
                    ${aliasText}
                </div>
                <span class="tag">${this.escapeHtml(meta.shortRule)}</span>
            </div>
            <p>${this.escapeHtml(meta.description)}</p>
            <p class="metric-hint-foot">统计规则：${this.escapeHtml(meta.formula)}</p>
        `;
    }

    renderMetricStats(stats) {
        const tbody = document.getElementById('publicMetricStatsBody');
        const metricOrder = ['number', 'big_small', 'odd_even', 'combo', 'double_group', 'kill_group'];
        const rows = metricOrder
            .map((key) => ({ key, metric: (stats.metrics || {})[key] }))
            .filter((item) => item.metric);

        if (!rows.length) {
            tbody.innerHTML = '<tr><td colspan="4" class="empty-cell">暂无统计数据</td></tr>';
            return;
        }

        tbody.innerHTML = rows.map(({ key, metric }) => {
            const meta = this.metricMeta(key);
            const aliasText = meta.alias ? `<span class="metric-alias">${this.escapeHtml(meta.alias)}</span>` : '';

            return `
                <tr>
                    <td>
                        <div class="metric-label-stack">
                            <strong>${this.escapeHtml(meta.label)}</strong>
                            ${aliasText}
                        </div>
                    </td>
                    <td>${this.formatRatioRate(metric.recent_20)}</td>
                    <td>${this.formatRatioRate(metric.recent_100)}</td>
                    <td>${this.formatRatioRate(metric.overall)}</td>
                </tr>
            `;
        }).join('');
    }

    renderStreakStats(stats) {
        const container = document.getElementById('publicStreakStats');
        const streaks = stats.streaks || {};

        container.innerHTML = `
            <article class="streak-card">
                <span class="stat-label">当前连中</span>
                <strong class="streak-value">${streaks.current_hit_streak || 0}</strong>
            </article>
            <article class="streak-card">
                <span class="stat-label">当前连挂</span>
                <strong class="streak-value">${streaks.current_miss_streak || 0}</strong>
            </article>
            <article class="streak-card">
                <span class="stat-label">100期最大连中</span>
                <strong class="streak-value">${streaks.recent_100_max_hit_streak || 0}</strong>
            </article>
            <article class="streak-card">
                <span class="stat-label">100期最大连挂</span>
                <strong class="streak-value">${streaks.recent_100_max_miss_streak || 0}</strong>
            </article>
            <article class="streak-card">
                <span class="stat-label">历史最大连中</span>
                <strong class="streak-value">${streaks.historical_max_hit_streak || 0}</strong>
            </article>
            <article class="streak-card">
                <span class="stat-label">历史最大连挂</span>
                <strong class="streak-value">${streaks.historical_max_miss_streak || 0}</strong>
            </article>
        `;
    }

    renderPredictions(items) {
        const tbody = document.getElementById('publicPredictionBody');
        const subtitle = document.getElementById('publicPredictionSubtitle');
        const predictor = window.PUBLIC_PREDICTOR || null;

        if (subtitle) {
            subtitle.textContent = predictor && predictor.can_view_analysis
                ? '当前公开层级包含预测记录与分析说明。'
                : predictor && predictor.can_view_records
                    ? '当前公开层级包含预测记录，但不包含详细分析说明。'
                    : '当前公开层级仅展示统计，不公开单期预测内容。';
        }

        if (!items.length) {
            tbody.innerHTML = '<tr><td colspan="7" class="empty-cell">当前公开层级未开放预测记录</td></tr>';
            return;
        }

        tbody.innerHTML = items.map((item) => `
            <tr>
                <td>${this.escapeHtml(item.issue_no || '--')}</td>
                <td>${this.escapeHtml(this.statusLabel(item.status))}</td>
                <td>${this.renderResultStack(item.prediction_number, item.prediction_big_small, item.prediction_odd_even, item.prediction_combo)}</td>
                <td>${this.renderResultStack(item.actual_number, item.actual_big_small, item.actual_odd_even, item.actual_combo, item.status)}</td>
                <td>${this.renderHitSummary(item)}</td>
                <td>${this.formatPercent(item.confidence !== null && item.confidence !== undefined ? item.confidence * 100 : null)}</td>
                <td>${this.escapeHtml(item.reasoning_summary || '无')}</td>
            </tr>
        `).join('');
    }

    statusLabel(status) {
        const mapping = {
            pending: '待开奖',
            settled: '已结算',
            expired: '过期未结算',
            failed: '执行失败'
        };
        return mapping[status] || status || '--';
    }

    formatPercent(value) {
        if (value === null || value === undefined || value === '') {
            return '--';
        }
        return `${Number(value).toFixed(2)}%`;
    }

    formatRatioRate(stat) {
        if (!stat || !stat.sample_count) {
            return '--';
        }
        return `${stat.hit_count}/${stat.sample_count} (${this.formatPercent(stat.hit_rate)})`;
    }

    renderResultStack(number, bigSmall, oddEven, combo, status = 'settled') {
        const snapshot = this.buildPlaySnapshot(number, bigSmall, oddEven, combo);
        const hasVisibleContent = snapshot.numberText || snapshot.bigSmall || snapshot.oddEven || snapshot.combo;

        if (!hasVisibleContent && status === 'pending') {
            return '<span class="hint-text">等待开奖</span>';
        }
        if (!hasVisibleContent && status === 'expired') {
            return '<span class="hint-text">超出可追溯窗口</span>';
        }
        if (!hasVisibleContent) {
            return '<span class="hint-text">--</span>';
        }

        return `
            <div class="result-stack">
                <strong>${this.escapeHtml(snapshot.numberText || '--')}</strong>
                <span class="result-meta-text">大小：${this.escapeHtml(snapshot.bigSmall || '--')} · 单双：${this.escapeHtml(snapshot.oddEven || '--')} · 四分类组合：${this.escapeHtml(snapshot.combo || '--')}</span>
                <span class="result-meta-text">二选一组：${this.escapeHtml(this.formatDoubleGroupText(snapshot))} · 排除对角组：${this.escapeHtml(snapshot.killGroup ? `杀${snapshot.killGroup}` : '--')}</span>
            </div>
        `;
    }

    renderHitSummary(item) {
        if (item.status === 'pending') {
            return '<span class="hint-text">未结算</span>';
        }
        if (item.status === 'expired') {
            return '<span class="hint-text">未补结算</span>';
        }
        if (item.status === 'failed') {
            return '<span class="hint-text">无结果</span>';
        }

        const hitValues = this.buildHitItems(item);
        if (!hitValues.length) {
            return '<span class="hint-text">无可统计结果</span>';
        }

        return `
            <div class="hit-summary-block">
                <div class="hit-list">
                    ${hitValues.map((hit) => `<span class="hit-pill ${this.hitClass(hit.value)}">${this.escapeHtml(hit.label)} ${this.hitMark(hit.value)}</span>`).join('')}
                </div>
            </div>
        `;
    }

    targetLabel(target) {
        return this.metricMeta(target).label;
    }

    metricMeta(metricKey) {
        const mapping = {
            number: {
                label: '号码',
                alias: '和值号码',
                shortRule: '精确匹配',
                description: '按精确和值是否命中统计，命中率最低，但口径最直接。',
                formula: '预测号码 = 开奖号码'
            },
            big_small: {
                label: '大小',
                alias: null,
                shortRule: '二分类',
                description: '把和值分成小(0-13)与大(14-27)，适合保守口径。',
                formula: '预测大小 = 开奖大小'
            },
            odd_even: {
                label: '单双',
                alias: null,
                shortRule: '二分类',
                description: '按和值奇偶统计，适合和大小并行观察。',
                formula: '预测单双 = 开奖单双'
            },
            combo: {
                label: '四分类组合',
                alias: '旧显示名：组合',
                shortRule: '四分类',
                description: '由大小和单双拼成大单 / 大双 / 小单 / 小双，不是盘口里的二选一组合玩法。',
                formula: '预测四分类组合 = 开奖四分类组合'
            },
            double_group: {
                label: '二选一组',
                alias: '旧显示名：双组',
                shortRule: '组别匹配',
                description: '把四分类组合合并成两组：大双 / 小单 为“双组”，大单 / 小双 为“单组”。',
                formula: '预测组别 = 开奖组别'
            },
            kill_group: {
                label: '排除对角组',
                alias: '旧显示名：杀组',
                shortRule: '排除统计',
                description: '按预测四分类组合的对角组合做排除，只要实际没开出被杀组合，就算命中。',
                formula: '开奖结果 ≠ 被杀组合'
            }
        };

        return mapping[metricKey] || {
            label: metricKey || '--',
            alias: null,
            shortRule: '未定义',
            description: '当前玩法说明暂未定义。',
            formula: '--'
        };
    }

    buildPlaySnapshot(number, bigSmall, oddEven, combo) {
        const resolvedCombo = combo || this.buildCombo(bigSmall, oddEven);
        const numberText = number !== null && number !== undefined
            ? String(number).padStart(2, '0')
            : null;
        const doubleGroup = this.deriveDoubleGroup(resolvedCombo);
        const pairTicket = this.derivePairTicket(resolvedCombo);
        const killGroup = this.deriveKillGroup(resolvedCombo);

        return {
            numberText,
            bigSmall: bigSmall || null,
            oddEven: oddEven || null,
            combo: resolvedCombo || null,
            doubleGroup,
            pairTicket,
            killGroup
        };
    }

    buildCombo(bigSmall, oddEven) {
        if (!bigSmall || !oddEven) {
            return null;
        }
        if (!['大', '小'].includes(bigSmall) || !['单', '双'].includes(oddEven)) {
            return null;
        }
        return `${bigSmall}${oddEven}`;
    }

    deriveDoubleGroup(combo) {
        if (!combo) {
            return null;
        }
        if (combo === '大单' || combo === '小双') {
            return '单组';
        }
        if (combo === '大双' || combo === '小单') {
            return '双组';
        }
        return null;
    }

    derivePairTicket(combo) {
        const group = this.deriveDoubleGroup(combo);
        if (!group) {
            return null;
        }
        return group === '单组' ? '大单 / 小双' : '大双 / 小单';
    }

    deriveKillGroup(combo) {
        const mapping = {
            大单: '小双',
            小双: '大单',
            大双: '小单',
            小单: '大双'
        };
        return mapping[combo] || null;
    }

    formatDoubleGroupText(snapshot) {
        if (!snapshot.doubleGroup) {
            return '--';
        }
        if (!snapshot.pairTicket) {
            return snapshot.doubleGroup;
        }
        return `${snapshot.doubleGroup}（${snapshot.pairTicket}）`;
    }

    buildHitItems(item) {
        const hitValues = [];

        if (item.hit_number !== null && item.hit_number !== undefined) {
            hitValues.push({ label: '号码', value: item.hit_number });
        }
        if (item.hit_big_small !== null && item.hit_big_small !== undefined) {
            hitValues.push({ label: '大小', value: item.hit_big_small });
        }
        if (item.hit_odd_even !== null && item.hit_odd_even !== undefined) {
            hitValues.push({ label: '单双', value: item.hit_odd_even });
        }
        if (item.hit_combo !== null && item.hit_combo !== undefined) {
            hitValues.push({ label: '四分类组合', value: item.hit_combo });
        }

        const predictedGroup = this.deriveDoubleGroup(item.prediction_combo);
        const actualGroup = this.deriveDoubleGroup(item.actual_combo);
        if (predictedGroup && actualGroup) {
            hitValues.push({ label: '二选一组', value: predictedGroup === actualGroup ? 1 : 0 });
        }

        const killGroup = this.deriveKillGroup(item.prediction_combo);
        if (killGroup && item.actual_combo) {
            hitValues.push({ label: '排除对角组', value: item.actual_combo !== killGroup ? 1 : 0 });
        }

        return hitValues;
    }

    hitClass(value) {
        if (value === 1) return 'hit';
        if (value === 0) return 'miss';
        return 'unknown';
    }

    hitMark(value) {
        if (value === 1) return '✓';
        if (value === 0) return '✗';
        return '-';
    }

    escapeHtml(text) {
        if (text === null || text === undefined) {
            return '';
        }
        const value = String(text);
        const map = {
            '&': '&amp;',
            '<': '&lt;',
            '>': '&gt;',
            '"': '&quot;',
            "'": '&#039;'
        };
        return value.replace(/[&<>"']/g, (char) => map[char]);
    }
}

new PublicPredictorPage();
