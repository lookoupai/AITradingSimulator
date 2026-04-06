class PublicPredictorPage {
    constructor() {
        this.predictorId = window.PUBLIC_PREDICTOR_ID;
        this.currentPredictor = window.PUBLIC_PREDICTOR || null;
        this.currentLotteryType = this.currentPredictor?.lottery_type || 'pc28';
        this.selectedProfitRuleId = 'pc28_netdisk';
        this.selectedProfitMetric = null;
        this.selectedProfitBetMode = 'flat';
        this.selectedProfitBaseStake = 10;
        this.selectedProfitMultiplier = 2;
        this.selectedProfitMaxSteps = 6;
        this.selectedProfitOrder = 'desc';
        this.selectedProfitOddsProfile = 'regular';
        this.profitChart = null;
        this.refreshTimer = null;
        this.init();
    }

    async init() {
        if (!this.predictorId) {
            return;
        }
        this.initEventListeners();
        await this.loadDetail(true);
        this.refreshTimer = setInterval(() => this.loadDetail(false), 10000);
    }

    initEventListeners() {
        const ruleSelect = document.getElementById('publicProfitRuleView');
        const metricSelect = document.getElementById('publicProfitMetricView');
        const betModeSelect = document.getElementById('publicProfitBetModeView');
        const baseStakeInput = document.getElementById('publicProfitBaseStakeView');
        const multiplierInput = document.getElementById('publicProfitMultiplierView');
        const maxStepsInput = document.getElementById('publicProfitMaxStepsView');
        const orderSelect = document.getElementById('publicProfitOrderView');
        const oddsSelect = document.getElementById('publicProfitOddsProfileView');

        if (ruleSelect) {
            ruleSelect.addEventListener('change', (event) => {
                this.selectedProfitRuleId = event.target.value;
                this.loadProfitSimulation();
            });
        }

        if (metricSelect) {
            metricSelect.addEventListener('change', (event) => {
                this.selectedProfitMetric = event.target.value;
                this.loadProfitSimulation();
            });
        }

        if (betModeSelect) {
            betModeSelect.addEventListener('change', (event) => {
                this.selectedProfitBetMode = event.target.value;
                this.syncProfitBetControlState();
                this.loadProfitSimulation();
            });
        }

        if (baseStakeInput) {
            baseStakeInput.addEventListener('change', (event) => {
                this.selectedProfitBaseStake = this.normalizePositiveNumber(event.target.value, 10, 0.01);
                event.target.value = String(this.selectedProfitBaseStake);
                this.loadProfitSimulation();
            });
        }

        if (multiplierInput) {
            multiplierInput.addEventListener('change', (event) => {
                this.selectedProfitMultiplier = this.normalizePositiveNumber(event.target.value, 2, 1.01);
                event.target.value = String(this.selectedProfitMultiplier);
                this.loadProfitSimulation();
            });
        }

        if (maxStepsInput) {
            maxStepsInput.addEventListener('change', (event) => {
                this.selectedProfitMaxSteps = this.normalizePositiveInt(event.target.value, 6, 1, 12);
                event.target.value = String(this.selectedProfitMaxSteps);
                this.loadProfitSimulation();
            });
        }

        if (orderSelect) {
            orderSelect.addEventListener('change', (event) => {
                this.selectedProfitOrder = event.target.value;
                this.loadProfitSimulation();
            });
        }

        if (oddsSelect) {
            oddsSelect.addEventListener('change', (event) => {
                this.selectedProfitOddsProfile = event.target.value;
                this.loadProfitSimulation();
            });
        }
    }

    async loadDetail(resetProfitMetric = false) {
        try {
            const response = await fetch(`/api/public/predictors/${this.predictorId}`);
            const data = await response.json();
            if (!response.ok) {
                throw new Error(data.error || '加载公开方案失败');
            }

            this.currentPredictor = data.predictor || {};
            this.currentLotteryType = this.currentPredictor.lottery_type || 'pc28';
            document.getElementById('publicProfitPanel').style.display = this.currentPredictor.capabilities?.supports_profit_simulation ? '' : 'none';
            this.renderHero(data);
            this.renderStats(data.stats);
            this.renderMetricStats(data.stats);
            this.renderStreakStats(data.stats);
            this.renderPredictions(data.recent_predictions || []);
            this.renderProfitControls(this.currentPredictor, resetProfitMetric);
            await this.loadProfitSimulation();
        } catch (error) {
            const hero = document.getElementById('publicPredictorHero');
            if (hero) {
                hero.innerHTML = `<div class="warning-banner">${this.escapeHtml(error.message)}</div>`;
            }
        }
    }

    renderHero(data) {
        if ((data.predictor?.lottery_type || 'pc28') === 'jingcai_football') {
            this.renderFootballHero(data);
            return;
        }
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

    renderFootballHero(data) {
        const hero = document.getElementById('publicPredictorHero');
        const predictor = data.predictor || {};
        const stats = data.stats || {};
        const currentRun = data.current_prediction && Array.isArray(data.current_prediction.items) ? data.current_prediction : null;
        const latestPrediction = data.latest_prediction || {};
        const actualPayload = latestPrediction.actual_payload || {};
        const recommendationTags = currentRun && Array.isArray(currentRun.recommended_tickets) && currentRun.recommended_tickets.length
            ? currentRun.recommended_tickets.map((ticket) => `<span class="tag">${this.escapeHtml(ticket.metric_label || '--')} ${this.escapeHtml(ticket.ticket_text || '--')}</span>`).join('')
            : `
                <span class="tag">胜平负 ${this.escapeHtml(this.buildFootballOutcomeText('spf', (latestPrediction.prediction_payload || {}).spf, latestPrediction.market_snapshot || {}))}</span>
                <span class="tag">让球 ${this.escapeHtml(this.buildFootballOutcomeText('rqspf', (latestPrediction.prediction_payload || {}).rqspf, latestPrediction.market_snapshot || {}))}</span>
                <span class="tag">${this.escapeHtml(actualPayload.score_text || '--')}</span>
            `;

        document.getElementById('publicHeroDescription').textContent = '公开展示该竞彩足球方案的统计表现、推荐票面与收益模拟结果，不展示 API 配置和原始调用数据。';
        document.getElementById('publicStatsSubtitle').textContent = '展示竞彩足球胜平负与让球胜平负的公开统计表现。';

        hero.innerHTML = `
            <div class="hero-metric">
                <span class="mini-label">发布者</span>
                <strong>${this.escapeHtml(predictor.username || '--')}</strong>
            </div>
            <div class="hero-metric">
                <span class="mini-label">彩种 / 主玩法</span>
                <strong>${this.escapeHtml((predictor.lottery_label || '竞彩足球') + ' · ' + (predictor.primary_metric_label || '--'))}</strong>
            </div>
            <div class="hero-metric">
                <span class="mini-label">最近100场表现</span>
                <strong>${this.formatRatioRate(((stats.metrics || {})[stats.primary_metric || 'spf'] || {}).recent_100)}</strong>
            </div>
            <div class="hero-metric">
                <span class="mini-label">最新公开预测</span>
                <div class="badge-row">
                    ${recommendationTags}
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
        const metricOrder = (this.currentLotteryType || 'pc28') === 'jingcai_football'
            ? ['spf', 'rqspf']
            : ['number', 'big_small', 'odd_even', 'combo', 'double_group', 'kill_group'];
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
        const cards = document.getElementById('publicPredictionCards');
        const subtitle = document.getElementById('publicPredictionSubtitle');
        const predictor = this.currentPredictor || null;

        if (subtitle) {
            subtitle.textContent = predictor && predictor.can_view_analysis
                ? '当前公开层级包含预测记录与分析说明。'
                : predictor && predictor.can_view_records
                    ? '当前公开层级包含预测记录，但不包含详细分析说明。'
                    : '当前公开层级仅展示统计，不公开单期预测内容。';
        }

        if (!items.length) {
            tbody.innerHTML = '<tr><td colspan="7" class="empty-cell">当前公开层级未开放预测记录</td></tr>';
            if (cards) {
                cards.innerHTML = '<div class="empty-panel">当前公开层级未开放预测记录</div>';
            }
            return;
        }

        if ((predictor?.lottery_type || 'pc28') === 'jingcai_football') {
            tbody.innerHTML = items.map((item) => `
                <tr>
                    <td>${this.escapeHtml(item.issue_no || '--')}</td>
                    <td>${this.escapeHtml(this.statusLabel(item.status))}</td>
                    <td>${this.renderFootballResultStack(item)}</td>
                    <td>${this.renderFootballActualStack(item.actual_payload || {}, item.status)}</td>
                    <td>${this.renderHitSummary(item)}</td>
                    <td>${this.formatPercent(item.confidence !== null && item.confidence !== undefined ? item.confidence * 100 : null)}</td>
                    <td>${this.escapeHtml(item.reasoning_summary || '无')}</td>
                </tr>
            `).join('');
            if (cards) {
                cards.innerHTML = items.map((item) => {
                    const marketSnapshot = item.market_snapshot || {};
                    const euroSummary = this.buildFootballSnapshotSummary(marketSnapshot);
                    return `
                        <article class="prediction-card">
                            <div class="detail-list">
                                <div class="detail-row"><span class="detail-label">期号</span><strong>${this.escapeHtml(item.issue_no || '--')}</strong></div>
                                <div class="detail-row"><span class="detail-label">状态</span><strong>${this.escapeHtml(this.statusLabel(item.status))}</strong></div>
                                <div class="detail-row"><span class="detail-label">预测</span><div>${this.renderFootballResultStack(item)}</div></div>
                                <div class="detail-row"><span class="detail-label">开奖</span><div>${this.renderFootballActualStack(item.actual_payload || {}, item.status)}</div></div>
                                <div class="detail-row"><span class="detail-label">命中</span><div>${this.renderHitSummary(item)}</div></div>
                                <div class="detail-row"><span class="detail-label">置信度</span><strong>${this.formatPercent(item.confidence !== null && item.confidence !== undefined ? item.confidence * 100 : null)}</strong></div>
                                <div class="detail-row"><span class="detail-label">说明</span><strong>${this.escapeHtml(item.reasoning_summary || '无')}</strong></div>
                            </div>
                            <details class="ai-log-block">
                                <summary>赔率快照与欧赔变化</summary>
                                <div class="detail-list">
                                    <div class="detail-row"><span class="detail-label">SPF赔率</span><strong>${this.escapeHtml(this.buildFootballOddsText('spf', (item.prediction_payload || {}).spf, marketSnapshot) || '--')}</strong></div>
                                    <div class="detail-row"><span class="detail-label">RQSPF赔率</span><strong>${this.escapeHtml(this.buildFootballOddsText('rqspf', (item.prediction_payload || {}).rqspf, marketSnapshot) || '--')}</strong></div>
                                    <div class="detail-row"><span class="detail-label">欧赔变化</span><strong>${this.escapeHtml(euroSummary || '--')}</strong></div>
                                </div>
                            </details>
                        </article>
                    `;
                }).join('');
            }
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
        if (cards) {
            cards.innerHTML = items.map((item) => `
                <article class="prediction-card">
                    <div class="detail-list">
                        <div class="detail-row"><span class="detail-label">期号</span><strong>${this.escapeHtml(item.issue_no || '--')}</strong></div>
                        <div class="detail-row"><span class="detail-label">状态</span><strong>${this.escapeHtml(this.statusLabel(item.status))}</strong></div>
                        <div class="detail-row"><span class="detail-label">预测</span><div>${this.renderResultStack(item.prediction_number, item.prediction_big_small, item.prediction_odd_even, item.prediction_combo)}</div></div>
                        <div class="detail-row"><span class="detail-label">开奖</span><div>${this.renderResultStack(item.actual_number, item.actual_big_small, item.actual_odd_even, item.actual_combo, item.status)}</div></div>
                        <div class="detail-row"><span class="detail-label">命中</span><div>${this.renderHitSummary(item)}</div></div>
                        <div class="detail-row"><span class="detail-label">置信度</span><strong>${this.formatPercent(item.confidence !== null && item.confidence !== undefined ? item.confidence * 100 : null)}</strong></div>
                        <div class="detail-row"><span class="detail-label">说明</span><strong>${this.escapeHtml(item.reasoning_summary || '无')}</strong></div>
                    </div>
                </article>
            `).join('');
        }
    }

    renderProfitControls(predictor, resetMetric = false) {
        const ruleSelect = document.getElementById('publicProfitRuleView');
        const metricSelect = document.getElementById('publicProfitMetricView');
        const betModeSelect = document.getElementById('publicProfitBetModeView');
        const baseStakeInput = document.getElementById('publicProfitBaseStakeView');
        const multiplierInput = document.getElementById('publicProfitMultiplierView');
        const maxStepsInput = document.getElementById('publicProfitMaxStepsView');
        const orderSelect = document.getElementById('publicProfitOrderView');
        const oddsSelect = document.getElementById('publicProfitOddsProfileView');
        const rules = predictor?.profit_rule_options || [];
        const metrics = predictor?.simulation_metrics || [];
        const oddsProfiles = predictor?.odds_profiles || [];

        if (!metrics.length || !rules.length) {
            ruleSelect.innerHTML = '<option value="">暂无规则</option>';
            ruleSelect.disabled = true;
            metricSelect.innerHTML = '<option value="">暂无玩法</option>';
            metricSelect.disabled = true;
            betModeSelect.disabled = true;
            baseStakeInput.disabled = true;
            multiplierInput.disabled = true;
            maxStepsInput.disabled = true;
            orderSelect.disabled = true;
            oddsSelect.disabled = true;
            return;
        }

        if (
            resetMetric ||
            !this.selectedProfitRuleId ||
            !rules.some((item) => item.key === this.selectedProfitRuleId)
        ) {
            this.selectedProfitRuleId = predictor.profit_rule_id || rules[0].key;
        }

        if (
            resetMetric ||
            !this.selectedProfitMetric ||
            !metrics.some((item) => item.key === this.selectedProfitMetric)
        ) {
            this.selectedProfitMetric = predictor.default_simulation_metric || metrics[0].key;
        }

        if (
            !this.selectedProfitOddsProfile ||
            !oddsProfiles.some((item) => item.key === this.selectedProfitOddsProfile)
        ) {
            this.selectedProfitOddsProfile = oddsProfiles[0]?.key || 'regular';
        }

        if (!['flat', 'martingale'].includes(this.selectedProfitBetMode)) {
            this.selectedProfitBetMode = 'flat';
        }
        this.selectedProfitBaseStake = this.normalizePositiveNumber(this.selectedProfitBaseStake, 10, 0.01);
        this.selectedProfitMultiplier = this.normalizePositiveNumber(this.selectedProfitMultiplier, 2, 1.01);
        this.selectedProfitMaxSteps = this.normalizePositiveInt(this.selectedProfitMaxSteps, 6, 1, 12);

        ruleSelect.innerHTML = rules.map((item) => `
            <option value="${this.escapeHtml(item.key)}">${this.escapeHtml(item.label)}</option>
        `).join('');
        ruleSelect.value = this.selectedProfitRuleId;
        ruleSelect.disabled = false;

        metricSelect.innerHTML = metrics.map((item) => `
            <option value="${this.escapeHtml(item.key)}">${this.escapeHtml(item.label)}</option>
        `).join('');
        metricSelect.value = this.selectedProfitMetric;
        metricSelect.disabled = false;
        betModeSelect.value = this.selectedProfitBetMode;
        betModeSelect.disabled = false;
        baseStakeInput.value = String(this.selectedProfitBaseStake);
        baseStakeInput.disabled = false;
        multiplierInput.value = String(this.selectedProfitMultiplier);
        maxStepsInput.value = String(this.selectedProfitMaxSteps);
        orderSelect.value = this.selectedProfitOrder;
        orderSelect.disabled = false;
        oddsSelect.innerHTML = oddsProfiles.map((item) => `
            <option value="${this.escapeHtml(item.key)}">${this.escapeHtml(item.label)}</option>
        `).join('');
        oddsSelect.disabled = false;
        oddsSelect.value = this.selectedProfitOddsProfile;
        this.syncProfitBetControlState();
    }

    async loadProfitSimulation() {
        if (!this.predictorId || !this.currentPredictor) {
            this.renderProfitSimulationEmpty('暂无公开收益模拟数据');
            return;
        }

        const metrics = this.currentPredictor.simulation_metrics || [];
        if (!metrics.length) {
            this.renderProfitSimulationEmpty('当前方案没有可用于收益模拟的玩法');
            return;
        }

        const rules = this.currentPredictor.profit_rule_options || [];
        if (!this.selectedProfitRuleId || !rules.some((item) => item.key === this.selectedProfitRuleId)) {
            this.selectedProfitRuleId = this.currentPredictor.profit_rule_id || rules[0]?.key || 'pc28_netdisk';
            document.getElementById('publicProfitRuleView').value = this.selectedProfitRuleId;
        }

        if (!this.selectedProfitMetric || !metrics.some((item) => item.key === this.selectedProfitMetric)) {
            this.selectedProfitMetric = this.currentPredictor.default_simulation_metric || metrics[0].key;
            document.getElementById('publicProfitMetricView').value = this.selectedProfitMetric;
        }

        try {
            const response = await fetch(
                `/api/public/predictors/${this.predictorId}/simulation?profit_rule_id=${encodeURIComponent(this.selectedProfitRuleId)}&metric=${encodeURIComponent(this.selectedProfitMetric)}&odds_profile=${encodeURIComponent(this.selectedProfitOddsProfile)}&bet_mode=${encodeURIComponent(this.selectedProfitBetMode)}&base_stake=${encodeURIComponent(this.selectedProfitBaseStake)}&multiplier=${encodeURIComponent(this.selectedProfitMultiplier)}&max_steps=${encodeURIComponent(this.selectedProfitMaxSteps)}`
            );
            const data = await response.json();
            if (!response.ok) {
                throw new Error(data.error || '加载收益模拟失败');
            }

            this.renderProfitSimulation(data.simulation, data.can_view_records);
        } catch (error) {
            console.error('Failed to load public profit simulation:', error);
            this.renderProfitSimulationEmpty(error.message);
        }
    }

    renderProfitSimulation(simulation, canViewRecords) {
        const orderedRecords = this.orderProfitRecords(canViewRecords ? (simulation.records || []) : []);
        this.renderProfitSimulationHint(simulation, canViewRecords);
        this.renderProfitSummary(simulation);
        this.renderProfitChart(orderedRecords);
        this.renderProfitTable(orderedRecords, canViewRecords);
    }

    renderProfitSimulationHint(simulation, canViewRecords) {
        const container = document.getElementById('publicProfitSimulationHint');
        const period = simulation.period || {};
        const summary = simulation.summary || {};
        const recordText = canViewRecords
            ? '当前公开层级允许查看单期收益明细。'
            : '当前公开层级仅公开收益汇总，不公开单期收益明细。';
        const periodText = period.start_time && period.end_time
            ? `${period.label || '模拟区间'}：${period.start_time} 至 ${period.end_time}`
            : `${period.label || '模拟区间'}：--`;
        const oddsText = simulation.odds_source_label
            ? `赔率口径：${simulation.odds_source_label}`
            : `赔率盘：${simulation.odds_profile_label || '--'}`;

        container.className = 'metric-hint';
        container.innerHTML = `
            <div class="metric-hint-head">
                <div>
                    <strong>${this.escapeHtml(simulation.profit_rule_label || '--')} · ${this.escapeHtml(simulation.metric_label || '--')} · ${this.escapeHtml(simulation.odds_profile_label || '--')}</strong>
                    <span class="metric-hint-alias">${this.escapeHtml(periodText)}</span>
                </div>
                <span class="tag">${this.escapeHtml(simulation.bet_strategy_label || '--')}</span>
            </div>
            <p>${recordText}</p>
            <p class="metric-hint-foot">基础注 ${this.formatUsd(simulation.bet_config?.base_stake || 0)} · ${this.escapeHtml(oddsText)} · ${this.escapeHtml(simulation.bet_config?.refund_action_label || '--')} · ${this.escapeHtml(simulation.bet_config?.cap_action_label || '--')} · 当前共计下注 ${summary.bet_count || 0} 笔模拟票。</p>
        `;
    }

    renderProfitSummary(simulation) {
        const container = document.getElementById('publicProfitSummaryGrid');
        const summary = simulation.summary || {};
        const cards = [
            ['收益规则', simulation.profit_rule_label || '--'],
            ['当前玩法', simulation.metric_label || '--'],
            ['下注策略', simulation.bet_strategy_label || '--'],
            ['赔率口径', simulation.odds_source_label || simulation.odds_profile_label || '--'],
            ['总下注', this.formatUsd(summary.total_stake || 0)],
            ['净收益', this.formatSignedUsd(summary.net_profit || 0)],
            ['ROI', this.formatPercent(summary.roi_percentage || 0)],
            ['单注平均收益', this.formatSignedUsd(summary.average_profit || 0)],
            ['命中 / 回本 / 未中', `${summary.hit_count || 0} / ${summary.refund_count || 0} / ${summary.miss_count || 0}`]
        ];

        container.innerHTML = cards.map(([label, value]) => `
            <article class="stat-card">
                <span class="stat-label">${this.escapeHtml(label)}</span>
                <strong class="stat-value ${this.profitValueClass(label === 'ROI' ? summary.roi_percentage || 0 : summary.net_profit || 0, label)}">${this.escapeHtml(String(value))}</strong>
            </article>
        `).join('');
    }

    renderProfitChart(records) {
        const chartDom = document.getElementById('publicProfitSimulationChart');
        if (!this.profitChart) {
            this.profitChart = echarts.init(chartDom);
        }

        if (!records.length) {
            this.profitChart.clear();
            this.profitChart.setOption({
                title: {
                    text: '当前公开层级暂无收益曲线',
                    left: 'center',
                    top: 'middle',
                    textStyle: { color: '#64748b', fontSize: 14 }
                }
            });
            return;
        }

        this.profitChart.setOption({
            animation: false,
            tooltip: { trigger: 'axis' },
            grid: { top: 36, left: 36, right: 24, bottom: 36, containLabel: true },
            xAxis: {
                type: 'category',
                inverse: this.selectedProfitOrder === 'desc',
                data: records.map((item) => item.issue_no),
                axisLabel: { color: '#64748b' },
                axisLine: { lineStyle: { color: '#cbd5e1' } }
            },
            yAxis: {
                type: 'value',
                axisLabel: {
                    formatter: (value) => `${value}U`,
                    color: '#64748b'
                },
                splitLine: { lineStyle: { color: '#e2e8f0' } }
            },
            series: [
                {
                    name: '累计盈亏',
                    type: 'line',
                    smooth: true,
                    data: records.map((item) => item.cumulative_profit),
                    lineStyle: { width: 3, color: '#f59e0b' },
                    itemStyle: { color: '#f59e0b' }
                }
            ]
        });
    }

    renderProfitTable(records, canViewRecords) {
        const tbody = document.getElementById('publicProfitSimulationBody');
        const cards = document.getElementById('publicProfitSimulationCards');
        if (!canViewRecords) {
            tbody.innerHTML = '<tr><td colspan="10" class="empty-cell">当前公开层级仅展示收益汇总，不公开单期收益明细</td></tr>';
            if (cards) {
                cards.innerHTML = '<div class="empty-panel">当前公开层级仅展示收益汇总，不公开单期收益明细</div>';
            }
            return;
        }
        if (!records.length) {
            tbody.innerHTML = '<tr><td colspan="10" class="empty-cell">当前区间暂无收益模拟记录</td></tr>';
            if (cards) {
                cards.innerHTML = '<div class="empty-panel">当前区间暂无收益模拟记录</div>';
            }
            return;
        }

        tbody.innerHTML = records.map((item) => `
            <tr>
                <td>${this.escapeHtml(item.issue_no || '--')}</td>
                <td>${this.escapeHtml(item.open_time || '--')}</td>
                <td>${this.escapeHtml(item.ticket_label || '--')}</td>
                <td>${this.escapeHtml(this.formatUsd(item.stake_amount || 0))}<br><span class="hint-text">${this.escapeHtml(item.bet_step_label || '--')}</span></td>
                <td>${this.escapeHtml(item.predicted_value || '--')}</td>
                <td>${this.escapeHtml(item.actual_value || '--')}</td>
                <td>${this.formatOdds(item.odds)}</td>
                <td>${this.renderProfitResult(item)}</td>
                <td><strong class="${this.profitValueClass(item.net_profit)}">${this.escapeHtml(this.formatSignedUsd(item.net_profit))}</strong></td>
                <td><strong class="${this.profitValueClass(item.cumulative_profit)}">${this.escapeHtml(this.formatSignedUsd(item.cumulative_profit))}</strong></td>
            </tr>
        `).join('');
        if (cards) {
            cards.innerHTML = records.map((item) => `
                <article class="prediction-card">
                    <div class="detail-list">
                        <div class="detail-row"><span class="detail-label">期号/批次</span><strong>${this.escapeHtml(item.issue_no || '--')}</strong></div>
                        <div class="detail-row"><span class="detail-label">开奖/开赛时间</span><strong>${this.escapeHtml(item.open_time || '--')}</strong></div>
                        <div class="detail-row"><span class="detail-label">下注内容</span><strong>${this.escapeHtml(item.ticket_label || '--')}</strong></div>
                        <div class="detail-row"><span class="detail-label">下注额/手数</span><strong>${this.escapeHtml(this.formatUsd(item.stake_amount || 0))} / ${this.escapeHtml(item.bet_step_label || '--')}</strong></div>
                        <div class="detail-row"><span class="detail-label">预测值</span><strong>${this.escapeHtml(item.predicted_value || '--')}</strong></div>
                        <div class="detail-row"><span class="detail-label">实际值</span><strong>${this.escapeHtml(item.actual_value || '--')}</strong></div>
                        <div class="detail-row"><span class="detail-label">赔率</span><strong>${this.formatOdds(item.odds)}</strong></div>
                        <div class="detail-row"><span class="detail-label">结果</span><div>${this.renderProfitResult(item)}</div></div>
                        <div class="detail-row"><span class="detail-label">单期盈亏</span><strong class="${this.profitValueClass(item.net_profit)}">${this.escapeHtml(this.formatSignedUsd(item.net_profit))}</strong></div>
                        <div class="detail-row"><span class="detail-label">累计盈亏</span><strong class="${this.profitValueClass(item.cumulative_profit)}">${this.escapeHtml(this.formatSignedUsd(item.cumulative_profit))}</strong></div>
                    </div>
                </article>
            `).join('');
        }
    }

    renderProfitResult(item) {
        const reasonText = item.refund_reason ? `<span class="result-meta-text">${this.escapeHtml(item.refund_reason)}</span>` : '';
        return `
            <div class="hit-summary-block">
                <span class="hit-pill ${this.profitResultClass(item.result_type)}">${this.escapeHtml(item.result_label || '--')}</span>
                ${reasonText}
            </div>
        `;
    }

    renderProfitSimulationEmpty(message) {
        document.getElementById('publicProfitSimulationHint').className = 'metric-hint empty-panel';
        document.getElementById('publicProfitSimulationHint').textContent = message || '暂无收益模拟数据';
        document.getElementById('publicProfitSummaryGrid').innerHTML = '';
        document.getElementById('publicProfitSimulationBody').innerHTML = '<tr><td colspan="10" class="empty-cell">暂无收益模拟数据</td></tr>';
        const cards = document.getElementById('publicProfitSimulationCards');
        if (cards) {
            cards.innerHTML = '<div class="empty-panel">暂无收益模拟数据</div>';
        }
        this.renderProfitChart([]);
    }

    orderProfitRecords(records) {
        const normalized = [...(records || [])];
        if (this.selectedProfitOrder === 'asc') {
            return normalized;
        }
        return normalized.reverse();
    }

    syncProfitBetControlState() {
        const multiplierInput = document.getElementById('publicProfitMultiplierView');
        const maxStepsInput = document.getElementById('publicProfitMaxStepsView');
        const isFlatMode = this.selectedProfitBetMode === 'flat';
        multiplierInput.disabled = isFlatMode;
        maxStepsInput.disabled = isFlatMode;
    }

    normalizePositiveNumber(value, fallback, min = 0.01, max = 1000000) {
        const parsed = Number(value);
        if (!Number.isFinite(parsed)) {
            return fallback;
        }
        return Math.min(max, Math.max(min, Number(parsed.toFixed(2))));
    }

    normalizePositiveInt(value, fallback, min = 1, max = 12) {
        const parsed = Number.parseInt(value, 10);
        if (!Number.isInteger(parsed)) {
            return fallback;
        }
        return Math.min(max, Math.max(min, parsed));
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
                <span class="result-meta-text">大/小：${this.escapeHtml(snapshot.bigSmall || '--')} · 单/双：${this.escapeHtml(snapshot.oddEven || '--')} · 组合结果：${this.escapeHtml(snapshot.combo || '--')}</span>
                <span class="result-meta-text">组合投注：${this.escapeHtml(snapshot.pairTicket || '--')} · 排除对角组：${this.escapeHtml(snapshot.killGroup ? `杀${snapshot.killGroup}` : '--')}</span>
            </div>
        `;
    }

    renderHitSummary(item) {
        if ((this.currentLotteryType || 'pc28') === 'jingcai_football') {
            return this.renderFootballHitSummary(item);
        }
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
        if ((this.currentLotteryType || 'pc28') === 'jingcai_football') {
            const footballMapping = {
                spf: {
                    label: '胜平负',
                    alias: '常规盘',
                    shortRule: '三分类',
                    description: '按 90 分钟含伤停补时赛果统计主胜、平局、客胜。',
                    formula: '预测胜平负 = 实际胜平负'
                },
                rqspf: {
                    label: '让球胜平负',
                    alias: '让球盘',
                    shortRule: '三分类',
                    description: '按官方让球数修正主队比分后统计胜平负。',
                    formula: '预测让球胜平负 = 实际让球胜平负'
                },
                spf_parlay: {
                    label: '胜平负二串一',
                    alias: '默认两场组合',
                    shortRule: '两场全中',
                    description: '先按置信度选出两场，再把两场胜平负票面组合成二串一。',
                    formula: '两场胜平负都命中才算命中'
                },
                rqspf_parlay: {
                    label: '让球胜平负二串一',
                    alias: '默认两场组合',
                    shortRule: '两场全中',
                    description: '先按置信度选出两场，再把两场让球胜平负票面组合成二串一。',
                    formula: '两场让球胜平负都命中才算命中'
                }
            };

            return footballMapping[metricKey] || {
                label: metricKey || '--',
                alias: null,
                shortRule: '未定义',
                description: '当前玩法说明暂未定义。',
                formula: '--'
            };
        }

        const mapping = {
            number: {
                label: '单点',
                alias: '和值号码',
                shortRule: '精确匹配',
                description: '按精确和值投注，直接对应 00-27 的单点玩法。',
                formula: '预测单点 = 开奖和值'
            },
            big_small: {
                label: '大/小',
                alias: null,
                shortRule: '二分类',
                description: '把和值分成小(00-13)与大(14-27)，按单独的大/小玩法结算。',
                formula: '预测大/小 = 开奖大/小'
            },
            odd_even: {
                label: '单/双',
                alias: null,
                shortRule: '二分类',
                description: '按和值奇偶结算，单独对应单/双玩法。',
                formula: '预测单/双 = 开奖单/双'
            },
            combo: {
                label: '组合投注',
                alias: '组合结果为：小双 / 小单 / 大双 / 大单',
                shortRule: '二组选一',
                description: '模型先给出组合结果，再映射到真实可下注的组合投注：小单/大双 或 大单/小双。',
                formula: '预测组合结果映射到对应组合投注票面'
            },
            double_group: {
                label: '组合分组统计',
                alias: '辅助统计口径',
                shortRule: '组别匹配',
                description: '把组合结果合并成两组：大双 / 小单 为“双组”，大单 / 小双 为“单组”。',
                formula: '预测组别 = 开奖组别'
            },
            kill_group: {
                label: '排除统计',
                alias: '旧显示名：杀组',
                shortRule: '排除统计',
                description: '按预测组合结果的对角组合做排除，只要实际没开出被杀组合，就算命中。',
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

    buildHitItems(item) {
        if ((this.currentLotteryType || 'pc28') === 'jingcai_football') {
            const hitPayload = item.hit_payload || {};
            const hitValues = [];
            if (hitPayload.spf !== null && hitPayload.spf !== undefined) {
                hitValues.push({ label: '胜平负', value: hitPayload.spf });
            }
            if (hitPayload.rqspf !== null && hitPayload.rqspf !== undefined) {
                hitValues.push({ label: '让球胜平负', value: hitPayload.rqspf });
            }
            return hitValues;
        }
        const hitValues = [];

        if (item.hit_number !== null && item.hit_number !== undefined) {
            hitValues.push({ label: '单点', value: item.hit_number });
        }
        if (item.hit_big_small !== null && item.hit_big_small !== undefined) {
            hitValues.push({ label: '大/小', value: item.hit_big_small });
        }
        if (item.hit_odd_even !== null && item.hit_odd_even !== undefined) {
            hitValues.push({ label: '单/双', value: item.hit_odd_even });
        }
        if (item.hit_combo !== null && item.hit_combo !== undefined) {
            hitValues.push({ label: '组合结果', value: item.hit_combo });
        }

        const predictedGroup = this.deriveDoubleGroup(item.prediction_combo);
        const actualGroup = this.deriveDoubleGroup(item.actual_combo);
        if (predictedGroup && actualGroup) {
            hitValues.push({ label: '组合分组', value: predictedGroup === actualGroup ? 1 : 0 });
        }

        const killGroup = this.deriveKillGroup(item.prediction_combo);
        if (killGroup && item.actual_combo) {
            hitValues.push({ label: '排除统计', value: item.actual_combo !== killGroup ? 1 : 0 });
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

    formatUsd(value) {
        const amount = Number(value || 0);
        return `${amount.toFixed(2)} USDT`;
    }

    formatSignedUsd(value) {
        const amount = Number(value || 0);
        const prefix = amount > 0 ? '+' : '';
        return `${prefix}${amount.toFixed(2)} USDT`;
    }

    formatOdds(value) {
        if (value === null || value === undefined || value === '') {
            return '--';
        }
        return `${Number(value).toFixed(2)} 倍`;
    }

    profitResultClass(resultType) {
        if (resultType === 'hit') return 'hit';
        if (resultType === 'refund') return 'refund';
        if (resultType === 'miss') return 'miss';
        return 'unknown';
    }

    profitValueClass(value, label = '') {
        const amount = Number(value || 0);
        if (label === '当前玩法' || label === '赔率盘' || label === '总下注' || label === '命中 / 回本 / 未中') {
            return '';
        }
        if (amount > 0) return 'profit-positive';
        if (amount < 0) return 'profit-negative';
        return 'profit-neutral';
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
            '\'': '&#039;'
        };
        return value.replace(/[&<>"']/g, (char) => map[char]);
    }

    renderFootballResultStack(item) {
        const payload = item.prediction_payload || {};
        const marketSnapshot = item.market_snapshot || {};
        const spfText = this.buildFootballOutcomeText('spf', payload.spf, marketSnapshot);
        const rqspfText = this.buildFootballOutcomeText('rqspf', payload.rqspf, marketSnapshot);
        const spfOdds = this.buildFootballOddsText('spf', payload.spf, marketSnapshot);
        const rqspfOdds = this.buildFootballOddsText('rqspf', payload.rqspf, marketSnapshot);
        const snapshotSummary = this.buildFootballSnapshotSummary(marketSnapshot);
        return `
            <div class="result-stack">
                <strong>${this.escapeHtml(item.title || item.issue_no || '--')}</strong>
                <span class="result-meta-text">胜平负：${this.escapeHtml(spfText || '--')} · 让球胜平负：${this.escapeHtml(rqspfText || '--')}</span>
                <span class="result-meta-text">赔率快照：SPF ${this.escapeHtml(spfOdds || '--')} · RQSPF ${this.escapeHtml(rqspfOdds || '--')}</span>
                ${snapshotSummary ? `<span class="result-meta-text">${this.escapeHtml(snapshotSummary)}</span>` : ''}
            </div>
        `;
    }

    renderFootballActualStack(payload, status = 'settled') {
        if (status === 'pending') {
            return '<span class="hint-text">等待赛果</span>';
        }

        return `
            <div class="result-stack">
                <strong>${this.escapeHtml(payload.score_text || '--')}</strong>
                <span class="result-meta-text">胜平负：${this.escapeHtml(payload.spf || '--')} · 让球胜平负：${this.escapeHtml(payload.rqspf || '--')}</span>
            </div>
        `;
    }

    renderFootballHitSummary(item) {
        if (item.status === 'pending') {
            return '<span class="hint-text">未结算</span>';
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

    buildFootballOutcomeText(metricKey, outcome, marketSnapshot = {}) {
        if (!outcome) {
            return '--';
        }
        if (metricKey !== 'rqspf') {
            return outcome;
        }
        const handicapText = ((marketSnapshot.rqspf || {}).handicap_text || '').trim();
        if (!handicapText) {
            return outcome;
        }
        return `${outcome}(让${handicapText})`;
    }

    buildFootballOddsText(metricKey, outcome, marketSnapshot = {}) {
        if (!outcome) {
            return '';
        }
        const rawValue = metricKey === 'rqspf'
            ? ((marketSnapshot.rqspf || {}).odds || {})[outcome]
            : (marketSnapshot.spf_odds || {})[outcome];
        if (rawValue === null || rawValue === undefined || rawValue === '') {
            return '';
        }
        return `${Number(rawValue).toFixed(2)} 倍`;
    }

    buildFootballSnapshotSummary(marketSnapshot = {}) {
        const snapshots = marketSnapshot.odds_snapshots || {};
        const euro = snapshots.euro || {};
        const initial = euro.initial || {};
        const current = euro.current || {};
        const hasEuro = initial.win !== undefined || current.win !== undefined;
        if (!hasEuro) {
            return '';
        }
        return `欧赔 ${this.formatFootballSnapshotTriplet(initial)} -> ${this.formatFootballSnapshotTriplet(current)}`;
    }

    formatFootballSnapshotTriplet(payload = {}) {
        const values = ['win', 'draw', 'lose'].map((key) => {
            const value = payload[key];
            return value === null || value === undefined || value === '' ? '--' : Number(value).toFixed(2);
        });
        return values.join('/');
    }
}

new PublicPredictorPage();
