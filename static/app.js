const PREDICTOR_PRESETS = [
    {
        id: 'statistical',
        title: '概率统计型',
        description: '适合先跑稳定版。偏重和值分布、遗漏、冷热和趋势，不依赖玄学解释。',
        method: '概率统计',
        injectionMode: 'summary',
        historyWindow: 100,
        temperature: 0.4,
        apiMode: 'auto',
        primaryMetric: 'big_small',
        targets: ['number', 'big_small', 'odd_even', 'combo'],
        tags: ['新手友好', '摘要模式', '推荐 100 期'],
        prompt: `角色：\n你是一个 PC28 预测引擎。\n\n目标：\n基于最近 {{history_window}} 期开奖摘要、遗漏统计、今日统计，预测下一期和值与大小单双。\n\n输入：\n{{recent_draws_summary}}\n\n遗漏统计：\n{{omission_summary}}\n\n今日统计：\n{{today_summary}}\n\n要求：\n1. 先分析和值分布、大小比例、单双比例、连续次数、遗漏值。\n2. 再给出下一期预测和值和对应的大小单双组合。\n3. 输出 JSON，不要附加多余解释。`
    },
    {
        id: 'six-ren',
        title: '小六壬型',
        description: '偏重时间起课与象意推断，适合你现在这种“小六壬预测”玩法。',
        method: '小六壬',
        injectionMode: 'raw',
        historyWindow: 80,
        temperature: 0.7,
        apiMode: 'chat_completions',
        primaryMetric: 'big_small',
        targets: ['number', 'big_small', 'odd_even', 'combo'],
        tags: ['原始模式', '时间变量', '玄学玩法'],
        prompt: `角色：\n你是一个 PC28 预测引擎。\n\n方法：\n小六壬 + 基础统计校验。\n\n输入：\n最近 {{history_window}} 期 PC28 数据：\n{{recent_draws_csv}}\n\n起课时间：\n年={{current_year}}\n月={{current_month}}\n日={{current_day}}\n时={{current_hour}}\n分={{current_minute}}\n\n规则：\n1. 先按大安、留连、速喜、赤口、小吉、空亡推演。\n2. 再用历史开奖做交叉验证，避免完全脱离统计。\n3. 输出下一期和值、大小单双、风险和一句策略建议。\n4. 只输出 JSON。`
    },
    {
        id: 'hybrid',
        title: '混合推演型',
        description: '统计模型 + 小六壬 + 概率回归的平衡方案，适合中级用户。',
        method: '统计 + 小六壬 + 回归',
        injectionMode: 'raw',
        historyWindow: 100,
        temperature: 0.55,
        apiMode: 'auto',
        primaryMetric: 'big_small',
        targets: ['number', 'big_small', 'odd_even', 'combo'],
        tags: ['推荐', '原始模式', '平衡型'],
        prompt: `角色：\n你是一个 PC28 预测引擎。\n\n方法：\n统计模型 + 小六壬 + 概率回归。\n\n输入：\n最近 {{history_window}} 期 PC28 数据：\n{{recent_draws_csv}}\n\n补充信息：\n遗漏统计：\n{{omission_summary}}\n\n今日统计：\n{{today_summary}}\n\n起课时间：\n{{current_time_beijing}}\n\n步骤：\n1. 统计和值分布、大小比例、单双比例、连续次数与极端概率。\n2. 计算移动平均、标准差、趋势方向和回归概率。\n3. 小六壬用于修正最终取值方向。\n4. 输出下一期和值、概率、推荐、大小单双、风险、策略。\n5. 只输出 JSON。`
    },
    {
        id: 'conservative',
        title: '保守大小单双型',
        description: '不强追精确和值，只重点判断大小单双与风险，适合风险控制型玩法。',
        method: '保守统计',
        injectionMode: 'summary',
        historyWindow: 60,
        temperature: 0.3,
        apiMode: 'auto',
        primaryMetric: 'big_small',
        targets: ['big_small', 'odd_even', 'combo'],
        tags: ['保守', '摘要模式', '不追和值'],
        prompt: `角色：\n你是 PC28 保守预测助手。\n\n输入：\n{{recent_draws_summary}}\n\n遗漏统计：\n{{omission_summary}}\n\n要求：\n1. 不强行预测精确和值。\n2. 重点输出大小、单双、组合和风险。\n3. 如果信号不明确，降低 confidence。\n4. 只输出 JSON。`
    },
    {
        id: 'extreme',
        title: '极值回归型',
        description: '关注大值、小值、连开和遗漏回补，适合喜欢追波动的激进玩家。',
        method: '极值回归',
        injectionMode: 'raw',
        historyWindow: 120,
        temperature: 0.65,
        apiMode: 'chat_completions',
        primaryMetric: 'kill_group',
        targets: ['number', 'big_small', 'odd_even', 'combo'],
        tags: ['激进', '120 期', '极值/遗漏'],
        prompt: `角色：\n你是一个擅长极值回归判断的 PC28 预测助手。\n\n输入：\n{{recent_draws_csv}}\n\n额外信息：\n{{omission_summary}}\n\n任务：\n1. 重点分析极端和值、长遗漏号码、连续大小单双。\n2. 判断下一期是否存在回归或继续延续的概率。\n3. 输出一个主预测和一句激进策略建议。\n4. 只输出 JSON。`
    }
];

class PredictionApp {
    constructor() {
        this.currentUser = null;
        this.currentPredictorId = null;
        this.currentPredictor = null;
        this.currentPredictions = [];
        this.currentStats = null;
        this.selectedStatsMetric = null;
        this.selectedProfitRuleId = 'pc28_netdisk';
        this.selectedProfitMetric = null;
        this.selectedProfitOddsProfile = 'regular';
        this.overview = null;
        this.chart = null;
        this.profitChart = null;
        this.refreshTimer = null;
        this.darkMode = localStorage.getItem('pc28Theme') === 'dark';
        this.presetExpanded = false;
        this.predictionStatusFilter = 'all';
        this.predictionOutcomeFilter = 'all';
        this.init();
    }

    async init() {
        this.applyTheme();
        this.initEventListeners();
        await this.checkAuth();
        await this.refresh(true);
        this.refreshTimer = setInterval(() => this.refresh(), 10000);
    }

    initEventListeners() {
        document.getElementById('themeToggle').addEventListener('click', () => this.toggleTheme());
        document.getElementById('refreshBtn').addEventListener('click', () => this.refresh(true));
        document.getElementById('logoutBtn').addEventListener('click', () => this.logout());
        document.getElementById('addPredictorBtn').addEventListener('click', () => this.openCreateModal());
        document.getElementById('closeModalBtn').addEventListener('click', () => this.hideModal());
        document.getElementById('cancelModalBtn').addEventListener('click', () => this.hideModal());
        document.getElementById('savePredictorBtn').addEventListener('click', () => this.submitPredictor());
        document.getElementById('testPredictorBtn').addEventListener('click', () => this.testPredictorConfig());
        document.getElementById('checkPromptBtn').addEventListener('click', () => this.checkPromptAssistant());
        document.getElementById('optimizePromptBtn').addEventListener('click', () => this.optimizePromptAssistant());
        document.getElementById('applyOptimizedPromptBtn').addEventListener('click', () => this.applyOptimizedPrompt());
        document.getElementById('predictNowBtn').addEventListener('click', () => this.predictNow());
        document.getElementById('editPredictorBtn').addEventListener('click', () => this.openEditModal());
        document.getElementById('togglePredictorBtn').addEventListener('click', () => this.toggleCurrentPredictor());
        document.getElementById('togglePresetListBtn').addEventListener('click', () => this.togglePresetList());
        document.getElementById('statsMetricView').addEventListener('change', (event) => {
            this.selectedStatsMetric = event.target.value;
            if (this.currentStats) {
                this.renderStats(this.currentStats);
            }
        });
        document.getElementById('primaryMetric').addEventListener('change', () => this.syncProfitMetricOptions());
        ['targetNumber', 'targetBigSmall', 'targetOddEven', 'targetCombo'].forEach((id) => {
            document.getElementById(id).addEventListener('change', () => this.syncProfitMetricOptions());
        });
        document.getElementById('predictionStatusFilter').addEventListener('change', (event) => {
            this.predictionStatusFilter = event.target.value;
            this.renderPredictionsTable(this.currentPredictions || []);
        });
        document.getElementById('predictionOutcomeFilter').addEventListener('change', (event) => {
            this.predictionOutcomeFilter = event.target.value;
            this.renderPredictionsTable(this.currentPredictions || []);
        });
        document.getElementById('profitRuleView').addEventListener('change', (event) => {
            this.selectedProfitRuleId = event.target.value;
            this.loadProfitSimulation();
        });
        document.getElementById('profitMetricView').addEventListener('change', (event) => {
            this.selectedProfitMetric = event.target.value;
            this.loadProfitSimulation();
        });
        document.getElementById('profitOddsProfileView').addEventListener('change', (event) => {
            this.selectedProfitOddsProfile = event.target.value;
            this.loadProfitSimulation();
        });
        document.getElementById('publicSharePanel').addEventListener('click', (event) => {
            const button = event.target.closest('[data-action="copy-public-url"]');
            if (!button) {
                return;
            }
            this.copyPublicUrl(button);
        });

        document.querySelectorAll('.tab-btn').forEach((button) => {
            button.addEventListener('click', (event) => this.switchTab(event.currentTarget.dataset.tab));
        });

        document.getElementById('predictorModal').addEventListener('click', (event) => {
            if (event.target.id === 'predictorModal') {
                this.hideModal();
            }
        });

        this.renderPresetCards();
    }

    async checkAuth() {
        const response = await fetch('/api/auth/me', { credentials: 'include' });
        if (!response.ok) {
            window.location.href = '/login';
            return;
        }

        this.currentUser = await response.json();
        document.getElementById('userInfo').textContent = `当前用户：${this.currentUser.username}`;
        const adminEntry = document.getElementById('adminEntryBtn');
        if (adminEntry) {
            adminEntry.style.display = this.currentUser.is_admin ? 'inline-flex' : 'none';
        }
    }

    async refresh(forceReloadPredictorList = false) {
        await this.loadOverview();
        await this.loadPredictors(forceReloadPredictorList);
        if (this.currentPredictorId) {
            await this.loadPredictorDashboard(this.currentPredictorId);
        } else {
            this.renderEmptyPredictorState();
        }
    }

    async loadOverview() {
        try {
            const response = await fetch('/api/pc28/overview?limit=20');
            const overview = await response.json();
            this.overview = overview;
            this.renderOverview(overview);
        } catch (error) {
            console.error('Failed to load overview:', error);
        }
    }

    renderOverview(overview) {
        const latestDraw = overview.latest_draw;
        const warning = overview.warning ? `<div class="warning-banner">${this.escapeHtml(overview.warning)}</div>` : '';
        const topOmissions = (overview.omission_preview?.top_numbers || []).slice(0, 4);
        const hotNumbers = (overview.today_preview?.hot_numbers || []).slice(0, 4);

        document.getElementById('overviewPanel').innerHTML = `
            ${warning}
            <div class="overview-primary">
                <div>
                    <span class="mini-label">下一期期号</span>
                    <strong>${this.escapeHtml(overview.next_issue_no || '--')}</strong>
                </div>
                <div>
                    <span class="mini-label">倒计时</span>
                    <strong>${this.escapeHtml(overview.countdown || '--:--:--')}</strong>
                </div>
            </div>
            <div class="overview-result">
                <span class="mini-label">最新开奖</span>
                <div class="result-number">${latestDraw ? latestDraw.result_number_text : '--'}</div>
                <div class="badge-row">
                    ${latestDraw ? this.renderBadge(latestDraw.big_small) : ''}
                    ${latestDraw ? this.renderBadge(latestDraw.odd_even) : ''}
                    ${latestDraw ? this.renderBadge(latestDraw.combo) : ''}
                </div>
                <div class="result-meta">${latestDraw ? `第 ${latestDraw.issue_no} 期 · ${this.escapeHtml(latestDraw.open_time || '')}` : '暂无数据'}</div>
            </div>
            <div class="overview-block">
                <span class="mini-label">高遗漏号码</span>
                <div class="tag-list compact">
                    ${topOmissions.length ? topOmissions.map((item) => `<span class="tag">${item.label} · ${item.value}期</span>`).join('') : '<span class="tag">暂无</span>'}
                </div>
            </div>
            <div class="overview-block">
                <span class="mini-label">今日热号</span>
                <div class="tag-list compact">
                    ${hotNumbers.length ? hotNumbers.map((item) => `<span class="tag">${item.label} · ${item.value}次</span>`).join('') : '<span class="tag">暂无</span>'}
                </div>
            </div>
        `;

        this.renderDrawsTable(overview.recent_draws || []);
    }

    async loadPredictors(forceReload = false) {
        try {
            const response = await fetch('/api/predictors', { credentials: 'include' });
            if (response.status === 401) {
                window.location.href = '/login';
                return;
            }

            const predictors = await response.json();
            this.predictors = predictors;

            if (!predictors.length) {
                this.currentPredictorId = null;
                this.currentPredictor = null;
                this.renderPredictorList([]);
                return;
            }

            if (!this.currentPredictorId || forceReload) {
                const exists = predictors.some((item) => item.id === this.currentPredictorId);
                if (!exists) {
                    this.currentPredictorId = predictors[0].id;
                }
            }

            this.renderPredictorList(predictors);
        } catch (error) {
            console.error('Failed to load predictors:', error);
        }
    }

    renderPredictorList(predictors) {
        const container = document.getElementById('predictorList');
        if (!predictors.length) {
            container.innerHTML = '<div class="empty-panel">暂无预测方案</div>';
            return;
        }

        container.innerHTML = predictors.map((predictor) => `
            <div class="predictor-item ${predictor.id === this.currentPredictorId ? 'active' : ''}" data-id="${predictor.id}">
                <div class="predictor-head">
                    <div>
                        <div class="predictor-name">${this.escapeHtml(predictor.name)}</div>
                        <div class="predictor-meta">${this.escapeHtml(predictor.model_name)} · ${this.escapeHtml(predictor.prediction_method || '自定义策略')}</div>
                    </div>
                    <span class="status-chip ${predictor.enabled ? 'enabled' : 'disabled'}">${predictor.enabled ? '启用' : '停用'}</span>
                </div>
                <div class="predictor-tags">
                    ${(predictor.prediction_targets || []).map((target) => `<span class="tag">${this.escapeHtml(this.targetLabel(target))}</span>`).join('')}
                </div>
                <div class="predictor-actions">
                    <button class="icon-btn" data-action="toggle" data-id="${predictor.id}" title="${predictor.enabled ? '暂停方案' : '恢复方案'}">
                        <i class="bi ${predictor.enabled ? 'bi-pause-circle' : 'bi-play-circle'}"></i>
                    </button>
                    <button class="icon-btn" data-action="edit" data-id="${predictor.id}" title="编辑方案"><i class="bi bi-pencil"></i></button>
                    <button class="icon-btn danger" data-action="delete" data-id="${predictor.id}" title="删除方案"><i class="bi bi-trash"></i></button>
                </div>
            </div>
        `).join('');

        container.querySelectorAll('.predictor-item').forEach((item) => {
            item.addEventListener('click', (event) => {
                const actionButton = event.target.closest('button[data-action]');
                if (actionButton) {
                    return;
                }
                this.selectPredictor(Number(item.dataset.id));
            });
        });

        container.querySelectorAll('button[data-action="toggle"]').forEach((button) => {
            button.addEventListener('click', (event) => {
                event.stopPropagation();
                const predictorId = Number(button.dataset.id);
                const predictor = (this.predictors || []).find((item) => item.id === predictorId);
                if (!predictor) {
                    return;
                }
                this.togglePredictorStatus(predictorId, !predictor.enabled);
            });
        });

        container.querySelectorAll('button[data-action="edit"]').forEach((button) => {
            button.addEventListener('click', (event) => {
                event.stopPropagation();
                this.selectPredictor(Number(button.dataset.id), false);
                this.openEditModal(Number(button.dataset.id));
            });
        });

        container.querySelectorAll('button[data-action="delete"]').forEach((button) => {
            button.addEventListener('click', (event) => {
                event.stopPropagation();
                this.deletePredictor(Number(button.dataset.id));
            });
        });
    }

    async selectPredictor(predictorId, shouldLoad = true) {
        this.currentPredictorId = predictorId;
        this.renderPredictorList(this.predictors || []);
        if (shouldLoad) {
            await this.loadPredictorDashboard(predictorId);
        }
    }

    async loadPredictorDashboard(predictorId) {
        try {
            const previousPredictorId = this.currentPredictor?.id || null;
            const response = await fetch(`/api/predictors/${predictorId}/dashboard`, {
                credentials: 'include'
            });
            if (response.status === 401) {
                window.location.href = '/login';
                return;
            }
            const data = await response.json();
            if (!response.ok) {
                throw new Error(data.error || '加载方案详情失败');
            }

            this.currentPredictor = data.predictor;
            this.currentStats = data.stats || null;
            this.currentPredictions = data.recent_predictions || [];
            const availableMetrics = Object.keys((data.stats && data.stats.metrics) || {});
            const defaultMetric = data.stats?.primary_metric || 'big_small';
            const shouldResetMetric =
                previousPredictorId !== predictorId ||
                !this.selectedStatsMetric ||
                !availableMetrics.includes(this.selectedStatsMetric);

            if (shouldResetMetric) {
                this.selectedStatsMetric = defaultMetric;
            }
            document.getElementById('statsMetricView').value = this.selectedStatsMetric;
            this.updatePredictorActionState(data.predictor);
            this.renderStats(this.currentStats);
            this.renderCurrentPrediction(data.current_prediction, data.latest_prediction, data.predictor);
            this.renderPublicSharePanel(data.predictor);
            this.renderProfitControls(data.predictor, previousPredictorId !== predictorId);
            await this.loadProfitSimulation();
            this.renderPredictionsTable(this.currentPredictions);
            this.renderDrawsTable(data.overview?.recent_draws || data.recent_draws || []);
            this.renderAILogs(this.currentPredictions);
            this.renderChart(this.currentPredictions);
        } catch (error) {
            console.error('Failed to load predictor dashboard:', error);
        }
    }

    renderStats(stats) {
        const container = document.getElementById('statsGrid');
        const currentMetricKey = this.selectedStatsMetric || stats.primary_metric || 'big_small';
        const currentMetric = (stats.metrics || {})[currentMetricKey] || {};
        const recent20 = currentMetric.recent_20 || {};
        const recent100 = currentMetric.recent_100 || {};
        const streaks = this.resolveMetricStreaks(stats, currentMetricKey);
        const metricLabel = this.targetLabel(currentMetricKey);

        container.innerHTML = `
            <article class="stat-card">
                <span class="stat-label">总预测次数</span>
                <strong class="stat-value">${stats.total_predictions || 0}</strong>
            </article>
            <article class="stat-card">
                <span class="stat-label">已结算期数</span>
                <strong class="stat-value">${stats.settled_predictions || 0}</strong>
            </article>
            <article class="stat-card">
                <span class="stat-label">过期未结算</span>
                <strong class="stat-value">${stats.expired_predictions || 0}</strong>
            </article>
            <article class="stat-card">
                <span class="stat-label">当前查看玩法</span>
                <strong class="stat-value">${this.escapeHtml(metricLabel || '--')}</strong>
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
                <span class="stat-label">当前连中 / 连挂</span>
                <strong class="stat-value">${streaks.current_hit_streak || 0} / ${streaks.current_miss_streak || 0}</strong>
            </article>
        `;

        this.renderStatsMetricHint(currentMetricKey);
        this.renderMetricStats(stats.metrics || {});
        this.renderStreakStats(stats, currentMetricKey);
    }

    renderStatsMetricHint(metricKey) {
        const container = document.getElementById('statsMetricHint');
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

    renderMetricStats(metrics) {
        const tbody = document.getElementById('metricStatsBody');
        const metricOrder = ['number', 'big_small', 'odd_even', 'combo', 'double_group', 'kill_group'];
        const rows = metricOrder
            .map((key) => ({ key, metric: metrics[key] }))
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

    renderStreakStats(stats, metricKey) {
        const container = document.getElementById('streakStats');
        const streaks = this.resolveMetricStreaks(stats, metricKey);
        const metricLabel = this.targetLabel(metricKey);

        container.innerHTML = `
            <article class="streak-card">
                <span class="stat-label">当前查看玩法</span>
                <strong class="streak-value">${this.escapeHtml(metricLabel)}</strong>
            </article>
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

    resolveMetricStreaks(stats, metricKey) {
        const metricStreaks = ((stats.metric_streaks || {})[metricKey]) || null;
        return metricStreaks || stats.streaks || {};
    }

    renderCurrentPrediction(currentPrediction, latestPrediction, predictor) {
        const container = document.getElementById('currentPrediction');

        if (!predictor) {
            container.className = 'prediction-summary empty-panel';
            container.textContent = '请选择预测方案';
            this.updatePredictorActionState(null);
            return;
        }

        const targetTags = (predictor.prediction_targets || []).map((item) => this.renderBadge(this.targetLabel(item))).join('');
        const prediction = currentPrediction || latestPrediction;

        if (!prediction) {
            container.className = 'prediction-summary empty-panel';
            container.innerHTML = `
                <div class="summary-head">
                    <div>
                        <h4>${this.escapeHtml(predictor.name)}</h4>
                        <p>${this.escapeHtml(predictor.model_name)} · ${this.escapeHtml(predictor.prediction_method || '自定义策略')}</p>
                    </div>
                    <div class="badge-row">${targetTags}</div>
                </div>
                <p>当前尚无预测记录，点击“立即预测”或等待自动轮询。</p>
            `;
            return;
        }

        const statusText = this.predictionStatusLabel(prediction.status);
        const errorBlock = prediction.error_message
            ? `<div class="warning-banner">${this.escapeHtml(prediction.error_message)}</div>`
            : '';
        const predictionSnapshot = this.buildPlaySnapshot(
            prediction.prediction_number,
            prediction.prediction_big_small,
            prediction.prediction_odd_even,
            prediction.prediction_combo
        );
        const actualSnapshot = this.buildPlaySnapshot(
            prediction.actual_number,
            prediction.actual_big_small,
            prediction.actual_odd_even,
            prediction.actual_combo
        );
        const confidenceText = this.formatPercent(
            prediction.confidence !== null && prediction.confidence !== undefined ? prediction.confidence * 100 : null
        );

        container.className = 'prediction-summary';
        container.innerHTML = `
            <div class="summary-head">
                <div>
                    <h4>${this.escapeHtml(predictor.name)}</h4>
                    <p>${this.escapeHtml(predictor.model_name)} · ${this.escapeHtml(predictor.prediction_method || '自定义策略')}</p>
                </div>
                <div class="badge-row">
                    ${targetTags}
                    <span class="status-chip ${prediction.status}">${statusText}</span>
                </div>
            </div>
            ${errorBlock}
            <div class="metric-hint prediction-note">
                <div class="metric-hint-head">
                    <div>
                        <strong>展示口径说明</strong>
                    </div>
                    <span class="tag">仅展示模型命中口径</span>
                </div>
                <p>组合结果由大/小和单/双拼装而成；组合投注与排除对角组为统计派生口径，不代表模型额外输出了新的独立下注项。</p>
                <p class="metric-hint-foot">当前页面不会直接折算赔率、回本或真实盘口收益。</p>
            </div>
            <div class="prediction-section">
                <div class="prediction-section-head">
                    <div>
                        <span class="mini-label">本期原始输出</span>
                        <p class="section-hint">先看模型直接输出的号码和基础标签。</p>
                    </div>
                </div>
                <div class="prediction-grid prediction-grid-compact">
                    ${this.renderCurrentPredictionCard('预测期号', prediction.issue_no || '--')}
                    ${this.renderCurrentPredictionCard('预测号码', predictionSnapshot.numberText || '--')}
                    ${this.renderCurrentPredictionCard('大/小', predictionSnapshot.bigSmall || '--')}
                    ${this.renderCurrentPredictionCard('单/双', predictionSnapshot.oddEven || '--')}
                    ${this.renderCurrentPredictionCard('组合结果', predictionSnapshot.combo || '--', predictionSnapshot.combo ? '组合结果固定为小双 / 小单 / 大双 / 大单' : '当前无可用组合标签')}
                    ${this.renderCurrentPredictionCard('置信度', confidenceText)}
                </div>
            </div>
            <div class="prediction-section">
                <div class="prediction-section-head">
                    <div>
                        <span class="mini-label">派生玩法映射</span>
                        <p class="section-hint">把组合结果翻译成真实可下注的组合投注分组。</p>
                    </div>
                </div>
                <div class="prediction-grid prediction-grid-compact">
                    ${this.renderCurrentPredictionCard('组合分组', predictionSnapshot.doubleGroup || '--', predictionSnapshot.doubleGroup ? '用于辅助解释组合投注的分组口径' : '依赖组合结果')}
                    ${this.renderCurrentPredictionCard('组合投注', predictionSnapshot.pairTicket || '--', predictionSnapshot.pairTicket ? '真实下注按这个组合票面结算' : '依赖组合结果')}
                    ${this.renderCurrentPredictionCard('排除对角组', predictionSnapshot.killGroup ? `杀${predictionSnapshot.killGroup}` : '--', predictionSnapshot.killGroup ? '实际未开出该组合即计为命中' : '依赖组合结果')}
                    ${this.renderCurrentPredictionCard('更新时间', prediction.updated_at || prediction.created_at || '--')}
                </div>
            </div>
            ${this.renderCurrentSettlement(prediction, predictionSnapshot, actualSnapshot)}
            <div class="summary-foot">
                <div>
                    <span class="mini-label">简要说明</span>
                    <p>${this.escapeHtml(prediction.reasoning_summary || '无')}</p>
                </div>
                <div>
                    <span class="mini-label">更新时间</span>
                    <p>${this.escapeHtml(prediction.updated_at || prediction.created_at || '--')}</p>
                </div>
            </div>
        `;
    }

    renderPublicSharePanel(predictor) {
        const container = document.getElementById('publicSharePanel');
        if (!container) {
            return;
        }

        if (!predictor) {
            container.className = 'prediction-summary empty-panel';
            container.textContent = '请选择预测方案';
            return;
        }

        const publicUrl = predictor.public_url || predictor.public_path || '--';
        const isAvailable = Boolean(predictor.public_page_available);
        const availabilityText = isAvailable ? '公开页可访问' : '方案已停用，公开页当前不可访问';
        const availabilityHint = isAvailable
            ? '当前地址可直接分享给访客；访客能看到的内容取决于公开层级。'
            : '启用自动预测后，公开页才会重新出现在首页榜单和公开详情中。';
        const disabledAttr = isAvailable ? '' : 'disabled';
        const openButton = isAvailable
            ? `<a class="btn ghost compact" href="${this.escapeHtml(publicUrl)}" target="_blank" rel="noopener noreferrer">打开公开页</a>`
            : '<button class="btn ghost compact" disabled>打开公开页</button>';

        container.className = 'prediction-summary';
        container.innerHTML = `
            <div class="summary-head">
                <div>
                    <h4>${this.escapeHtml(predictor.name || '--')}</h4>
                    <p>${this.escapeHtml(predictor.share_level_label || '--')} · ${this.escapeHtml(availabilityText)}</p>
                </div>
                <div class="badge-row">
                    <span class="tag">${this.escapeHtml(this.targetLabel(predictor.primary_metric || 'combo'))}</span>
                    <span class="tag">${this.escapeHtml(predictor.share_level_label || '--')}</span>
                </div>
            </div>
            <div class="prediction-card">
                <span class="mini-label">公开地址</span>
                <strong class="share-link-text">${this.escapeHtml(publicUrl)}</strong>
                <span class="card-hint">${this.escapeHtml(availabilityHint)}</span>
            </div>
            <div class="share-panel-actions">
                <button class="btn primary compact" data-action="copy-public-url" data-url="${this.escapeHtml(publicUrl)}" ${disabledAttr}>复制地址</button>
                ${openButton}
            </div>
        `;
    }

    renderProfitControls(predictor, resetMetric = false) {
        const ruleSelect = document.getElementById('profitRuleView');
        const metricSelect = document.getElementById('profitMetricView');
        const oddsSelect = document.getElementById('profitOddsProfileView');
        const rules = predictor?.profit_rule_options || [];
        const metrics = predictor?.simulation_metrics || [];
        const oddsProfiles = predictor?.odds_profiles || [];

        if (!metrics.length || !rules.length) {
            ruleSelect.innerHTML = '<option value="">暂无规则</option>';
            ruleSelect.disabled = true;
            metricSelect.innerHTML = '<option value="">暂无玩法</option>';
            metricSelect.disabled = true;
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
        oddsSelect.innerHTML = oddsProfiles.map((item) => `
            <option value="${this.escapeHtml(item.key)}">${this.escapeHtml(item.label)}</option>
        `).join('');
        oddsSelect.disabled = false;
        oddsSelect.value = this.selectedProfitOddsProfile;
    }

    async loadProfitSimulation() {
        const predictor = this.currentPredictor;
        if (!predictor || !this.currentPredictorId) {
            this.renderProfitSimulationEmpty('请选择预测方案');
            return;
        }

        const metrics = predictor.simulation_metrics || [];
        if (!metrics.length) {
            this.renderProfitSimulationEmpty('当前方案没有可用于收益模拟的玩法');
            return;
        }

        const rules = predictor.profit_rule_options || [];
        if (!this.selectedProfitRuleId || !rules.some((item) => item.key === this.selectedProfitRuleId)) {
            this.selectedProfitRuleId = predictor.profit_rule_id || rules[0]?.key || 'pc28_netdisk';
            document.getElementById('profitRuleView').value = this.selectedProfitRuleId;
        }

        if (!this.selectedProfitMetric || !metrics.some((item) => item.key === this.selectedProfitMetric)) {
            this.selectedProfitMetric = predictor.default_simulation_metric || metrics[0].key;
            document.getElementById('profitMetricView').value = this.selectedProfitMetric;
        }

        try {
            const response = await fetch(
                `/api/predictors/${this.currentPredictorId}/simulation?profit_rule_id=${encodeURIComponent(this.selectedProfitRuleId)}&metric=${encodeURIComponent(this.selectedProfitMetric)}&odds_profile=${encodeURIComponent(this.selectedProfitOddsProfile)}`,
                { credentials: 'include' }
            );
            if (response.status === 401) {
                window.location.href = '/login';
                return;
            }

            const data = await response.json();
            if (!response.ok) {
                throw new Error(data.error || '加载收益模拟失败');
            }

            this.renderProfitSimulation(data.simulation, predictor);
        } catch (error) {
            console.error('Failed to load profit simulation:', error);
            this.renderProfitSimulationEmpty(error.message);
        }
    }

    renderProfitSimulation(simulation, predictor) {
        this.renderProfitSimulationHint(simulation, predictor);
        this.renderProfitSummary(simulation);
        this.renderProfitChart(simulation.records || []);
        this.renderProfitTable(simulation.records || []);
    }

    renderProfitSimulationHint(simulation, predictor) {
        const container = document.getElementById('profitSimulationHint');
        const period = simulation.period || {};
        const summary = simulation.summary || {};
        const primaryMetric = predictor?.primary_metric || '';
        const fallbackText = primaryMetric && primaryMetric !== simulation.metric
            ? `当前方案主玩法为 ${this.escapeHtml(this.targetLabel(primaryMetric))}，收益模拟已回退为 ${this.escapeHtml(simulation.metric_label || '--')}。`
            : '当前默认按方案主玩法计算；切换其他玩法时才会额外发起计算。';

        container.className = 'metric-hint';
        container.innerHTML = `
            <div class="metric-hint-head">
                <div>
                    <strong>${this.escapeHtml(simulation.profit_rule_label || '--')} · ${this.escapeHtml(simulation.metric_label || '--')} · ${this.escapeHtml(simulation.odds_profile_label || '--')}</strong>
                    <span class="metric-hint-alias">盘日区间：${this.escapeHtml(period.start_time || '--')} 至 ${this.escapeHtml(period.end_time || '--')} 前</span>
                </div>
                <span class="tag">每期固定下注 ${this.formatUsd(simulation.stake_amount || 0)}</span>
            </div>
            <p>${fallbackText}</p>
            <p class="metric-hint-foot">今日共计下注 ${summary.bet_count || 0} 期，命中 ${summary.hit_count || 0} 期，回本 ${summary.refund_count || 0} 期，未中 ${summary.miss_count || 0} 期。</p>
        `;
    }

    renderProfitSummary(simulation) {
        const container = document.getElementById('profitSummaryGrid');
        const summary = simulation.summary || {};
        const cards = [
            ['收益规则', simulation.profit_rule_label || '--'],
            ['当前玩法', simulation.metric_label || '--'],
            ['赔率盘', simulation.odds_profile_label || '--'],
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
        const chartDom = document.getElementById('profitSimulationChart');
        if (!this.profitChart) {
            this.profitChart = echarts.init(chartDom, this.darkMode ? 'dark' : null);
        }

        if (!records.length) {
            this.profitChart.clear();
            this.profitChart.setOption({
                title: {
                    text: '当前盘日暂无收益数据',
                    left: 'center',
                    top: 'middle',
                    textStyle: { color: this.darkMode ? '#94a3b8' : '#64748b', fontSize: 14 }
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
                data: records.map((item) => item.issue_no),
                axisLabel: { color: this.darkMode ? '#94a3b8' : '#64748b' },
                axisLine: { lineStyle: { color: this.darkMode ? '#334155' : '#cbd5e1' } }
            },
            yAxis: {
                type: 'value',
                axisLabel: {
                    formatter: (value) => `${value}U`,
                    color: this.darkMode ? '#94a3b8' : '#64748b'
                },
                splitLine: { lineStyle: { color: this.darkMode ? '#1e293b' : '#e2e8f0' } }
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

    renderProfitTable(records) {
        const tbody = document.getElementById('profitSimulationBody');
        if (!records.length) {
            tbody.innerHTML = '<tr><td colspan="9" class="empty-cell">当前盘日暂无收益模拟记录</td></tr>';
            return;
        }

        tbody.innerHTML = records.map((item) => `
            <tr>
                <td>${this.escapeHtml(item.issue_no || '--')}</td>
                <td>${this.escapeHtml(item.open_time || '--')}</td>
                <td>${this.escapeHtml(item.ticket_label || '--')}</td>
                <td>${this.escapeHtml(item.predicted_value || '--')}</td>
                <td>${this.escapeHtml(item.actual_value || '--')}</td>
                <td>${this.formatOdds(item.odds)}</td>
                <td>${this.renderProfitResult(item)}</td>
                <td><strong class="${this.profitValueClass(item.net_profit)}">${this.escapeHtml(this.formatSignedUsd(item.net_profit))}</strong></td>
                <td><strong class="${this.profitValueClass(item.cumulative_profit)}">${this.escapeHtml(this.formatSignedUsd(item.cumulative_profit))}</strong></td>
            </tr>
        `).join('');
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
        document.getElementById('profitSimulationHint').className = 'metric-hint empty-panel';
        document.getElementById('profitSimulationHint').textContent = message || '暂无收益模拟数据';
        document.getElementById('profitSummaryGrid').innerHTML = '';
        document.getElementById('profitSimulationBody').innerHTML = '<tr><td colspan="9" class="empty-cell">暂无收益模拟数据</td></tr>';
        this.renderProfitChart([]);
    }

    renderPredictionsTable(predictions) {
        const tbody = document.getElementById('predictionsBody');
        const filteredPredictions = this.filterPredictions(predictions);

        if (!filteredPredictions.length) {
            tbody.innerHTML = '<tr><td colspan="8" class="empty-cell">当前筛选条件下暂无记录</td></tr>';
            return;
        }

        tbody.innerHTML = filteredPredictions.map((prediction) => `
            <tr>
                <td>${this.escapeHtml(prediction.issue_no)}</td>
                <td><span class="status-chip ${prediction.status}">${this.predictionStatusLabel(prediction.status)}</span></td>
                <td>${this.renderPredictionResult(prediction)}</td>
                <td>${this.renderActualResult(prediction)}</td>
                <td>${this.renderHitSummary(prediction)}</td>
                <td>${this.formatPercent(prediction.confidence !== null && prediction.confidence !== undefined ? prediction.confidence * 100 : null)}</td>
                <td>${this.formatPercent(prediction.score_percentage)}</td>
                <td>${this.escapeHtml(prediction.settled_at || '--')}</td>
            </tr>
        `).join('');
    }

    filterPredictions(predictions) {
        return (predictions || []).filter((prediction) => {
            if (this.predictionStatusFilter !== 'all' && prediction.status !== this.predictionStatusFilter) {
                return false;
            }

            if (this.predictionOutcomeFilter === 'all') {
                return true;
            }

            const hitValues = this.buildHitItems(prediction).map((item) => item.value);

            if (!hitValues.length) {
                return false;
            }

            if (this.predictionOutcomeFilter === 'hit') {
                return hitValues.some((value) => value === 1);
            }

            if (this.predictionOutcomeFilter === 'miss') {
                return hitValues.every((value) => value === 0);
            }

            return true;
        });
    }

    renderDrawsTable(draws) {
        const tbody = document.getElementById('drawsBody');
        if (!draws.length) {
            tbody.innerHTML = '<tr><td colspan="6" class="empty-cell">暂无官方开奖数据</td></tr>';
            return;
        }

        tbody.innerHTML = draws.map((draw) => `
            <tr>
                <td>${this.escapeHtml(draw.issue_no)}</td>
                <td><strong>${this.escapeHtml(draw.result_number_text)}</strong></td>
                <td>${draw.big_small}</td>
                <td>${draw.odd_even}</td>
                <td>${draw.combo}</td>
                <td>${this.escapeHtml(draw.open_time || '')}</td>
            </tr>
        `).join('');
    }

    renderAILogs(predictions) {
        const container = document.getElementById('aiLogs');
        if (!predictions.length) {
            container.innerHTML = '<div class="empty-panel">暂无 AI 输出记录</div>';
            return;
        }

        container.innerHTML = predictions.map((prediction) => `
            <article class="ai-log-card">
                <div class="ai-log-head">
                    <div>
                        <strong>第 ${this.escapeHtml(prediction.issue_no)} 期</strong>
                        <span class="status-chip ${prediction.status}">${this.predictionStatusLabel(prediction.status)}</span>
                    </div>
                    <span>${this.escapeHtml(prediction.created_at || '--')}</span>
                </div>
                ${prediction.error_message ? `<div class="warning-banner">${this.escapeHtml(prediction.error_message)}</div>` : ''}
                <div class="ai-log-block">
                    <span class="mini-label">简要说明</span>
                    <p>${this.escapeHtml(prediction.reasoning_summary || '无')}</p>
                </div>
                <details class="ai-log-block">
                    <summary>查看原始输出</summary>
                    <pre>${this.escapeHtml(prediction.raw_response || '无')}</pre>
                </details>
            </article>
        `).join('');
    }

    renderChart(predictions) {
        const chartDom = document.getElementById('predictionChart');
        if (!this.chart) {
            this.chart = echarts.init(chartDom, this.darkMode ? 'dark' : null);
        }

        if (!predictions.length) {
            this.chart.clear();
            this.chart.setOption({
                title: {
                    text: '暂无数据',
                    left: 'center',
                    top: 'middle',
                    textStyle: { color: this.darkMode ? '#94a3b8' : '#64748b', fontSize: 14 }
                }
            });
            return;
        }

        const ordered = [...predictions].slice(0, 20).reverse();
        const xAxisData = ordered.map((item) => item.issue_no);
        const scoreData = ordered.map((item) => item.score_percentage);
        const confidenceData = ordered.map((item) => (
            item.confidence !== null && item.confidence !== undefined ? Number(item.confidence * 100).toFixed(2) : null
        ));

        this.chart.setOption({
            animation: false,
            tooltip: { trigger: 'axis' },
            legend: {
                data: ['单期得分', '置信度'],
                textStyle: { color: this.darkMode ? '#cbd5f5' : '#334155' }
            },
            grid: { top: 48, left: 36, right: 24, bottom: 36, containLabel: true },
            xAxis: {
                type: 'category',
                data: xAxisData,
                axisLabel: { color: this.darkMode ? '#94a3b8' : '#64748b' },
                axisLine: { lineStyle: { color: this.darkMode ? '#334155' : '#cbd5e1' } }
            },
            yAxis: {
                type: 'value',
                min: 0,
                max: 100,
                axisLabel: {
                    formatter: '{value}%',
                    color: this.darkMode ? '#94a3b8' : '#64748b'
                },
                splitLine: { lineStyle: { color: this.darkMode ? '#1e293b' : '#e2e8f0' } }
            },
            series: [
                {
                    name: '单期得分',
                    type: 'line',
                    smooth: true,
                    data: scoreData,
                    lineStyle: { width: 3, color: '#38bdf8' },
                    itemStyle: { color: '#38bdf8' }
                },
                {
                    name: '置信度',
                    type: 'bar',
                    data: confidenceData,
                    itemStyle: { color: '#6366f1', opacity: 0.75 }
                }
            ]
        });
    }

    renderEmptyPredictorState() {
        this.currentStats = null;
        this.selectedStatsMetric = null;
        this.selectedProfitRuleId = 'pc28_netdisk';
        this.selectedProfitMetric = null;
        this.currentPredictions = [];
        document.getElementById('currentPrediction').className = 'prediction-summary empty-panel';
        document.getElementById('currentPrediction').textContent = '暂无预测方案，请先新建方案';
        document.getElementById('publicSharePanel').className = 'prediction-summary empty-panel';
        document.getElementById('publicSharePanel').textContent = '暂无预测方案，请先新建方案';
        document.getElementById('statsGrid').innerHTML = '';
        document.getElementById('statsMetricHint').className = 'metric-hint empty-panel';
        document.getElementById('statsMetricHint').textContent = '请选择玩法查看统计口径';
        document.getElementById('metricStatsBody').innerHTML = '<tr><td colspan="4" class="empty-cell">暂无统计数据</td></tr>';
        document.getElementById('streakStats').innerHTML = '<div class="empty-panel">暂无连中连挂数据</div>';
        document.getElementById('profitRuleView').innerHTML = '<option value="">暂无规则</option>';
        document.getElementById('profitRuleView').disabled = true;
        document.getElementById('profitMetricView').innerHTML = '<option value="">暂无玩法</option>';
        document.getElementById('profitMetricView').disabled = true;
        document.getElementById('profitOddsProfileView').value = this.selectedProfitOddsProfile;
        document.getElementById('profitOddsProfileView').disabled = true;
        document.getElementById('profitSimulationHint').className = 'metric-hint empty-panel';
        document.getElementById('profitSimulationHint').textContent = '请选择预测方案';
        document.getElementById('profitSummaryGrid').innerHTML = '';
        document.getElementById('profitSimulationBody').innerHTML = '<tr><td colspan="9" class="empty-cell">暂无收益模拟数据</td></tr>';
        document.getElementById('predictionsBody').innerHTML = '<tr><td colspan="8" class="empty-cell">暂无预测记录</td></tr>';
        document.getElementById('aiLogs').innerHTML = '<div class="empty-panel">暂无 AI 输出记录</div>';
        if (this.chart) {
            this.chart.clear();
        }
        if (this.profitChart) {
            this.profitChart.clear();
        }
    }

    switchTab(tabName) {
        document.querySelectorAll('.tab-btn').forEach((button) => {
            button.classList.toggle('active', button.dataset.tab === tabName);
        });
        document.querySelectorAll('.tab-panel').forEach((panel) => {
            panel.classList.toggle('active', panel.id === `${tabName}Tab`);
        });
    }

    openCreateModal() {
        document.getElementById('modalTitle').textContent = '新建预测方案';
        document.getElementById('predictorId').value = '';
        this.resetForm();
        this.presetExpanded = false;
        this.renderPresetCards();
        this.showModal();
    }

    async openEditModal(predictorId = this.currentPredictorId) {
        if (!predictorId) {
            alert('请先选择一个预测方案');
            return;
        }

        const response = await fetch(`/api/predictors/${predictorId}`, { credentials: 'include' });
        const data = await response.json();
        if (!response.ok) {
            alert(data.error || '加载预测方案失败');
            return;
        }

        document.getElementById('modalTitle').textContent = '编辑预测方案';
        document.getElementById('predictorId').value = data.id;
        document.getElementById('predictorName').value = data.name || '';
        document.getElementById('predictionMethod').value = data.prediction_method || '';
        document.getElementById('apiUrl').value = data.api_url || '';
        document.getElementById('modelName').value = data.model_name || '';
        document.getElementById('apiMode').value = data.api_mode || 'auto';
        document.getElementById('apiKey').value = '';
        document.getElementById('historyWindow').value = data.history_window || 60;
        document.getElementById('temperature').value = data.temperature ?? 0.7;
        document.getElementById('dataInjectionMode').value = data.data_injection_mode || 'summary';
        document.getElementById('primaryMetric').value = data.primary_metric || 'combo';
        document.getElementById('profitRuleId').value = data.profit_rule_id || 'pc28_netdisk';
        document.getElementById('profitDefaultMetric').value = data.profit_default_metric || data.default_simulation_metric || 'big_small';
        document.getElementById('systemPrompt').value = data.system_prompt || '';
        document.getElementById('predictorEnabled').checked = Boolean(data.enabled);
        document.getElementById('shareLevel').value = data.share_level || (data.share_predictions ? 'records' : 'stats_only');
        document.getElementById('targetNumber').checked = data.prediction_targets.includes('number');
        document.getElementById('targetBigSmall').checked = data.prediction_targets.includes('big_small');
        document.getElementById('targetOddEven').checked = data.prediction_targets.includes('odd_even');
        document.getElementById('targetCombo').checked = data.prediction_targets.includes('combo');
        this.syncProfitMetricOptions();
        this.presetExpanded = false;
        this.renderPresetCards();
        this.showModal();
    }

    showModal() {
        this.hideTestResult();
        this.hidePromptAssistantResult();
        document.getElementById('predictorModal').classList.add('show');
    }

    hideModal() {
        document.getElementById('predictorModal').classList.remove('show');
    }

    resetForm() {
        document.getElementById('predictorName').value = '';
        document.getElementById('predictionMethod').value = '';
        document.getElementById('apiUrl').value = '';
        document.getElementById('modelName').value = '';
        document.getElementById('apiMode').value = 'auto';
        document.getElementById('apiKey').value = '';
        document.getElementById('historyWindow').value = '60';
        document.getElementById('temperature').value = '0.7';
        document.getElementById('dataInjectionMode').value = 'summary';
        document.getElementById('primaryMetric').value = 'big_small';
        document.getElementById('profitRuleId').value = 'pc28_netdisk';
        document.getElementById('profitDefaultMetric').value = 'big_small';
        document.getElementById('systemPrompt').value = '';
        document.getElementById('predictorEnabled').checked = true;
        document.getElementById('shareLevel').value = 'stats_only';
        document.getElementById('targetNumber').checked = true;
        document.getElementById('targetBigSmall').checked = true;
        document.getElementById('targetOddEven').checked = true;
        document.getElementById('targetCombo').checked = true;
        this.syncProfitMetricOptions();
        this.hideTestResult();
        this.hidePromptAssistantResult();
    }

    renderPresetCards() {
        const container = document.getElementById('presetCards');
        const toggleButton = document.getElementById('togglePresetListBtn');
        const visibleCount = 3;

        container.innerHTML = PREDICTOR_PRESETS.map((preset, index) => `
            <article class="preset-card ${index >= visibleCount && !this.presetExpanded ? 'hidden' : ''}" data-preset-id="${preset.id}">
                <div class="preset-card-head">
                    <div>
                        <h4>${this.escapeHtml(preset.title)}</h4>
                        <p>${this.escapeHtml(preset.description)}</p>
                    </div>
                </div>
                <div class="preset-meta">
                    ${preset.tags.map((tag) => `<span class="tag">${this.escapeHtml(tag)}</span>`).join('')}
                    <span class="tag">历史 ${preset.historyWindow} 期</span>
                    <span class="tag">${preset.injectionMode === 'raw' ? '原始模式' : '摘要模式'}</span>
                    <span class="tag">${preset.apiMode === 'responses' ? 'Responses' : preset.apiMode === 'chat_completions' ? 'Chat Completions' : '自动模式'}</span>
                </div>
                <div class="preset-actions">
                    <button type="button" class="btn ghost compact" data-apply-preset="${preset.id}">一键填充</button>
                </div>
            </article>
        `).join('');

        container.querySelectorAll('[data-apply-preset]').forEach((button) => {
            button.addEventListener('click', () => this.applyPreset(button.dataset.applyPreset));
        });

        if (PREDICTOR_PRESETS.length <= visibleCount) {
            toggleButton.style.display = 'none';
            return;
        }

        toggleButton.style.display = 'inline-flex';
        toggleButton.textContent = this.presetExpanded ? '收起示例' : '查看更多示例';
    }

    togglePresetList() {
        this.presetExpanded = !this.presetExpanded;
        this.renderPresetCards();
    }

    applyPreset(presetId) {
        const preset = PREDICTOR_PRESETS.find((item) => item.id === presetId);
        if (!preset) {
            return;
        }

        if (!document.getElementById('predictorName').value.trim()) {
            document.getElementById('predictorName').value = preset.title;
        }
        document.getElementById('predictionMethod').value = preset.method;
        document.getElementById('apiMode').value = preset.apiMode || 'auto';
        document.getElementById('historyWindow').value = String(preset.historyWindow);
        document.getElementById('temperature').value = String(preset.temperature);
        document.getElementById('dataInjectionMode').value = preset.injectionMode;
        document.getElementById('primaryMetric').value = preset.primaryMetric || 'combo';
        document.getElementById('profitRuleId').value = preset.profitRuleId || 'pc28_netdisk';
        document.getElementById('profitDefaultMetric').value = preset.profitDefaultMetric || (['big_small', 'odd_even', 'combo', 'number'].includes(preset.primaryMetric) ? preset.primaryMetric : 'combo');
        document.getElementById('systemPrompt').value = preset.prompt;
        document.getElementById('targetNumber').checked = preset.targets.includes('number');
        document.getElementById('targetBigSmall').checked = preset.targets.includes('big_small');
        document.getElementById('targetOddEven').checked = preset.targets.includes('odd_even');
        document.getElementById('targetCombo').checked = preset.targets.includes('combo');
        this.syncProfitMetricOptions();
    }

    collectFormData() {
        const predictionTargets = [];
        if (document.getElementById('targetNumber').checked) predictionTargets.push('number');
        if (document.getElementById('targetBigSmall').checked) predictionTargets.push('big_small');
        if (document.getElementById('targetOddEven').checked) predictionTargets.push('odd_even');
        if (document.getElementById('targetCombo').checked) predictionTargets.push('combo');

        return {
            name: document.getElementById('predictorName').value.trim(),
            prediction_method: document.getElementById('predictionMethod').value.trim(),
            api_url: document.getElementById('apiUrl').value.trim(),
            model_name: document.getElementById('modelName').value.trim(),
            api_mode: document.getElementById('apiMode').value,
            api_key: document.getElementById('apiKey').value.trim(),
            history_window: Number(document.getElementById('historyWindow').value || 60),
            temperature: Number(document.getElementById('temperature').value || 0.7),
            data_injection_mode: document.getElementById('dataInjectionMode').value,
            primary_metric: document.getElementById('primaryMetric').value,
            profit_rule_id: document.getElementById('profitRuleId').value,
            profit_default_metric: document.getElementById('profitDefaultMetric').value,
            system_prompt: document.getElementById('systemPrompt').value.trim(),
            enabled: document.getElementById('predictorEnabled').checked,
            share_level: document.getElementById('shareLevel').value,
            prediction_targets: predictionTargets
        };
    }

    syncProfitMetricOptions() {
        const select = document.getElementById('profitDefaultMetric');
        if (!select) {
            return;
        }

        const options = [];
        if (document.getElementById('targetBigSmall').checked) {
            options.push({ key: 'big_small', label: '大/小：默认按大小单双盘看当盘日盈亏' });
        }
        if (document.getElementById('targetOddEven').checked) {
            options.push({ key: 'odd_even', label: '单/双：默认按单双盘看当盘日盈亏' });
        }
        if (document.getElementById('targetCombo').checked) {
            options.push({ key: 'combo', label: '组合投注：默认按组合票面看当盘日盈亏' });
        }
        if (document.getElementById('targetNumber').checked) {
            options.push({ key: 'number', label: '单点：默认按精确和值看当盘日盈亏' });
        }

        const previousValue = select.value;
        if (!options.length) {
            select.innerHTML = '<option value="">暂无可用收益玩法</option>';
            select.disabled = true;
            return;
        }

        select.innerHTML = options.map((item) => `
            <option value="${this.escapeHtml(item.key)}">${this.escapeHtml(item.label)}</option>
        `).join('');
        select.disabled = false;
        select.value = options.some((item) => item.key === previousValue) ? previousValue : options[0].key;
    }

    async submitPredictor() {
        const predictorId = document.getElementById('predictorId').value;
        const method = predictorId ? 'PUT' : 'POST';
        const url = predictorId ? `/api/predictors/${predictorId}` : '/api/predictors';
        const payload = this.collectFormData();

        try {
            const response = await fetch(url, {
                method,
                credentials: 'include',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });
            const data = await response.json();
            if (!response.ok) {
                throw new Error(data.error || '保存预测方案失败');
            }

            this.hideModal();
            this.currentPredictorId = data.predictor?.id || Number(predictorId) || this.currentPredictorId;
            await this.refresh(true);
        } catch (error) {
            alert(error.message);
        }
    }

    async testPredictorConfig() {
        const predictorId = document.getElementById('predictorId').value;
        const payload = this.collectFormData();
        payload.predictor_id = predictorId ? Number(predictorId) : null;

        const button = document.getElementById('testPredictorBtn');
        button.disabled = true;
        button.textContent = '测试中...';
        this.showTestResult('info', '正在测试模型连通性，请稍候...');

        try {
            const response = await fetch('/api/predictors/test', {
                method: 'POST',
                credentials: 'include',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });
            const data = await response.json();
            if (!response.ok) {
                throw new Error(data.error || '模型测试失败');
            }

            const preview = data.response_preview || '模型已返回响应';
            this.showTestResult('success', this.buildTestResultMessage(data, preview));
        } catch (error) {
            this.showTestResult('error', error.message);
        } finally {
            button.disabled = false;
            button.textContent = '测试模型';
        }
    }

    async checkPromptAssistant() {
        const predictorId = document.getElementById('predictorId').value;
        const payload = this.collectFormData();
        payload.predictor_id = predictorId ? Number(predictorId) : null;

        try {
            const response = await fetch('/api/predictors/prompt-check', {
                method: 'POST',
                credentials: 'include',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });
            const data = await response.json();
            if (!response.ok) {
                throw new Error(data.error || '提示词检查失败');
            }
            this.renderPromptAssistantResult({
                mode: 'check',
                ...data
            });
        } catch (error) {
            this.renderPromptAssistantResult({
                mode: 'check',
                risk_level: 'high',
                summary: error.message,
                issues: []
            });
        }
    }

    async optimizePromptAssistant() {
        const predictorId = document.getElementById('predictorId').value;
        const payload = this.collectFormData();
        payload.predictor_id = predictorId ? Number(predictorId) : null;

        const button = document.getElementById('optimizePromptBtn');
        button.disabled = true;
        button.textContent = '优化中...';
        this.renderPromptAssistantResult({
            mode: 'optimize',
            risk_level: 'low',
            summary: '正在调用 AI 生成优化建议，请稍候...',
            issues: []
        });

        try {
            const response = await fetch('/api/predictors/prompt-optimize', {
                method: 'POST',
                credentials: 'include',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });
            const data = await response.json();
            if (!response.ok) {
                throw new Error(data.error || 'AI 优化失败');
            }
            this.renderPromptAssistantResult({
                mode: 'optimize',
                ...data
            });
        } catch (error) {
            this.renderPromptAssistantResult({
                mode: 'optimize',
                risk_level: 'high',
                summary: error.message,
                issues: []
            });
        } finally {
            button.disabled = false;
            button.textContent = 'AI 优化';
        }
    }

    applyOptimizedPrompt() {
        const optimizedPrompt = document.getElementById('promptAssistantResult').dataset.optimizedPrompt || '';
        if (!optimizedPrompt) {
            alert('当前没有可应用的优化结果');
            return;
        }
        document.getElementById('systemPrompt').value = optimizedPrompt;
    }

    async deletePredictor(predictorId) {
        if (!confirm('确定删除这个预测方案吗？相关预测记录也会一并删除。')) {
            return;
        }

        try {
            const response = await fetch(`/api/predictors/${predictorId}`, {
                method: 'DELETE',
                credentials: 'include'
            });
            const data = await response.json();
            if (!response.ok) {
                throw new Error(data.error || '删除预测方案失败');
            }

            if (this.currentPredictorId === predictorId) {
                this.currentPredictorId = null;
            }
            await this.refresh(true);
        } catch (error) {
            alert(error.message);
        }
    }

    async toggleCurrentPredictor() {
        if (!this.currentPredictor) {
            alert('请先选择一个预测方案');
            return;
        }

        await this.togglePredictorStatus(this.currentPredictor.id, !this.currentPredictor.enabled);
    }

    async togglePredictorStatus(predictorId, enabled) {
        try {
            const response = await fetch(`/api/predictors/${predictorId}`, {
                method: 'PUT',
                credentials: 'include',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ enabled })
            });
            const data = await response.json();
            if (!response.ok) {
                throw new Error(data.error || '更新方案状态失败');
            }

            if (this.currentPredictorId === predictorId) {
                this.currentPredictor = data.predictor;
                this.updatePredictorActionState(data.predictor);
            }

            await this.refresh(true);
        } catch (error) {
            alert(error.message);
        }
    }

    async predictNow() {
        if (!this.currentPredictorId) {
            alert('请先选择一个预测方案');
            return;
        }

        try {
            const response = await fetch(`/api/predictors/${this.currentPredictorId}/predict-now`, {
                method: 'POST',
                credentials: 'include'
            });
            const data = await response.json();
            if (!response.ok) {
                throw new Error(data.error || '立即预测失败');
            }

            await this.loadPredictorDashboard(this.currentPredictorId);
        } catch (error) {
            alert(error.message);
        }
    }

    async logout() {
        await fetch('/api/auth/logout', {
            method: 'POST',
            credentials: 'include'
        });
        window.location.href = '/login';
    }

    toggleTheme() {
        this.darkMode = !this.darkMode;
        localStorage.setItem('pc28Theme', this.darkMode ? 'dark' : 'light');
        this.applyTheme();
        if (this.chart) {
            this.chart.dispose();
            this.chart = null;
        }
        if (this.currentPredictorId) {
            this.loadPredictorDashboard(this.currentPredictorId);
        }
    }

    applyTheme() {
        document.body.classList.toggle('dark', this.darkMode);
        const icon = document.querySelector('#themeToggle i');
        if (icon) {
            icon.className = this.darkMode ? 'bi bi-sun' : 'bi bi-moon-stars';
        }
    }

    updatePredictorActionState(predictor) {
        const toggleButton = document.getElementById('togglePredictorBtn');
        const predictNowButton = document.getElementById('predictNowBtn');

        if (!toggleButton || !predictNowButton) {
            return;
        }

        if (!predictor) {
            toggleButton.disabled = true;
            toggleButton.innerHTML = '<i class="bi bi-pause-circle"></i> 暂停方案';
            predictNowButton.disabled = true;
            return;
        }

        toggleButton.disabled = false;
        toggleButton.innerHTML = predictor.enabled
            ? '<i class="bi bi-pause-circle"></i> 暂停方案'
            : '<i class="bi bi-play-circle"></i> 恢复方案';
        predictNowButton.disabled = false;
    }

    buildTestResultMessage(data, preview) {
        const details = [];
        if (data.api_mode) {
            details.push(`模式：${data.api_mode}`);
        }
        if (data.response_model) {
            details.push(`模型：${data.response_model}`);
        }
        if (data.finish_reason) {
            details.push(`finish_reason：${data.finish_reason}`);
        }
        if (data.latency_ms !== null && data.latency_ms !== undefined) {
            details.push(`耗时：${data.latency_ms}ms`);
        }

        return `测试成功\n${details.join(' | ')}\n响应预览：${preview}`;
    }

    showTestResult(type, message) {
        const result = document.getElementById('testPredictorResult');
        if (!result) {
            return;
        }
        result.className = `test-result ${type}`;
        result.textContent = message;
        result.style.display = 'block';
    }

    hideTestResult() {
        const result = document.getElementById('testPredictorResult');
        if (!result) {
            return;
        }
        result.textContent = '';
        result.className = 'test-result';
        result.style.display = 'none';
    }

    renderPromptAssistantResult(data) {
        const container = document.getElementById('promptAssistantResult');
        const applyButton = document.getElementById('applyOptimizedPromptBtn');
        if (!container || !applyButton) {
            return;
        }

        const riskLevel = data.risk_level || 'low';
        const issues = data.issues || [];
        const why = data.why || [];
        const optimizedPrompt = data.optimized_prompt || '';
        const staticAnalysis = data.static_analysis || null;
        const summary = data.summary || '暂无结果';
        const recommendedVariables = data.recommended_variables || [];
        const recommendedSnippets = data.recommended_snippets || [];

        const issueHtml = issues.length
            ? issues.map((item) => {
                if (typeof item === 'string') {
                    return `<li>${this.escapeHtml(item)}</li>`;
                }
                const suggestion = item.suggestion ? ` 建议：${item.suggestion}` : '';
                return `<li><strong>${this.escapeHtml(item.title || item.level || '提示')}</strong>：${this.escapeHtml((item.detail || item) + suggestion)}</li>`;
            }).join('')
            : '<li>暂无明显问题</li>';

        const whyHtml = why.length
            ? `<div class="assistant-block"><span class="mini-label">优化思路</span><ul>${why.map((item) => `<li>${this.escapeHtml(item)}</li>`).join('')}</ul></div>`
            : '';

        const staticHtml = staticAnalysis
            ? `<div class="assistant-block"><span class="mini-label">静态检查摘要</span><p>${this.escapeHtml(staticAnalysis.summary || '')}</p></div>`
            : '';

        const variableHtml = recommendedVariables.length
            ? `<div class="assistant-block"><span class="mini-label">推荐变量</span><ul>${recommendedVariables.map((item) => `<li><strong>${this.escapeHtml(`{{${item.name}}}`)}</strong>：${this.escapeHtml(item.reason || '')}</li>`).join('')}</ul></div>`
            : '';

        const snippetHtml = recommendedSnippets.length
            ? `<div class="assistant-block"><span class="mini-label">建议插入片段</span><pre>${this.escapeHtml(recommendedSnippets.map((item) => item.snippet).join('\n\n'))}</pre></div>`
            : '';

        const metaBits = [];
        if (data.api_mode) {
            metaBits.push(`模式：${data.api_mode}`);
        }
        if (data.response_model) {
            metaBits.push(`模型：${data.response_model}`);
        }
        if (data.finish_reason) {
            metaBits.push(`finish_reason：${data.finish_reason}`);
        }
        if (data.latency_ms !== null && data.latency_ms !== undefined) {
            metaBits.push(`耗时：${data.latency_ms}ms`);
        }

        container.dataset.optimizedPrompt = optimizedPrompt;
        container.className = `prompt-assistant-result ${riskLevel}`;
        container.style.display = 'block';
        container.innerHTML = `
            <div class="assistant-block">
                <span class="mini-label">检查结论</span>
                <p>${this.escapeHtml(summary)}</p>
                ${metaBits.length ? `<p class="field-hint">${this.escapeHtml(metaBits.join(' | '))}</p>` : ''}
            </div>
            <div class="assistant-block">
                <span class="mini-label">问题列表</span>
                <ul>${issueHtml}</ul>
            </div>
            ${whyHtml}
            ${variableHtml}
            ${snippetHtml}
            ${optimizedPrompt ? `<div class="assistant-block"><span class="mini-label">优化后提示词</span><pre>${this.escapeHtml(optimizedPrompt)}</pre></div>` : ''}
            ${staticHtml}
        `;

        applyButton.style.display = optimizedPrompt ? 'inline-flex' : 'none';
    }

    hidePromptAssistantResult() {
        const container = document.getElementById('promptAssistantResult');
        const applyButton = document.getElementById('applyOptimizedPromptBtn');
        if (container) {
            container.innerHTML = '';
            container.dataset.optimizedPrompt = '';
            container.className = 'prompt-assistant-result';
            container.style.display = 'none';
        }
        if (applyButton) {
            applyButton.style.display = 'none';
        }
    }

    targetLabel(target) {
        return this.metricMeta(target).label;
    }

    predictionStatusLabel(status) {
        const mapping = {
            pending: '待开奖',
            settled: '已结算',
            expired: '过期未结算',
            failed: '执行失败'
        };
        return mapping[status] || status;
    }

    renderPredictionResult(prediction) {
        const snapshot = this.buildPlaySnapshot(
            prediction.prediction_number,
            prediction.prediction_big_small,
            prediction.prediction_odd_even,
            prediction.prediction_combo
        );
        return `
            <div class="result-stack">
                <strong>${this.escapeHtml(snapshot.numberText || '--')}</strong>
                <span class="result-meta-text">大/小：${this.escapeHtml(snapshot.bigSmall || '--')} · 单/双：${this.escapeHtml(snapshot.oddEven || '--')} · 组合结果：${this.escapeHtml(snapshot.combo || '--')}</span>
                <span class="result-meta-text">组合投注：${this.escapeHtml(snapshot.pairTicket || '--')} · 排除对角组：${this.escapeHtml(snapshot.killGroup ? `杀${snapshot.killGroup}` : '--')}</span>
            </div>
        `;
    }

    renderActualResult(prediction) {
        if (prediction.status === 'pending') {
            return '<span class="hint-text">等待开奖</span>';
        }
        if (prediction.status === 'expired') {
            return '<span class="hint-text">超出可追溯窗口</span>';
        }
        if (prediction.actual_number === null || prediction.actual_number === undefined) {
            return '<span class="hint-text">无</span>';
        }

        const snapshot = this.buildPlaySnapshot(
            prediction.actual_number,
            prediction.actual_big_small,
            prediction.actual_odd_even,
            prediction.actual_combo
        );
        return `
            <div class="result-stack">
                <strong>${this.escapeHtml(snapshot.numberText || '--')}</strong>
                <span class="result-meta-text">大/小：${this.escapeHtml(snapshot.bigSmall || '--')} · 单/双：${this.escapeHtml(snapshot.oddEven || '--')} · 组合结果：${this.escapeHtml(snapshot.combo || '--')}</span>
                <span class="result-meta-text">组合投注：${this.escapeHtml(snapshot.pairTicket || '--')} · 排除对角组：${this.escapeHtml(snapshot.killGroup ? `杀${snapshot.killGroup}` : '--')}</span>
            </div>
        `;
    }

    renderHitSummary(prediction, options = {}) {
        if (prediction.status === 'pending') {
            return '<span class="hint-text">未结算</span>';
        }
        if (prediction.status === 'expired') {
            return '<span class="hint-text">未补结算</span>';
        }
        if (prediction.status === 'failed') {
            return '<span class="hint-text">无结果</span>';
        }

        const items = this.buildHitItems(prediction);
        if (!items.length) {
            return '<span class="hint-text">无可统计结果</span>';
        }

        const hitCount = items.filter((item) => item.value === 1).length;
        const summaryText = `本期纳入统计 ${items.length} 项，命中 ${hitCount} 项`;

        return `
            <div class="hit-summary-block">
                <div class="hit-list ${options.detailed ? 'detailed' : ''}">
                    ${items.map((item) => `<span class="hit-pill ${this.hitClass(item.value)}">${this.escapeHtml(item.label)} ${this.hitMark(item.value)}</span>`).join('')}
                </div>
                ${options.detailed ? `<span class="result-meta-text">${this.escapeHtml(summaryText)}</span>` : ''}
            </div>
        `;
    }

    renderCurrentPredictionCard(label, value, hint = '') {
        return `
            <div class="prediction-card">
                <span class="mini-label">${this.escapeHtml(label)}</span>
                <strong>${this.escapeHtml(value)}</strong>
                ${hint ? `<span class="card-hint">${this.escapeHtml(hint)}</span>` : ''}
            </div>
        `;
    }

    renderCurrentSettlement(prediction, predictionSnapshot, actualSnapshot) {
        if (prediction.status === 'pending') {
            return `
                <div class="prediction-section">
                    <div class="prediction-section-head">
                        <div>
                            <span class="mini-label">结算状态</span>
                            <p class="section-hint">等待官方开奖后，会自动补出开奖快照和派生玩法命中情况。</p>
                        </div>
                    </div>
                    <div class="prediction-card">
                        <span class="mini-label">当前状态</span>
                        <strong>待开奖</strong>
                        <span class="card-hint">当前只能查看预测侧映射，不能判断命中。</span>
                    </div>
                </div>
            `;
        }

        if (prediction.status === 'expired') {
            return `
                <div class="prediction-section">
                    <div class="prediction-card">
                        <span class="mini-label">结算状态</span>
                        <strong>过期未结算</strong>
                        <span class="card-hint">当前记录已超出补结算窗口，无法生成完整开奖对照。</span>
                    </div>
                </div>
            `;
        }

        if (prediction.status === 'failed') {
            return `
                <div class="prediction-section">
                    <div class="prediction-card">
                        <span class="mini-label">结算状态</span>
                        <strong>执行失败</strong>
                        <span class="card-hint">本期没有有效预测结果，因此不存在开奖对照和命中统计。</span>
                    </div>
                </div>
            `;
        }

        return `
            <div class="prediction-section">
                <div class="prediction-section-head">
                    <div>
                        <span class="mini-label">最新结算对照</span>
                        <p class="section-hint">同一期开奖结果会同时展示基础标签和派生玩法，便于直接核对。</p>
                    </div>
                </div>
                <div class="prediction-compare-grid">
                    <div class="prediction-card">
                        <span class="mini-label">预测快照</span>
                        ${this.renderSnapshotDetails(predictionSnapshot)}
                    </div>
                    <div class="prediction-card">
                        <span class="mini-label">开奖快照</span>
                        ${this.renderSnapshotDetails(actualSnapshot)}
                    </div>
                    <div class="prediction-card">
                        <span class="mini-label">命中明细</span>
                        ${this.renderHitSummary(prediction, { detailed: true })}
                    </div>
                </div>
            </div>
        `;
    }

    renderSnapshotDetails(snapshot) {
        const rows = [
            { label: '单点', value: snapshot.numberText || '--' },
            { label: '大/小', value: snapshot.bigSmall || '--' },
            { label: '单/双', value: snapshot.oddEven || '--' },
            { label: '组合结果', value: snapshot.combo || '--' },
            { label: '组合投注', value: snapshot.pairTicket || '--' },
            { label: '排除对角组', value: snapshot.killGroup ? `杀${snapshot.killGroup}` : '--' }
        ];

        return `
            <div class="detail-list">
                ${rows.map((row) => `
                    <div class="detail-row">
                        <span class="detail-label">${this.escapeHtml(row.label)}</span>
                        <strong>${this.escapeHtml(row.value)}</strong>
                    </div>
                `).join('')}
            </div>
        `;
    }

    buildHitItems(prediction) {
        const items = [];

        if (prediction.hit_number !== null && prediction.hit_number !== undefined) {
            items.push({ label: '单点', value: prediction.hit_number });
        }
        if (prediction.hit_big_small !== null && prediction.hit_big_small !== undefined) {
            items.push({ label: '大/小', value: prediction.hit_big_small });
        }
        if (prediction.hit_odd_even !== null && prediction.hit_odd_even !== undefined) {
            items.push({ label: '单/双', value: prediction.hit_odd_even });
        }
        if (prediction.hit_combo !== null && prediction.hit_combo !== undefined) {
            items.push({ label: '组合结果', value: prediction.hit_combo });
        }

        const predictedGroup = this.deriveDoubleGroup(prediction.prediction_combo);
        const actualGroup = this.deriveDoubleGroup(prediction.actual_combo);
        if (predictedGroup && actualGroup) {
            items.push({ label: '组合分组', value: predictedGroup === actualGroup ? 1 : 0 });
        }

        const killGroup = this.deriveKillGroup(prediction.prediction_combo);
        if (killGroup && prediction.actual_combo) {
            items.push({ label: '排除统计', value: prediction.actual_combo !== killGroup ? 1 : 0 });
        }

        return items;
    }

    metricMeta(metricKey) {
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

    formatDoubleGroupText(snapshot) {
        if (!snapshot.doubleGroup) {
            return '--';
        }
        if (!snapshot.pairTicket) {
            return snapshot.doubleGroup;
        }
        return `${snapshot.doubleGroup}（${snapshot.pairTicket}）`;
    }

    async copyPublicUrl(button) {
        const url = button?.dataset?.url;
        if (!url || button.disabled) {
            return;
        }

        const originalText = button.textContent;

        try {
            if (navigator.clipboard && navigator.clipboard.writeText) {
                await navigator.clipboard.writeText(url);
            } else {
                const input = document.createElement('input');
                input.value = url;
                document.body.appendChild(input);
                input.select();
                document.execCommand('copy');
                input.remove();
            }
            button.textContent = '已复制';
        } catch (error) {
            console.error('Failed to copy public url:', error);
            button.textContent = '复制失败';
        }

        window.setTimeout(() => {
            button.textContent = originalText;
        }, 1500);
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

    renderBadge(text) {
        return `<span class="tag">${this.escapeHtml(text)}</span>`;
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
            "'": '&#039;'
        };
        return value.replace(/[&<>"']/g, (char) => map[char]);
    }
}

const app = new PredictionApp();
