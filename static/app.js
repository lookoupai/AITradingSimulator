const PREDICTOR_PRESETS = [
    {
        id: 'statistical',
        title: '概率统计型',
        description: '适合先跑稳定版。偏重和值分布、遗漏、冷热和趋势，不依赖玄学解释。',
        method: '概率统计',
        injectionMode: 'summary',
        historyWindow: 100,
        temperature: 0.4,
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
        this.overview = null;
        this.chart = null;
        this.refreshTimer = null;
        this.darkMode = localStorage.getItem('pc28Theme') === 'dark';
        this.presetExpanded = false;
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
        document.getElementById('predictNowBtn').addEventListener('click', () => this.predictNow());
        document.getElementById('editPredictorBtn').addEventListener('click', () => this.openEditModal());
        document.getElementById('togglePredictorBtn').addEventListener('click', () => this.toggleCurrentPredictor());
        document.getElementById('togglePresetListBtn').addEventListener('click', () => this.togglePresetList());

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
            this.updatePredictorActionState(data.predictor);
            this.renderStats(data.stats);
            this.renderCurrentPrediction(data.current_prediction, data.latest_prediction, data.predictor);
            this.renderPredictionsTable(data.recent_predictions || []);
            this.renderDrawsTable(data.overview?.recent_draws || data.recent_draws || []);
            this.renderAILogs(data.recent_predictions || []);
            this.renderChart(data.recent_predictions || []);
        } catch (error) {
            console.error('Failed to load predictor dashboard:', error);
        }
    }

    renderStats(stats) {
        const values = [
            stats.total_predictions || 0,
            this.formatPercent(stats.number_hit_rate),
            this.formatPercent(stats.big_small_hit_rate),
            this.formatPercent(stats.odd_even_hit_rate)
        ];

        document.querySelectorAll('#statsGrid .stat-value').forEach((element, index) => {
            element.textContent = values[index];
        });
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

        const statusText = prediction.status === 'pending' ? '待开奖' : prediction.status === 'settled' ? '已结算' : '执行失败';
        const errorBlock = prediction.error_message
            ? `<div class="warning-banner">${this.escapeHtml(prediction.error_message)}</div>`
            : '';

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
            <div class="prediction-grid">
                <div class="prediction-card">
                    <span class="mini-label">预测期号</span>
                    <strong>${this.escapeHtml(prediction.issue_no || '--')}</strong>
                </div>
                <div class="prediction-card">
                    <span class="mini-label">预测号码</span>
                    <strong>${prediction.prediction_number !== null && prediction.prediction_number !== undefined ? String(prediction.prediction_number).padStart(2, '0') : '--'}</strong>
                </div>
                <div class="prediction-card">
                    <span class="mini-label">大小 / 单双 / 组合</span>
                    <div class="badge-row">
                        ${this.renderBadge(prediction.prediction_big_small || '--')}
                        ${this.renderBadge(prediction.prediction_odd_even || '--')}
                        ${this.renderBadge(prediction.prediction_combo || '--')}
                    </div>
                </div>
                <div class="prediction-card">
                    <span class="mini-label">置信度</span>
                    <strong>${this.formatPercent(prediction.confidence !== null && prediction.confidence !== undefined ? prediction.confidence * 100 : null)}</strong>
                </div>
            </div>
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

    renderPredictionsTable(predictions) {
        const tbody = document.getElementById('predictionsBody');
        if (!predictions.length) {
            tbody.innerHTML = '<tr><td colspan="8" class="empty-cell">暂无预测记录</td></tr>';
            return;
        }

        tbody.innerHTML = predictions.map((prediction) => `
            <tr>
                <td>${this.escapeHtml(prediction.issue_no)}</td>
                <td><span class="status-chip ${prediction.status}">${this.predictionStatusLabel(prediction.status)}</span></td>
                <td>${prediction.prediction_number !== null && prediction.prediction_number !== undefined ? String(prediction.prediction_number).padStart(2, '0') : '--'}</td>
                <td>${prediction.prediction_big_small || '--'}</td>
                <td>${prediction.prediction_odd_even || '--'}</td>
                <td>${prediction.prediction_combo || '--'}</td>
                <td>${this.formatPercent(prediction.confidence !== null && prediction.confidence !== undefined ? prediction.confidence * 100 : null)}</td>
                <td>${this.formatPercent(prediction.score_percentage)}</td>
            </tr>
        `).join('');
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
        document.getElementById('currentPrediction').className = 'prediction-summary empty-panel';
        document.getElementById('currentPrediction').textContent = '暂无预测方案，请先新建方案';
        document.getElementById('predictionsBody').innerHTML = '<tr><td colspan="8" class="empty-cell">暂无预测记录</td></tr>';
        document.getElementById('aiLogs').innerHTML = '<div class="empty-panel">暂无 AI 输出记录</div>';
        if (this.chart) {
            this.chart.clear();
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
        document.getElementById('apiKey').value = '';
        document.getElementById('historyWindow').value = data.history_window || 60;
        document.getElementById('temperature').value = data.temperature ?? 0.7;
        document.getElementById('dataInjectionMode').value = data.data_injection_mode || 'summary';
        document.getElementById('systemPrompt').value = data.system_prompt || '';
        document.getElementById('predictorEnabled').checked = Boolean(data.enabled);
        document.getElementById('targetNumber').checked = data.prediction_targets.includes('number');
        document.getElementById('targetBigSmall').checked = data.prediction_targets.includes('big_small');
        document.getElementById('targetOddEven').checked = data.prediction_targets.includes('odd_even');
        document.getElementById('targetCombo').checked = data.prediction_targets.includes('combo');
        this.presetExpanded = false;
        this.renderPresetCards();
        this.showModal();
    }

    showModal() {
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
        document.getElementById('apiKey').value = '';
        document.getElementById('historyWindow').value = '60';
        document.getElementById('temperature').value = '0.7';
        document.getElementById('dataInjectionMode').value = 'summary';
        document.getElementById('systemPrompt').value = '';
        document.getElementById('predictorEnabled').checked = true;
        document.getElementById('targetNumber').checked = true;
        document.getElementById('targetBigSmall').checked = true;
        document.getElementById('targetOddEven').checked = true;
        document.getElementById('targetCombo').checked = true;
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
        document.getElementById('historyWindow').value = String(preset.historyWindow);
        document.getElementById('temperature').value = String(preset.temperature);
        document.getElementById('dataInjectionMode').value = preset.injectionMode;
        document.getElementById('systemPrompt').value = preset.prompt;
        document.getElementById('targetNumber').checked = preset.targets.includes('number');
        document.getElementById('targetBigSmall').checked = preset.targets.includes('big_small');
        document.getElementById('targetOddEven').checked = preset.targets.includes('odd_even');
        document.getElementById('targetCombo').checked = preset.targets.includes('combo');
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
            api_key: document.getElementById('apiKey').value.trim(),
            history_window: Number(document.getElementById('historyWindow').value || 60),
            temperature: Number(document.getElementById('temperature').value || 0.7),
            data_injection_mode: document.getElementById('dataInjectionMode').value,
            system_prompt: document.getElementById('systemPrompt').value.trim(),
            enabled: document.getElementById('predictorEnabled').checked,
            prediction_targets: predictionTargets
        };
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

    targetLabel(target) {
        const mapping = {
            number: '号码',
            big_small: '大小',
            odd_even: '单双',
            combo: '组合'
        };
        return mapping[target] || target;
    }

    predictionStatusLabel(status) {
        const mapping = {
            pending: '待开奖',
            settled: '已结算',
            failed: '执行失败'
        };
        return mapping[status] || status;
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
