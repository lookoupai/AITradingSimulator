class AdminPage {
    constructor() {
        this.currentUser = null;
        this.refreshTimer = null;
        this.darkMode = localStorage.getItem('pc28Theme') === 'dark';
        this.init();
    }

    async init() {
        this.applyTheme();
        this.initEventListeners();
        await this.checkAuth();
        await this.loadDashboard();
        this.refreshTimer = setInterval(() => this.loadDashboard(), 15000);
    }

    initEventListeners() {
        document.getElementById('adminThemeToggle').addEventListener('click', () => this.toggleTheme());
        document.getElementById('adminRefreshBtn').addEventListener('click', () => this.loadDashboard());
        document.getElementById('adminLogoutBtn').addEventListener('click', () => this.logout());
        document.getElementById('adminUsersBody').addEventListener('click', (event) => {
            const button = event.target.closest('[data-action="toggle-admin"]');
            if (!button) {
                return;
            }
            this.toggleAdmin(button.dataset.userId);
        });
        const usersCards = document.getElementById('adminUsersCards');
        if (usersCards) {
            usersCards.addEventListener('click', (event) => {
                const button = event.target.closest('[data-action="toggle-admin"]');
                if (!button) {
                    return;
                }
                this.toggleAdmin(button.dataset.userId);
            });
        }
        document.getElementById('adminPredictorsBody').addEventListener('click', (event) => {
            const button = event.target.closest('[data-action="toggle-predictor"], [data-action="resume-predictor"]');
            if (!button) {
                return;
            }
            if (button.dataset.action === 'resume-predictor') {
                this.resumePredictor(button.dataset.predictorId);
                return;
            }
            this.togglePredictor(button.dataset.predictorId);
        });
        const predictorsCards = document.getElementById('adminPredictorsCards');
        if (predictorsCards) {
            predictorsCards.addEventListener('click', (event) => {
                const button = event.target.closest('[data-action="toggle-predictor"], [data-action="resume-predictor"]');
                if (!button) {
                    return;
                }
                if (button.dataset.action === 'resume-predictor') {
                    this.resumePredictor(button.dataset.predictorId);
                    return;
                }
                this.togglePredictor(button.dataset.predictorId);
            });
        }
        document.getElementById('adminPredictionGuardPanel').addEventListener('click', (event) => {
            const button = event.target.closest('[data-action="save-guard-settings"]');
            if (!button) {
                return;
            }
            this.savePredictionGuardSettings();
        });
        document.getElementById('adminNotificationSettingsPanel').addEventListener('click', (event) => {
            const saveButton = event.target.closest('[data-action="save-notification-settings"]');
            if (saveButton) {
                this.saveNotificationSettings();
                return;
            }
            const testButton = event.target.closest('[data-action="send-notification-test"]');
            if (testButton) {
                this.sendNotificationTest();
            }
        });
        const dataHealthPanel = document.getElementById('adminJingcaiDataHealthPanel');
        if (dataHealthPanel) {
            dataHealthPanel.addEventListener('click', (event) => {
                const button = event.target.closest('[data-action="run-jingcai-backfill"]');
                if (!button) {
                    return;
                }
                this.runJingcaiBackfill();
            });
        }
        const addToggle = document.getElementById('adminExternalAddToggle');
        if (addToggle) {
            addToggle.addEventListener('click', () => {
                const block = document.getElementById('adminExternalSourceCreate');
                if (block) {
                    block.style.display = block.style.display === 'none' ? '' : 'none';
                }
            });
        }
        const createButton = document.querySelector('[data-action="create-external-source"]');
        if (createButton) {
            createButton.addEventListener('click', () => this.createExternalSource());
        }
        const pluginKeySelect = document.getElementById('adminExternalPluginKey');
        if (pluginKeySelect) {
            pluginKeySelect.addEventListener('change', () => this.syncExternalApiKeyField());
        }
        const externalPanel = document.getElementById('adminExternalSourcesPanel');
        if (externalPanel) {
            externalPanel.addEventListener('click', (event) => {
                const button = event.target.closest('[data-ext-action]');
                if (!button) {
                    return;
                }
                const action = button.dataset.extAction;
                const sourceId = button.dataset.sourceId;
                if (action === 'test-source') {
                    this.testExternalSource(sourceId, button);
                    return;
                }
                if (action === 'refresh-catalog') {
                    this.refreshExternalCatalog(sourceId, button);
                    return;
                }
                if (action === 'toggle-source') {
                    this.toggleExternalSource(sourceId, button.dataset.enabled === '1');
                    return;
                }
                if (action === 'save-source') {
                    this.saveExternalSourceSettings(sourceId);
                    return;
                }
                if (action === 'toggle-models') {
                    this.toggleExternalModelsView(sourceId);
                    return;
                }
                if (action === 'models-enable') {
                    this.setExternalModelsEnabled(sourceId, 'all', true);
                    return;
                }
                if (action === 'models-disable') {
                    this.setExternalModelsEnabled(sourceId, 'all', false);
                    return;
                }
                if (action === 'delete-source') {
                    this.deleteExternalSource(sourceId);
                    return;
                }
                if (action === 'toggle-model') {
                    this.setExternalModelsEnabled(sourceId, [button.dataset.modelKey], button.dataset.enabled !== '1');
                }
            });
        }
    }

    async checkAuth() {
        const response = await fetch('/api/auth/me', { credentials: 'include' });
        if (!response.ok) {
            window.location.href = '/login';
            return;
        }

        const data = await response.json();
        if (!data.is_admin) {
            window.location.href = '/dashboard';
            return;
        }

        this.currentUser = data;
        document.getElementById('adminUserInfo').textContent = `管理员：${data.username}`;
    }

    async loadDashboard() {
        try {
            const response = await fetch('/api/admin/dashboard', { credentials: 'include' });
            if (response.status === 401) {
                window.location.href = '/login';
                return;
            }
            if (response.status === 403) {
                window.location.href = '/dashboard';
                return;
            }

            const data = await response.json();
            if (!response.ok) {
                throw new Error(data.error || '加载后台数据失败');
            }

            this.renderSummary(data.summary || {});
            this.renderScheduler(data.scheduler || {});
            this.renderJingcaiDataHealth(data.jingcai_data_health || {});
            this.renderPredictionGuard(data.prediction_guard || {});
            this.renderNotificationSettings(data.notification_settings || {});
            this.renderUsers(data.users || []);
            this.renderPredictors(data.predictors || []);
            this.renderFailures(data.recent_failures || []);
            this.renderExternalSources(data.external_sources || {});
        } catch (error) {
            console.error('Failed to load admin dashboard:', error);
            document.getElementById('adminSchedulerPanel').innerHTML = `<div class="warning-banner">${this.escapeHtml(error.message)}</div>`;
        }
    }

    renderSummary(summary) {
        const container = document.getElementById('adminSummaryGrid');
        const items = [
            ['总用户数', summary.total_users || 0],
            ['管理员数', summary.admin_users || 0],
            ['总方案数', summary.total_predictors || 0],
            ['启用方案数', summary.enabled_predictors || 0],
            ['自动暂停数', summary.auto_paused_predictors || 0],
            ['公开方案数', summary.shared_predictors || 0],
            ['待结算预测', summary.pending_predictions || 0],
            ['失败预测', summary.failed_predictions || 0],
            ['已结算预测', summary.settled_predictions || 0]
        ];

        container.innerHTML = items.map(([label, value]) => `
            <article class="stat-card">
                <span class="stat-label">${this.escapeHtml(label)}</span>
                <strong class="stat-value">${this.escapeHtml(String(value))}</strong>
            </article>
        `).join('');
    }

    renderPredictionGuard(settings) {
        const container = document.getElementById('adminPredictionGuardPanel');
        const enabled = Boolean(settings.enabled);
        const threshold = Number(settings.threshold || 3);

        container.className = 'prediction-summary';
        container.innerHTML = `
            <div class="form-grid">
                <label class="toggle-row">
                    <span>启用自动暂停</span>
                    <input type="checkbox" id="adminPredictionGuardEnabled" ${enabled ? 'checked' : ''}>
                </label>
                <label class="form-field">
                    <span>连续失败阈值</span>
                    <input type="number" id="adminPredictionGuardThreshold" min="1" max="20" value="${this.escapeHtml(String(threshold))}">
                    <small class="field-hint">按预测执行周期计数：PC28 按期号，竞彩足球按批次。</small>
                </label>
            </div>
            <div class="panel-actions">
                <button class="btn primary" data-action="save-guard-settings">
                    <i class="bi bi-shield-check"></i>
                    保存设置
                </button>
            </div>
        `;
    }

    renderNotificationSettings(settings) {
        const container = document.getElementById('adminNotificationSettingsPanel');
        const enabled = Boolean(settings.enabled);
        const botName = String(settings.telegram_bot_name || '');
        const maskedToken = String(settings.telegram_bot_token_masked || '');

        container.className = 'prediction-summary';
        container.innerHTML = `
            <div class="form-grid">
                <label class="toggle-row">
                    <span>启用通知服务</span>
                    <input type="checkbox" id="adminNotificationEnabled" ${enabled ? 'checked' : ''}>
                </label>
                <label class="form-field">
                    <span>Bot 名称</span>
                    <input type="text" id="adminTelegramBotName" value="${this.escapeHtml(botName)}" placeholder="例如：predictor_bot">
                    <small class="field-hint">仅用于后台展示和通知消息头部标识。</small>
                </label>
                <label class="form-field span-2">
                    <span>Telegram Bot Token</span>
                    <input type="password" id="adminTelegramBotToken" value="" placeholder="${this.escapeHtml(maskedToken || '留空表示保持原值')}">
                    <small class="field-hint">如果当前已配置 Token，这里留空将保持原值，不会覆盖。</small>
                </label>
                <label class="form-field">
                    <span>测试接收端标识</span>
                    <input type="text" id="adminTelegramTestChatId" value="" placeholder="例如：123456789 或 @channel_name">
                </label>
                <label class="form-field span-2">
                    <span>测试消息内容</span>
                    <textarea id="adminTelegramTestMessage" rows="3" placeholder="例如：这是一条后台测试消息。">AITradingSimulator Telegram 测试消息</textarea>
                </label>
            </div>
            <div class="panel-actions">
                <button class="btn primary" data-action="save-notification-settings">
                    <i class="bi bi-bell"></i>
                    保存通知设置
                </button>
                <button class="btn ghost" data-action="send-notification-test">
                    <i class="bi bi-send"></i>
                    发送测试消息
                </button>
            </div>
            <div class="metric-hint">
                <div class="metric-hint-head">
                    <div><strong>当前状态</strong></div>
                    <span class="tag">${enabled ? '已启用' : '未启用'}</span>
                </div>
                <p>当前 Bot 名称：${this.escapeHtml(botName || '--')}</p>
                <p class="metric-hint-foot">当前 Token：${this.escapeHtml(maskedToken || '未配置')}</p>
            </div>
        `;
    }

    renderScheduler(scheduler) {
        const container = document.getElementById('adminSchedulerPanel');
        const heartbeatText = scheduler.heartbeat_at || '--';
        const ageText = scheduler.seconds_since_heartbeat === null || scheduler.seconds_since_heartbeat === undefined
            ? '--'
            : `${scheduler.seconds_since_heartbeat} 秒前`;
        const statusText = scheduler.auto_prediction_enabled ? '自动预测已启用' : '自动预测已关闭';

        container.className = 'prediction-summary';
        container.innerHTML = `
            <div class="prediction-grid prediction-grid-compact">
                <div class="prediction-card">
                    <span class="mini-label">调度任务</span>
                    <strong>${this.escapeHtml(scheduler.name || '--')}</strong>
                </div>
                <div class="prediction-card">
                    <span class="mini-label">运行状态</span>
                    <strong>${this.escapeHtml(statusText)}</strong>
                </div>
                <div class="prediction-card">
                    <span class="mini-label">轮询间隔</span>
                    <strong>${this.escapeHtml(String(scheduler.poll_interval_seconds || '--'))} 秒</strong>
                </div>
                <div class="prediction-card">
                    <span class="mini-label">最近心跳</span>
                    <strong>${this.escapeHtml(heartbeatText)}</strong>
                    <span class="card-hint">${this.escapeHtml(ageText)}</span>
                </div>
                <div class="prediction-card">
                    <span class="mini-label">持有者</span>
                    <strong class="share-link-text">${this.escapeHtml(scheduler.owner_id || '--')}</strong>
                </div>
            </div>
        `;
    }

    renderJingcaiDataHealth(health) {
        const container = document.getElementById('adminJingcaiDataHealthPanel');
        if (!container) {
            return;
        }
        const metrics = health.metrics || {};
        const scheduler = health.scheduler || {};
        const recentJobs = health.recent_jobs || [];
        const metricRows = [
            ['已开奖样本', metrics.settled],
            ['SPF 赔率', metrics.spf_odds],
            ['RQSPF 赔率', metrics.rqspf_odds],
            ['近期战绩', metrics.recent_form],
            ['伤停', metrics.injury],
            ['欧赔快照', metrics.euro_odds_snapshot]
        ];
        const today = new Date();
        const endDate = new Date(today.getTime() - 24 * 60 * 60 * 1000);
        const startDate = new Date(endDate.getTime() - 6 * 24 * 60 * 60 * 1000);
        const formatDate = (date) => date.toISOString().slice(0, 10);
        container.className = 'prediction-summary';
        container.innerHTML = `
            <div class="prediction-grid prediction-grid-compact">
                <div class="prediction-card">
                    <span class="mini-label">本地历史总场次</span>
                    <strong>${this.escapeHtml(String(health.total_event_count || 0))}</strong>
                </div>
                <div class="prediction-card">
                    <span class="mini-label">健康抽样</span>
                    <strong>${this.escapeHtml(String(health.sample_count || 0))}</strong>
                    <span class="card-hint">${this.escapeHtml(health.earliest_sample_date || '--')} 至 ${this.escapeHtml(health.latest_sample_date || '--')}</span>
                </div>
                <div class="prediction-card">
                    <span class="mini-label">回测基础</span>
                    <strong>${health.enough_for_backtest ? '可用' : '需补齐'}</strong>
                </div>
                <div class="prediction-card">
                    <span class="mini-label">自动补齐</span>
                    <strong>${scheduler.enabled ? '已启用' : '未启用'}</strong>
                    <span class="card-hint">最近 ${this.escapeHtml(String(scheduler.lookback_days || 0))} 天</span>
                </div>
            </div>
            <div class="form-grid">
                ${metricRows.map(([label, metric]) => `
                    <div class="metric-hint">
                        <div class="metric-hint-head">
                            <div><strong>${this.escapeHtml(label)}</strong></div>
                            <span class="tag">${metric?.rate === null || metric?.rate === undefined ? '--' : `${this.escapeHtml(String(metric.rate))}%`}</span>
                        </div>
                        <p>覆盖样本：${this.escapeHtml(String(metric?.count || 0))} / ${this.escapeHtml(String(health.sample_count || 0))}</p>
                    </div>
                `).join('')}
            </div>
            <div class="form-grid">
                <label class="form-field">
                    <span>开始日期</span>
                    <input type="date" id="adminJingcaiBackfillStartDate" value="${this.escapeHtml(formatDate(startDate))}">
                </label>
                <label class="form-field">
                    <span>结束日期</span>
                    <input type="date" id="adminJingcaiBackfillEndDate" value="${this.escapeHtml(formatDate(endDate))}">
                </label>
                <label class="toggle-row">
                    <span>补齐详情字段</span>
                    <input type="checkbox" id="adminJingcaiBackfillIncludeDetails" checked>
                </label>
                <div class="panel-actions">
                    <button class="btn primary" data-action="run-jingcai-backfill">
                        <i class="bi bi-cloud-arrow-down"></i>
                        补齐历史数据
                    </button>
                </div>
            </div>
            <div class="metric-hint">
                <div class="metric-hint-head">
                    <div><strong>最近补齐任务</strong></div>
                    <span class="tag">${this.escapeHtml(String(recentJobs.length || 0))}</span>
                </div>
                ${recentJobs.length ? recentJobs.map((job) => `
                    <p>
                        #${this.escapeHtml(String(job.id || '--'))}
                        ${this.escapeHtml(job.start_date || '--')} 至 ${this.escapeHtml(job.end_date || '--')}
                        · ${this.escapeHtml(job.status || '--')}
                        · 场次 ${this.escapeHtml(String(job.match_count || 0))}
                        · 详情 ${this.escapeHtml(String(job.detail_count || 0))}
                        ${job.error_message ? `· ${this.escapeHtml(job.error_message)}` : ''}
                    </p>
                `).join('') : '<p class="metric-hint-foot">暂无补齐任务</p>'}
            </div>
        `;
    }

    renderUsers(users) {
        const tbody = document.getElementById('adminUsersBody');
        const cards = document.getElementById('adminUsersCards');
        if (!users.length) {
            tbody.innerHTML = '<tr><td colspan="9" class="empty-cell">暂无用户数据</td></tr>';
            if (cards) {
                cards.innerHTML = '<div class="empty-panel">暂无用户数据</div>';
            }
            return;
        }

        tbody.innerHTML = users.map((user) => `
            <tr>
                <td>${user.id}</td>
                <td>${this.escapeHtml(user.username)}</td>
                <td>${this.escapeHtml(user.email || '--')}</td>
                <td>${user.is_admin ? '<span class="tag">管理员</span>' : '<span class="hint-text">普通用户</span>'}</td>
                <td>${user.predictor_count || 0}</td>
                <td>${user.enabled_predictor_count || 0}</td>
                <td>${this.escapeHtml(user.latest_predictor_update || '--')}</td>
                <td>${this.escapeHtml(user.created_at || '--')}</td>
                <td>
                    <button class="btn ghost compact" data-action="toggle-admin" data-user-id="${user.id}">
                        ${user.is_admin ? '取消管理员' : '设为管理员'}
                    </button>
                </td>
            </tr>
        `).join('');
        if (cards) {
            cards.innerHTML = users.map((user) => `
                <article class="prediction-card">
                    <div class="detail-list">
                        <div class="detail-row"><span class="detail-label">用户</span><strong>${this.escapeHtml(user.username)}</strong></div>
                        <div class="detail-row"><span class="detail-label">ID</span><strong>${this.escapeHtml(String(user.id || '--'))}</strong></div>
                        <div class="detail-row"><span class="detail-label">邮箱</span><strong>${this.escapeHtml(user.email || '--')}</strong></div>
                        <div class="detail-row"><span class="detail-label">角色</span><strong>${user.is_admin ? '管理员' : '普通用户'}</strong></div>
                        <div class="detail-row"><span class="detail-label">方案数/启用</span><strong>${this.escapeHtml(String(user.predictor_count || 0))} / ${this.escapeHtml(String(user.enabled_predictor_count || 0))}</strong></div>
                        <div class="detail-row"><span class="detail-label">最近方案更新时间</span><strong>${this.escapeHtml(user.latest_predictor_update || '--')}</strong></div>
                        <div class="detail-row"><span class="detail-label">注册时间</span><strong>${this.escapeHtml(user.created_at || '--')}</strong></div>
                    </div>
                    <div class="share-panel-actions">
                        <button class="btn ghost compact" data-action="toggle-admin" data-user-id="${user.id}">
                            ${user.is_admin ? '取消管理员' : '设为管理员'}
                        </button>
                    </div>
                </article>
            `).join('');
        }
    }

    renderPredictors(predictors) {
        const tbody = document.getElementById('adminPredictorsBody');
        const cards = document.getElementById('adminPredictorsCards');
        if (!predictors.length) {
            tbody.innerHTML = '<tr><td colspan="13" class="empty-cell">暂无方案数据</td></tr>';
            if (cards) {
                cards.innerHTML = '<div class="empty-panel">暂无方案数据</div>';
            }
            return;
        }

        tbody.innerHTML = predictors.map((item) => `
            <tr>
                <td>${item.id}</td>
                <td>${this.escapeHtml(item.username || '--')}</td>
                <td>
                    <strong>${this.escapeHtml(item.name)}</strong><br>
                    <span class="hint-text">${this.escapeHtml(item.model_name || '--')}</span>
                </td>
                <td>${this.escapeHtml(item.primary_metric_label || '--')}</td>
                <td>
                    <strong>${this.escapeHtml(item.profit_rule_label || '--')}</strong><br>
                    <span class="hint-text">${this.escapeHtml(item.profit_default_metric_label || '--')}</span>
                </td>
                <td>${this.escapeHtml(item.share_level || '--')}</td>
                <td>${item.prediction_count || 0}</td>
                <td>${item.failed_prediction_count || 0}</td>
                <td>${this.escapeHtml(String(item.consecutive_ai_failures || 0))}</td>
                <td>${this.escapeHtml(item.latest_issue_no || '--')}</td>
                <td>${this.escapeHtml(item.latest_prediction_update || item.updated_at || '--')}</td>
                <td>${this.renderPredictorStatus(item)}</td>
                <td>
                    <button class="btn ghost compact" data-action="${item.auto_paused ? 'resume-predictor' : 'toggle-predictor'}" data-predictor-id="${item.id}">
                        ${item.auto_paused ? '恢复运行' : (item.enabled ? '停用' : '启用')}
                    </button>
                </td>
            </tr>
        `).join('');
        if (cards) {
            cards.innerHTML = predictors.map((item) => `
                <article class="prediction-card">
                    <div class="detail-list">
                        <div class="detail-row"><span class="detail-label">方案</span><strong>${this.escapeHtml(item.name || '--')}</strong></div>
                        <div class="detail-row"><span class="detail-label">ID / 用户</span><strong>${this.escapeHtml(String(item.id || '--'))} / ${this.escapeHtml(item.username || '--')}</strong></div>
                        <div class="detail-row"><span class="detail-label">模型</span><strong>${this.escapeHtml(item.model_name || '--')}</strong></div>
                        <div class="detail-row"><span class="detail-label">主玩法</span><strong>${this.escapeHtml(item.primary_metric_label || '--')}</strong></div>
                        <div class="detail-row"><span class="detail-label">默认收益规则/玩法</span><strong>${this.escapeHtml(item.profit_rule_label || '--')} / ${this.escapeHtml(item.profit_default_metric_label || '--')}</strong></div>
                        <div class="detail-row"><span class="detail-label">公开层级</span><strong>${this.escapeHtml(item.share_level || '--')}</strong></div>
                        <div class="detail-row"><span class="detail-label">预测/失败</span><strong>${this.escapeHtml(String(item.prediction_count || 0))} / ${this.escapeHtml(String(item.failed_prediction_count || 0))}</strong></div>
                        <div class="detail-row"><span class="detail-label">连续失败</span><strong>${this.escapeHtml(String(item.consecutive_ai_failures || 0))}</strong></div>
                        <div class="detail-row"><span class="detail-label">最近期号</span><strong>${this.escapeHtml(item.latest_issue_no || '--')}</strong></div>
                        <div class="detail-row"><span class="detail-label">最近更新时间</span><strong>${this.escapeHtml(item.latest_prediction_update || item.updated_at || '--')}</strong></div>
                        <div class="detail-row"><span class="detail-label">状态</span><strong>${this.escapeHtml(item.runtime_status_label || '--')}</strong></div>
                        <div class="detail-row"><span class="detail-label">最近错误</span><strong>${this.escapeHtml(item.last_ai_error_message || item.auto_pause_reason || '--')}</strong></div>
                    </div>
                    <div class="share-panel-actions">
                        <button class="btn ghost compact" data-action="${item.auto_paused ? 'resume-predictor' : 'toggle-predictor'}" data-predictor-id="${item.id}">
                            ${item.auto_paused ? '恢复运行' : (item.enabled ? '停用' : '启用')}
                        </button>
                    </div>
                </article>
            `).join('');
        }
    }

    renderFailures(items) {
        const tbody = document.getElementById('adminFailuresBody');
        const cards = document.getElementById('adminFailuresCards');
        if (!items.length) {
            tbody.innerHTML = '<tr><td colspan="7" class="empty-cell">最近暂无失败记录</td></tr>';
            if (cards) {
                cards.innerHTML = '<div class="empty-panel">最近暂无失败记录</div>';
            }
            return;
        }

        tbody.innerHTML = items.map((item) => `
            <tr>
                <td>${this.escapeHtml(item.updated_at || '--')}</td>
                <td>${this.escapeHtml(item.lottery_label || '--')}</td>
                <td>${this.escapeHtml(item.username || '--')}</td>
                <td>${this.escapeHtml(item.predictor_name || '--')}</td>
                <td>${this.escapeHtml(item.issue_no || '--')}</td>
                <td>${this.escapeHtml(item.status || '--')}</td>
                <td>${this.escapeHtml(item.error_message || '--')}</td>
            </tr>
        `).join('');
        if (cards) {
            cards.innerHTML = items.map((item) => `
                <article class="prediction-card">
                    <div class="detail-list">
                        <div class="detail-row"><span class="detail-label">更新时间</span><strong>${this.escapeHtml(item.updated_at || '--')}</strong></div>
                        <div class="detail-row"><span class="detail-label">彩种</span><strong>${this.escapeHtml(item.lottery_label || '--')}</strong></div>
                        <div class="detail-row"><span class="detail-label">用户</span><strong>${this.escapeHtml(item.username || '--')}</strong></div>
                        <div class="detail-row"><span class="detail-label">方案</span><strong>${this.escapeHtml(item.predictor_name || '--')}</strong></div>
                        <div class="detail-row"><span class="detail-label">期号/批次</span><strong>${this.escapeHtml(item.issue_no || '--')}</strong></div>
                        <div class="detail-row"><span class="detail-label">状态</span><strong>${this.escapeHtml(item.status || '--')}</strong></div>
                        <div class="detail-row"><span class="detail-label">错误</span><strong>${this.escapeHtml(item.error_message || '--')}</strong></div>
                    </div>
                </article>
            `).join('');
        }
    }

    renderPredictorStatus(item) {
        if (!item.enabled) {
            return '<span class="hint-text">手动停用</span>';
        }
        if (item.auto_paused) {
            const reason = item.auto_pause_reason || item.last_ai_error_message || '--';
            return `
                <span class="status-chip paused">自动暂停</span><br>
                <span class="hint-text" title="${this.escapeHtml(reason)}">连错 ${this.escapeHtml(String(item.consecutive_ai_failures || 0))} 次</span>
            `;
        }
        return '<span class="tag">启用中</span>';
    }

    async toggleAdmin(userId) {
        try {
            const response = await fetch(`/api/admin/users/${userId}/toggle-admin`, {
                method: 'POST',
                credentials: 'include'
            });
            const data = await response.json();
            if (!response.ok) {
                throw new Error(data.error || '更新管理员状态失败');
            }
            await this.loadDashboard();
        } catch (error) {
            alert(error.message);
        }
    }

    async togglePredictor(predictorId) {
        try {
            const response = await fetch(`/api/admin/predictors/${predictorId}/toggle-enabled`, {
                method: 'POST',
                credentials: 'include'
            });
            const data = await response.json();
            if (!response.ok) {
                throw new Error(data.error || '更新方案状态失败');
            }
            await this.loadDashboard();
        } catch (error) {
            alert(error.message);
        }
    }

    async resumePredictor(predictorId) {
        try {
            const response = await fetch(`/api/admin/predictors/${predictorId}/resume-auto-pause`, {
                method: 'POST',
                credentials: 'include'
            });
            const data = await response.json();
            if (!response.ok) {
                throw new Error(data.error || '恢复方案运行失败');
            }
            await this.loadDashboard();
        } catch (error) {
            alert(error.message);
        }
    }

    async savePredictionGuardSettings() {
        const enabled = document.getElementById('adminPredictionGuardEnabled')?.checked ?? true;
        const thresholdValue = document.getElementById('adminPredictionGuardThreshold')?.value ?? '3';
        try {
            const response = await fetch('/api/admin/settings/prediction-guard', {
                method: 'POST',
                credentials: 'include',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    enabled,
                    threshold: Number(thresholdValue)
                })
            });
            const data = await response.json();
            if (!response.ok) {
                throw new Error(data.error || '保存 AI 故障保护设置失败');
            }
            this.renderPredictionGuard(data.settings || {});
            await this.loadDashboard();
        } catch (error) {
            alert(error.message);
        }
    }

    async saveNotificationSettings() {
        const enabled = document.getElementById('adminNotificationEnabled')?.checked ?? false;
        const telegram_bot_name = document.getElementById('adminTelegramBotName')?.value ?? '';
        const telegram_bot_token = document.getElementById('adminTelegramBotToken')?.value ?? '';
        try {
            const response = await fetch('/api/admin/settings/notifications', {
                method: 'POST',
                credentials: 'include',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    enabled,
                    telegram_bot_name,
                    telegram_bot_token
                })
            });
            const data = await response.json();
            if (!response.ok) {
                throw new Error(data.error || '保存通知设置失败');
            }
            this.renderNotificationSettings(data.settings || {});
            await this.loadDashboard();
        } catch (error) {
            alert(error.message);
        }
    }

    async sendNotificationTest() {
        const chat_id = document.getElementById('adminTelegramTestChatId')?.value ?? '';
        const message = document.getElementById('adminTelegramTestMessage')?.value ?? '';
        try {
            const response = await fetch('/api/admin/settings/notifications/test', {
                method: 'POST',
                credentials: 'include',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    chat_id,
                    message
                })
            });
            const data = await response.json();
            if (!response.ok) {
                throw new Error(data.error || '发送测试消息失败');
            }
            alert(data.message || '测试消息发送成功');
        } catch (error) {
            alert(error.message);
        }
    }

    async runJingcaiBackfill() {
        const start_date = document.getElementById('adminJingcaiBackfillStartDate')?.value || '';
        const end_date = document.getElementById('adminJingcaiBackfillEndDate')?.value || start_date;
        const include_details = document.getElementById('adminJingcaiBackfillIncludeDetails')?.checked ?? true;
        try {
            const response = await fetch('/api/jingcai-football/history-backfill', {
                method: 'POST',
                credentials: 'include',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    start_date,
                    end_date,
                    include_details
                })
            });
            const data = await response.json();
            if (!response.ok) {
                throw new Error(data.error || '补齐历史数据失败');
            }
            alert(data.message || '补齐历史数据完成');
            await this.loadDashboard();
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

    applyTheme() {
        document.body.classList.toggle('dark-mode', this.darkMode);
        const icon = document.querySelector('#adminThemeToggle i');
        if (icon) {
            icon.className = this.darkMode ? 'bi bi-sun' : 'bi bi-moon-stars';
        }
    }

    toggleTheme() {
        this.darkMode = !this.darkMode;
        localStorage.setItem('pc28Theme', this.darkMode ? 'dark' : 'light');
        this.applyTheme();
    }

    externalSourceStatusChip(source) {
        const map = {
            ok: '<span class="tag">正常</span>',
            error: `<span class="status-chip paused" title="${this.escapeHtml(source.last_error || '')}">异常</span>`,
            disabled: '<span class="hint-text">已停用</span>'
        };
        return map[source.status] || '<span class="hint-text">未知</span>';
    }

    syncExternalPluginOptions(plugins) {
        const select = document.getElementById('adminExternalPluginKey');
        if (!select || !Array.isArray(plugins) || !plugins.length) {
            return;
        }
        const current = select.value;
        select.innerHTML = plugins.map((plugin) => `
            <option value="${this.escapeHtml(plugin.plugin_key)}">${this.escapeHtml(plugin.display_name || plugin.plugin_key)}</option>
        `).join('');
        if (plugins.some((plugin) => plugin.plugin_key === current)) {
            select.value = current;
        }
    }

    captureExternalUiState(container) {
        const state = {};
        const activeElement = document.activeElement;
        if (activeElement && container.contains(activeElement) && activeElement.id) {
            state._activeElementId = activeElement.id;
            try {
                state._activeSelectionStart = activeElement.selectionStart ?? null;
            } catch (error) {
                state._activeSelectionStart = null;
            }
        }
        container.querySelectorAll('[data-ext-source]').forEach((card) => {
            const sourceId = card.dataset.extSource;
            const modelsWrapper = document.getElementById(`extModels-${sourceId}`);
            const expanded = Boolean(modelsWrapper) && modelsWrapper.style.display !== 'none';
            const modelsBody = document.getElementById(`extModelsBody-${sourceId}`);
            state[sourceId] = {
                modelsExpanded: expanded,
                modelsBodyHtml: expanded && modelsBody ? modelsBody.innerHTML : '',
                testResult: document.getElementById(`extTestResult-${sourceId}`)?.innerHTML || '',
                form: {
                    name: document.getElementById(`extSourceName-${sourceId}`)?.value ?? null,
                    baseUrl: document.getElementById(`extSourceBaseUrl-${sourceId}`)?.value ?? null,
                    interval: document.getElementById(`extSourceInterval-${sourceId}`)?.value ?? null
                }
            };
        });
        return state;
    }

    restoreExternalUiState(container, state) {
        if (!state) {
            return;
        }
        Object.entries(state).forEach(([sourceId, snapshot]) => {
            const form = snapshot.form || {};
            const nameInput = document.getElementById(`extSourceName-${sourceId}`);
            if (nameInput && form.name !== null) {
                nameInput.value = form.name;
            }
            const baseUrlInput = document.getElementById(`extSourceBaseUrl-${sourceId}`);
            if (baseUrlInput && form.baseUrl !== null) {
                baseUrlInput.value = form.baseUrl;
            }
            const intervalInput = document.getElementById(`extSourceInterval-${sourceId}`);
            if (intervalInput && form.interval !== null) {
                intervalInput.value = form.interval;
            }
            const testNode = document.getElementById(`extTestResult-${sourceId}`);
            if (testNode && snapshot.testResult) {
                testNode.innerHTML = snapshot.testResult;
            }
            if (snapshot.modelsExpanded) {
                const wrapper = document.getElementById(`extModels-${sourceId}`);
                if (wrapper) {
                    wrapper.style.display = '';
                    const body = document.getElementById(`extModelsBody-${sourceId}`);
                    if (body) {
                        if (snapshot.modelsBodyHtml && !snapshot.modelsBodyHtml.includes('加载中')) {
                            body.innerHTML = snapshot.modelsBodyHtml;
                        } else {
                            this.loadExternalModels(sourceId);
                        }
                    }
                }
            }
        });
        const activeElement = state._activeElementId ? document.getElementById(state._activeElementId) : null;
        if (activeElement && container.contains(activeElement)) {
            activeElement.focus();
            if (state._activeSelectionStart !== null && typeof activeElement.setSelectionRange === 'function') {
                try {
                    activeElement.setSelectionRange(state._activeSelectionStart, state._activeSelectionStart);
                } catch (error) {
                    // 非文本输入（如 number 被部分浏览器限制）时忽略光标恢复
                }
            }
        }
    }

    renderExternalSources(payload) {
        const container = document.getElementById('adminExternalSourcesPanel');
        if (!container) {
            return;
        }
        this.externalPlugins = payload.plugins || [];
        this.syncExternalPluginOptions(this.externalPlugins);
        this.syncExternalApiKeyField();
        const scheduler = payload.scheduler || {};
        const sources = payload.sources || [];
        this.externalSources = sources;
        const schedulerRow = `
            <div class="metric-hint">
                <div class="metric-hint-head">
                    <div><strong>外部采集线程</strong></div>
                    <span class="tag">${scheduler.enabled ? '已启用' : '未启用'}</span>
                </div>
                <p>最近心跳：${this.escapeHtml(scheduler.heartbeat_at || '--')}${scheduler.seconds_since_heartbeat !== null && scheduler.seconds_since_heartbeat !== undefined ? `（${this.escapeHtml(String(scheduler.seconds_since_heartbeat))} 秒前）` : ''}</p>
            </div>
        `;
        if (!sources.length) {
            container.className = 'prediction-summary';
            container.innerHTML = `
                ${schedulerRow}
                <div class="warning-banner">还没有外部预测来源。点击右上角「添加来源」接入第一个 API；创建后请先测试连接、刷新目录并开放模型，再启用来源。</div>
            `;
            return;
        }
        container.className = 'prediction-summary';
        const previousUiState = this.captureExternalUiState(container);
        container.innerHTML = [schedulerRow].concat(sources.map((source) => {
            const statusLine = source.last_error
                ? `<p class="hint-text">最近错误：${this.escapeHtml(source.last_error || '--')}（${this.escapeHtml(source.last_error_at || '--')}）</p>`
                : '<p class="hint-text">最近没有错误。</p>';
            const batches = (source.recent_batches || []).map((batch) => `
                <li>
                    期号 ${this.escapeHtml(batch.target_issue_no || '--')}
                    · v${this.escapeHtml(String(batch.batch_version || 1))}
                    ${Number(batch.batch_version) > 1 ? '<span class="tag">上游修订</span>' : ''}
                    · ${this.escapeHtml(batch.fetched_at || '--')}
                    · 模型 ${this.escapeHtml(String(batch.model_count || 0))}${Number(batch.invalid_model_count) > 0 ? ` / 异常 ${this.escapeHtml(String(batch.invalid_model_count))}` : ''}
                </li>
            `).join('');
            return `
            <div class="metric-hint" data-ext-source="${this.escapeHtml(String(source.id))}">
                <div class="metric-hint-head">
                    <div>
                        <strong>${this.escapeHtml(source.name || `来源 #${source.id}`)}</strong>
                        <span class="tag">${this.escapeHtml(source.plugin_display_name || source.plugin_key || '')}</span>
                        ${this.externalSourceStatusChip(source)}
                    </div>
                    <span class="hint-text">模型 ${source.model_enabled_count}/${source.model_total} 开放 · 已采用快照 ${source.adopted_prediction_count} 条${source.bound_predictor_count ? ` · ${source.bound_predictor_count} 个方案绑定` : ''}</span>
                </div>
                <div class="form-grid">
                    <label class="form-field">
                        <span>来源名称</span>
                        <input type="text" id="extSourceName-${source.id}" value="${this.escapeHtml(source.name || '')}">
                    </label>
                    <label class="form-field">
                        <span>采集间隔（秒）</span>
                        <input type="number" id="extSourceInterval-${source.id}" min="30" max="3600" value="${this.escapeHtml(String(source.interval_seconds || 60))}">
                    </label>
                    <label class="form-field span-2">
                        <span>来源基址</span>
                        <input type="text" id="extSourceBaseUrl-${source.id}" value="${this.escapeHtml(source.base_url || '')}">
                        <small class="field-hint">最近目标期号：${this.escapeHtml(source.last_target_issue || '--')} · 最近成功：${this.escapeHtml(source.last_success_at || '--')} · 最近尝试：${this.escapeHtml(source.last_attempt_at || '--')}</small>
                    </label>
                    ${source.requires_api_key ? `
                    <label class="form-field span-2">
                        <span>API Key</span>
                        <input type="password" id="extSourceApiKey-${source.id}" value="" placeholder="${this.escapeHtml(source.masked_api_key || '未设置')}（留空表示保持原值）">
                    </label>
                    ` : ''}
                </div>
                ${statusLine}
                <div class="panel-actions">
                    <button class="btn ghost" data-ext-action="test-source" data-source-id="${source.id}"><i class="bi bi-plug"></i> 测试连接</button>
                    <button class="btn ghost" data-ext-action="refresh-catalog" data-source-id="${source.id}"><i class="bi bi-arrow-repeat"></i> 刷新目录</button>
                    <button class="btn ghost" data-ext-action="toggle-models" data-source-id="${source.id}"><i class="bi bi-list-check"></i> 模型目录</button>
                    <button class="btn ghost" data-ext-action="save-source" data-source-id="${source.id}"><i class="bi bi-save"></i> 保存修改</button>
                    <button class="btn ${source.enabled ? 'ghost' : 'primary'}" data-ext-action="toggle-source" data-source-id="${source.id}" data-enabled="${source.enabled ? '1' : '0'}">
                        <i class="bi ${source.enabled ? 'bi-pause-circle' : 'bi-play-circle'}"></i> ${source.enabled ? '停用采集' : '启用采集'}
                    </button>
                    <button class="btn danger" data-ext-action="delete-source" data-source-id="${source.id}">
                        <i class="bi bi-trash"></i> 删除来源
                    </button>
                </div>
                <div id="extTestResult-${source.id}" class="hint-text"></div>
                <div id="extModels-${source.id}" style="display: none;">
                    <div class="panel-actions">
                        <button class="btn ghost" data-ext-action="models-enable" data-source-id="${source.id}">全部开放</button>
                        <button class="btn ghost" data-ext-action="models-disable" data-source-id="${source.id}">全部关闭</button>
                        <span class="hint-text">开放后用户方可绑定该模型；关闭不影响已有历史。</span>
                    </div>
                    <div id="extModelsBody-${source.id}" class="hint-text">加载中...</div>
                </div>
                <div class="metric-hint">
                    <div class="metric-hint-head"><div><strong>最近采集批次</strong></div></div>
                    <ul class="hint-text">${batches || '<li>暂无批次，等待采集线程运行或手动刷新目录。</li>'}</ul>
                </div>
            </div>
            `;
        }).join(''));
        this.restoreExternalUiState(container, previousUiState);
    }

    syncExternalApiKeyField() {
        const select = document.getElementById('adminExternalPluginKey');
        const field = document.getElementById('adminExternalApiKeyField');
        if (!select || !field) {
            return;
        }
        const plugins = this.externalPlugins || [];
        const plugin = plugins.find((item) => item.plugin_key === select.value);
        field.style.display = plugin && plugin.requires_api_key ? '' : 'none';
    }

    async createExternalSource() {
        const payload = {
            plugin_key: document.getElementById('adminExternalPluginKey')?.value || 'jnd28',
            name: document.getElementById('adminExternalSourceName')?.value || '',
            base_url: document.getElementById('adminExternalSourceBaseUrl')?.value || '',
            interval_seconds: Number(document.getElementById('adminExternalSourceInterval')?.value || 60),
            api_key: document.getElementById('adminExternalSourceApiKey')?.value || ''
        };
        try {
            const response = await fetch('/api/admin/external-sources', {
                method: 'POST',
                credentials: 'include',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });
            const data = await response.json();
            if (!response.ok) {
                throw new Error(data.error || '创建外部预测来源失败');
            }
            const block = document.getElementById('adminExternalSourceCreate');
            if (block) {
                block.style.display = 'none';
            }
            await this.loadDashboard();
            alert(data.message || '已创建');
        } catch (error) {
            alert(error.message);
        }
    }

    async testExternalSource(sourceId, button) {
        const resultNode = document.getElementById(`extTestResult-${sourceId}`);
        if (button) {
            button.disabled = true;
        }
        if (resultNode) {
            resultNode.textContent = '测试中...';
        }
        try {
            const response = await fetch(`/api/admin/external-sources/${sourceId}/test`, { method: 'POST', credentials: 'include' });
            const data = await response.json();
            if (!response.ok) {
                throw new Error(data.error || '测试失败');
            }
            if (!data.ok) {
                if (resultNode) {
                    resultNode.textContent = `测试失败：${data.error}`;
                }
                return;
            }
            if (resultNode) {
                resultNode.textContent = `连接正常：目标期号 ${data.target_issue_no}，模型 ${data.model_count} 个${data.invalid_model_count ? `（${data.invalid_model_count} 个无法解析）` : ''}，耗时 ${data.elapsed_ms}ms，上游发布时间 ${data.upstream_published_at || '--'}。`;
            }
        } catch (error) {
            if (resultNode) {
                resultNode.textContent = `测试失败：${error.message}`;
            }
        } finally {
            if (button) {
                button.disabled = false;
            }
        }
    }

    async refreshExternalCatalog(sourceId, button) {
        if (button) {
            button.disabled = true;
        }
        try {
            const response = await fetch(`/api/admin/external-sources/${sourceId}/refresh-catalog`, { method: 'POST', credentials: 'include' });
            const data = await response.json();
            if (!response.ok) {
                throw new Error(data.error || '刷新目录失败');
            }
            await this.loadDashboard();
            alert(data.message || '目录已刷新');
        } catch (error) {
            alert(error.message);
        } finally {
            if (button) {
                button.disabled = false;
            }
        }
    }

    async toggleExternalSource(sourceId, currentlyEnabled) {
        try {
            const response = await fetch(`/api/admin/external-sources/${sourceId}`, {
                method: 'PUT',
                credentials: 'include',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ enabled: !currentlyEnabled })
            });
            const data = await response.json();
            if (!response.ok) {
                throw new Error(data.error || '更新来源状态失败');
            }
            await this.loadDashboard();
        } catch (error) {
            alert(error.message);
        }
    }

    async deleteExternalSource(sourceId) {
        const source = (this.externalSources || []).find((item) => Number(item.id) === Number(sourceId));
        const sourceName = source ? (source.name || `来源 #${sourceId}`) : `来源 #${sourceId}`;
        const boundCount = source ? Number(source.bound_predictor_count || 0) : 0;
        const warning = boundCount
            ? `该来源下有 ${boundCount} 个用户方案绑定：删除后这些方案将停止产出新预测（历史记录与来源标识保留）。`
            : '当前没有用户方案绑定该来源。';
        if (!window.confirm(`确定删除来源「${sourceName}」？\n\n${warning}\n\n模型目录、采集批次与共享快照将一并删除，此操作不可恢复。`)) {
            return;
        }
        try {
            const response = await fetch(`/api/admin/external-sources/${sourceId}`, { method: 'DELETE', credentials: 'include' });
            const data = await response.json();
            if (!response.ok) {
                throw new Error(data.error || '删除来源失败');
            }
            await this.loadDashboard();
            alert(data.message || '来源已删除');
        } catch (error) {
            alert(error.message);
        }
    }

    async saveExternalSourceSettings(sourceId) {
        const payload = {
            name: document.getElementById(`extSourceName-${sourceId}`)?.value || '',
            base_url: document.getElementById(`extSourceBaseUrl-${sourceId}`)?.value || '',
            interval_seconds: Number(document.getElementById(`extSourceInterval-${sourceId}`)?.value || 60)
        };
        const apiKeyInput = document.getElementById(`extSourceApiKey-${sourceId}`);
        if (apiKeyInput && apiKeyInput.value.trim()) {
            payload.api_key = apiKeyInput.value.trim();
        }
        try {
            const response = await fetch(`/api/admin/external-sources/${sourceId}`, {
                method: 'PUT',
                credentials: 'include',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });
            const data = await response.json();
            if (!response.ok) {
                throw new Error(data.error || '保存来源设置失败');
            }
            await this.loadDashboard();
        } catch (error) {
            alert(error.message);
        }
    }

    async toggleExternalModelsView(sourceId) {
        const wrapper = document.getElementById(`extModels-${sourceId}`);
        if (!wrapper) {
            return;
        }
        const show = wrapper.style.display === 'none';
        wrapper.style.display = show ? '' : 'none';
        if (!show) {
            return;
        }
        await this.loadExternalModels(sourceId);
    }

    async loadExternalModels(sourceId) {
        const body = document.getElementById(`extModelsBody-${sourceId}`);
        if (!body) {
            return;
        }
        try {
            const response = await fetch(`/api/admin/external-sources/${sourceId}/models`, { credentials: 'include' });
            const data = await response.json();
            if (!response.ok) {
                throw new Error(data.error || '加载模型目录失败');
            }
            const models = data.models || [];
            if (!models.length) {
                body.innerHTML = '<div class="warning-banner">模型目录为空，请先「刷新目录」。</div>';
                return;
            }
            body.innerHTML = models.map((model) => `
                <label class="toggle-row">
                    <span>${this.escapeHtml(model.display_name)} <small class="hint-text">${this.escapeHtml(model.model_key)}${model.seen_recently ? '' : ' · 最近未见'}</small></span>
                    <input type="checkbox" data-ext-action="toggle-model" data-source-id="${sourceId}" data-model-key="${this.escapeHtml(model.model_key)}" data-enabled="${model.enabled ? '1' : '0'}" ${model.enabled ? 'checked' : ''}>
                </label>
            `).join('');
        } catch (error) {
            body.innerHTML = `<div class="warning-banner">${this.escapeHtml(error.message)}</div>`;
        }
    }

    async setExternalModelsEnabled(sourceId, modelKeys, enabled) {
        try {
            const response = await fetch(`/api/admin/external-sources/${sourceId}/models`, {
                method: 'PUT',
                credentials: 'include',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ model_keys: modelKeys, enabled })
            });
            const data = await response.json();
            if (!response.ok) {
                throw new Error(data.error || '更新模型开放状态失败');
            }
            await this.loadExternalModels(sourceId);
            await this.loadDashboard();
        } catch (error) {
            alert(error.message);
        }
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
}

new AdminPage();
