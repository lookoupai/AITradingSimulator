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
            this.renderPredictionGuard(data.prediction_guard || {});
            this.renderNotificationSettings(data.notification_settings || {});
            this.renderUsers(data.users || []);
            this.renderPredictors(data.predictors || []);
            this.renderFailures(data.recent_failures || []);
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
