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
            const button = event.target.closest('[data-action="toggle-predictor"]');
            if (!button) {
                return;
            }
            this.togglePredictor(button.dataset.predictorId);
        });
        const predictorsCards = document.getElementById('adminPredictorsCards');
        if (predictorsCards) {
            predictorsCards.addEventListener('click', (event) => {
                const button = event.target.closest('[data-action="toggle-predictor"]');
                if (!button) {
                    return;
                }
                this.togglePredictor(button.dataset.predictorId);
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
            tbody.innerHTML = '<tr><td colspan="12" class="empty-cell">暂无方案数据</td></tr>';
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
                <td>${this.escapeHtml(item.latest_issue_no || '--')}</td>
                <td>${this.escapeHtml(item.latest_prediction_update || item.updated_at || '--')}</td>
                <td>${item.enabled ? '<span class="tag">启用中</span>' : '<span class="hint-text">已停用</span>'}</td>
                <td>
                    <button class="btn ghost compact" data-action="toggle-predictor" data-predictor-id="${item.id}">
                        ${item.enabled ? '停用' : '启用'}
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
                        <div class="detail-row"><span class="detail-label">最近期号</span><strong>${this.escapeHtml(item.latest_issue_no || '--')}</strong></div>
                        <div class="detail-row"><span class="detail-label">最近更新时间</span><strong>${this.escapeHtml(item.latest_prediction_update || item.updated_at || '--')}</strong></div>
                        <div class="detail-row"><span class="detail-label">状态</span><strong>${item.enabled ? '启用中' : '已停用'}</strong></div>
                    </div>
                    <div class="share-panel-actions">
                        <button class="btn ghost compact" data-action="toggle-predictor" data-predictor-id="${item.id}">
                            ${item.enabled ? '停用' : '启用'}
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
            tbody.innerHTML = '<tr><td colspan="6" class="empty-cell">最近暂无失败记录</td></tr>';
            if (cards) {
                cards.innerHTML = '<div class="empty-panel">最近暂无失败记录</div>';
            }
            return;
        }

        tbody.innerHTML = items.map((item) => `
            <tr>
                <td>${this.escapeHtml(item.updated_at || '--')}</td>
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
                        <div class="detail-row"><span class="detail-label">用户</span><strong>${this.escapeHtml(item.username || '--')}</strong></div>
                        <div class="detail-row"><span class="detail-label">方案</span><strong>${this.escapeHtml(item.predictor_name || '--')}</strong></div>
                        <div class="detail-row"><span class="detail-label">期号</span><strong>${this.escapeHtml(item.issue_no || '--')}</strong></div>
                        <div class="detail-row"><span class="detail-label">状态</span><strong>${this.escapeHtml(item.status || '--')}</strong></div>
                        <div class="detail-row"><span class="detail-label">错误</span><strong>${this.escapeHtml(item.error_message || '--')}</strong></div>
                    </div>
                </article>
            `).join('');
        }
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
