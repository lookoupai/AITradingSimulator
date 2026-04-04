class HomePage {
    constructor() {
        this.publicMetric = 'combo';
        this.publicSort = 'recent100';
        this.init();
    }

    async init() {
        await this.checkLoginStatus();
        this.initPublicFilters();
        await this.loadOverview();
        await this.loadPublicPredictors();
        setInterval(() => {
            this.loadOverview();
            this.loadPublicPredictors();
        }, 15000);
    }

    initPublicFilters() {
        const metricSelect = document.getElementById('publicMetricSelect');
        const sortSelect = document.getElementById('publicSortSelect');

        if (metricSelect) {
            metricSelect.addEventListener('change', (event) => {
                this.publicMetric = event.target.value;
                this.loadPublicPredictors();
            });
        }

        if (sortSelect) {
            sortSelect.addEventListener('change', (event) => {
                this.publicSort = event.target.value;
                this.loadPublicPredictors();
            });
        }
    }

    async checkLoginStatus() {
        try {
            const response = await fetch('/api/auth/me', { credentials: 'include' });
            if (response.ok) {
                document.getElementById('loginBtn').style.display = 'none';
                document.getElementById('dashboardBtn').style.display = 'inline-flex';
            }
        } catch (error) {
            console.error('Failed to check login status:', error);
        }
    }

    async loadOverview() {
        try {
            const response = await fetch('/api/pc28/overview?limit=20');
            const overview = await response.json();
            this.renderHero(overview);
            this.renderSummary(overview);
            this.renderDraws(overview.recent_draws || []);
            this.renderTags('omissionList', overview.omission_preview?.top_numbers || [], '期');
            this.renderTags('hotNumberList', overview.today_preview?.hot_numbers || [], '次');
        } catch (error) {
            console.error('Failed to load overview:', error);
        }
    }

    async loadPublicPredictors() {
        try {
            const response = await fetch(`/api/public/predictors?metric=${encodeURIComponent(this.publicMetric)}&sort_by=${encodeURIComponent(this.publicSort)}&limit=10`);
            const data = await response.json();
            this.renderPublicPredictors(data.items || []);
        } catch (error) {
            console.error('Failed to load public predictors:', error);
        }
    }

    renderHero(overview) {
        const latestDraw = overview.latest_draw;
        const warning = overview.warning ? `<div class="warning-banner">${this.escapeHtml(overview.warning)}</div>` : '';

        document.getElementById('heroOverview').innerHTML = `
            ${warning}
            <div class="hero-metric">
                <span class="mini-label">下一期期号</span>
                <strong>${this.escapeHtml(overview.next_issue_no || '--')}</strong>
            </div>
            <div class="hero-metric">
                <span class="mini-label">倒计时</span>
                <strong>${this.escapeHtml(overview.countdown || '--:--:--')}</strong>
            </div>
            <div class="hero-metric">
                <span class="mini-label">最新开奖</span>
                <div class="result-number">${latestDraw ? latestDraw.result_number_text : '--'}</div>
                <div class="badge-row">
                    ${latestDraw ? this.renderBadge(latestDraw.big_small) : ''}
                    ${latestDraw ? this.renderBadge(latestDraw.odd_even) : ''}
                    ${latestDraw ? this.renderBadge(latestDraw.combo) : ''}
                </div>
            </div>
        `;
    }

    renderSummary(overview) {
        const latestDraw = overview.latest_draw;
        const totalIssues = overview.today_preview?.summary?.['总期数'] || '--';
        const values = [
            overview.next_issue_no || '--',
            overview.countdown || '--:--:--',
            latestDraw ? latestDraw.result_number_text : '--',
            totalIssues
        ];

        document.querySelectorAll('#summaryGrid .stat-value').forEach((element, index) => {
            element.textContent = values[index];
        });
    }

    renderDraws(draws) {
        const tbody = document.getElementById('recentDrawsBody');
        if (!draws.length) {
            tbody.innerHTML = '<tr><td colspan="6" class="empty-cell">暂无开奖数据</td></tr>';
            return;
        }

        tbody.innerHTML = draws.map((draw) => `
            <tr>
                <td>${this.escapeHtml(draw.issue_no)}</td>
                <td><strong>${this.escapeHtml(draw.result_number_text)}</strong></td>
                <td>${this.escapeHtml(draw.big_small)}</td>
                <td>${this.escapeHtml(draw.odd_even)}</td>
                <td>${this.escapeHtml(draw.combo)}</td>
                <td>${this.escapeHtml(draw.open_time || '')}</td>
            </tr>
        `).join('');
    }

    renderPublicPredictors(items) {
        const tbody = document.getElementById('publicPredictorBody');
        if (!items.length) {
            tbody.innerHTML = '<tr><td colspan="7" class="empty-cell">暂无公开方案数据</td></tr>';
            return;
        }

        tbody.innerHTML = items.map((item, index) => `
            <tr>
                <td>${index + 1}</td>
                <td>
                    <strong>${this.escapeHtml(item.predictor_name)}</strong><br>
                    <span class="hint-text">${this.escapeHtml(item.username)} · ${this.escapeHtml(item.model_name)}</span>
                </td>
                <td>${this.escapeHtml(item.metric_label || '--')}</td>
                <td>${this.formatRatioRate(item.recent_20)}</td>
                <td>${this.formatRatioRate(item.recent_100)}</td>
                <td>${item.current_hit_streak || 0}</td>
                <td>${item.historical_max_hit_streak || 0}</td>
            </tr>
        `).join('');
    }

    renderTags(targetId, items, suffix) {
        const container = document.getElementById(targetId);
        if (!items.length) {
            container.innerHTML = '<span class="tag">暂无数据</span>';
            return;
        }

        container.innerHTML = items.slice(0, 6).map((item) => `
            <span class="tag">${this.escapeHtml(item.label)} · ${item.value}${suffix}</span>
        `).join('');
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

new HomePage();
