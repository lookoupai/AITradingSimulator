class HomePage {
    constructor() {
        this.init();
    }

    async init() {
        await this.checkLoginStatus();
        await this.loadOverview();
        setInterval(() => this.loadOverview(), 15000);
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
