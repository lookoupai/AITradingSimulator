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
        const primaryMetric = stats.primary_metric_label || predictor.primary_metric_label || '--';
        const currentPrediction = data.current_prediction || data.latest_prediction;

        hero.innerHTML = `
            <div class="hero-metric">
                <span class="mini-label">发布者</span>
                <strong>${this.escapeHtml(predictor.username || '--')}</strong>
            </div>
            <div class="hero-metric">
                <span class="mini-label">主玩法</span>
                <strong>${this.escapeHtml(primaryMetric)}</strong>
            </div>
            <div class="hero-metric">
                <span class="mini-label">最新分享预测</span>
                <div class="badge-row">
                    <span class="tag">${this.escapeHtml(currentPrediction?.prediction_number !== null && currentPrediction?.prediction_number !== undefined ? String(currentPrediction.prediction_number).padStart(2, '0') : '--')}</span>
                    <span class="tag">${this.escapeHtml(currentPrediction?.prediction_big_small || '--')}</span>
                    <span class="tag">${this.escapeHtml(currentPrediction?.prediction_odd_even || '--')}</span>
                    <span class="tag">${this.escapeHtml(currentPrediction?.prediction_combo || '--')}</span>
                </div>
            </div>
        `;
    }

    renderStats(stats) {
        const container = document.getElementById('publicStatsGrid');
        const primaryMetric = (stats.metrics || {})[stats.primary_metric || 'combo'] || {};
        const recent20 = primaryMetric.recent_20 || {};
        const recent100 = primaryMetric.recent_100 || {};
        const streaks = stats.streaks || {};

        container.innerHTML = `
            <article class="stat-card">
                <span class="stat-label">20期胜率</span>
                <strong class="stat-value">${this.formatRatioRate(recent20)}</strong>
            </article>
            <article class="stat-card">
                <span class="stat-label">100期胜率</span>
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
        `;
    }

    renderPredictions(items) {
        const tbody = document.getElementById('publicPredictionBody');
        if (!items.length) {
            tbody.innerHTML = '<tr><td colspan="8" class="empty-cell">暂无公开预测记录</td></tr>';
            return;
        }

        tbody.innerHTML = items.map((item) => `
            <tr>
                <td>${this.escapeHtml(item.issue_no || '--')}</td>
                <td>${this.escapeHtml(this.statusLabel(item.status))}</td>
                <td>${this.escapeHtml(item.prediction_number !== null && item.prediction_number !== undefined ? String(item.prediction_number).padStart(2, '0') : '--')}</td>
                <td>${this.escapeHtml(item.prediction_big_small || '--')}</td>
                <td>${this.escapeHtml(item.prediction_odd_even || '--')}</td>
                <td>${this.escapeHtml(item.prediction_combo || '--')}</td>
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
