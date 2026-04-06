const HOME_LOTTERY_CONFIG = {
    pc28: {
        label: '加拿大28 / PC28',
        eyebrow: 'Canada 28 / PC28',
        description: '为不同朋友配置不同提示词、模型和玩法目标，统一预测下一期加拿大28结果。',
        drawSubtitle: '最新 20 期官方结果',
        rankingSubtitle: '基于已启用方案的最近 20 期、最近 100 期和连中表现做公开排序。',
        rankingHint: '“方案主玩法”来自方案配置；“当前查看玩法”来自你上方筛选器选择的统计口径。只有用户主动开启“公开预测内容”后，访客才能查看该方案的预测详情。',
        metricOptions: [
            { value: 'combo', label: '组合投注' },
            { value: 'number', label: '单点' },
            { value: 'big_small', label: '大/小' },
            { value: 'odd_even', label: '单/双' },
            { value: 'double_group', label: '组合分组统计' },
            { value: 'kill_group', label: '排除统计' }
        ],
        summaryLabels: ['下一期期号', '倒计时', '最新开奖号码', '今日总期数']
    },
    jingcai_football: {
        label: '竞彩足球',
        eyebrow: 'Jingcai Football',
        description: '查看竞彩足球当前批次、近期赛事与公开方案表现，首版支持胜平负和让球胜平负预测。',
        drawSubtitle: '当前批次与近期开奖赛程',
        rankingSubtitle: '基于已启用竞彩足球方案的近期命中表现做公开排序。',
        rankingHint: '竞彩足球公开页会展示命中统计、公开预测记录和基于赔率快照的收益模拟。',
        metricOptions: [
            { value: 'spf', label: '胜平负' },
            { value: 'rqspf', label: '让球胜平负' }
        ],
        summaryLabels: ['当前批次', '已开售场次', '待开奖场次', '已开奖场次']
    }
};


class HomePage {
    constructor() {
        this.currentLotteryType = 'pc28';
        this.publicMetric = 'combo';
        this.publicSort = 'recent100';
        this.init();
    }

    async init() {
        await this.checkLoginStatus();
        this.initPublicFilters();
        this.syncLotteryUi();
        await this.loadOverview();
        await this.loadPublicPredictors();
        setInterval(() => {
            this.loadOverview();
            this.loadPublicPredictors();
        }, 15000);
    }

    initPublicFilters() {
        const lotterySelect = document.getElementById('homeLotteryTypeSelect');
        const metricSelect = document.getElementById('publicMetricSelect');
        const sortSelect = document.getElementById('publicSortSelect');

        if (lotterySelect) {
            lotterySelect.addEventListener('change', (event) => {
                this.currentLotteryType = event.target.value || 'pc28';
                this.publicMetric = this.getMetricOptions()[0]?.value || 'combo';
                this.syncLotteryUi();
                this.loadOverview();
                this.loadPublicPredictors();
            });
        }

        if (metricSelect) {
            metricSelect.addEventListener('change', (event) => {
                this.publicMetric = event.target.value;
                this.renderPublicMetricHint();
                this.loadPublicPredictors();
            });
        }

        if (sortSelect) {
            sortSelect.addEventListener('change', (event) => {
                this.publicSort = event.target.value;
                this.renderPublicMetricHint();
                this.loadPublicPredictors();
            });
        }

        this.renderPublicMetricHint();
    }

    getLotteryConfig() {
        return HOME_LOTTERY_CONFIG[this.currentLotteryType] || HOME_LOTTERY_CONFIG.pc28;
    }

    getMetricOptions() {
        return this.getLotteryConfig().metricOptions || [];
    }

    syncLotteryUi() {
        const config = this.getLotteryConfig();
        document.getElementById('homeEyebrow').textContent = config.eyebrow;
        document.getElementById('homeHeroDescription').textContent = config.description;
        document.getElementById('recentDrawsSubtitle').textContent = config.drawSubtitle;
        document.getElementById('publicRankingSubtitle').textContent = config.rankingSubtitle;
        document.getElementById('publicRankingHint').textContent = config.rankingHint;

        const metricSelect = document.getElementById('publicMetricSelect');
        metricSelect.innerHTML = this.getMetricOptions().map((item) => `
            <option value="${this.escapeHtml(item.value)}">${this.escapeHtml(item.label)}</option>
        `).join('');
        metricSelect.value = this.getMetricOptions().some((item) => item.value === this.publicMetric)
            ? this.publicMetric
            : (this.getMetricOptions()[0]?.value || '');
        this.publicMetric = metricSelect.value;

        const insightGrid = document.getElementById('pc28InsightGrid');
        insightGrid.style.display = this.currentLotteryType === 'pc28' ? '' : 'none';
        this.renderPublicMetricHint();
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
            const response = await fetch(`/api/lotteries/${encodeURIComponent(this.currentLotteryType)}/overview?limit=20`);
            const overview = await response.json();
            this.renderHero(overview);
            this.renderSummary(overview);
            this.renderDraws(overview.recent_draws || overview.recent_events || []);
            if (this.currentLotteryType === 'pc28') {
                this.renderTags('omissionList', overview.omission_preview?.top_numbers || [], '期');
                this.renderTags('hotNumberList', overview.today_preview?.hot_numbers || [], '次');
            }
        } catch (error) {
            console.error('Failed to load overview:', error);
        }
    }

    async loadPublicPredictors() {
        try {
            const response = await fetch(`/api/public/predictors?lottery_type=${encodeURIComponent(this.currentLotteryType)}&metric=${encodeURIComponent(this.publicMetric)}&sort_by=${encodeURIComponent(this.publicSort)}&limit=10`);
            const data = await response.json();
            this.renderPublicPredictors(data.items || []);
        } catch (error) {
            console.error('Failed to load public predictors:', error);
        }
    }

    renderPublicMetricHint() {
        const hint = document.getElementById('publicMetricHint');
        if (!hint) {
            return;
        }

        const metricInfo = this.metricInfo(this.publicMetric);
        const sortInfo = this.sortInfo(this.publicSort);

        hint.innerHTML = `
            <span class="tag">${this.escapeHtml(metricInfo.label)}</span>
            <span>${this.escapeHtml(metricInfo.description)}</span>
            <span class="hint-separator">·</span>
            <span>${this.escapeHtml(sortInfo)}</span>
        `;
    }

    renderHero(overview) {
        if ((overview?.lottery_type || 'pc28') === 'jingcai_football') {
            this.renderFootballHero(overview);
            return;
        }

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

    renderFootballHero(overview) {
        const warning = overview.warning ? `<div class="warning-banner">${this.escapeHtml(overview.warning)}</div>` : '';
        document.getElementById('heroOverview').innerHTML = `
            ${warning}
            <div class="hero-metric">
                <span class="mini-label">当前批次</span>
                <strong>${this.escapeHtml(overview.batch_key || '--')}</strong>
            </div>
            <div class="hero-metric">
                <span class="mini-label">已开售场次</span>
                <strong>${this.escapeHtml(String(overview.open_match_count ?? 0))}</strong>
            </div>
            <div class="hero-metric">
                <span class="mini-label">下一场比赛</span>
                <div class="result-number football-title">${this.escapeHtml(overview.next_match_name || '--')}</div>
                <div class="result-meta">${this.escapeHtml(overview.next_match_time || '')}</div>
            </div>
        `;
    }

    renderSummary(overview) {
        if ((overview?.lottery_type || 'pc28') === 'jingcai_football') {
            const values = [
                overview.batch_key || '--',
                String(overview.open_match_count ?? '--'),
                String(overview.awaiting_result_match_count ?? '--'),
                String(overview.settled_match_count ?? '--')
            ];
            document.querySelectorAll('#summaryGrid .stat-label').forEach((element, index) => {
                element.textContent = this.getLotteryConfig().summaryLabels[index];
            });
            document.querySelectorAll('#summaryGrid .stat-value').forEach((element, index) => {
                element.textContent = values[index];
            });
            return;
        }

        const latestDraw = overview.latest_draw;
        const totalIssues = overview.today_preview?.summary?.['总期数'] || '--';
        const values = [
            overview.next_issue_no || '--',
            overview.countdown || '--:--:--',
            latestDraw ? latestDraw.result_number_text : '--',
            totalIssues
        ];

        document.querySelectorAll('#summaryGrid .stat-label').forEach((element, index) => {
            element.textContent = this.getLotteryConfig().summaryLabels[index];
        });
        document.querySelectorAll('#summaryGrid .stat-value').forEach((element, index) => {
            element.textContent = values[index];
        });
    }

    renderDraws(draws) {
        const tbody = document.getElementById('recentDrawsBody');
        const cards = document.getElementById('recentDrawsCards');
        if (this.currentLotteryType === 'jingcai_football') {
            this.updateFootballDrawHeaders();
            if (!draws.length) {
                tbody.innerHTML = '<tr><td colspan="6" class="empty-cell">暂无赛事数据</td></tr>';
                if (cards) {
                    cards.innerHTML = '<div class="empty-panel">暂无赛事数据</div>';
                }
                return;
            }

            tbody.innerHTML = draws.map((draw) => {
                const meta = draw.meta_payload || {};
                const result = draw.result_payload || {};
                const teams = draw.home_team && draw.away_team
                    ? `${this.escapeHtml(draw.home_team)} vs ${this.escapeHtml(draw.away_team)}`
                    : this.escapeHtml(draw.event_name || '--');
                const spfOdds = meta.spf_odds || {};
                const rqspf = meta.rqspf || {};
                const spfText = ['胜', '平', '负'].map((key) => `${key}:${spfOdds[key] ?? '--'}`).join(' / ');
                const rqText = ['胜', '平', '负'].map((key) => `${key}:${(rqspf.odds || {})[key] ?? '--'}`).join(' / ');
                const scoreText = result.score1 !== null && result.score1 !== undefined && result.score2 !== null && result.score2 !== undefined
                    ? `${result.score1}:${result.score2}`
                    : '--';
                const statusLabel = this.footballMatchStatusLabel(draw);
                return `
                    <tr>
                        <td>${this.escapeHtml(meta.match_no || draw.event_key || '--')}</td>
                        <td>${this.escapeHtml(draw.league || '--')}</td>
                        <td>${teams}</td>
                        <td>${this.escapeHtml(spfText)}</td>
                        <td>${this.escapeHtml((rqspf.handicap_text || '--') + ' [' + rqText + ']')}</td>
                        <td><strong>${this.escapeHtml(statusLabel)}</strong><br><span class="hint-text">${this.escapeHtml(draw.event_time || '')}</span><br><span class="hint-text">${this.escapeHtml(scoreText)}</span></td>
                    </tr>
                `;
            }).join('');
            if (cards) {
                cards.innerHTML = draws.map((draw) => this.renderDrawCard(draw)).join('');
            }
            return;
        }

        this.updatePc28DrawHeaders();
        if (!draws.length) {
            tbody.innerHTML = '<tr><td colspan="6" class="empty-cell">暂无开奖数据</td></tr>';
            if (cards) {
                cards.innerHTML = '<div class="empty-panel">暂无开奖数据</div>';
            }
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
        if (cards) {
            cards.innerHTML = draws.map((draw) => this.renderDrawCard(draw)).join('');
        }
    }

    updatePc28DrawHeaders() {
        document.getElementById('homeDrawHead1').textContent = '期号';
        document.getElementById('homeDrawHead2').textContent = '开奖号码';
        document.getElementById('homeDrawHead3').textContent = '大/小';
        document.getElementById('homeDrawHead4').textContent = '单/双';
        document.getElementById('homeDrawHead5').textContent = '组合结果';
        document.getElementById('homeDrawHead6').textContent = '开奖时间';
    }

    renderDrawCard(draw) {
        if ((this.currentLotteryType || 'pc28') === 'jingcai_football') {
            const meta = draw.meta_payload || {};
            const result = draw.result_payload || {};
            const teams = draw.home_team && draw.away_team
                ? `${this.escapeHtml(draw.home_team)} vs ${this.escapeHtml(draw.away_team)}`
                : this.escapeHtml(draw.event_name || '--');
            const spfOdds = meta.spf_odds || {};
            const rqspf = meta.rqspf || {};
            const spfText = ['胜', '平', '负'].map((key) => `${key}:${spfOdds[key] ?? '--'}`).join(' / ');
            const rqText = ['胜', '平', '负'].map((key) => `${key}:${(rqspf.odds || {})[key] ?? '--'}`).join(' / ');
            const scoreText = result.score1 !== null && result.score1 !== undefined && result.score2 !== null && result.score2 !== undefined
                ? `${result.score1}:${result.score2}`
                : '--';
            const statusLabel = this.footballMatchStatusLabel(draw);
            return `
                <article class="home-card">
                    <div class="home-card-row">
                        <div>
                            <div class="home-card-label">${this.escapeHtml(meta.match_no || draw.event_key || '--')}</div>
                            <strong>${this.escapeHtml(teams)}</strong>
                        </div>
                        <span class="tag home-card-badge">${this.escapeHtml(statusLabel)}</span>
                    </div>
                    <p class="home-card-meta">${this.escapeHtml(draw.league || '--')} · ${this.escapeHtml(draw.event_time || '')}</p>
                    <div class="home-card-row">
                        <span>胜平负：${this.escapeHtml(spfText)}</span>
                        <span>让球：${this.escapeHtml((rqspf.handicap_text || '--') + ' [' + rqText + ']')}</span>
                    </div>
                    <p class="home-card-meta">${this.escapeHtml(scoreText)}</p>
                </article>
            `;
        }
        const number = draw.issue_no;
        const openTime = draw.open_time || '--';
        return `
            <article class="home-card">
                <div class="home-card-row">
                    <div>
                        <div class="home-card-label">期号</div>
                        <strong>${this.escapeHtml(number)}</strong>
                    </div>
                    <div>
                        <div class="home-card-label">开奖号码</div>
                        <strong>${this.escapeHtml(draw.result_number_text)}</strong>
                    </div>
                </div>
                <div class="home-card-row">
                    <span>大/小：${this.escapeHtml(draw.big_small)}</span>
                    <span>单/双：${this.escapeHtml(draw.odd_even)}</span>
                    <span>组合：${this.escapeHtml(draw.combo)}</span>
                </div>
                <p class="home-card-meta">${this.escapeHtml(openTime)}</p>
            </article>
        `;
    }

    updateFootballDrawHeaders() {
        document.getElementById('homeDrawHead1').textContent = '场次';
        document.getElementById('homeDrawHead2').textContent = '联赛';
        document.getElementById('homeDrawHead3').textContent = '对阵';
        document.getElementById('homeDrawHead4').textContent = '胜平负';
        document.getElementById('homeDrawHead5').textContent = '让球胜平负';
        document.getElementById('homeDrawHead6').textContent = '状态 / 时间 / 比分';
    }

    renderPublicPredictors(items) {
        const tbody = document.getElementById('publicPredictorBody');
        const cards = document.getElementById('publicPredictorCards');
        if (!items.length) {
            tbody.innerHTML = '<tr><td colspan="10" class="empty-cell">暂无公开方案数据</td></tr>';
            if (cards) {
                cards.innerHTML = '<div class="empty-panel">暂无公开方案数据</div>';
            }
            return;
        }

        tbody.innerHTML = items.map((item, index) => `
            <tr>
                <td>${index + 1}</td>
                <td>
                    <strong>${this.escapeHtml(item.predictor_name)}</strong><br>
                    <span class="hint-text">${this.escapeHtml(item.username)} · ${this.escapeHtml(item.model_name)}</span>
                </td>
                <td>${this.escapeHtml((item.lottery_label || '--') + ' / ' + (item.primary_metric_label || '--'))}</td>
                <td>${this.escapeHtml(item.metric_label || '--')}</td>
                <td>${this.formatRatioRate(item.recent_20)}</td>
                <td>${this.formatRatioRate(item.recent_100)}</td>
                <td>${item.current_hit_streak || 0}</td>
                <td>${item.historical_max_hit_streak || 0}</td>
                <td>${this.escapeHtml(item.share_level_label || '--')}</td>
                <td><a class="btn ghost compact" href="/public/predictors/${item.predictor_id}">${item.share_level === 'stats_only' ? '查看统计' : '查看方案'}</a></td>
            </tr>
        `).join('');
        if (cards) {
            cards.innerHTML = items.map((item, index) => this.renderPublicPredictorCard(item, index)).join('');
        }
    }

    renderPublicPredictorCard(item, index) {
        return `
            <article class="home-card">
                <div class="home-card-row">
                    <div>
                        <div class="home-card-label">排名</div>
                        <strong>${index + 1}</strong>
                    </div>
                    <span class="home-card-badge">${this.escapeHtml(item.share_level_label || '--')}</span>
                </div>
                <p class="home-card-meta">${this.escapeHtml(item.username)} · ${this.escapeHtml(item.model_name || '--')}</p>
                <strong>${this.escapeHtml(item.predictor_name)}</strong>
                <div class="home-card-row">
                    <span>${this.escapeHtml((item.lottery_label || '--') + ' / ' + (item.primary_metric_label || '--'))}</span>
                    <span>${this.escapeHtml(item.metric_label || '--')}</span>
                </div>
                <div class="home-card-row">
                    <span>20期 ${this.escapeHtml(item.recent_20 ? this.formatRatioRate(item.recent_20) : '--')}</span>
                    <span>100期 ${this.escapeHtml(item.recent_100 ? this.formatRatioRate(item.recent_100) : '--')}</span>
                </div>
                <div class="home-card-row">
                    <span>当前连中 ${this.escapeHtml(String(item.current_hit_streak || 0))}</span>
                    <span>历史最高 ${this.escapeHtml(String(item.historical_max_hit_streak || 0))}</span>
                </div>
                <div class="home-card-row">
                    <span>${this.escapeHtml(item.share_level_label || '--')}</span>
                    <a class="btn ghost compact" href="/public/predictors/${item.predictor_id}">${item.share_level === 'stats_only' ? '查看统计' : '查看方案'}</a>
                </div>
            </article>
        `;
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

    metricInfo(metric) {
        if (this.currentLotteryType === 'jingcai_football') {
            const footballMapping = {
                spf: {
                    label: '胜平负',
                    description: '按 90 分钟含伤停补时赛果统计主胜、平局、客胜。'
                },
                rqspf: {
                    label: '让球胜平负',
                    description: '按官方让球数修正主队比分后统计胜平负。'
                }
            };
            return footballMapping[metric] || { label: metric || '--', description: '当前玩法说明暂未定义。' };
        }

        const mapping = {
            number: {
                label: '单点',
                description: '按精确和值是否命中统计，直接对应 00-27 的单点玩法。'
            },
            big_small: {
                label: '大/小',
                description: '按和值落在小(00-13)还是大(14-27)统计，适合保守玩法。'
            },
            odd_even: {
                label: '单/双',
                description: '按和值奇偶统计，适合偏保守的提示词。'
            },
            combo: {
                label: '组合投注',
                description: '模型先给出小双 / 小单 / 大双 / 大单，再映射到真实可下注的组合投注票面。'
            },
            double_group: {
                label: '组合分组统计',
                description: '按分组口径统计：大双/小单为“双组”，大单/小双为“单组”。'
            },
            kill_group: {
                label: '排除统计',
                description: '按排除口径统计：预测某组合结果的对角组合不出现即视为命中。'
            }
        };
        return mapping[metric] || { label: metric || '--', description: '当前玩法说明暂未定义。' };
    }

    sortInfo(sortBy) {
        const mapping = {
            recent100: '当前按最近100期命中率排序，适合看中期稳定性。',
            recent20: '当前按最近20期命中率排序，更适合观察短期状态。',
            current_streak: '当前按当前连中排序，适合找正在走强的方案。',
            historical_streak: '当前按历史最大连中排序，适合看峰值表现。'
        };
        return mapping[sortBy] || '当前排序说明暂未定义。';
    }

    footballMatchStatusCode(item) {
        return String(item?.status || item?.show_sell_status || '').trim();
    }

    footballMatchStatusLabel(item) {
        const code = this.footballMatchStatusCode(item);
        const fallback = String(item?.status_label || item?.show_sell_status_label || '').trim();
        const mapping = {
            '0': '未开售',
            '1': '已开售',
            '2': '待开奖',
            '3': '已开奖'
        };
        return mapping[code] || fallback || '--';
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
