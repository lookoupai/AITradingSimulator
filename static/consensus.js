/* 共识分析页面前端逻辑 */
(function () {
    'use strict';

    const SCOPE = window.CONSENSUS_PAGE_SCOPE || 'user';
    const LOTTERY_TYPE = 'jingcai_football';
    const LS_AI_CONFIG_KEY = 'consensus_ai_config';

    let currentAnalysis = null;
    let currentTodayDetail = null;  // /api/consensus/today-detail 返回的明细
    let chatHistory = [];

    // ---------- DOM 引用 ----------
    const $ = (id) => document.getElementById(id);
    const poolMeta = $('poolMeta');
    const poolChips = $('poolChips');
    const todayList = $('todayList');
    const windowSelect = $('windowSelect');
    const refreshBtn = $('consensusRefreshBtn');
    const exportBtn = $('consensusExportBtn');
    const copyBtn = $('consensusCopyBtn');
    const minAgreeSlider = $('minAgreeSlider');
    const minAgreeLabel = $('minAgreeLabel');
    const minRateSlider = $('minRateSlider');
    const minRateLabel = $('minRateLabel');
    const pairTableBody = $('pairTableBody');
    const byCountTableBody = $('byCountTableBody');
    const perPredictorTableBody = $('perPredictorTableBody');

    const aiPanelToggle = $('aiPanelToggle');
    const aiPanelBody = $('aiPanelBody');
    const aiPanelToggleIcon = $('aiPanelToggleIcon');
    const aiPredictorPicker = $('aiPredictorPicker');
    const aiPredictorSelect = $('aiPredictorSelect');
    const aiManualForm = $('aiManualForm');
    const aiKeyInput = $('aiKeyInput');
    const aiUrlInput = $('aiUrlInput');
    const aiModelInput = $('aiModelInput');
    const aiModeSelect = $('aiModeSelect');
    const aiRememberInput = $('aiRememberInput');
    const aiChatBox = $('aiChatBox');
    const aiChatInput = $('aiChatInput');
    const aiChatSendBtn = $('aiChatSendBtn');

    // ---------- 工具 ----------
    function escapeHtml(text) {
        return String(text == null ? '' : text)
            .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
    }
    function fmtRate(rate) {
        if (rate == null) return '-';
        return rate.toFixed(1) + '%';
    }
    function predictorNameById(pid) {
        if (!currentAnalysis) return '#' + pid;
        const p = currentAnalysis.predictors.find(x => x.id === pid);
        return p ? p.name : '#' + pid;
    }

    // ---------- 加载分析数据 ----------
    async function loadAnalysis() {
        const window_ = windowSelect.value;
        poolMeta.textContent = '加载中...';
        try {
            const res = await fetch(`/api/consensus/analysis?scope=${SCOPE}&lottery_type=${LOTTERY_TYPE}&window=${window_}`);
            if (!res.ok) {
                const err = await res.json().catch(() => ({}));
                throw new Error(err.error || `HTTP ${res.status}`);
            }
            currentAnalysis = await res.json();
            renderAll();
        } catch (e) {
            poolMeta.textContent = '加载失败：' + e.message;
            todayList.innerHTML = `<div class="empty-panel">加载失败：${escapeHtml(e.message)}</div>`;
        }
    }

    async function loadTodayDetail() {
        try {
            const res = await fetch(`/api/consensus/today-detail?scope=${SCOPE}&lottery_type=${LOTTERY_TYPE}`);
            if (!res.ok) return;
            currentTodayDetail = await res.json();
        } catch (e) {
            console.warn('today-detail 加载失败', e);
        }
    }

    // ---------- 渲染 ----------
    function renderAll() {
        if (!currentAnalysis) return;
        renderPoolMeta();
        renderPoolChips();
        renderToday();
        renderPair();
        renderByCount();
        renderPerPredictor();
        renderAIPredictorOptions();
    }

    function renderPoolMeta() {
        const a = currentAnalysis;
        const windowText = a.window_days == null ? '保留窗口内全部' : `最近 ${a.window_days} 天`;
        const archiveNote = a.archive_used
            ? ' · <span style="color:#ffa726;">部分历史已归档为日聚合，单方案命中率已合并归档数据；两两组合/共识指标只反映明细样本</span>'
            : '';
        poolMeta.classList.remove('empty-panel');
        poolMeta.innerHTML = `
            <div>
                <strong>${a.predictors.length}</strong> 个方案 ·
                <strong>${a.sample_count}</strong> 场比赛样本 ·
                <strong>${a.settled_item_count}</strong> 条已结算预测 ·
                <strong>${a.pending_item_count}</strong> 条待结算
                <span style="color:var(--muted,#888);margin-left:8px;">（窗口：${windowText}${archiveNote}）</span>
            </div>
        `;
    }

    function renderPoolChips() {
        if (!currentAnalysis.predictors.length) {
            poolChips.innerHTML = '<div class="empty-panel">该范围内没有可分析的方案。请先创建竞彩足球方案并启用。</div>';
            return;
        }
        const parts = currentAnalysis.predictors.map(p => {
            const stat = currentAnalysis.per_predictor.find(x => x.predictor_id === p.id);
            const sampleSpf = stat ? stat.metrics.spf.total : 0;
            const sampleRq = stat ? stat.metrics.rqspf.total : 0;
            const engineClass = p.engine_type === 'machine' ? 'machine' : 'ai';
            return `
                <span class="consensus-chip">
                    <strong>${escapeHtml(p.name)}</strong>
                    <span class="badge ${engineClass}">${p.engine_type === 'machine' ? '机器' : 'AI'}</span>
                    <span class="sample">spf:${sampleSpf} / rqspf:${sampleRq}</span>
                </span>
            `;
        });
        poolChips.innerHTML = parts.join('');
    }

    function renderToday() {
        const recs = currentAnalysis.today_recommendations || [];
        const minAgree = parseInt(minAgreeSlider.value, 10);
        const minRate = parseInt(minRateSlider.value, 10);

        if (!recs.length) {
            todayList.innerHTML = '<div class="empty-panel">今日暂无未结算的竞彩足球比赛预测，等数据同步后再看。</div>';
            return;
        }

        const cards = recs.map(rec => {
            // 判断是否高亮：任一字段 满足 agree>=minAgree 且 rate>=minRate
            let highlight = false;
            const fieldRows = rec.fields.map(f => {
                const ok = f.agree_count >= minAgree && (f.historical_rate || 0) >= minRate;
                if (ok) highlight = true;
                const rateClass = (f.historical_rate || 0) >= 50 ? '' : 'low';
                const supporters = (f.supporter_names || []).join('、');
                return `
                    <div class="field-row">
                        <div>
                            <span class="field-label">${escapeHtml(f.field_label)}</span>
                            <div class="field-supporters">${escapeHtml(supporters)}</div>
                        </div>
                        <div class="consensus-pick">
                            <span>${escapeHtml(f.consensus_value)}</span>
                            <span class="agree">${f.agree_count}人</span>
                            <span class="rate ${rateClass}">${fmtRate(f.historical_rate)}</span>
                        </div>
                    </div>
                `;
            }).join('');
            return `
                <div class="consensus-match-card ${highlight ? 'highlight' : ''}">
                    <div class="match-title">${escapeHtml(rec.title || rec.event_key)}</div>
                    ${fieldRows}
                </div>
            `;
        });
        todayList.innerHTML = cards.join('');
    }

    // ---------- 历史规律：tab 切换 ----------
    let activeTab = 'pair';
    let pairField = 'rqspf';
    let byCountField = 'rqspf';

    function setupTabs() {
        document.querySelectorAll('.consensus-tabs .tab-btn').forEach(btn => {
            btn.addEventListener('click', () => {
                activeTab = btn.dataset.tab;
                document.querySelectorAll('.consensus-tabs .tab-btn').forEach(b => b.classList.toggle('active', b === btn));
                document.querySelectorAll('[data-tab-panel]').forEach(p => {
                    p.hidden = p.dataset.tabPanel !== activeTab;
                });
            });
        });
        document.querySelectorAll('[data-field]').forEach(btn => {
            btn.addEventListener('click', () => {
                pairField = btn.dataset.field;
                document.querySelectorAll('[data-field]').forEach(b => b.classList.toggle('active', b === btn));
                renderPair();
            });
        });
        document.querySelectorAll('[data-field-bycount]').forEach(btn => {
            btn.addEventListener('click', () => {
                byCountField = btn.dataset.fieldBycount;
                document.querySelectorAll('[data-field-bycount]').forEach(b => b.classList.toggle('active', b === btn));
                renderByCount();
            });
        });
    }

    function renderPair() {
        const rows = (currentAnalysis.pair_combinations[pairField] || []);
        if (!rows.length) {
            const archiveNote = currentAnalysis.archive_used
                ? '当前窗口内没有足够的明细样本。归档机制保留的是日聚合，无法重建两两组合指标，需要新明细积累一段时间后再查看。'
                : '没有足够样本';
            pairTableBody.innerHTML = `<tr><td colspan="4" class="empty-cell">${escapeHtml(archiveNote)}</td></tr>`;
            return;
        }
        pairTableBody.innerHTML = rows.map(r => {
            const [p1, p2] = r.pair;
            return `
                <tr>
                    <td>${escapeHtml(predictorNameById(p1))} + ${escapeHtml(predictorNameById(p2))}</td>
                    <td>${r.total}</td>
                    <td>${r.hit}</td>
                    <td>${fmtRate(r.rate)}</td>
                </tr>
            `;
        }).join('');
    }

    function renderByCount() {
        const rows = (currentAnalysis.consensus_by_count[byCountField] || []);
        if (!rows.length) {
            const archiveNote = currentAnalysis.archive_used
                ? '当前窗口内没有足够的明细样本。归档机制保留的是日聚合，无法重建共识强度指标，需要新明细积累一段时间后再查看。'
                : '没有足够样本';
            byCountTableBody.innerHTML = `<tr><td colspan="5" class="empty-cell">${escapeHtml(archiveNote)}</td></tr>`;
            return;
        }
        // 排序：先按共识数升序，再按命中率降序
        const sorted = rows.slice().sort((a, b) => a.agree_count - b.agree_count || (b.rate || 0) - (a.rate || 0));
        byCountTableBody.innerHTML = sorted.map(r => `
            <tr>
                <td>${r.agree_count}</td>
                <td>${escapeHtml(r.value)}</td>
                <td>${r.total}</td>
                <td>${r.hit}</td>
                <td>${fmtRate(r.rate)}</td>
            </tr>
        `).join('');
    }

    function renderPerPredictor() {
        const rows = currentAnalysis.per_predictor || [];
        if (!rows.length) {
            perPredictorTableBody.innerHTML = '<tr><td colspan="6" class="empty-cell">没有数据</td></tr>';
            return;
        }
        perPredictorTableBody.innerHTML = rows.map(p => `
            <tr>
                <td>${escapeHtml(p.predictor_name)}</td>
                <td>${p.engine_type === 'machine' ? '机器' : 'AI'}</td>
                <td>${fmtRate(p.metrics.spf.rate)}</td>
                <td>${p.metrics.spf.hit}/${p.metrics.spf.total}</td>
                <td>${fmtRate(p.metrics.rqspf.rate)}</td>
                <td>${p.metrics.rqspf.hit}/${p.metrics.rqspf.total}</td>
            </tr>
        `).join('');
    }

    // ---------- 导出 JSON ----------
    function buildExportUrl() {
        return `/api/export/consensus/${LOTTERY_TYPE}?scope=${SCOPE}&window=${windowSelect.value}`;
    }

    async function downloadExport() {
        const url = buildExportUrl();
        const res = await fetch(url);
        if (!res.ok) {
            const err = await res.json().catch(() => ({}));
            alert('导出失败：' + (err.error || res.status));
            return;
        }
        const data = await res.json();
        const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' });
        const a = document.createElement('a');
        a.href = URL.createObjectURL(blob);
        const stamp = new Date().toISOString().replace(/[:T]/g, '-').slice(0, 16);
        a.download = `consensus-${SCOPE}-${LOTTERY_TYPE}-${stamp}.json`;
        a.click();
        URL.revokeObjectURL(a.href);
    }

    async function copyExport() {
        const url = buildExportUrl();
        const res = await fetch(url);
        if (!res.ok) {
            alert('复制失败：HTTP ' + res.status);
            return;
        }
        const text = JSON.stringify(await res.json(), null, 2);
        try {
            await navigator.clipboard.writeText(text);
            copyBtn.innerHTML = '<i class="bi bi-check2"></i> 已复制';
            setTimeout(() => copyBtn.innerHTML = '<i class="bi bi-clipboard"></i> 复制 JSON', 1600);
        } catch (e) {
            alert('复制失败，请手动复制：\n' + text.slice(0, 200) + '...');
        }
    }

    // ---------- AI 聊天 ----------
    function renderAIPredictorOptions() {
        if (!currentAnalysis) return;
        const aiCandidates = (currentAnalysis.predictors || []).filter(p => p.engine_type === 'ai');
        if (!aiCandidates.length) {
            aiPredictorSelect.innerHTML = '<option value="">（当前方案池没有 AI 类型方案，请用自定义 API）</option>';
            // 自动切到 manual
            const manualRadio = document.querySelector('input[name="aiSource"][value="manual"]');
            if (manualRadio) {
                manualRadio.checked = true;
                aiPredictorPicker.hidden = true;
                aiManualForm.hidden = false;
            }
            return;
        }
        aiPredictorSelect.innerHTML = aiCandidates.map(p =>
            `<option value="${p.id}">${escapeHtml(p.name)}</option>`
        ).join('');
    }

    function setupAIToggle() {
        function toggle() {
            const opening = aiPanelBody.hidden;
            aiPanelBody.hidden = !opening;
            aiPanelToggleIcon.className = opening ? 'bi bi-chevron-up' : 'bi bi-chevron-down';
            if (opening && !currentTodayDetail) {
                loadTodayDetail();
            }
        }
        aiPanelToggle.addEventListener('click', toggle);
        aiPanelToggle.addEventListener('keydown', (e) => {
            if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); toggle(); }
        });

        document.querySelectorAll('input[name="aiSource"]').forEach(r => {
            r.addEventListener('change', () => {
                const useManual = r.value === 'manual' && r.checked;
                if (r.checked) {
                    aiPredictorPicker.hidden = useManual;
                    aiManualForm.hidden = !useManual;
                }
            });
        });

        // 加载本地保存的 AI 配置
        try {
            const saved = JSON.parse(localStorage.getItem(LS_AI_CONFIG_KEY) || '{}');
            if (saved.api_key) aiKeyInput.value = saved.api_key;
            if (saved.api_url) aiUrlInput.value = saved.api_url;
            if (saved.model_name) aiModelInput.value = saved.model_name;
            if (saved.api_mode) aiModeSelect.value = saved.api_mode;
        } catch (e) { /* ignore */ }

        document.querySelectorAll('.chip-btn.suggest').forEach(btn => {
            btn.addEventListener('click', () => {
                aiChatInput.value = btn.dataset.prompt;
                aiChatInput.focus();
            });
        });

        aiChatInput.addEventListener('keydown', (e) => {
            if (e.key === 'Enter' && !e.shiftKey) {
                e.preventDefault();
                sendChatMessage();
            }
        });
        aiChatSendBtn.addEventListener('click', sendChatMessage);
    }

    function appendChatMessage(role, content, meta) {
        // 清空 empty-panel
        const empty = aiChatBox.querySelector('.empty-panel');
        if (empty) empty.remove();
        const div = document.createElement('div');
        div.className = 'chat-message ' + role;
        if (meta) {
            const metaEl = document.createElement('div');
            metaEl.className = 'meta';
            metaEl.textContent = meta;
            div.appendChild(metaEl);
        }
        const body = document.createElement('div');
        body.textContent = content;
        div.appendChild(body);
        aiChatBox.appendChild(div);
        aiChatBox.scrollTop = aiChatBox.scrollHeight;
    }

    // 公共：从 AI 配置表单读取设置；返回 {api_key,...} 或 {predictor_id}，失败返回 null（已 alert）
    function readAIConfigFromForm() {
        const useManual = document.querySelector('input[name="aiSource"][value="manual"]').checked;
        if (useManual) {
            const cfg = {
                api_key: aiKeyInput.value.trim(),
                api_url: aiUrlInput.value.trim(),
                model_name: aiModelInput.value.trim(),
                api_mode: aiModeSelect.value
            };
            if (!cfg.api_key || !cfg.api_url || !cfg.model_name) {
                alert('请填写完整的 API Key / URL / 模型名');
                expandAIPanel();
                return null;
            }
            if (aiRememberInput.checked) {
                localStorage.setItem(LS_AI_CONFIG_KEY, JSON.stringify(cfg));
            } else {
                localStorage.removeItem(LS_AI_CONFIG_KEY);
            }
            return cfg;
        }
        const pid = aiPredictorSelect.value;
        if (!pid) {
            alert('请先在下方"AI 深度分析"面板选择一个 AI 方案，或切换到自定义 API');
            expandAIPanel();
            return null;
        }
        return { predictor_id: parseInt(pid, 10) };
    }

    function expandAIPanel() {
        if (aiPanelBody && aiPanelBody.hidden) {
            aiPanelBody.hidden = false;
            aiPanelToggleIcon.className = 'bi bi-chevron-up';
            if (!currentTodayDetail) loadTodayDetail();
            aiPanelBody.scrollIntoView({ behavior: 'smooth', block: 'start' });
        }
    }

    async function sendChatMessage() {
        const message = aiChatInput.value.trim();
        if (!message) return;

        const config = readAIConfigFromForm();
        if (!config) return;  // readAIConfigFromForm 内部已 alert

        const payload = {
            message: message,
            chat_history: chatHistory,
            consensus_summary: currentAnalysis,
            today_matches: (currentTodayDetail && currentTodayDetail.today_matches) || [],
            history_sample: (currentTodayDetail && currentTodayDetail.history_sample) || [],
            ...config
        };

        appendChatMessage('user', message);
        chatHistory.push({ role: 'user', content: message });
        aiChatInput.value = '';
        aiChatSendBtn.disabled = true;
        const thinking = document.createElement('div');
        thinking.className = 'chat-message assistant';
        thinking.textContent = 'AI 正在思考...';
        aiChatBox.appendChild(thinking);
        aiChatBox.scrollTop = aiChatBox.scrollHeight;

        try {
            const res = await fetch('/api/consensus/chat', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });
            thinking.remove();
            if (!res.ok) {
                const err = await res.json().catch(() => ({}));
                appendChatMessage('error', '调用失败：' + (err.error || res.status));
                return;
            }
            const data = await res.json();
            const reply = data.reply || '（无回复）';
            const meta = `${data.response_model || ''} · ${data.latency_ms || '-'}ms`;
            appendChatMessage('assistant', reply, meta);
            chatHistory.push({ role: 'assistant', content: reply });
        } catch (e) {
            thinking.remove();
            appendChatMessage('error', '请求失败：' + e.message);
        } finally {
            aiChatSendBtn.disabled = false;
        }
    }

    // ---------- 事件绑定 ----------
    function setupEvents() {
        refreshBtn.addEventListener('click', loadAnalysis);
        windowSelect.addEventListener('change', loadAnalysis);
        exportBtn.addEventListener('click', downloadExport);
        copyBtn.addEventListener('click', copyExport);

        minAgreeSlider.addEventListener('input', () => {
            minAgreeLabel.textContent = minAgreeSlider.value;
            renderToday();
        });
        minRateSlider.addEventListener('input', () => {
            minRateLabel.textContent = minRateSlider.value;
            renderToday();
        });
    }

    // ============= "我的规则" 模块 =============
    const rulesPanel = $('myRulesPanel');
    const rulesGenerateBtn = $('rulesGenerateBtn');
    const rulesGenerateBtnText = $('rulesGenerateBtnText');
    const rulesDeleteBtn = $('rulesDeleteBtn');
    const rulesEmptyState = $('rulesEmptyState');
    const rulesContent = $('rulesContent');
    const rulesDriftBanner = $('rulesDriftBanner');
    const rulesSummary = $('rulesSummary');
    const rulesList = $('rulesList');
    const rulesTodayMatches = $('rulesTodayMatches');
    const rulesTodayMatchesHeader = $('rulesTodayMatchesHeader');
    const rulesTodayMatchesCount = $('rulesTodayMatchesCount');

    async function loadMyRules() {
        if (!rulesPanel) return;  // admin 视角没有这个 panel
        try {
            const res = await fetch('/api/consensus/rules?lottery_type=' + LOTTERY_TYPE);
            if (!res.ok) {
                const err = await res.json().catch(() => ({}));
                console.warn('加载规则失败:', err);
                showRulesEmptyState();
                return;
            }
            const data = await res.json();
            if (!data.has_rules) {
                showRulesEmptyState();
                return;
            }
            renderRules(data);
            // 同时拉评分
            loadRulesScore();
        } catch (e) {
            console.warn('加载规则异常:', e);
            showRulesEmptyState();
        }
    }

    function showRulesEmptyState() {
        rulesEmptyState.hidden = false;
        rulesContent.hidden = true;
        rulesDeleteBtn.hidden = true;
        rulesDriftBanner.hidden = true;
        rulesGenerateBtnText.textContent = '用 AI 生成规则';
    }

    function renderRules(data) {
        const rules = data.rules || {};
        const drift = data.drift || {};

        rulesEmptyState.hidden = true;
        rulesContent.hidden = false;
        rulesDeleteBtn.hidden = false;
        rulesGenerateBtnText.textContent = '重新生成';

        // 漂移提示
        if (drift.is_drifted) {
            rulesDriftBanner.hidden = false;
            rulesDriftBanner.className = 'rules-drift-banner severity-' + (drift.severity || 'minor');
            const addedNames = (drift.added || []).map(p => p.name).join('、');
            const removedNames = (drift.removed || []).map(p => p.name).join('、');
            const parts = [];
            if (addedNames) parts.push(`新增了 ${addedNames}`);
            if (removedNames) parts.push(`移除了 ${removedNames}`);
            rulesDriftBanner.innerHTML = `
                <i class="bi bi-exclamation-triangle"></i>
                <div>
                    <strong>方案池已变化（漂移 ${(drift.drift_ratio * 100).toFixed(0)}%）</strong>：${escapeHtml(parts.join('，'))}。
                    规则的统计基础已不完整，建议点击右上角"重新生成"。
                </div>
            `;
        } else {
            rulesDriftBanner.hidden = true;
        }

        // Summary
        const summary = rules.summary || '（AI 未生成整体描述）';
        const meta = `生成于 ${rules.created_at || ''} · 模型 ${rules.generated_by_model || '未知'} · 基于 ${rules.sample_count || 0} 条样本`;
        rulesSummary.innerHTML = `
            <div>${escapeHtml(summary)}</div>
            <div class="rules-meta">${escapeHtml(meta)}</div>
        `;

        // 规则卡片
        const ruleItems = rules.rules || [];
        if (!ruleItems.length) {
            rulesList.innerHTML = '<div class="empty-panel">规则集为空</div>';
            return;
        }
        rulesList.innerHTML = ruleItems.map(rule => {
            const conf = rule.confidence || 'medium';
            const action = rule.action || '参考';
            const actionClass =
                /反向|警惕|降低|忽略/.test(action) ? 'warn' :
                /禁止|拒绝|危险/.test(action) ? 'danger' : '';
            const fieldLabel = rule.field === 'spf' ? '胜平负' : (rule.field === 'rqspf' ? '让球胜平负' : rule.field);
            const scorableBadge = rule.auto_scorable
                ? '<span class="rule-badge scorable" title="可对今日自动评分">可评分</span>'
                : '<span class="rule-badge not-scorable" title="自定义类型，不参与自动评分">仅展示</span>';
            return `
                <div class="rule-card confidence-${escapeHtml(conf)}">
                    <div class="rule-card-header">
                        <span class="rule-title">${escapeHtml(rule.title || '未命名规则')}</span>
                        <span class="rule-badges">
                            <span class="rule-badge field">${escapeHtml(fieldLabel)}</span>
                            <span class="rule-badge confidence-${escapeHtml(conf)}">${escapeHtml(conf)}</span>
                            ${scorableBadge}
                        </span>
                    </div>
                    <div class="rule-condition">${escapeHtml(rule.condition_natural || '')}</div>
                    <div class="rule-action ${actionClass}">→ ${escapeHtml(action)}</div>
                    ${rule.rationale ? `<div class="rule-rationale">${escapeHtml(rule.rationale)}</div>` : ''}
                </div>
            `;
        }).join('');
    }

    async function loadRulesScore() {
        try {
            const res = await fetch('/api/consensus/rules/score?lottery_type=' + LOTTERY_TYPE);
            if (!res.ok) return;
            const data = await res.json();
            renderRulesScore(data);
        } catch (e) {
            console.warn('规则评分加载失败:', e);
        }
    }

    function renderRulesScore(data) {
        const matches = data.matched_matches || [];
        const total = data.total_today_matches || 0;
        if (!matches.length) {
            rulesTodayMatchesHeader.hidden = true;
            rulesTodayMatches.innerHTML = total > 0
                ? `<div class="empty-panel">今天 ${total} 场未结算比赛中没有命中你的规则</div>`
                : '';
            return;
        }
        rulesTodayMatchesHeader.hidden = false;
        rulesTodayMatchesCount.textContent = `${matches.length} / ${total} 场`;
        rulesTodayMatches.innerHTML = matches.map(m => {
            const ruleLines = m.matched_rules.map(r => {
                const fieldLabel = r.field === 'spf' ? '胜平负' : (r.field === 'rqspf' ? '让球胜平负' : r.field);
                const valueChip = r.consensus_value
                    ? `<span class="small-chip">${escapeHtml(r.consensus_value)}</span>`
                    : '';
                return `
                    <div class="matched-rule">
                        <strong>${escapeHtml(r.rule_title)}</strong>
                        <span class="small-chip">${escapeHtml(fieldLabel)}</span>
                        ${valueChip}
                        <span>→ ${escapeHtml(r.action || '')}</span>
                    </div>
                `;
            }).join('');
            return `
                <div class="rules-match-card">
                    <div class="match-title">${escapeHtml(m.title || m.event_key)}</div>
                    ${ruleLines}
                </div>
            `;
        }).join('');
    }

    async function generateRules() {
        if (!rulesPanel) return;
        const config = readAIConfigFromForm();
        if (!config) return;

        rulesGenerateBtn.disabled = true;
        const oldText = rulesGenerateBtnText.textContent;
        rulesGenerateBtnText.textContent = '生成中...';

        try {
            const res = await fetch('/api/consensus/rules', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    lottery_type: LOTTERY_TYPE,
                    window_days: parseInt(windowSelect.value, 10) || 30,
                    ...config
                })
            });
            if (!res.ok) {
                const err = await res.json().catch(() => ({}));
                alert('生成失败：' + (err.error || `HTTP ${res.status}`));
                return;
            }
            const data = await res.json();
            renderRules(data);
            loadRulesScore();
        } catch (e) {
            alert('请求失败：' + e.message);
        } finally {
            rulesGenerateBtn.disabled = false;
            rulesGenerateBtnText.textContent = oldText;
        }
    }

    async function deleteRules() {
        if (!confirm('确认清空当前规则？这个操作不可撤销，但你随时可以让 AI 重新生成。')) {
            return;
        }
        try {
            const res = await fetch('/api/consensus/rules?lottery_type=' + LOTTERY_TYPE, { method: 'DELETE' });
            if (!res.ok) {
                alert('删除失败');
                return;
            }
            showRulesEmptyState();
        } catch (e) {
            alert('删除失败：' + e.message);
        }
    }

    function setupRulesEvents() {
        if (!rulesPanel) return;
        rulesGenerateBtn.addEventListener('click', generateRules);
        rulesDeleteBtn.addEventListener('click', deleteRules);
    }

    // ---------- 启动 ----------
    function init() {
        setupTabs();
        setupEvents();
        setupAIToggle();
        setupRulesEvents();
        loadAnalysis();
        loadMyRules();
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
