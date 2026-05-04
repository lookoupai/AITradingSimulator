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

    async function sendChatMessage() {
        const message = aiChatInput.value.trim();
        if (!message) return;

        const useManual = document.querySelector('input[name="aiSource"][value="manual"]').checked;
        const payload = {
            message: message,
            chat_history: chatHistory,
            consensus_summary: currentAnalysis,
            today_matches: (currentTodayDetail && currentTodayDetail.today_matches) || [],
            history_sample: (currentTodayDetail && currentTodayDetail.history_sample) || []
        };

        if (useManual) {
            payload.api_key = aiKeyInput.value.trim();
            payload.api_url = aiUrlInput.value.trim();
            payload.model_name = aiModelInput.value.trim();
            payload.api_mode = aiModeSelect.value;
            if (!payload.api_key || !payload.api_url || !payload.model_name) {
                alert('请填写完整的 API Key / URL / 模型名');
                return;
            }
            if (aiRememberInput.checked) {
                localStorage.setItem(LS_AI_CONFIG_KEY, JSON.stringify({
                    api_key: payload.api_key, api_url: payload.api_url,
                    model_name: payload.model_name, api_mode: payload.api_mode
                }));
            } else {
                localStorage.removeItem(LS_AI_CONFIG_KEY);
            }
        } else {
            const pid = aiPredictorSelect.value;
            if (!pid) {
                alert('请选择一个 AI 方案，或切换到自定义 API');
                return;
            }
            payload.predictor_id = parseInt(pid, 10);
        }

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

    // ---------- 启动 ----------
    function init() {
        setupTabs();
        setupEvents();
        setupAIToggle();
        loadAnalysis();
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
