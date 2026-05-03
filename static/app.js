const PREDICTOR_PRESETS = [
    {
        id: 'statistical',
        title: '概率统计型',
        description: '适合先跑稳定版。偏重和值分布、遗漏、冷热和趋势，不依赖玄学解释。',
        method: '概率统计',
        injectionMode: 'summary',
        historyWindow: 100,
        temperature: 0.4,
        apiMode: 'auto',
        primaryMetric: 'big_small',
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
        apiMode: 'chat_completions',
        primaryMetric: 'big_small',
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
        apiMode: 'auto',
        primaryMetric: 'big_small',
        targets: ['number', 'big_small', 'odd_even', 'combo'],
        tags: ['推荐', '原始模式', '平衡型'],
        prompt: `角色：\n你是一个 PC28 预测引擎。\n\n方法：\n统计模型 + 小六壬 + 概率回归。\n\n输入：\n最近 {{history_window}} 期 PC28 数据：\n{{recent_draws_csv}}\n\n补充信息：\n遗漏统计：\n{{omission_summary}}\n\n今日统计：\n{{today_summary}}\n\n起课时间：\n{{current_time_beijing}}\n\n步骤：\n1. 统计和值分布、大小比例、单双比例、连续次数与极端概率。\n2. 计算移动平均、标准差、趋势方向和回归概率。\n3. 小六壬用于修正最终取值方向。\n4. 输出下一期和值、概率、推荐、大小单双、风险、策略。\n5. 只输出 JSON。`
    },
    {
        id: 'conservative',
        title: '保守大小单双型',
        description: '号码保持保守输出，重点判断大小单双与风险，适合风险控制型玩法。',
        method: '保守统计',
        injectionMode: 'summary',
        historyWindow: 60,
        temperature: 0.3,
        apiMode: 'auto',
        primaryMetric: 'big_small',
        targets: ['number', 'big_small', 'odd_even', 'combo'],
        tags: ['保守', '摘要模式', '号码保守'],
        prompt: `角色：\n你是 PC28 保守预测助手。\n\n输入：\n{{recent_draws_summary}}\n\n遗漏统计：\n{{omission_summary}}\n\n要求：\n1. 必须给出一个保守的下一期和值预测，不追极端号。\n2. 重点输出大小、单双、组合和风险。\n3. 如果信号不明确，降低 confidence。\n4. 只输出 JSON。`
    },
    {
        id: 'extreme',
        title: '极值回归型',
        description: '关注大值、小值、连开和遗漏回补，适合喜欢追波动的激进玩家。',
        method: '极值回归',
        injectionMode: 'raw',
        historyWindow: 120,
        temperature: 0.65,
        apiMode: 'chat_completions',
        primaryMetric: 'kill_group',
        targets: ['number', 'big_small', 'odd_even', 'combo'],
        tags: ['激进', '120 期', '极值/遗漏'],
        prompt: `角色：\n你是一个擅长极值回归判断的 PC28 预测助手。\n\n输入：\n{{recent_draws_csv}}\n\n额外信息：\n{{omission_summary}}\n\n任务：\n1. 重点分析极端和值、长遗漏号码、连续大小单双。\n2. 判断下一期是否存在回归或继续延续的概率。\n3. 输出一个主预测和一句激进策略建议。\n4. 只输出 JSON。`
    }
];

const FOOTBALL_PREDICTOR_PRESETS = [
    {
        id: 'football-conservative',
        title: '保守赔率型',
        description: '优先参考欧赔、让球、积分与近期战绩，适合先跑稳定版。',
        method: '赔率 + 基本面',
        injectionMode: 'summary',
        historyWindow: 30,
        temperature: 0.3,
        apiMode: 'auto',
        primaryMetric: 'spf',
        targets: ['spf', 'rqspf'],
        tags: ['竞彩足球', '稳健', '30场'],
        prompt: `角色：\n你是一个中国竞彩足球预测助手。\n\n目标：\n基于待售比赛列表、市场赔率、积分排名、历史交锋、近期战绩与伤停信息，给出稳健的胜平负与让球胜平负判断。\n\n要求：\n1. 优先相信结构化信息，不要让平台情报文本压过赔率与排名。\n2. 如果信号冲突，优先选择更稳健的一侧，并适当降低 confidence。\n3. reasoning_summary 要简短，不要空泛。\n4. 只输出 JSON。`
    },
    {
        id: 'football-market',
        title: '盘口变化型',
        description: '更重视欧赔、亚盘和大小球的初赔/即赔变化，适合关注市场预期变化。',
        method: '盘口变化',
        injectionMode: 'raw',
        historyWindow: 30,
        temperature: 0.35,
        apiMode: 'chat_completions',
        primaryMetric: 'rqspf',
        targets: ['spf', 'rqspf'],
        tags: ['竞彩足球', '盘口', '原始模式'],
        prompt: `角色：\n你是一个擅长赔率与盘口变化分析的竞彩足球预测助手。\n\n任务：\n1. 重点分析欧赔、亚盘、大小球的初赔到即赔变化。\n2. 结合让球盘口方向、积分差距、近期战绩，判断当前市场是否过热或低估。\n3. 输出胜平负和让球胜平负预测；如果某个目标把握不足，可以输出 null。\n4. 只输出 JSON。`
    }
];

const DEFAULT_PROFIT_BET_MODE = 'flat';
const DEFAULT_PROFIT_BASE_STAKE = 10;
const DEFAULT_PROFIT_MULTIPLIER = 2;
const DEFAULT_PROFIT_MAX_STEPS = 6;
const DEFAULT_PROFIT_PERIOD_KEY = 'day';
const LOTTERY_UI_CONFIG = {
    pc28: {
        label: '加拿大28 / PC28',
        supportsProfitSimulation: true,
        supportsPromptAssistant: true,
        supportsPresets: true,
        defaultHistoryWindow: 60,
        defaultTargets: ['number', 'big_small', 'odd_even', 'combo'],
        defaultProfitRuleId: 'pc28_netdisk',
        defaultProfitMetric: 'big_small',
        historyWindowLabel: '历史窗口',
        historyWindowHint: 'PC28 按最近 N 期历史开奖构造上下文。',
        targetOptions: [
            { key: 'number', label: '单点（固定开启）', fixed: true },
            { key: 'big_small', label: '大/小' },
            { key: 'odd_even', label: '单/双' },
            { key: 'combo', label: '组合投注' }
        ],
        primaryMetricOptions: [
            { key: 'combo', label: '组合投注：适合看组合票面的稳定性' },
            { key: 'number', label: '单点：只按精确和值判断连中' },
            { key: 'big_small', label: '大/小：适合保守玩法' },
            { key: 'odd_even', label: '单/双：适合保守玩法' },
            { key: 'double_group', label: '组合分组统计：大双/小单 vs 大单/小双' },
            { key: 'kill_group', label: '排除统计：按预测组合结果的对角组合做排除统计' }
        ],
        profitRuleOptions: [
            { key: 'pc28_netdisk', label: '加拿大28网盘：默认规则更直，13/14 正常赔付' },
            { key: 'pc28_high', label: '加拿大28高倍：大小单双/组合命中且遇特殊号时退本金' }
        ],
        machineAlgorithms: [
            {
                key: 'pc28_frequency_v1',
                label: '频次趋势 V1',
                description: '基于最近开奖的加权频次、遗漏和组合偏好做 deterministic 预测。'
            },
            {
                key: 'pc28_omission_reversion_v1',
                label: '遗漏回补 V1',
                description: '更重视和值遗漏、冷热切换和组合回补，适合偏反转风格。'
            },
            {
                key: 'pc28_combo_markov_v1',
                label: '组合马尔可夫 V1',
                description: '按大单/大双/小单/小双的历史转移关系预测下一组合，再反推和值。'
            }
        ],
        defaultPrimaryMetric: 'big_small',
        targetHint: '单点固定开启，号码是主预测结果；大/小、单/双、组合投注围绕号码展开。'
    },
    jingcai_football: {
        label: '竞彩足球',
        supportsProfitSimulation: true,
        supportsPromptAssistant: true,
        supportsPresets: true,
        defaultHistoryWindow: 30,
        defaultTargets: ['spf', 'rqspf'],
        defaultProfitRuleId: 'jingcai_snapshot',
        defaultProfitMetric: 'spf',
        historyWindowLabel: '历史窗口（场）',
        historyWindowHint: '竞彩足球按最近 N 场已结束比赛构造上下文，默认建议 30 场。',
        targetOptions: [
            { key: 'spf', label: '胜平负' },
            { key: 'rqspf', label: '让球胜平负' }
        ],
        primaryMetricOptions: [
            { key: 'spf', label: '胜平负：按常规胜平负玩法统计命中' },
            { key: 'rqspf', label: '让球胜平负：按让球盘结果统计命中' }
        ],
        profitRuleOptions: [
            { key: 'jingcai_snapshot', label: '竞彩足球赔率快照：按预测批次落库赔率做收益模拟' }
        ],
        machineAlgorithms: [
            {
                key: 'football_odds_baseline_v1',
                label: '赔率基线 V1',
                description: '按市场赔率隐含概率输出胜平负与让球胜平负，不依赖 LLM。'
            },
            {
                key: 'football_odds_form_weighted_v1',
                label: '赔率+状态加权 V1',
                description: '在赔率基线上叠加近期战绩、积分排名、伤停和欧赔变动做稳健修正。'
            },
            {
                key: 'football_handicap_consistency_v1',
                label: '让球一致性 V1',
                description: '更重视让球方向、SPF 与 RQSPF 一致性，以及欧赔/亚盘共振。'
            },
            {
                key: 'football_value_edge_v1',
                label: '价值优势 V1',
                description: '按模型概率相对赔率隐含概率的 edge/EV 筛选，过滤低赔率热门。'
            }
        ],
        defaultPrimaryMetric: 'spf',
        targetHint: '竞彩足球支持胜平负与让球胜平负预测；收益模拟会基于预测批次赔率快照计算单关与默认二串一。'
    }
};

class PredictionApp {
    constructor() {
        this.pageMode = document.body?.dataset?.page || 'dashboard';
        this.historyPredictorId = Number(document.body?.dataset?.predictorId || 0);
        this.activeContentTab = document.body?.dataset?.initialTab || 'predictions';
        this.historyDataCache = {};
        this.historyState = {
            predictions: { page: 1, pageSize: 50, status: 'all', outcome: 'all', pagination: null },
            draws: { page: 1, pageSize: 50, pagination: null },
            ai: { page: 1, pageSize: 20, pagination: null }
        };
        this.currentUser = null;
        this.currentPredictorId = null;
        this.currentPredictor = null;
        this.predictors = [];
        this.userAlgorithms = [];
        this.algorithmTemplates = [];
        this.selectedUserAlgorithmId = null;
        this.algorithmChatMessages = [];
        this.lastUserAlgorithmBacktest = null;
        this.betProfiles = [];
        this.notificationSenders = [];
        this.notificationEndpoints = [];
        this.notificationSubscriptions = [];
        this.notificationDeliveries = [];
        this.currentLotteryType = 'pc28';
        this.formLotteryType = 'pc28';
        this.selectedPredictorLotteryFilter = 'all';
        this.selectedPredictorEngineFilter = 'all';
        this.selectedPredictorStyleFilter = 'all';
        this.formStateByLottery = this.buildInitialFormStateByLottery();
        this.currentPredictions = [];
        this.currentStats = null;
        this.selectedStatsMetric = null;
        this.selectedProfitRuleId = 'pc28_netdisk';
        this.selectedProfitMetric = null;
        this.selectedProfitPeriodKey = DEFAULT_PROFIT_PERIOD_KEY;
        this.selectedProfitBetProfileId = '';
        this.selectedProfitBetMode = DEFAULT_PROFIT_BET_MODE;
        this.selectedProfitBaseStake = DEFAULT_PROFIT_BASE_STAKE;
        this.selectedProfitMultiplier = DEFAULT_PROFIT_MULTIPLIER;
        this.selectedProfitMaxSteps = DEFAULT_PROFIT_MAX_STEPS;
        this.selectedProfitOrder = 'desc';
        this.selectedProfitOddsProfile = 'regular';
        this.currentProfitSimulation = null;
        this.overview = null;
        this.overviewCollapsed = true;
        this.dashboardLoadSequence = 0;
        this.profitSimulationLoadSequence = 0;
        this.profitSimulationDeferredTimer = null;
        this.dashboardFetchController = null;
        this.dashboardLoadingPredictorId = null;
        this.dashboardLoadPromise = null;
        this.profitSimulationFetchController = null;
        this.chart = null;
        this.profitChart = null;
        this.refreshTimer = null;
        this.darkMode = localStorage.getItem('pc28Theme') === 'dark';
        this.presetExpanded = false;
        this.predictionStatusFilter = 'all';
        this.predictionOutcomeFilter = 'all';
        this.handleChartResizeBound = () => this.handleChartResize();
        this.init();
    }

    isDashboardPage() {
        return this.pageMode === 'dashboard';
    }

    isSettingsPage() {
        return this.pageMode === 'settings';
    }

    isHistoryPage() {
        return this.pageMode === 'predictor-history';
    }

    getElement(id) {
        return document.getElementById(id);
    }

    bindEvent(id, eventName, handler) {
        const element = this.getElement(id);
        if (element) {
            element.addEventListener(eventName, handler);
        }
        return element;
    }

    async init() {
        this.applyTheme();
        this.initEventListeners();
        await this.checkAuth();
        if (this.isSettingsPage()) {
            await this.loadUserSettings();
            return;
        }
        if (this.isHistoryPage()) {
            this.showHistoryLoading(this.activeContentTab || 'predictions');
            await this.loadPredictorHistory();
            return;
        }
        await this.refresh(true, true);
        this.refreshTimer = setInterval(() => this.refresh(), 10000);
    }

    initEventListeners() {
        this.bindEvent('themeToggle', 'click', () => this.toggleTheme());
        this.bindEvent('toggleOverviewPanelBtn', 'click', () => this.toggleOverviewPanel());
        this.bindEvent('toggleOverviewPanelBtn', 'keydown', (event) => {
            if (event.key !== 'Enter' && event.key !== ' ') {
                return;
            }
            event.preventDefault();
            this.toggleOverviewPanel();
        });
        this.bindEvent('refreshBtn', 'click', async () => {
            if (this.isSettingsPage()) {
                await this.loadUserSettings();
                return;
            }
            if (this.isHistoryPage()) {
                await this.loadPredictorHistory();
                return;
            }
            await this.refresh(true, true);
        });
        this.bindEvent('logoutBtn', 'click', () => this.logout());
        this.bindEvent('addPredictorBtn', 'click', () => this.openCreateModal());
        this.bindEvent('userAlgorithmsBtn', 'click', () => this.openUserAlgorithmModal());
        this.bindEvent('closeModalBtn', 'click', () => this.hideModal());
        this.bindEvent('cancelModalBtn', 'click', () => this.hideModal());
        this.bindEvent('closeUserAlgorithmModalBtn', 'click', () => this.hideUserAlgorithmModal());
        this.bindEvent('newUserAlgorithmBtn', 'click', () => this.resetUserAlgorithmForm());
        this.bindEvent('loadAlgorithmSampleBtn', 'click', () => this.loadUserAlgorithmSample());
        this.bindEvent('applyAlgorithmTemplateBtn', 'click', () => this.applyUserAlgorithmTemplate());
        this.bindEvent('adjustUserAlgorithmBtn', 'click', () => this.adjustUserAlgorithm());
        this.bindEvent('generateUserAlgorithmBtn', 'click', () => this.generateUserAlgorithmDraft());
        this.bindEvent('aiAdjustUserAlgorithmBtn', 'click', () => this.aiAdjustUserAlgorithm());
        this.bindEvent('clearAlgorithmChatBtn', 'click', () => this.clearAlgorithmChat());
        this.bindEvent('validateUserAlgorithmBtn', 'click', () => this.validateUserAlgorithmForm());
        this.bindEvent('dryRunUserAlgorithmBtn', 'click', () => this.dryRunUserAlgorithm());
        this.bindEvent('backtestUserAlgorithmBtn', 'click', () => this.backtestUserAlgorithm());
        this.bindEvent('disableUserAlgorithmBtn', 'click', () => this.disableUserAlgorithm());
        this.bindEvent('saveUserAlgorithmBtn', 'click', () => this.saveUserAlgorithm());
        this.bindEvent('userAlgorithmLotteryFilter', 'change', async (event) => {
            this.renderUserAlgorithmList(event.target.value || 'jingcai_football');
        });
        this.bindEvent('userAlgorithmLotteryType', 'change', () => this.loadUserAlgorithmSample());
        this.bindEvent('savePredictorBtn', 'click', () => this.submitPredictor());
        this.bindEvent('testPredictorBtn', 'click', () => this.testPredictorConfig());
        this.bindEvent('checkPromptBtn', 'click', () => this.checkPromptAssistant());
        this.bindEvent('optimizePromptBtn', 'click', () => this.optimizePromptAssistant());
        this.bindEvent('applyOptimizedPromptBtn', 'click', () => this.applyOptimizedPrompt());
        this.bindEvent('buildExternalPromptBtn', 'click', () => this.buildExternalPromptTemplate());
        this.bindEvent('copyExternalPromptBtn', 'click', () => this.copyExternalPromptTemplate());
        this.bindEvent('predictNowBtn', 'click', () => this.predictNow());
        this.bindEvent('editPredictorBtn', 'click', () => this.openEditModal());
        this.bindEvent('togglePredictorBtn', 'click', () => this.toggleCurrentPredictor());
        this.bindEvent('footballManualSettleBtn', 'click', () => this.manualSettleFootball());
        this.bindEvent('footballReplayBtn', 'click', () => this.replayFootballScheduleByDate());
        this.bindEvent('togglePresetListBtn', 'click', () => this.togglePresetList());
        this.bindEvent('predictorLotteryFilter', 'change', (event) => {
            this.selectedPredictorLotteryFilter = event.target.value || 'all';
            this.syncPredictorStyleFilterOptions();
            this.renderPredictorList(this.getFilteredPredictors());
        });
        this.bindEvent('predictorEngineFilter', 'change', (event) => {
            this.selectedPredictorEngineFilter = event.target.value || 'all';
            this.syncPredictorStyleFilterOptions();
            this.renderPredictorList(this.getFilteredPredictors());
        });
        this.bindEvent('predictorStyleFilter', 'change', (event) => {
            this.selectedPredictorStyleFilter = event.target.value || 'all';
            this.renderPredictorList(this.getFilteredPredictors());
        });
        this.bindEvent('statsMetricView', 'change', (event) => {
            this.selectedStatsMetric = event.target.value;
            if (this.currentStats) {
                this.renderStats(this.currentStats);
            }
        });
        this.bindEvent('lotteryType', 'change', (event) => {
            this.saveCurrentFormState(this.formLotteryType);
            this.formLotteryType = event.target.value || 'pc28';
            this.updateLotteryForm();
        });
        this.bindEvent('engineType', 'change', () => this.updateLotteryForm());
        this.bindEvent('algorithmSource', 'change', () => this.updateLotteryForm());
        this.bindEvent('primaryMetric', 'change', () => this.syncProfitMetricOptions());
        ['targetNumber', 'targetBigSmall', 'targetOddEven', 'targetCombo'].forEach((id) => {
            this.bindEvent(id, 'change', () => this.syncProfitMetricOptions());
        });
        this.bindEvent('profitRuleId', 'change', () => this.saveCurrentFormState());
        this.bindEvent('profitDefaultMetric', 'change', () => this.saveCurrentFormState());
        this.bindEvent('predictionStatusFilter', 'change', (event) => {
            this.predictionStatusFilter = event.target.value;
            if (this.isHistoryPage()) {
                this.historyState.predictions.page = 1;
                this.historyState.predictions.status = this.predictionStatusFilter;
                this.loadPredictorHistoryTab('predictions', true);
                return;
            }
            this.renderPredictionsTable(this.currentPredictions || []);
        });
        this.bindEvent('predictionOutcomeFilter', 'change', (event) => {
            this.predictionOutcomeFilter = event.target.value;
            if (this.isHistoryPage()) {
                this.historyState.predictions.page = 1;
                this.historyState.predictions.outcome = this.predictionOutcomeFilter;
                this.loadPredictorHistoryTab('predictions', true);
                return;
            }
            this.renderPredictionsTable(this.currentPredictions || []);
        });
        this.bindEvent('profitRuleView', 'change', (event) => {
            this.selectedProfitRuleId = event.target.value;
            this.refreshProfitSimulation();
        });
        this.bindEvent('profitMetricView', 'change', (event) => {
            this.selectedProfitMetric = event.target.value;
            this.refreshProfitSimulation();
        });
        this.bindEvent('profitPeriodView', 'change', (event) => {
            this.selectedProfitPeriodKey = event.target.value;
            this.refreshProfitSimulation();
        });
        this.bindEvent('profitBetProfileView', 'change', (event) => {
            this.selectedProfitBetProfileId = event.target.value || '';
            this.syncProfitBetControlState();
            this.refreshProfitSimulation();
        });
        this.bindEvent('profitBetModeView', 'change', (event) => {
            this.selectedProfitBetMode = event.target.value;
            this.syncProfitBetControlState();
            this.refreshProfitSimulation();
        });
        this.bindEvent('profitBaseStakeView', 'change', (event) => {
            this.selectedProfitBaseStake = this.normalizePositiveNumber(event.target.value, DEFAULT_PROFIT_BASE_STAKE, 0.01);
            event.target.value = String(this.selectedProfitBaseStake);
            this.refreshProfitSimulation();
        });
        this.bindEvent('profitMultiplierView', 'change', (event) => {
            this.selectedProfitMultiplier = this.normalizePositiveNumber(event.target.value, DEFAULT_PROFIT_MULTIPLIER, 1.01);
            event.target.value = String(this.selectedProfitMultiplier);
            this.refreshProfitSimulation();
        });
        this.bindEvent('profitMaxStepsView', 'change', (event) => {
            this.selectedProfitMaxSteps = this.normalizePositiveInt(event.target.value, DEFAULT_PROFIT_MAX_STEPS, 1, 12);
            event.target.value = String(this.selectedProfitMaxSteps);
            this.refreshProfitSimulation();
        });
        this.bindEvent('profitOrderView', 'change', (event) => {
            this.selectedProfitOrder = event.target.value;
            if (this.currentProfitSimulation) {
                this.renderProfitSimulation(this.currentProfitSimulation, this.currentPredictor);
                return;
            }
            this.refreshProfitSimulation();
        });
        this.bindEvent('profitOddsProfileView', 'change', (event) => {
            this.selectedProfitOddsProfile = event.target.value;
            this.refreshProfitSimulation();
        });
        this.bindEvent('saveBetProfileBtn', 'click', () => this.submitBetProfile());
        this.bindEvent('resetBetProfileFormBtn', 'click', () => this.resetBetProfileForm());
        this.bindEvent('betProfileMode', 'change', () => this.syncBetProfileModeState());
        this.bindEvent('betProfilesBody', 'click', (event) => this.handleBetProfileTableClick(event));
        const betProfileCards = this.getElement('betProfilesCards');
        if (betProfileCards) {
            betProfileCards.addEventListener('click', (event) => this.handleBetProfileTableClick(event));
        }
        this.bindEvent('saveNotificationSenderBtn', 'click', () => this.submitNotificationSender());
        this.bindEvent('testNotificationSenderBtn', 'click', () => this.testNotificationSender());
        this.bindEvent('resetSenderFormBtn', 'click', () => this.resetNotificationSenderForm());
        this.bindEvent('notificationSendersBody', 'click', (event) => this.handleNotificationSenderTableClick(event));
        const notificationSenderCards = this.getElement('notificationSendersCards');
        if (notificationSenderCards) {
            notificationSenderCards.addEventListener('click', (event) => this.handleNotificationSenderTableClick(event));
        }
        this.bindEvent('saveNotificationEndpointBtn', 'click', () => this.submitNotificationEndpoint());
        this.bindEvent('testNotificationEndpointBtn', 'click', () => this.testNotificationEndpoint());
        this.bindEvent('resetEndpointFormBtn', 'click', () => this.resetNotificationEndpointForm());
        this.bindEvent('notificationEndpointsBody', 'click', (event) => this.handleNotificationEndpointTableClick(event));
        const notificationEndpointCards = this.getElement('notificationEndpointsCards');
        if (notificationEndpointCards) {
            notificationEndpointCards.addEventListener('click', (event) => this.handleNotificationEndpointTableClick(event));
        }
        this.bindEvent('saveNotificationSubscriptionBtn', 'click', () => this.submitNotificationSubscription());
        this.bindEvent('resetSubscriptionFormBtn', 'click', () => this.resetNotificationSubscriptionForm());
        this.bindEvent('notificationSubscriptionPredictorId', 'change', () => {
            this.syncSubscriptionBetProfileOptions();
            this.renderNotificationSubscriptionEventOptions();
        });
        this.bindEvent('notificationSubscriptionSenderMode', 'change', () => this.syncSubscriptionSenderState());
        this.bindEvent('notificationSubscriptionDeliveryMode', 'change', () => this.syncSubscriptionBetProfileState());
        this.bindEvent('notificationSubscriptionEventType', 'change', () => this.syncNotificationSubscriptionEventState());
        this.bindEvent('notificationSubscriptionsBody', 'click', (event) => this.handleNotificationSubscriptionTableClick(event));
        const notificationSubscriptionCards = this.getElement('notificationSubscriptionsCards');
        if (notificationSubscriptionCards) {
            notificationSubscriptionCards.addEventListener('click', (event) => this.handleNotificationSubscriptionTableClick(event));
        }
        this.bindEvent('notificationDeliveriesBody', 'click', (event) => this.handleNotificationDeliveryTableClick(event));
        const notificationDeliveryCards = this.getElement('notificationDeliveriesCards');
        if (notificationDeliveryCards) {
            notificationDeliveryCards.addEventListener('click', (event) => this.handleNotificationDeliveryTableClick(event));
        }
        this.bindEvent('publicSharePanel', 'click', (event) => {
            const button = event.target.closest('[data-action="copy-public-url"]');
            if (!button) {
                return;
            }
            this.copyPublicUrl(button);
        });

        document.querySelectorAll('.tab-btn').forEach((button) => {
            button.addEventListener('click', (event) => this.switchTab(event.currentTarget.dataset.tab));
        });
        ['predictions', 'draws', 'ai'].forEach((tabName) => {
            this.bindEvent(`${tabName}PrevPageBtn`, 'click', () => this.changeHistoryPage(tabName, -1));
            this.bindEvent(`${tabName}NextPageBtn`, 'click', () => this.changeHistoryPage(tabName, 1));
        });
        if (this.isDashboardPage()) {
            window.addEventListener('resize', this.handleChartResizeBound);
            window.addEventListener('orientationchange', this.handleChartResizeBound);
        }

        this.bindEvent('predictorModal', 'click', (event) => {
            if (event.target.id === 'predictorModal') {
                this.hideModal();
            }
        });
        this.bindEvent('userAlgorithmModal', 'click', (event) => {
            if (event.target.id === 'userAlgorithmModal') {
                this.hideUserAlgorithmModal();
            }
        });

        if (this.getElement('lotteryType')) {
            this.renderPresetCards();
            this.enforceNumberTarget();
            this.updateLotteryForm();
        }
        this.syncOverviewPanelState();
        if (this.getElement('betProfileId')) {
            this.resetBetProfileForm();
        }
        if (this.getElement('notificationSenderId')) {
            this.resetNotificationSenderForm();
        }
        if (this.getElement('notificationEndpointId')) {
            this.resetNotificationEndpointForm();
        }
        if (this.getElement('notificationSubscriptionId')) {
            this.resetNotificationSubscriptionForm();
        }
        const footballReplayDate = this.getElement('footballReplayDate');
        if (footballReplayDate && !footballReplayDate.value) {
            footballReplayDate.value = this.getTodayDateValue();
        }
    }

    toggleOverviewPanel() {
        this.overviewCollapsed = !this.overviewCollapsed;
        this.syncOverviewPanelState();
    }

    nextDashboardLoadSequence() {
        this.dashboardLoadSequence += 1;
        return this.dashboardLoadSequence;
    }

    isDashboardLoadStale(sequence, predictorId) {
        return sequence !== this.dashboardLoadSequence || predictorId !== this.currentPredictorId;
    }

    syncOverviewPanelState() {
        const section = this.getElement('sidebarOverviewSection');
        const panel = this.getElement('overviewPanel');
        const button = this.getElement('toggleOverviewPanelBtn');
        const icon = this.getElement('overviewPanelToggleIcon');
        if (!section || !panel || !button || !icon) {
            return;
        }

        const expanded = !this.overviewCollapsed;
        section.classList.toggle('is-collapsed', !expanded);
        panel.hidden = !expanded;
        button.setAttribute('aria-expanded', String(expanded));
        button.setAttribute('title', expanded ? '收起实时状态' : '展开实时状态');
        icon.className = `bi ${expanded ? 'bi-chevron-up' : 'bi-chevron-down'}`;
    }

    enforceNumberTarget() {
        if (this.getFormLotteryType() !== 'pc28') {
            return;
        }
        const targetNumber = document.getElementById('targetNumber');
        if (!targetNumber) {
            return;
        }
        targetNumber.checked = true;
    }

    getLotteryConfig(lotteryType = this.currentLotteryType) {
        return LOTTERY_UI_CONFIG[lotteryType] || LOTTERY_UI_CONFIG.pc28;
    }

    buildInitialFormStateByLottery() {
        return {
            pc28: this.buildDefaultFormState('pc28'),
            jingcai_football: this.buildDefaultFormState('jingcai_football')
        };
    }

    buildDefaultFormState(lotteryType) {
        const config = this.getLotteryConfig(lotteryType);
        return {
            targets: [...(config.defaultTargets || config.targetOptions.map((item) => item.key))],
            primaryMetric: config.defaultPrimaryMetric,
            profitRuleId: config.defaultProfitRuleId || '',
            profitDefaultMetric: config.defaultProfitMetric || config.defaultPrimaryMetric
        };
    }

    getFormLotteryType() {
        return document.getElementById('lotteryType')?.value || this.formLotteryType || 'pc28';
    }

    getFormEngineType() {
        return document.getElementById('engineType')?.value || 'ai';
    }

    getFormAlgorithmSource() {
        return document.getElementById('algorithmSource')?.value || 'builtin';
    }

    getMachineAlgorithmOptions(lotteryType = this.getFormLotteryType()) {
        return this.getLotteryConfig(lotteryType).machineAlgorithms || [];
    }

    getUserAlgorithmOptions(lotteryType = this.getFormLotteryType()) {
        return (this.userAlgorithms || [])
            .filter((item) => item.lottery_type === lotteryType && item.status === 'validated');
    }

    isMachineEngineSelected() {
        return this.getFormEngineType() === 'machine';
    }

    syncMachineAlgorithmOptions(lotteryType = this.getFormLotteryType(), preferredValue = null) {
        const select = document.getElementById('algorithmKey');
        const hint = document.getElementById('algorithmHint');
        if (!select) {
            return;
        }

        const source = this.getFormAlgorithmSource();
        if (source === 'user') {
            const userOptions = this.getUserAlgorithmOptions(lotteryType);
            if (!userOptions.length) {
                select.innerHTML = '<option value="">当前彩种暂无已校验用户算法</option>';
                select.disabled = true;
                if (hint) {
                    hint.textContent = lotteryType === 'jingcai_football'
                        ? '请先在“我的算法”中创建并通过校验。'
                        : '当前仅支持竞彩足球用户算法绑定预测方案。';
                }
                return;
            }
            select.innerHTML = userOptions.map((item) => `
                <option value="${this.escapeHtml(item.key)}">${this.escapeHtml(item.name)}</option>
            `).join('');
            select.disabled = false;
            const nextValue = userOptions.some((item) => item.key === preferredValue)
                ? preferredValue
                : userOptions[0].key;
            select.value = nextValue;
            const selectedOption = userOptions.find((item) => item.key === nextValue) || userOptions[0];
            if (hint) {
                hint.textContent = selectedOption?.description || '选择当前方案要使用的用户自定义算法。';
            }
            return;
        }

        const options = this.getMachineAlgorithmOptions(lotteryType);
        if (!options.length) {
            select.innerHTML = '<option value="">当前彩种暂无可用机器算法</option>';
            select.disabled = true;
            if (hint) {
                hint.textContent = '当前彩种暂未提供内置机器算法。';
            }
            return;
        }

        select.innerHTML = options.map((item) => `
            <option value="${this.escapeHtml(item.key)}">${this.escapeHtml(item.label)}</option>
        `).join('');
        select.disabled = false;
        const nextValue = options.some((item) => item.key === preferredValue)
            ? preferredValue
            : options[0].key;
        select.value = nextValue;
        const selectedOption = options.find((item) => item.key === nextValue) || options[0];
        if (hint) {
            hint.textContent = selectedOption?.description || '选择当前方案要使用的内置机器算法。';
        }
    }

    getSelectedFormTargets() {
        return ['targetNumber', 'targetBigSmall', 'targetOddEven', 'targetCombo']
            .map((id) => document.getElementById(id))
            .filter((input) => input && input.checked && input.dataset.targetKey)
            .map((input) => input.dataset.targetKey);
    }

    getFormState(lotteryType = this.getFormLotteryType()) {
        const defaults = this.buildDefaultFormState(lotteryType);
        return {
            ...defaults,
            ...(this.formStateByLottery?.[lotteryType] || {})
        };
    }

    saveCurrentFormState(lotteryType = this.formLotteryType) {
        if (!lotteryType) {
            return;
        }

        const defaults = this.buildDefaultFormState(lotteryType);
        this.formStateByLottery[lotteryType] = {
            ...defaults,
            targets: this.getSelectedFormTargets(),
            primaryMetric: document.getElementById('primaryMetric')?.value || defaults.primaryMetric,
            profitRuleId: document.getElementById('profitRuleId')?.value || defaults.profitRuleId,
            profitDefaultMetric: document.getElementById('profitDefaultMetric')?.value || defaults.profitDefaultMetric
        };
    }

    getFormProfitMetricOptions(lotteryType, selectedTargets) {
        if (lotteryType === 'jingcai_football') {
            const options = [];
            if (selectedTargets.includes('spf')) {
                options.push({ key: 'spf', label: '胜平负：默认按单关胜平负看批次盈亏' });
            }
            if (selectedTargets.includes('rqspf')) {
                options.push({ key: 'rqspf', label: '让球胜平负：默认按单关让球胜平负看批次盈亏' });
            }
            return options;
        }

        const options = [];
        if (selectedTargets.includes('big_small')) {
            options.push({ key: 'big_small', label: '大/小：默认按大小单双盘看当盘日盈亏' });
        }
        if (selectedTargets.includes('odd_even')) {
            options.push({ key: 'odd_even', label: '单/双：默认按单双盘看当盘日盈亏' });
        }
        if (selectedTargets.includes('combo')) {
            options.push({ key: 'combo', label: '组合投注：默认按组合票面看当盘日盈亏' });
        }
        if (selectedTargets.includes('number')) {
            options.push({ key: 'number', label: '单点：默认按精确和值看当盘日盈亏' });
        }
        return options;
    }

    resolveFormProfitMetric(lotteryType, options, primaryMetric, preferredValue) {
        if (options.some((item) => item.key === preferredValue)) {
            return preferredValue;
        }

        const derivedMetric = lotteryType === 'pc28' && ['double_group', 'kill_group'].includes(primaryMetric)
            ? 'combo'
            : primaryMetric;
        if (options.some((item) => item.key === derivedMetric)) {
            return derivedMetric;
        }

        const defaultMetric = this.getLotteryConfig(lotteryType).defaultProfitMetric;
        if (options.some((item) => item.key === defaultMetric)) {
            return defaultMetric;
        }

        return options[0]?.key || '';
    }

    syncProfitRuleOptions(lotteryType = this.getFormLotteryType(), preferredValue = null) {
        const select = document.getElementById('profitRuleId');
        if (!select) {
            return;
        }

        const config = this.getLotteryConfig(lotteryType);
        const options = config.supportsProfitSimulation ? (config.profitRuleOptions || []) : [];
        const fallbackValue = preferredValue || this.getFormState(lotteryType).profitRuleId || config.defaultProfitRuleId || '';

        if (!options.length) {
            select.innerHTML = '<option value="">当前彩种暂不支持收益模拟</option>';
            select.disabled = true;
            return;
        }

        select.innerHTML = options.map((item) => `
            <option value="${this.escapeHtml(item.key)}">${this.escapeHtml(item.label)}</option>
        `).join('');
        select.disabled = false;
        select.value = options.some((item) => item.key === fallbackValue)
            ? fallbackValue
            : (config.defaultProfitRuleId || options[0].key);
    }

    updateLotteryForm(options = {}) {
        const hasSelectedTargets = Object.prototype.hasOwnProperty.call(options, 'selectedTargets');
        const hasSelectedPrimaryMetric = Object.prototype.hasOwnProperty.call(options, 'selectedPrimaryMetric');
        const hasSelectedProfitRuleId = Object.prototype.hasOwnProperty.call(options, 'selectedProfitRuleId');
        const hasSelectedProfitDefaultMetric = Object.prototype.hasOwnProperty.call(options, 'selectedProfitDefaultMetric');
        const hasSelectedAlgorithmKey = Object.prototype.hasOwnProperty.call(options, 'selectedAlgorithmKey');
        const currentType = document.getElementById('lotteryType')?.value || this.formLotteryType || 'pc28';
        const currentEngineType = this.getFormEngineType();
        const selectedAlgorithmKey = hasSelectedAlgorithmKey
            ? (options.selectedAlgorithmKey || '')
            : (document.getElementById('algorithmKey')?.value || '');
        const algorithmSource = selectedAlgorithmKey.startsWith('user:')
            ? 'user'
            : this.getFormAlgorithmSource();
        this.formLotteryType = currentType;
        const config = this.getLotteryConfig(currentType);
        const nextState = {
            ...this.getFormState(currentType),
            ...(hasSelectedTargets ? { targets: [...(options.selectedTargets || [])] } : {}),
            ...(hasSelectedPrimaryMetric ? { primaryMetric: options.selectedPrimaryMetric } : {}),
            ...(hasSelectedProfitRuleId ? { profitRuleId: options.selectedProfitRuleId } : {}),
            ...(hasSelectedProfitDefaultMetric ? { profitDefaultMetric: options.selectedProfitDefaultMetric } : {})
        };
        this.formStateByLottery[currentType] = nextState;
        const targetSlots = [
            { wrapper: 'targetOptionNumber', input: 'targetNumber', label: 'targetLabelNumber' },
            { wrapper: 'targetOptionBigSmall', input: 'targetBigSmall', label: 'targetLabelBigSmall' },
            { wrapper: 'targetOptionOddEven', input: 'targetOddEven', label: 'targetLabelOddEven' },
            { wrapper: 'targetOptionCombo', input: 'targetCombo', label: 'targetLabelCombo' }
        ];

        targetSlots.forEach((slot, index) => {
            const wrapper = document.getElementById(slot.wrapper);
            const input = document.getElementById(slot.input);
            const label = document.getElementById(slot.label);
            const option = config.targetOptions[index];
            if (!wrapper || !input || !label) {
                return;
            }

            if (!option) {
                wrapper.style.display = 'none';
                input.checked = false;
                input.disabled = true;
                input.dataset.targetKey = '';
                return;
            }

            wrapper.style.display = '';
            input.dataset.targetKey = option.key;
            label.textContent = option.label;
            input.disabled = Boolean(option.fixed);
            input.setAttribute('aria-disabled', option.fixed ? 'true' : 'false');
            input.checked = option.fixed ? true : nextState.targets.includes(option.key);
        });

        document.getElementById('targetHint').textContent = config.targetHint;
        document.getElementById('historyWindowLabel').textContent = config.historyWindowLabel || '历史窗口';
        document.getElementById('historyWindowHint').textContent = config.historyWindowHint || '';
        this.renderPrimaryMetricOptions(currentType, nextState.primaryMetric);
        const algorithmSourceSelect = document.getElementById('algorithmSource');
        if (algorithmSourceSelect) {
            algorithmSourceSelect.value = algorithmSource === 'user' ? 'user' : 'builtin';
        }
        this.syncMachineAlgorithmOptions(
            currentType,
            selectedAlgorithmKey || null
        );

        const showProfit = config.supportsProfitSimulation;
        document.getElementById('profitRuleField').style.display = showProfit ? '' : 'none';
        document.getElementById('profitMetricField').style.display = showProfit ? '' : 'none';
        document.getElementById('profitPanel').style.display = showProfit ? '' : 'none';

        const showMachineAlgorithm = currentEngineType === 'machine';
        const showPromptAssistant = config.supportsPromptAssistant && !showMachineAlgorithm;
        const showPresets = config.supportsPresets && !showMachineAlgorithm;
        document.getElementById('algorithmField').style.display = showMachineAlgorithm ? '' : 'none';
        const fallbackField = document.getElementById('userAlgorithmFallbackField');
        if (fallbackField) {
            fallbackField.style.display = showMachineAlgorithm && algorithmSource === 'user' ? '' : 'none';
        }
        ['apiUrlField', 'modelNameField', 'apiModeField', 'apiKeyField', 'temperatureField', 'dataInjectionModeField', 'systemPromptField']
            .forEach((id) => {
                const element = document.getElementById(id);
                if (element) {
                    element.style.display = showMachineAlgorithm ? 'none' : '';
                }
            });
        document.getElementById('promptAssistantActions').style.display = showPromptAssistant ? 'flex' : 'none';
        document.getElementById('promptVariablesBlock').style.display = showPromptAssistant ? '' : 'none';
        document.getElementById('externalPromptBlock').style.display = showPromptAssistant ? '' : 'none';
        document.getElementById('presetBlock').style.display = showPresets ? '' : 'none';
        document.getElementById('testPredictorBtn').textContent = showMachineAlgorithm ? '检查算法' : '测试模型';

        if (!showPromptAssistant) {
            this.hidePromptAssistantResult();
        }
        this.clearExternalPromptTemplate();
        this.updatePromptVariableVisibility(currentType);
        this.updatePromptTemplateExample(currentType);

        if (!showPresets) {
            document.getElementById('presetCards').innerHTML = '<div class="empty-panel">当前彩种暂未提供内置方案示例</div>';
            document.getElementById('togglePresetListBtn').style.display = 'none';
        } else {
            this.renderPresetCards();
        }

        if (!options.selectedHistoryWindow) {
            document.getElementById('historyWindow').value = String(config.defaultHistoryWindow || 60);
        }
        this.syncProfitRuleOptions(currentType, nextState.profitRuleId);
        this.syncProfitMetricOptions(currentType, nextState.profitDefaultMetric);
    }

    updatePromptVariableVisibility(lotteryType) {
        document.querySelectorAll('#promptVariablesBlock .code-tag').forEach((element) => {
            const scope = element.dataset.scope || 'common';
            element.style.display = (scope === 'common' || scope === lotteryType) ? 'inline-flex' : 'none';
        });
    }

    updatePromptTemplateExample(lotteryType) {
        const example = document.getElementById('promptTemplateExample');
        if (!example) {
            return;
        }

        if (lotteryType === 'jingcai_football') {
            example.textContent = `角色：你是一个中国竞彩足球预测助手。

待预测比赛：
{{match_batch_summary}}

市场赔率摘要：
{{market_odds_summary}}

比赛详情摘要：
{{match_detail_summary}}

近期赛果：
{{recent_results_summary}}

请结合赔率、让球、积分排名、历史交锋、近期战绩和伤停信息，输出每场比赛的胜平负与让球胜平负预测，只输出 JSON。`;
            return;
        }

        example.textContent = `角色：你是一个 PC28 预测引擎。

输入：
最近 {{history_window}} 期 PC28 数据：
{{recent_draws_csv}}

当前北京时间：
{{current_time_beijing}}

下一期期号：
{{next_issue_no}}

请结合统计模型 + 小六壬输出下一期的和值、大小单双、风险和策略，只输出 JSON。`;
    }

    renderPrimaryMetricOptions(lotteryType, selectedValue = null) {
        const select = document.getElementById('primaryMetric');
        const config = this.getLotteryConfig(lotteryType);
        const previousValue = selectedValue || select.value || config.defaultPrimaryMetric;
        select.innerHTML = config.primaryMetricOptions.map((item) => `
            <option value="${this.escapeHtml(item.key)}">${this.escapeHtml(item.label)}</option>
        `).join('');
        select.value = config.primaryMetricOptions.some((item) => item.key === previousValue)
            ? previousValue
            : config.defaultPrimaryMetric;
    }

    async checkAuth() {
        const response = await fetch('/api/auth/me', { credentials: 'include' });
        if (!response.ok) {
            window.location.href = '/login';
            return;
        }

        this.currentUser = await response.json();
        const userInfo = this.getElement('userInfo');
        if (userInfo) {
            userInfo.textContent = `当前用户：${this.currentUser.username}`;
        }
        const adminEntry = this.getElement('adminEntryBtn');
        if (adminEntry) {
            adminEntry.style.display = this.currentUser.is_admin ? 'inline-flex' : 'none';
        }
    }

    async refresh(forceReloadPredictorList = false, reloadUserSettings = false) {
        await this.loadUserAlgorithms();
        await this.loadPredictors(forceReloadPredictorList);
        if (reloadUserSettings) {
            await this.loadUserSettings();
        }
        if (this.currentPredictorId) {
            await this.loadPredictorDashboard(this.currentPredictorId);
        } else {
            await this.loadOverview();
            this.renderEmptyPredictorState();
        }
    }

    async loadPredictorHistory() {
        this.historyDataCache = {};
        await this.loadPredictorHistoryTab(this.activeContentTab || 'predictions', true);
    }

    async loadPredictorHistoryTab(tabName, forceReload = false) {
        if (!this.historyPredictorId) {
            return;
        }
        const normalizedTab = ['predictions', 'draws', 'ai'].includes(tabName) ? tabName : 'predictions';
        const queryKey = this.buildHistoryCacheKey(normalizedTab);
        if (!forceReload && this.historyDataCache[queryKey]) {
            this.applyPredictorHistoryPayload(this.historyDataCache[queryKey], normalizedTab);
            return;
        }
        this.showHistoryLoading(normalizedTab);
        try {
            const response = await fetch(this.buildHistoryRequestUrl(normalizedTab), {
                credentials: 'include'
            });
            if (response.status === 401) {
                window.location.href = '/login';
                return;
            }
            const data = await response.json();
            if (!response.ok) {
                throw new Error(data.error || '加载方案完整记录失败');
            }
            this.historyDataCache[queryKey] = data;
            this.applyPredictorHistoryPayload(data, normalizedTab);
        } catch (error) {
            console.error('Failed to load predictor history:', error);
        }
    }

    buildHistoryRequestUrl(tabName) {
        const state = this.historyState[tabName] || {};
        const params = new URLSearchParams({
            tab: tabName,
            page: String(state.page || 1),
            page_size: String(state.pageSize || (tabName === 'ai' ? 20 : 50))
        });
        if (tabName === 'predictions') {
            params.set('status', state.status || 'all');
            params.set('outcome', state.outcome || 'all');
        }
        return `/api/predictors/${this.historyPredictorId}/history?${params.toString()}`;
    }

    buildHistoryCacheKey(tabName) {
        const state = this.historyState[tabName] || {};
        if (tabName === 'predictions') {
            return `${tabName}:${state.page || 1}:${state.pageSize || 50}:${state.status || 'all'}:${state.outcome || 'all'}`;
        }
        return `${tabName}:${state.page || 1}:${state.pageSize || (tabName === 'ai' ? 20 : 50)}`;
    }

    applyPredictorHistoryPayload(data, tabName) {
        this.currentPredictorId = this.historyPredictorId;
        this.currentPredictor = data.predictor || null;
        this.currentLotteryType = data.predictor?.lottery_type || 'pc28';
        this.updateHistoryLinks();
        this.historyState[tabName].pagination = data.pagination || null;
        this.historyState[tabName].page = data.pagination?.page || this.historyState[tabName].page;
        this.historyState[tabName].pageSize = data.pagination?.page_size || this.historyState[tabName].pageSize;
        if (tabName === 'predictions' && data.filters) {
            this.historyState.predictions.status = data.filters.status || 'all';
            this.historyState.predictions.outcome = data.filters.outcome || 'all';
            const statusFilter = this.getElement('predictionStatusFilter');
            const outcomeFilter = this.getElement('predictionOutcomeFilter');
            if (statusFilter) {
                statusFilter.value = this.historyState.predictions.status;
            }
            if (outcomeFilter) {
                outcomeFilter.value = this.historyState.predictions.outcome;
            }
        }
        if (tabName === 'draws') {
            this.renderDrawsTable(data.recent_draws || []);
            this.renderHistoryPagination('draws', data.pagination);
            return;
        }
        this.currentPredictions = data.recent_predictions || [];
        if (tabName === 'ai') {
            this.renderAILogs(this.currentPredictions);
            this.renderHistoryPagination('ai', data.pagination);
            return;
        }
        this.renderPredictionsTable(this.currentPredictions);
        this.renderHistoryPagination('predictions', data.pagination);
    }

    changeHistoryPage(tabName, direction) {
        const state = this.historyState[tabName];
        if (!state?.pagination) {
            return;
        }
        const nextPage = state.page + direction;
        if (nextPage < 1 || nextPage > (state.pagination.total_pages || 1)) {
            return;
        }
        state.page = nextPage;
        this.loadPredictorHistoryTab(tabName, true);
    }

    renderHistoryPagination(tabName, pagination) {
        const info = this.getElement(`${tabName}PaginationInfo`);
        const prevButton = this.getElement(`${tabName}PrevPageBtn`);
        const nextButton = this.getElement(`${tabName}NextPageBtn`);
        if (!info || !prevButton || !nextButton) {
            return;
        }
        const payload = pagination || { page: 1, total_pages: 1, total: 0, page_size: this.historyState[tabName]?.pageSize || 0, has_prev: false, has_next: false };
        info.textContent = `第 ${payload.page} / ${payload.total_pages} 页，共 ${payload.total} 条`;
        prevButton.disabled = !payload.has_prev;
        nextButton.disabled = !payload.has_next;
    }

    showHistoryLoading(tabName) {
        if (!this.isHistoryPage()) {
            return;
        }
        this.renderHistoryPagination(tabName, {
            page: this.historyState[tabName]?.page || 1,
            total_pages: this.historyState[tabName]?.pagination?.total_pages || 1,
            total: this.historyState[tabName]?.pagination?.total || 0,
            has_prev: false,
            has_next: false
        });
        if (tabName === 'predictions') {
            const tbody = this.getElement('predictionsBody');
            const cards = this.getElement('predictionsCards');
            if (tbody) {
                tbody.innerHTML = '<tr><td colspan="8" class="empty-cell">加载中...</td></tr>';
            }
            if (cards) {
                cards.innerHTML = '<div class="empty-panel">加载中...</div>';
            }
            return;
        }
        if (tabName === 'draws') {
            const tbody = this.getElement('drawsBody');
            const cards = this.getElement('drawsCards');
            if (tbody) {
                tbody.innerHTML = '<tr><td colspan="6" class="empty-cell">加载中...</td></tr>';
            }
            if (cards) {
                cards.innerHTML = '<div class="empty-panel">加载中...</div>';
            }
            return;
        }
        const container = this.getElement('aiLogs');
        if (container) {
            container.innerHTML = '<div class="empty-panel">加载中...</div>';
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
        if ((overview?.lottery_type || 'pc28') === 'jingcai_football') {
            this.renderFootballOverview(overview);
            return;
        }
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

    renderFootballOverview(overview) {
        const warning = overview.warning ? `<div class="warning-banner">${this.escapeHtml(overview.warning)}</div>` : '';
        const recentEvents = overview.recent_events || [];
        const upcoming = recentEvents.filter((item) => this.footballMatchStatusCode(item) === '1').slice(0, 4);

        document.getElementById('overviewPanel').innerHTML = `
            ${warning}
            <div class="overview-primary">
                <div>
                    <span class="mini-label">当前批次</span>
                    <strong>${this.escapeHtml(overview.batch_key || '--')}</strong>
                </div>
                <div>
                    <span class="mini-label">已开售场次</span>
                    <strong>${this.escapeHtml(String(overview.open_match_count ?? 0))}</strong>
                </div>
            </div>
            <div class="overview-result">
                <span class="mini-label">下一场比赛</span>
                <div class="result-number football-title">${this.escapeHtml(overview.next_match_name || '暂无')}</div>
                <div class="result-meta">${this.escapeHtml(overview.next_match_time || '')}</div>
            </div>
            <div class="overview-block">
                <span class="mini-label">状态分布</span>
                <div class="tag-list compact">
                    <span class="tag">已开售 ${this.escapeHtml(String(overview.open_match_count ?? 0))}</span>
                    <span class="tag">待开奖 ${this.escapeHtml(String(overview.awaiting_result_match_count ?? 0))}</span>
                    <span class="tag">已开奖 ${this.escapeHtml(String(overview.settled_match_count ?? 0))}</span>
                </div>
            </div>
            <div class="overview-block">
                <span class="mini-label">近期开售</span>
                <div class="tag-list compact">
                    ${upcoming.length ? upcoming.map((item) => `<span class="tag">${this.escapeHtml(item.match_no || item.event_key || '--')} · ${this.escapeHtml(item.home_team || item.event_name || '--')}</span>`).join('') : '<span class="tag">暂无</span>'}
                </div>
            </div>
        `;
        this.renderDrawsTable(recentEvents);
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
            this.syncPredictorStyleFilterOptions();

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

            this.renderPredictorList(this.getFilteredPredictors());
        } catch (error) {
            console.error('Failed to load predictors:', error);
        }
    }

    async loadUserAlgorithms() {
        if (!this.isDashboardPage()) {
            return;
        }
        try {
            const response = await fetch('/api/user-algorithms?include_disabled=1', { credentials: 'include' });
            if (response.status === 401) {
                window.location.href = '/login';
                return;
            }
            if (!response.ok) {
                return;
            }
            this.userAlgorithms = await response.json();
            const activeFilter = document.getElementById('userAlgorithmLotteryFilter')?.value || 'jingcai_football';
            this.renderUserAlgorithmList(activeFilter);
        } catch (error) {
            console.error('Failed to load user algorithms:', error);
        }
    }

    async loadUserSettings() {
        try {
            const [betProfilesResponse, sendersResponse, endpointsResponse, subscriptionsResponse, deliveriesResponse] = await Promise.all([
                fetch('/api/bet-profiles', { credentials: 'include' }),
                fetch('/api/notification-senders', { credentials: 'include' }),
                fetch('/api/notification-endpoints', { credentials: 'include' }),
                fetch('/api/notification-subscriptions', { credentials: 'include' }),
                fetch('/api/notification-deliveries', { credentials: 'include' })
            ]);
            const responses = [betProfilesResponse, sendersResponse, endpointsResponse, subscriptionsResponse, deliveriesResponse];
            if (responses.some((item) => item.status === 401)) {
                window.location.href = '/login';
                return;
            }
            const [betProfiles, senders, endpoints, subscriptions, deliveries] = await Promise.all(
                responses.map((item) => this.parseJsonSafely(item))
            );
            this.betProfiles = Array.isArray(betProfiles) ? betProfiles : [];
            this.notificationSenders = Array.isArray(senders) ? senders : [];
            this.notificationEndpoints = Array.isArray(endpoints) ? endpoints : [];
            this.notificationSubscriptions = Array.isArray(subscriptions) ? subscriptions : [];
            this.notificationDeliveries = Array.isArray(deliveries) ? deliveries : [];
            this.renderUserSettingsSummary();
            this.renderBetProfiles(this.betProfiles);
            this.renderNotificationSenders(this.notificationSenders);
            this.renderNotificationEndpoints(this.notificationEndpoints);
            this.renderNotificationSubscriptions(this.notificationSubscriptions);
            this.renderNotificationDeliveries(this.notificationDeliveries);
            this.renderSubscriptionPredictorOptions();
            this.renderNotificationSubscriptionEventOptions();
            this.renderNotificationSenderOptions();
            this.renderNotificationEndpointOptions();
            this.syncSubscriptionBetProfileOptions();
            this.syncSubscriptionSenderState();
            this.syncNotificationSubscriptionEventState();
            this.syncBetProfileModeState();
        } catch (error) {
            console.error('Failed to load user settings:', error);
        }
    }

    renderUserSettingsSummary() {
        const activeSubscriptions = (this.notificationSubscriptions || []).filter((item) => item.enabled).length;
        const mappings = [
            ['settingsSummaryBetProfiles', this.betProfiles.length],
            ['settingsSummarySenders', this.notificationSenders.length],
            ['settingsSummaryEndpoints', this.notificationEndpoints.length],
            ['settingsSummarySubscriptions', activeSubscriptions],
            ['settingsSummaryDeliveries', this.notificationDeliveries.length]
        ];
        mappings.forEach(([id, value]) => {
            const element = this.getElement(id);
            if (element) {
                element.textContent = String(value);
            }
        });
    }

    getPreviewLimit() {
        return this.isDashboardPage() ? 10 : null;
    }

    getPreviewItems(items, limit = this.getPreviewLimit()) {
        if (!Array.isArray(items)) {
            return [];
        }
        if (!limit || limit <= 0) {
            return [...items];
        }
        return items.slice(0, limit);
    }

    updateHistoryLinks() {
        const mappings = [
            ['viewAllPredictionsLink', 'predictions'],
            ['viewAllDrawsLink', 'draws'],
            ['viewAllAILogsLink', 'ai']
        ];
        mappings.forEach(([id, tab]) => {
            const element = this.getElement(id);
            if (!element) {
                return;
            }
            if (!this.currentPredictorId) {
                element.setAttribute('href', '/dashboard');
                element.setAttribute('aria-disabled', 'true');
                return;
            }
            element.setAttribute('href', `/predictors/${this.currentPredictorId}/history?tab=${tab}`);
            element.removeAttribute('aria-disabled');
        });
    }

    resetBetProfileForm() {
        if (!this.getElement('betProfileId')) {
            return;
        }
        document.getElementById('betProfileId').value = '';
        document.getElementById('betProfileName').value = '';
        document.getElementById('betProfileLotteryType').value = this.currentLotteryType || 'pc28';
        document.getElementById('betProfileMode').value = 'flat';
        document.getElementById('betProfileBaseStake').value = '10';
        document.getElementById('betProfileMultiplier').value = '2';
        document.getElementById('betProfileMaxSteps').value = '6';
        document.getElementById('betProfileEnabled').value = 'true';
        document.getElementById('betProfileIsDefault').value = 'false';
        this.syncBetProfileModeState();
    }

    syncBetProfileModeState() {
        const modeInput = this.getElement('betProfileMode');
        const multiplierInput = this.getElement('betProfileMultiplier');
        const maxStepsInput = this.getElement('betProfileMaxSteps');
        if (!modeInput || !multiplierInput || !maxStepsInput) {
            return;
        }
        const mode = modeInput.value || 'flat';
        const isFlat = mode === 'flat';
        multiplierInput.disabled = isFlat;
        maxStepsInput.disabled = isFlat;
    }

    renderBetProfiles(items) {
        const tbody = this.getElement('betProfilesBody');
        const cards = this.getElement('betProfilesCards');
        if (!tbody) {
            return;
        }
        if (!items.length) {
            tbody.innerHTML = '<tr><td colspan="5" class="empty-cell">暂无下注策略</td></tr>';
            if (cards) {
                cards.innerHTML = '<div class="empty-panel">暂无下注策略</div>';
            }
            return;
        }

        tbody.innerHTML = items.map((item) => `
            <tr>
                <td>
                    <div class="result-stack">
                        <strong>${this.escapeHtml(item.name)}</strong>
                        <span class="result-meta-text">${item.is_default ? '默认策略' : '普通策略'}</span>
                    </div>
                </td>
                <td>${this.escapeHtml(item.lottery_label || '--')}</td>
                <td>${this.escapeHtml(item.strategy_label || '--')}</td>
                <td><span class="status-chip ${item.enabled ? 'enabled' : 'disabled'}">${item.enabled ? '启用' : '停用'}</span></td>
                <td>
                    <div class="predictor-actions">
                        <button class="icon-btn" data-action="edit-bet-profile" data-id="${item.id}" title="编辑下注策略"><i class="bi bi-pencil"></i></button>
                        <button class="icon-btn danger" data-action="delete-bet-profile" data-id="${item.id}" title="删除下注策略"><i class="bi bi-trash"></i></button>
                    </div>
                </td>
            </tr>
        `).join('');

        if (cards) {
            cards.innerHTML = items.map((item) => this.renderMobileDataCard({
                title: this.escapeHtml(item.name),
                badgeHtml: `<span class="status-chip ${item.enabled ? 'enabled' : 'disabled'}">${item.enabled ? '启用' : '停用'}</span>`,
                sections: [
                    { label: '彩种', content: this.escapeHtml(item.lottery_label || '--') },
                    { label: '策略', content: this.escapeHtml(item.strategy_label || '--') },
                    { label: '默认', content: this.escapeHtml(item.is_default ? '是' : '否') }
                ],
                footer: `
                    <div class="predictor-actions">
                        <button class="icon-btn" data-action="edit-bet-profile" data-id="${item.id}" title="编辑下注策略"><i class="bi bi-pencil"></i></button>
                        <button class="icon-btn danger" data-action="delete-bet-profile" data-id="${item.id}" title="删除下注策略"><i class="bi bi-trash"></i></button>
                    </div>
                `
            })).join('');
        }
    }

    handleBetProfileTableClick(event) {
        const button = event.target.closest('[data-action]');
        if (!button) {
            return;
        }
        const profileId = Number(button.dataset.id);
        if (!profileId) {
            return;
        }
        if (button.dataset.action === 'edit-bet-profile') {
            this.populateBetProfileForm(profileId);
            return;
        }
        if (button.dataset.action === 'delete-bet-profile') {
            this.deleteBetProfile(profileId);
        }
    }

    populateBetProfileForm(profileId) {
        const item = (this.betProfiles || []).find((row) => row.id === profileId);
        if (!item) {
            return;
        }
        document.getElementById('betProfileId').value = String(item.id);
        document.getElementById('betProfileName').value = item.name || '';
        document.getElementById('betProfileLotteryType').value = item.lottery_type || 'pc28';
        document.getElementById('betProfileMode').value = item.mode || 'flat';
        document.getElementById('betProfileBaseStake').value = String(item.base_stake || 10);
        document.getElementById('betProfileMultiplier').value = String(item.multiplier || 2);
        document.getElementById('betProfileMaxSteps').value = String(item.max_steps || 6);
        document.getElementById('betProfileEnabled').value = item.enabled ? 'true' : 'false';
        document.getElementById('betProfileIsDefault').value = item.is_default ? 'true' : 'false';
        this.syncBetProfileModeState();
    }

    async submitBetProfile() {
        const profileId = document.getElementById('betProfileId').value;
        const payload = {
            name: document.getElementById('betProfileName').value.trim(),
            lottery_type: document.getElementById('betProfileLotteryType').value || 'pc28',
            mode: document.getElementById('betProfileMode').value || 'flat',
            base_stake: Number(document.getElementById('betProfileBaseStake').value || 10),
            multiplier: Number(document.getElementById('betProfileMultiplier').value || 2),
            max_steps: Number(document.getElementById('betProfileMaxSteps').value || 6),
            enabled: document.getElementById('betProfileEnabled').value === 'true',
            is_default: document.getElementById('betProfileIsDefault').value === 'true'
        };
        const url = profileId ? `/api/bet-profiles/${profileId}` : '/api/bet-profiles';
        const method = profileId ? 'PUT' : 'POST';
        try {
            const response = await fetch(url, {
                method,
                credentials: 'include',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });
            const data = await this.parseJsonSafely(response);
            if (!response.ok) {
                throw new Error(data.error || '保存下注策略失败');
            }
            this.resetBetProfileForm();
            await this.loadUserSettings();
            if (this.currentPredictorId) {
                await this.refreshProfitSimulation();
            }
        } catch (error) {
            alert(error.message);
        }
    }

    async deleteBetProfile(profileId) {
        if (!window.confirm('确定删除这个下注策略吗？关联订阅会同时解除绑定。')) {
            return;
        }
        try {
            const response = await fetch(`/api/bet-profiles/${profileId}`, {
                method: 'DELETE',
                credentials: 'include'
            });
            const data = await this.parseJsonSafely(response);
            if (!response.ok) {
                throw new Error(data.error || '删除下注策略失败');
            }
            this.resetBetProfileForm();
            await this.loadUserSettings();
        } catch (error) {
            alert(error.message);
        }
    }

    resetNotificationSenderForm() {
        document.getElementById('notificationSenderId').value = '';
        document.getElementById('notificationSenderChannelType').value = 'telegram';
        document.getElementById('notificationSenderName').value = '';
        document.getElementById('notificationSenderBotName').value = '';
        document.getElementById('notificationSenderBotToken').value = '';
        document.getElementById('notificationSenderStatus').value = 'active';
        document.getElementById('notificationSenderIsDefault').value = 'false';
        document.getElementById('notificationSenderTestChatId').value = '';
    }

    renderNotificationSenders(items) {
        const tbody = this.getElement('notificationSendersBody');
        const cards = this.getElement('notificationSendersCards');
        if (!tbody) {
            return;
        }
        if (!items.length) {
            tbody.innerHTML = '<tr><td colspan="5" class="empty-cell">暂无通知发送方</td></tr>';
            if (cards) {
                cards.innerHTML = '<div class="empty-panel">暂无通知发送方</div>';
            }
            return;
        }
        tbody.innerHTML = items.map((item) => `
            <tr>
                <td>
                    <div class="result-stack">
                        <strong>${this.escapeHtml(item.sender_name || '--')}</strong>
                        <span class="result-meta-text">${item.is_default ? '默认发送方' : '普通发送方'}</span>
                    </div>
                </td>
                <td>${this.escapeHtml(item.channel_label || '--')}</td>
                <td>${this.escapeHtml(item.bot_name || '--')}</td>
                <td><span class="status-chip ${item.status === 'active' ? 'enabled' : 'disabled'}">${this.escapeHtml(item.status_label || '--')}</span></td>
                <td>
                    <div class="predictor-actions">
                        <button class="icon-btn" data-action="edit-sender" data-id="${item.id}" title="编辑通知发送方"><i class="bi bi-pencil"></i></button>
                        <button class="icon-btn danger" data-action="delete-sender" data-id="${item.id}" title="删除通知发送方"><i class="bi bi-trash"></i></button>
                    </div>
                </td>
            </tr>
        `).join('');
        if (cards) {
            cards.innerHTML = items.map((item) => this.renderMobileDataCard({
                title: this.escapeHtml(item.sender_name || '--'),
                badgeHtml: `<span class="status-chip ${item.status === 'active' ? 'enabled' : 'disabled'}">${this.escapeHtml(item.status_label || '--')}</span>`,
                sections: [
                    { label: '渠道', content: this.escapeHtml(item.channel_label || '--') },
                    { label: 'Bot 名称', content: this.escapeHtml(item.bot_name || '--') },
                    { label: '默认', content: this.escapeHtml(item.is_default ? '是' : '否') }
                ],
                footer: `
                    <div class="predictor-actions">
                        <button class="icon-btn" data-action="edit-sender" data-id="${item.id}" title="编辑通知发送方"><i class="bi bi-pencil"></i></button>
                        <button class="icon-btn danger" data-action="delete-sender" data-id="${item.id}" title="删除通知发送方"><i class="bi bi-trash"></i></button>
                    </div>
                `
            })).join('');
        }
    }

    handleNotificationSenderTableClick(event) {
        const button = event.target.closest('[data-action]');
        if (!button) {
            return;
        }
        const senderId = Number(button.dataset.id);
        if (!senderId) {
            return;
        }
        if (button.dataset.action === 'edit-sender') {
            this.populateNotificationSenderForm(senderId);
            return;
        }
        if (button.dataset.action === 'delete-sender') {
            this.deleteNotificationSender(senderId);
        }
    }

    populateNotificationSenderForm(senderId) {
        const item = (this.notificationSenders || []).find((row) => row.id === senderId);
        if (!item) {
            return;
        }
        document.getElementById('notificationSenderId').value = String(item.id);
        document.getElementById('notificationSenderChannelType').value = item.channel_type || 'telegram';
        document.getElementById('notificationSenderName').value = item.sender_name || '';
        document.getElementById('notificationSenderBotName').value = item.bot_name || '';
        document.getElementById('notificationSenderBotToken').value = '';
        document.getElementById('notificationSenderStatus').value = item.status || 'active';
        document.getElementById('notificationSenderIsDefault').value = item.is_default ? 'true' : 'false';
    }

    async submitNotificationSender() {
        const senderId = document.getElementById('notificationSenderId').value;
        const payload = {
            channel_type: document.getElementById('notificationSenderChannelType').value || 'telegram',
            sender_name: document.getElementById('notificationSenderName').value.trim(),
            bot_name: document.getElementById('notificationSenderBotName').value.trim(),
            bot_token: document.getElementById('notificationSenderBotToken').value.trim(),
            status: document.getElementById('notificationSenderStatus').value || 'active',
            is_default: document.getElementById('notificationSenderIsDefault').value === 'true'
        };
        const url = senderId ? `/api/notification-senders/${senderId}` : '/api/notification-senders';
        const method = senderId ? 'PUT' : 'POST';
        try {
            const response = await fetch(url, {
                method,
                credentials: 'include',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });
            const data = await this.parseJsonSafely(response);
            if (!response.ok) {
                throw new Error(data.error || '保存通知发送方失败');
            }
            this.resetNotificationSenderForm();
            await this.loadUserSettings();
        } catch (error) {
            alert(error.message);
        }
    }

    async testNotificationSender() {
        const senderId = document.getElementById('notificationSenderId').value;
        const payload = {
            sender_id: senderId ? Number(senderId) : null,
            channel_type: document.getElementById('notificationSenderChannelType').value || 'telegram',
            sender_name: document.getElementById('notificationSenderName').value.trim(),
            bot_name: document.getElementById('notificationSenderBotName').value.trim(),
            bot_token: document.getElementById('notificationSenderBotToken').value.trim(),
            status: document.getElementById('notificationSenderStatus').value || 'active',
            is_default: document.getElementById('notificationSenderIsDefault').value === 'true',
            chat_id: document.getElementById('notificationSenderTestChatId').value.trim(),
            message: 'AITradingSimulator 用户自有机器人测试消息'
        };
        try {
            const response = await fetch('/api/notification-senders/test', {
                method: 'POST',
                credentials: 'include',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });
            const data = await this.parseJsonSafely(response);
            if (!response.ok) {
                throw new Error(data.error || '测试通知发送方失败');
            }
            alert(data.message || '测试消息发送成功');
        } catch (error) {
            alert(error.message);
        }
    }

    async deleteNotificationSender(senderId) {
        if (!window.confirm('确定删除这个通知发送方吗？相关订阅会回退到平台机器人。')) {
            return;
        }
        try {
            const response = await fetch(`/api/notification-senders/${senderId}`, {
                method: 'DELETE',
                credentials: 'include'
            });
            const data = await this.parseJsonSafely(response);
            if (!response.ok) {
                throw new Error(data.error || '删除通知发送方失败');
            }
            this.resetNotificationSenderForm();
            await this.loadUserSettings();
        } catch (error) {
            alert(error.message);
        }
    }

    resetNotificationEndpointForm() {
        if (!this.getElement('notificationEndpointId')) {
            return;
        }
        document.getElementById('notificationEndpointId').value = '';
        document.getElementById('notificationEndpointChannelType').value = 'telegram';
        document.getElementById('notificationEndpointKey').value = '';
        document.getElementById('notificationEndpointLabel').value = '';
        document.getElementById('notificationEndpointChatType').value = 'private';
        document.getElementById('notificationEndpointStatus').value = 'active';
        document.getElementById('notificationEndpointIsDefault').value = 'false';
    }

    renderNotificationEndpoints(items) {
        const tbody = this.getElement('notificationEndpointsBody');
        const cards = this.getElement('notificationEndpointsCards');
        if (!tbody) {
            return;
        }
        if (!items.length) {
            tbody.innerHTML = '<tr><td colspan="5" class="empty-cell">暂无通知接收端</td></tr>';
            if (cards) {
                cards.innerHTML = '<div class="empty-panel">暂无通知接收端</div>';
            }
            return;
        }
        tbody.innerHTML = items.map((item) => `
            <tr>
                <td>
                    <div class="result-stack">
                        <strong>${this.escapeHtml(item.endpoint_label || '--')}</strong>
                        <span class="result-meta-text">${item.is_default ? '默认接收端' : '普通接收端'}</span>
                    </div>
                </td>
                <td>${this.escapeHtml(item.channel_label || '--')}</td>
                <td>${this.escapeHtml(item.endpoint_key || '--')}</td>
                <td><span class="status-chip ${item.status === 'active' ? 'enabled' : 'disabled'}">${this.escapeHtml(item.status_label || '--')}</span></td>
                <td>
                    <div class="predictor-actions">
                        <button class="icon-btn" data-action="edit-endpoint" data-id="${item.id}" title="编辑通知接收端"><i class="bi bi-pencil"></i></button>
                        <button class="icon-btn danger" data-action="delete-endpoint" data-id="${item.id}" title="删除通知接收端"><i class="bi bi-trash"></i></button>
                    </div>
                </td>
            </tr>
        `).join('');
        if (cards) {
            cards.innerHTML = items.map((item) => this.renderMobileDataCard({
                title: this.escapeHtml(item.endpoint_label || '--'),
                badgeHtml: `<span class="status-chip ${item.status === 'active' ? 'enabled' : 'disabled'}">${this.escapeHtml(item.status_label || '--')}</span>`,
                sections: [
                    { label: '渠道', content: this.escapeHtml(item.channel_label || '--') },
                    { label: '标识', content: this.escapeHtml(item.endpoint_key || '--') },
                    { label: '类型', content: this.escapeHtml((item.config || {}).chat_type || '--') }
                ],
                footer: `
                    <div class="predictor-actions">
                        <button class="icon-btn" data-action="edit-endpoint" data-id="${item.id}" title="编辑通知接收端"><i class="bi bi-pencil"></i></button>
                        <button class="icon-btn danger" data-action="delete-endpoint" data-id="${item.id}" title="删除通知接收端"><i class="bi bi-trash"></i></button>
                    </div>
                `
            })).join('');
        }
    }

    handleNotificationEndpointTableClick(event) {
        const button = event.target.closest('[data-action]');
        if (!button) {
            return;
        }
        const endpointId = Number(button.dataset.id);
        if (!endpointId) {
            return;
        }
        if (button.dataset.action === 'edit-endpoint') {
            this.populateNotificationEndpointForm(endpointId);
            return;
        }
        if (button.dataset.action === 'delete-endpoint') {
            this.deleteNotificationEndpoint(endpointId);
        }
    }

    populateNotificationEndpointForm(endpointId) {
        const item = (this.notificationEndpoints || []).find((row) => row.id === endpointId);
        if (!item) {
            return;
        }
        document.getElementById('notificationEndpointId').value = String(item.id);
        document.getElementById('notificationEndpointChannelType').value = item.channel_type || 'telegram';
        document.getElementById('notificationEndpointKey').value = item.endpoint_key || '';
        document.getElementById('notificationEndpointLabel').value = item.endpoint_label || '';
        document.getElementById('notificationEndpointChatType').value = (item.config || {}).chat_type || 'private';
        document.getElementById('notificationEndpointStatus').value = item.status || 'active';
        document.getElementById('notificationEndpointIsDefault').value = item.is_default ? 'true' : 'false';
    }

    async submitNotificationEndpoint() {
        const endpointId = document.getElementById('notificationEndpointId').value;
        const payload = {
            channel_type: document.getElementById('notificationEndpointChannelType').value || 'telegram',
            endpoint_key: document.getElementById('notificationEndpointKey').value.trim(),
            endpoint_label: document.getElementById('notificationEndpointLabel').value.trim(),
            config: {
                chat_type: document.getElementById('notificationEndpointChatType').value || 'private'
            },
            status: document.getElementById('notificationEndpointStatus').value || 'active',
            is_default: document.getElementById('notificationEndpointIsDefault').value === 'true'
        };
        const url = endpointId ? `/api/notification-endpoints/${endpointId}` : '/api/notification-endpoints';
        const method = endpointId ? 'PUT' : 'POST';
        try {
            const response = await fetch(url, {
                method,
                credentials: 'include',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });
            const data = await this.parseJsonSafely(response);
            if (!response.ok) {
                throw new Error(data.error || '保存通知接收端失败');
            }
            this.resetNotificationEndpointForm();
            await this.loadUserSettings();
        } catch (error) {
            alert(error.message);
        }
    }

    async testNotificationEndpoint() {
        const payload = {
            channel_type: document.getElementById('notificationEndpointChannelType').value || 'telegram',
            endpoint_key: document.getElementById('notificationEndpointKey').value.trim(),
            endpoint_label: document.getElementById('notificationEndpointLabel').value.trim(),
            config: {
                chat_type: document.getElementById('notificationEndpointChatType').value || 'private'
            },
            status: document.getElementById('notificationEndpointStatus').value || 'active',
            is_default: document.getElementById('notificationEndpointIsDefault').value === 'true',
            message: 'AITradingSimulator 用户侧接收端测试消息'
        };
        try {
            const response = await fetch('/api/notification-endpoints/test', {
                method: 'POST',
                credentials: 'include',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });
            const data = await this.parseJsonSafely(response);
            if (!response.ok) {
                throw new Error(data.error || '测试通知接收端失败');
            }
            alert(data.message || '测试消息发送成功');
        } catch (error) {
            alert(error.message);
        }
    }

    async deleteNotificationEndpoint(endpointId) {
        if (!window.confirm('确定删除这个通知接收端吗？关联订阅和投递记录会同时清理。')) {
            return;
        }
        try {
            const response = await fetch(`/api/notification-endpoints/${endpointId}`, {
                method: 'DELETE',
                credentials: 'include'
            });
            const data = await this.parseJsonSafely(response);
            if (!response.ok) {
                throw new Error(data.error || '删除通知接收端失败');
            }
            this.resetNotificationEndpointForm();
            await this.loadUserSettings();
        } catch (error) {
            alert(error.message);
        }
    }

    renderSubscriptionPredictorOptions() {
        const select = this.getElement('notificationSubscriptionPredictorId');
        if (!select) {
            return;
        }
        const currentValue = select.value;
        if (this.isDashboardPage()) {
            const currentPredictor = (this.predictors || []).find((item) => String(item.id) === String(this.currentPredictorId)) || this.currentPredictor;
            if (!currentPredictor) {
                select.innerHTML = '<option value="">暂无预测方案</option>';
                select.disabled = true;
                return;
            }
            select.disabled = true;
            select.innerHTML = `<option value="${currentPredictor.id}">${this.escapeHtml(currentPredictor.name)} · ${this.escapeHtml(currentPredictor.lottery_label || '--')}</option>`;
            select.value = String(currentPredictor.id);
            return;
        }
        if (!(this.predictors || []).length) {
            select.innerHTML = '<option value="">暂无预测方案</option>';
            select.disabled = true;
            return;
        }
        select.disabled = false;
        select.innerHTML = (this.predictors || []).map((item) => `
            <option value="${item.id}">${this.escapeHtml(item.name)} · ${this.escapeHtml(item.lottery_label || '--')}</option>
        `).join('');
        const nextValue = currentValue && this.predictors.some((item) => String(item.id) === String(currentValue))
            ? currentValue
            : String(this.currentPredictorId || this.predictors[0].id);
        select.value = nextValue;
    }

    renderNotificationEndpointOptions() {
        const select = this.getElement('notificationSubscriptionEndpointId');
        if (!select) {
            return;
        }
        const currentValue = select.value;
        if (!(this.notificationEndpoints || []).length) {
            select.innerHTML = '<option value="">暂无通知接收端</option>';
            select.disabled = true;
            return;
        }
        select.disabled = false;
        select.innerHTML = (this.notificationEndpoints || []).map((item) => `
            <option value="${item.id}">${this.escapeHtml(item.endpoint_label || item.endpoint_key || '--')} · ${this.escapeHtml(item.channel_label || '--')}</option>
        `).join('');
        const nextValue = currentValue && this.notificationEndpoints.some((item) => String(item.id) === String(currentValue))
            ? currentValue
            : String(this.notificationEndpoints[0].id);
        select.value = nextValue;
    }

    renderNotificationSenderOptions() {
        const select = this.getElement('notificationSubscriptionSenderAccountId');
        if (!select) {
            return;
        }
        const currentValue = select.value;
        const items = this.getCurrentSenderOptions();
        select.innerHTML = ['<option value="">请选择我的机器人</option>'].concat(
            items.map((item) => `<option value="${item.id}">${this.escapeHtml(item.sender_name || '--')} · ${this.escapeHtml(item.bot_name || '--')}</option>`)
        ).join('');
        if (currentValue && items.some((item) => String(item.id) === String(currentValue))) {
            select.value = currentValue;
        } else {
            const defaultItem = items.find((item) => item.is_default) || null;
            select.value = defaultItem ? String(defaultItem.id) : '';
        }
    }

    getCurrentSenderOptions() {
        return (this.notificationSenders || []).filter((item) => (item.channel_type || 'telegram') === 'telegram' && item.status === 'active');
    }

    resetNotificationSubscriptionForm() {
        if (!this.getElement('notificationSubscriptionId')) {
            return;
        }
        document.getElementById('notificationSubscriptionId').value = '';
        this.renderSubscriptionPredictorOptions();
        this.renderNotificationSubscriptionEventOptions();
        document.getElementById('notificationSubscriptionSenderMode').value = 'platform';
        this.renderNotificationSenderOptions();
        this.renderNotificationEndpointOptions();
        document.getElementById('notificationSubscriptionEventType').value = 'prediction_created';
        document.getElementById('notificationSubscriptionDeliveryMode').value = 'notify_only';
        document.getElementById('notificationSubscriptionConfidenceGte').value = '';
        document.getElementById('notificationPerformanceMetric').value = 'big_small';
        document.getElementById('notificationPerformanceWindow').value = '100';
        document.getElementById('notificationPerformanceOperator').value = 'lt';
        document.getElementById('notificationPerformanceThreshold').value = '40';
        document.getElementById('notificationPerformanceMinSample').value = '100';
        document.getElementById('notificationPerformanceInvalidateMissing').value = '3';
        document.getElementById('notificationPerformanceCooldownIssues').value = '20';
        document.getElementById('notificationSubscriptionEnabled').value = 'true';
        this.syncSubscriptionBetProfileOptions();
        this.syncSubscriptionBetProfileState();
        this.syncSubscriptionSenderState();
        this.syncNotificationSubscriptionEventState();
    }

    syncSubscriptionBetProfileOptions() {
        const predictorSelect = this.getElement('notificationSubscriptionPredictorId');
        const select = this.getElement('notificationSubscriptionBetProfileId');
        if (!predictorSelect || !select) {
            return;
        }
        const predictorId = Number(predictorSelect.value || 0);
        const predictor = (this.predictors || []).find((item) => item.id === predictorId) || this.currentPredictor;
        const lotteryType = predictor?.lottery_type || this.currentLotteryType || 'pc28';
        const currentValue = select.value;
        const items = (this.betProfiles || []).filter((item) => item.lottery_type === lotteryType);
        select.innerHTML = ['<option value="">不绑定下注策略</option>'].concat(
            items.map((item) => `<option value="${item.id}">${this.escapeHtml(item.name)} · ${this.escapeHtml(item.strategy_label || '--')}</option>`)
        ).join('');
        if (currentValue && items.some((item) => String(item.id) === String(currentValue))) {
            select.value = currentValue;
        } else {
            const defaultItem = items.find((item) => item.is_default) || null;
            select.value = defaultItem ? String(defaultItem.id) : '';
        }
        this.syncSubscriptionBetProfileState();
    }

    getSelectedSubscriptionPredictor() {
        const predictorSelect = this.getElement('notificationSubscriptionPredictorId');
        const predictorId = Number(predictorSelect?.value || 0);
        return (this.predictors || []).find((item) => item.id === predictorId) || this.currentPredictor || null;
    }

    renderNotificationSubscriptionEventOptions(preferredValue = null) {
        const select = this.getElement('notificationSubscriptionEventType');
        if (!select) {
            return;
        }
        const predictor = this.getSelectedSubscriptionPredictor();
        const lotteryType = predictor?.lottery_type || this.currentLotteryType || 'pc28';
        const currentValue = preferredValue || select.value || 'prediction_created';
        const options = [
            { value: 'prediction_created', label: '预测生成' }
        ];
        if (lotteryType === 'pc28') {
            options.push({ value: 'performance_threshold', label: '表现告警' });
        }
        select.innerHTML = options.map((item) => `<option value="${item.value}">${this.escapeHtml(item.label)}</option>`).join('');
        const nextValue = options.some((item) => item.value === currentValue) ? currentValue : 'prediction_created';
        select.value = nextValue;
        this.syncNotificationSubscriptionEventState();
    }

    syncSubscriptionBetProfileState() {
        const modeInput = this.getElement('notificationSubscriptionDeliveryMode');
        const betProfileSelect = this.getElement('notificationSubscriptionBetProfileId');
        if (!modeInput || !betProfileSelect) {
            return;
        }
        const mode = modeInput.value || 'notify_only';
        betProfileSelect.disabled = mode !== 'follow_bet';
        if (mode !== 'follow_bet') {
            betProfileSelect.value = '';
        } else if (!betProfileSelect.value) {
            const firstAvailable = Array.from(betProfileSelect.options).find((item) => item.value);
            if (firstAvailable) {
                betProfileSelect.value = firstAvailable.value;
            }
        }
    }

    syncNotificationSubscriptionEventState() {
        const eventInput = this.getElement('notificationSubscriptionEventType');
        const deliveryModeInput = this.getElement('notificationSubscriptionDeliveryMode');
        const confidenceField = this.getElement('notificationPredictionConfidenceField');
        const confidenceInput = this.getElement('notificationSubscriptionConfidenceGte');
        const performancePanel = this.getElement('notificationPerformanceRulePanel');
        const betProfileSelect = this.getElement('notificationSubscriptionBetProfileId');
        if (!eventInput || !deliveryModeInput || !confidenceField || !confidenceInput || !performancePanel) {
            return;
        }

        const isPerformanceEvent = (eventInput.value || 'prediction_created') === 'performance_threshold';
        confidenceField.hidden = isPerformanceEvent;
        performancePanel.hidden = !isPerformanceEvent;

        if (isPerformanceEvent) {
            confidenceInput.value = '';
            deliveryModeInput.value = 'notify_only';
            deliveryModeInput.disabled = true;
            if (betProfileSelect) {
                betProfileSelect.value = '';
            }
        } else {
            deliveryModeInput.disabled = false;
        }
        this.syncSubscriptionBetProfileState();
    }

    summarizePerformanceRule(rule) {
        if (!rule) {
            return '--';
        }
        const metricLabel = this.targetLabel(rule.metric || 'big_small', 'pc28');
        const windowSize = Number(rule?.window?.size || 0) || '--';
        const operatorLabelMap = {
            lt: '低于',
            lte: '低于或等于',
            gt: '高于',
            gte: '高于或等于',
            eq: '等于'
        };
        const operator = operatorLabelMap[rule?.trigger?.operator || 'lt'] || (rule?.trigger?.operator || '--');
        const threshold = rule?.trigger?.value ?? '--';
        const invalidateMissingGte = rule?.validity?.invalidate_missing_gte ?? '--';
        const cooldownIssues = rule?.cooldown?.issues ?? '--';
        return `${metricLabel} 最近${windowSize}期 ${operator} ${threshold}% · 断档≥${invalidateMissingGte}期无效 · 冷却${cooldownIssues}期`;
    }

    getPrimaryPerformanceRule(item) {
        const rules = Array.isArray(item?.filter?.rules) ? item.filter.rules : [];
        return rules[0] || null;
    }

    syncSubscriptionSenderState() {
        const modeInput = this.getElement('notificationSubscriptionSenderMode');
        const senderSelect = this.getElement('notificationSubscriptionSenderAccountId');
        if (!modeInput || !senderSelect) {
            return;
        }
        const mode = modeInput.value || 'platform';
        senderSelect.disabled = mode !== 'user_sender';
        if (mode !== 'user_sender') {
            senderSelect.value = '';
            return;
        }
        if (!senderSelect.value) {
            const firstAvailable = Array.from(senderSelect.options).find((item) => item.value);
            if (firstAvailable) {
                senderSelect.value = firstAvailable.value;
            }
        }
    }

    getVisibleNotificationSubscriptions(items = this.notificationSubscriptions || []) {
        if (!this.isDashboardPage()) {
            return [...(items || [])];
        }
        if (!this.currentPredictorId) {
            return [];
        }
        return (items || []).filter((item) => String(item.predictor_id) === String(this.currentPredictorId));
    }

    renderCurrentPredictorSubscriptionSummary(items = this.getVisibleNotificationSubscriptions()) {
        const totalElement = this.getElement('currentPredictorSubscriptionCount');
        const notifyOnlyElement = this.getElement('currentPredictorNotifyOnlyCount');
        const followBetElement = this.getElement('currentPredictorFollowBetCount');
        const enabledElement = this.getElement('currentPredictorEnabledSubscriptionCount');
        const container = this.getElement('currentPredictorSubscriptionSummary');
        if (!container) {
            return;
        }

        const notifyOnlyCount = items.filter((item) => item.delivery_mode === 'notify_only').length;
        const followBetCount = items.filter((item) => item.delivery_mode === 'follow_bet').length;
        const enabledCount = items.filter((item) => item.enabled).length;
        if (totalElement) totalElement.textContent = String(items.length);
        if (notifyOnlyElement) notifyOnlyElement.textContent = String(notifyOnlyCount);
        if (followBetElement) followBetElement.textContent = String(followBetCount);
        if (enabledElement) enabledElement.textContent = String(enabledCount);

        if (!this.currentPredictorId) {
            container.className = 'prediction-summary empty-panel';
            container.textContent = '请先选择一个预测方案，再配置它的通知订阅。';
            return;
        }

        if (!items.length) {
            container.className = 'prediction-summary empty-panel';
            container.textContent = '当前方案暂无通知订阅。';
            return;
        }

        container.className = 'prediction-summary';
        container.innerHTML = items.map((item) => `
            <article class="prediction-card">
                <div class="prediction-section-head">
                    <strong>${this.escapeHtml(item.endpoint_label || '--')}</strong>
                    <span class="status-chip ${item.enabled ? 'enabled' : 'disabled'}">${item.enabled ? '启用' : '停用'}</span>
                </div>
                <div class="detail-list">
                    <div class="detail-row">
                        <span class="detail-label">发送方</span>
                        <div>${this.escapeHtml(item.sender_mode === 'user_sender' ? (item.sender_account_name || '--') : '平台机器人')}</div>
                    </div>
                    <div class="detail-row">
                        <span class="detail-label">事件</span>
                        <div>${this.escapeHtml(item.event_label || '--')} · ${this.escapeHtml(item.delivery_mode_label || '--')}</div>
                    </div>
                    <div class="detail-row">
                        <span class="detail-label">规则</span>
                        <div>${this.escapeHtml(
                            item.event_type === 'performance_threshold'
                                ? this.summarizePerformanceRule(this.getPrimaryPerformanceRule(item))
                                : (item.bet_profile_name || '未绑定')
                        )}</div>
                    </div>
                    <div class="detail-row">
                        <span class="detail-label">过滤条件</span>
                        <div>${this.escapeHtml(
                            item.event_type === 'performance_threshold'
                                ? `当前规则数 ${(item.filter?.rules || []).length || 0}`
                                : String((item.filter || {}).confidence_gte ?? '未设置')
                        )}</div>
                    </div>
                </div>
            </article>
        `).join('');
    }

    renderNotificationSubscriptions(items) {
        const tbody = this.getElement('notificationSubscriptionsBody');
        const cards = this.getElement('notificationSubscriptionsCards');
        if (!tbody) {
            return;
        }
        const visibleItems = this.getVisibleNotificationSubscriptions(items);
        this.renderCurrentPredictorSubscriptionSummary(visibleItems);
        if (!visibleItems.length) {
            tbody.innerHTML = '<tr><td colspan="7" class="empty-cell">暂无通知订阅</td></tr>';
            if (cards) {
                cards.innerHTML = '<div class="empty-panel">暂无通知订阅</div>';
            }
            return;
        }
        tbody.innerHTML = visibleItems.map((item) => `
            <tr>
                <td>
                    <div class="result-stack">
                        <strong>${this.escapeHtml(item.predictor_name || '--')}</strong>
                        <span class="result-meta-text">${this.escapeHtml(item.lottery_label || '--')}</span>
                    </div>
                </td>
                <td>${this.escapeHtml(item.endpoint_label || '--')}</td>
                <td>${this.escapeHtml(item.sender_mode === 'user_sender' ? (item.sender_account_name || '--') : '平台机器人')}</td>
                <td>${this.escapeHtml(`${item.event_label || '--'} / ${item.delivery_mode_label || '--'}`)}</td>
                <td>${this.escapeHtml(
                    item.event_type === 'performance_threshold'
                        ? this.summarizePerformanceRule(this.getPrimaryPerformanceRule(item))
                        : (item.bet_profile_name || '--')
                )}</td>
                <td><span class="status-chip ${item.enabled ? 'enabled' : 'disabled'}">${item.enabled ? '启用' : '停用'}</span></td>
                <td>
                    <div class="predictor-actions">
                        <button class="icon-btn" data-action="edit-subscription" data-id="${item.id}" title="编辑通知订阅"><i class="bi bi-pencil"></i></button>
                        <button class="icon-btn danger" data-action="delete-subscription" data-id="${item.id}" title="删除通知订阅"><i class="bi bi-trash"></i></button>
                    </div>
                </td>
            </tr>
        `).join('');
        if (cards) {
            cards.innerHTML = visibleItems.map((item) => this.renderMobileDataCard({
                title: this.escapeHtml(item.predictor_name || '--'),
                badgeHtml: `<span class="status-chip ${item.enabled ? 'enabled' : 'disabled'}">${item.enabled ? '启用' : '停用'}</span>`,
                sections: [
                    { label: '接收端', content: this.escapeHtml(item.endpoint_label || '--') },
                    { label: '发送方', content: this.escapeHtml(item.sender_mode === 'user_sender' ? (item.sender_account_name || '--') : '平台机器人') },
                    { label: '事件/模式', content: this.escapeHtml(`${item.event_label || '--'} / ${item.delivery_mode_label || '--'}`) },
                    {
                        label: item.event_type === 'performance_threshold' ? '规则' : '下注策略',
                        content: this.escapeHtml(
                            item.event_type === 'performance_threshold'
                                ? this.summarizePerformanceRule(this.getPrimaryPerformanceRule(item))
                                : (item.bet_profile_name || '--')
                        )
                    },
                    {
                        label: item.event_type === 'performance_threshold' ? '规则数' : '最低置信度',
                        content: this.escapeHtml(
                            item.event_type === 'performance_threshold'
                                ? String((item.filter?.rules || []).length || 0)
                                : String((item.filter || {}).confidence_gte ?? '--')
                        )
                    }
                ],
                footer: `
                    <div class="predictor-actions">
                        <button class="icon-btn" data-action="edit-subscription" data-id="${item.id}" title="编辑通知订阅"><i class="bi bi-pencil"></i></button>
                        <button class="icon-btn danger" data-action="delete-subscription" data-id="${item.id}" title="删除通知订阅"><i class="bi bi-trash"></i></button>
                    </div>
                `
            })).join('');
        }
    }

    handleNotificationSubscriptionTableClick(event) {
        const button = event.target.closest('[data-action]');
        if (!button) {
            return;
        }
        const subscriptionId = Number(button.dataset.id);
        if (!subscriptionId) {
            return;
        }
        if (button.dataset.action === 'edit-subscription') {
            this.populateNotificationSubscriptionForm(subscriptionId);
            return;
        }
        if (button.dataset.action === 'delete-subscription') {
            this.deleteNotificationSubscription(subscriptionId);
        }
    }

    populateNotificationSubscriptionForm(subscriptionId) {
        const item = (this.notificationSubscriptions || []).find((row) => row.id === subscriptionId);
        if (!item) {
            return;
        }
        document.getElementById('notificationSubscriptionId').value = String(item.id);
        this.renderSubscriptionPredictorOptions();
        document.getElementById('notificationSubscriptionPredictorId').value = String(item.predictor_id);
        this.renderNotificationSubscriptionEventOptions(item.event_type || 'prediction_created');
        document.getElementById('notificationSubscriptionSenderMode').value = item.sender_mode || 'platform';
        this.renderNotificationSenderOptions();
        document.getElementById('notificationSubscriptionSenderAccountId').value = item.sender_account_id ? String(item.sender_account_id) : '';
        this.renderNotificationEndpointOptions();
        document.getElementById('notificationSubscriptionEndpointId').value = String(item.endpoint_id);
        document.getElementById('notificationSubscriptionEventType').value = item.event_type || 'prediction_created';
        this.syncNotificationSubscriptionEventState();
        document.getElementById('notificationSubscriptionDeliveryMode').value = item.delivery_mode || 'notify_only';
        document.getElementById('notificationSubscriptionConfidenceGte').value = (item.filter || {}).confidence_gte ?? '';
        const primaryRule = this.getPrimaryPerformanceRule(item);
        document.getElementById('notificationPerformanceMetric').value = primaryRule?.metric || 'big_small';
        document.getElementById('notificationPerformanceWindow').value = String(primaryRule?.window?.size ?? 100);
        document.getElementById('notificationPerformanceOperator').value = primaryRule?.trigger?.operator || 'lt';
        document.getElementById('notificationPerformanceThreshold').value = String(primaryRule?.trigger?.value ?? 40);
        document.getElementById('notificationPerformanceMinSample').value = String(primaryRule?.validity?.min_sample_count ?? 100);
        document.getElementById('notificationPerformanceInvalidateMissing').value = String(primaryRule?.validity?.invalidate_missing_gte ?? 3);
        document.getElementById('notificationPerformanceCooldownIssues').value = String(primaryRule?.cooldown?.issues ?? 20);
        document.getElementById('notificationSubscriptionEnabled').value = item.enabled ? 'true' : 'false';
        this.syncSubscriptionBetProfileOptions();
        document.getElementById('notificationSubscriptionBetProfileId').value = item.bet_profile_id ? String(item.bet_profile_id) : '';
        this.syncSubscriptionBetProfileState();
        this.syncSubscriptionSenderState();
    }

    async submitNotificationSubscription() {
        const subscriptionId = document.getElementById('notificationSubscriptionId').value;
        const confidenceGteText = document.getElementById('notificationSubscriptionConfidenceGte').value.trim();
        const eventType = document.getElementById('notificationSubscriptionEventType').value || 'prediction_created';
        const payload = {
            predictor_id: Number(document.getElementById('notificationSubscriptionPredictorId').value || 0),
            endpoint_id: Number(document.getElementById('notificationSubscriptionEndpointId').value || 0),
            sender_mode: document.getElementById('notificationSubscriptionSenderMode').value || 'platform',
            sender_account_id: document.getElementById('notificationSubscriptionSenderAccountId').value
                ? Number(document.getElementById('notificationSubscriptionSenderAccountId').value)
                : null,
            bet_profile_id: document.getElementById('notificationSubscriptionBetProfileId').value
                ? Number(document.getElementById('notificationSubscriptionBetProfileId').value)
                : null,
            event_type: eventType,
            delivery_mode: document.getElementById('notificationSubscriptionDeliveryMode').value || 'notify_only',
            enabled: document.getElementById('notificationSubscriptionEnabled').value === 'true',
            filter: {}
        };
        if (eventType === 'performance_threshold') {
            payload.filter.rules = [{
                metric: document.getElementById('notificationPerformanceMetric').value || 'big_small',
                window: {
                    type: 'settled_metric_samples',
                    size: Number(document.getElementById('notificationPerformanceWindow').value || 100)
                },
                validity: {
                    min_sample_count: Number(document.getElementById('notificationPerformanceMinSample').value || 100),
                    invalidate_missing_gte: Number(document.getElementById('notificationPerformanceInvalidateMissing').value || 3),
                    require_numeric_issue: true
                },
                trigger: {
                    field: 'hit_rate',
                    operator: document.getElementById('notificationPerformanceOperator').value || 'lt',
                    value: Number(document.getElementById('notificationPerformanceThreshold').value || 40)
                },
                cooldown: {
                    issues: Number(document.getElementById('notificationPerformanceCooldownIssues').value || 20)
                }
            }];
        } else if (confidenceGteText !== '') {
            payload.filter.confidence_gte = Number(confidenceGteText);
        }
        const url = subscriptionId ? `/api/notification-subscriptions/${subscriptionId}` : '/api/notification-subscriptions';
        const method = subscriptionId ? 'PUT' : 'POST';
        try {
            const response = await fetch(url, {
                method,
                credentials: 'include',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });
            const data = await this.parseJsonSafely(response);
            if (!response.ok) {
                throw new Error(data.error || '保存通知订阅失败');
            }
            this.resetNotificationSubscriptionForm();
            await this.loadUserSettings();
        } catch (error) {
            alert(error.message);
        }
    }

    async deleteNotificationSubscription(subscriptionId) {
        if (!window.confirm('确定删除这个通知订阅吗？')) {
            return;
        }
        try {
            const response = await fetch(`/api/notification-subscriptions/${subscriptionId}`, {
                method: 'DELETE',
                credentials: 'include'
            });
            const data = await this.parseJsonSafely(response);
            if (!response.ok) {
                throw new Error(data.error || '删除通知订阅失败');
            }
            this.resetNotificationSubscriptionForm();
            await this.loadUserSettings();
        } catch (error) {
            alert(error.message);
        }
    }

    renderNotificationDeliveries(items) {
        const tbody = this.getElement('notificationDeliveriesBody');
        const cards = this.getElement('notificationDeliveriesCards');
        if (!tbody) {
            return;
        }
        if (!items.length) {
            tbody.innerHTML = '<tr><td colspan="6" class="empty-cell">暂无通知投递记录</td></tr>';
            if (cards) {
                cards.innerHTML = '<div class="empty-panel">暂无通知投递记录</div>';
            }
            return;
        }
        tbody.innerHTML = items.map((item) => `
            <tr>
                <td>${this.escapeHtml(item.predictor_name || '--')}</td>
                <td>${this.escapeHtml(item.endpoint_label || '--')}</td>
                <td>${this.escapeHtml(item.event_label || '--')}</td>
                <td>${this.escapeHtml(item.record_key || '--')}</td>
                <td><span class="status-chip ${item.status === 'delivered' ? 'enabled' : item.status === 'failed' ? 'failed' : 'pending'}">${this.escapeHtml(item.status_label || '--')}</span></td>
                <td>
                    <div class="result-stack">
                        <span>${this.escapeHtml(item.sent_at || item.created_at || '--')}</span>
                        ${item.error_message ? `<span class="result-meta-text">${this.escapeHtml(item.error_message)}</span>` : ''}
                        ${item.can_retry ? `<div class="predictor-actions"><button class="btn ghost compact" data-action="retry-delivery" data-id="${item.id}">重发</button></div>` : ''}
                    </div>
                </td>
            </tr>
        `).join('');
        if (cards) {
            cards.innerHTML = items.map((item) => this.renderMobileDataCard({
                title: this.escapeHtml(item.predictor_name || '--'),
                badgeHtml: `<span class="status-chip ${item.status === 'delivered' ? 'enabled' : item.status === 'failed' ? 'failed' : 'pending'}">${this.escapeHtml(item.status_label || '--')}</span>`,
                sections: [
                    { label: '接收端', content: this.escapeHtml(item.endpoint_label || '--') },
                    { label: '事件', content: this.escapeHtml(item.event_label || '--') },
                    { label: '记录键', content: this.escapeHtml(item.record_key || '--') },
                    { label: '时间', content: this.escapeHtml(item.sent_at || item.created_at || '--') },
                    { label: '错误', content: this.escapeHtml(item.error_message || '--') }
                ],
                footer: item.can_retry
                    ? `<div class="predictor-actions"><button class="btn ghost compact" data-action="retry-delivery" data-id="${item.id}">重发</button></div>`
                    : ''
            })).join('');
        }
    }

    handleNotificationDeliveryTableClick(event) {
        const button = event.target.closest('[data-action="retry-delivery"]');
        if (!button) {
            return;
        }
        const deliveryId = Number(button.dataset.id);
        if (!deliveryId) {
            return;
        }
        this.retryNotificationDelivery(deliveryId);
    }

    async retryNotificationDelivery(deliveryId) {
        try {
            const response = await fetch(`/api/notification-deliveries/${deliveryId}/retry`, {
                method: 'POST',
                credentials: 'include'
            });
            const data = await this.parseJsonSafely(response);
            if (!response.ok) {
                throw new Error(data.error || '通知重发失败');
            }
            await this.loadUserSettings();
        } catch (error) {
            alert(error.message);
        }
    }

    predictorStyleFilterKey(predictor) {
        if (!predictor) {
            return '';
        }
        if (predictor.engine_type === 'machine') {
            return `machine:${predictor.algorithm_key || predictor.execution_label || predictor.id}`;
        }
        return `ai:${(predictor.prediction_method || predictor.execution_label || predictor.id || '').trim()}`;
    }

    predictorStyleFilterLabel(predictor) {
        if (!predictor) {
            return '--';
        }
        if (predictor.engine_type === 'machine') {
            return `机器 / ${predictor.algorithm_label || predictor.execution_label || '--'}`;
        }
        return `AI / ${predictor.prediction_method || predictor.execution_label || '--'}`;
    }

    syncPredictorLotteryFilterOptions() {
        const select = this.getElement('predictorLotteryFilter');
        if (!select) {
            return;
        }

        const optionMap = new Map();
        (this.predictors || []).forEach((predictor) => {
            const key = predictor.lottery_type || 'pc28';
            if (!optionMap.has(key)) {
                optionMap.set(key, predictor.lottery_label || this.getLotteryConfig(key).label || key);
            }
        });

        const options = Array.from(optionMap.entries());
        select.innerHTML = [
            '<option value="all">全部彩种</option>',
            ...options.map(([value, label]) => `<option value="${this.escapeHtml(value)}">${this.escapeHtml(label)}</option>`)
        ].join('');
        if (!options.some(([value]) => value === this.selectedPredictorLotteryFilter)) {
            this.selectedPredictorLotteryFilter = 'all';
        }
        select.value = this.selectedPredictorLotteryFilter;
    }

    predictorMatchesSidebarFilters(predictor) {
        const lotteryMatches = this.selectedPredictorLotteryFilter === 'all'
            || (predictor.lottery_type || 'pc28') === this.selectedPredictorLotteryFilter;
        const engineMatches = this.selectedPredictorEngineFilter === 'all'
            || (predictor.engine_type || 'ai') === this.selectedPredictorEngineFilter;
        return lotteryMatches && engineMatches;
    }

    syncPredictorStyleFilterOptions() {
        const select = this.getElement('predictorStyleFilter');
        this.syncPredictorLotteryFilterOptions();
        const engineSelect = this.getElement('predictorEngineFilter');
        if (!select) {
            return;
        }

        const predictors = (this.predictors || []).filter((predictor) => this.predictorMatchesSidebarFilters(predictor));
        const optionMap = new Map();
        predictors.forEach((predictor) => {
            const key = this.predictorStyleFilterKey(predictor);
            if (!key || optionMap.has(key)) {
                return;
            }
            optionMap.set(key, this.predictorStyleFilterLabel(predictor));
        });

        const options = Array.from(optionMap.entries());
        select.innerHTML = [
            '<option value="all">全部风格</option>',
            ...options.map(([value, label]) => `<option value="${this.escapeHtml(value)}">${this.escapeHtml(label)}</option>`)
        ].join('');
        if (!options.some(([value]) => value === this.selectedPredictorStyleFilter)) {
            this.selectedPredictorStyleFilter = 'all';
        }
        select.value = this.selectedPredictorStyleFilter;
        if (engineSelect) {
            engineSelect.value = this.selectedPredictorEngineFilter;
        }
    }

    getFilteredPredictors() {
        return (this.predictors || []).filter((predictor) => {
            if (!this.predictorMatchesSidebarFilters(predictor)) {
                return false;
            }
            if (this.selectedPredictorStyleFilter === 'all') {
                return true;
            }
            return this.predictorStyleFilterKey(predictor) === this.selectedPredictorStyleFilter;
        });
    }

    renderPredictorFilterSummary(visiblePredictors) {
        const summary = this.getElement('predictorFilterSummary');
        if (!summary) {
            return;
        }
        const total = (this.predictors || []).length;
        const visibleCount = (visiblePredictors || []).length;
        if (!total) {
            summary.textContent = '当前没有可用方案';
            return;
        }
        let text = `显示 ${visibleCount} / ${total} 个方案`;
        const isCurrentHidden = Boolean(this.currentPredictorId)
            && !(visiblePredictors || []).some((item) => item.id === this.currentPredictorId);
        if (isCurrentHidden) {
            text += '，当前已选方案被筛选隐藏';
        }
        summary.textContent = text;
    }

    renderPredictorList(predictors) {
        const container = document.getElementById('predictorList');
        this.renderPredictorFilterSummary(predictors);
        if (!predictors.length) {
            const hasAnyPredictor = Boolean((this.predictors || []).length);
            container.innerHTML = hasAnyPredictor
                ? '<div class="empty-panel">当前筛选条件下暂无预测方案</div>'
                : '<div class="empty-panel">暂无预测方案</div>';
            return;
        }

        container.innerHTML = predictors.map((predictor) => {
            const status = this.predictorStatusMeta(predictor);
            const styleDescription = this.predictorStyleDescription(predictor);
            const engineBadge = `<span class="tag">${this.escapeHtml(this.predictorEngineLabel(predictor))}</span>`;
            const executionBadge = predictor.engine_type === 'machine'
                ? `<span class="tag">${this.escapeHtml(this.predictorExecutionLabel(predictor))}</span>`
                : '';
            return `
                <div class="predictor-item ${predictor.id === this.currentPredictorId ? 'active' : ''}" data-id="${predictor.id}">
                    <div class="predictor-head">
                        <div>
                            <div class="predictor-name">${this.escapeHtml(predictor.name)}</div>
                            <div class="predictor-meta">${this.escapeHtml(this.predictorMetaLabel(predictor))}</div>
                        </div>
                        <span class="status-chip ${status.className}">${this.escapeHtml(status.label)}</span>
                    </div>
                    ${predictor.auto_paused ? `<div class="hint-text">${this.escapeHtml(`AI 连续失败 ${predictor.consecutive_ai_failures || 0} 次，已自动暂停`)}</div>` : ''}
                    ${styleDescription ? `<div class="hint-text">${this.escapeHtml(styleDescription)}</div>` : ''}
                    <div class="predictor-tags">
                        ${engineBadge}
                        ${executionBadge}
                        ${(predictor.prediction_targets || []).map((target) => `<span class="tag">${this.escapeHtml(this.targetLabel(target, predictor.lottery_type || 'pc28'))}</span>`).join('')}
                    </div>
                    <div class="predictor-actions">
                        <button class="icon-btn" data-action="toggle" data-id="${predictor.id}" title="${this.escapeHtml(status.actionTitle)}">
                            <i class="bi ${status.iconClass}"></i>
                        </button>
                        <button class="icon-btn" data-action="edit" data-id="${predictor.id}" title="编辑方案"><i class="bi bi-pencil"></i></button>
                        <button class="icon-btn danger" data-action="delete" data-id="${predictor.id}" title="删除方案"><i class="bi bi-trash"></i></button>
                    </div>
                </div>
            `;
        }).join('');

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
                if (predictor.auto_paused) {
                    this.resumeAutoPausedPredictor(predictorId);
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
        this.cancelPendingProfitSimulation();
        this.currentPredictorId = predictorId;
        this.renderPredictorList(this.getFilteredPredictors());
        if (shouldLoad) {
            await this.loadPredictorDashboard(predictorId);
        }
    }

    async loadPredictorDashboard(predictorId) {
        if (this.dashboardLoadPromise && this.dashboardLoadingPredictorId === predictorId) {
            return this.dashboardLoadPromise;
        }
        const sequence = this.nextDashboardLoadSequence();
        if (this.dashboardFetchController && this.dashboardLoadingPredictorId !== predictorId) {
            this.dashboardFetchController.abort();
        }
        const controller = new AbortController();
        this.dashboardFetchController = controller;
        this.dashboardLoadingPredictorId = predictorId;
        let requestPromise = null;
        requestPromise = (async () => {
            const previousPredictorId = this.currentPredictor?.id || null;
            try {
                const response = await fetch(`/api/predictors/${predictorId}/dashboard`, {
                    credentials: 'include',
                    signal: controller.signal
                });
                if (response.status === 401) {
                    window.location.href = '/login';
                    return;
                }
                const data = await response.json();
                if (!response.ok) {
                    throw new Error(data.error || '加载方案详情失败');
                }
                if (this.isDashboardLoadStale(sequence, predictorId)) {
                    return;
                }

                this.currentPredictor = data.predictor;
                this.currentLotteryType = data.predictor?.lottery_type || 'pc28';
                this.updateHistoryLinks();
                this.renderSubscriptionPredictorOptions();
                this.renderNotificationSenderOptions();
                this.syncSubscriptionBetProfileOptions();
                if (previousPredictorId !== predictorId) {
                    this.resetNotificationSubscriptionForm();
                }
                this.renderNotificationSubscriptions(this.notificationSubscriptions);
                document.getElementById('profitPanel').style.display = data.predictor?.capabilities?.supports_profit_simulation ? '' : 'none';
                this.currentStats = data.stats || null;
                this.currentPredictions = data.recent_predictions || [];
                const availableMetrics = Object.keys((data.stats && data.stats.metrics) || {});
                const defaultMetric = data.stats?.primary_metric || 'big_small';
                const shouldResetMetric =
                    previousPredictorId !== predictorId ||
                    !this.selectedStatsMetric ||
                    !availableMetrics.includes(this.selectedStatsMetric);

                if (shouldResetMetric) {
                    this.selectedStatsMetric = defaultMetric;
                }
                this.renderStatsMetricOptions(this.currentLotteryType, availableMetrics, this.selectedStatsMetric);
                document.getElementById('statsMetricView').value = this.selectedStatsMetric;
                this.updatePredictorActionState(data.predictor);
                this.renderOverview(data.overview || this.overview || {});
                this.renderPredictorGuardPanel(data.predictor);
                this.renderStats(this.currentStats);
                this.renderCurrentPrediction(data.current_prediction, data.latest_prediction, data.predictor);
                this.renderPublicSharePanel(data.predictor);
                this.renderProfitControls(data.predictor, previousPredictorId !== predictorId);
                this.renderPredictionsTable(this.currentPredictions);
                this.renderDrawsTable(data.overview?.recent_draws || data.overview?.recent_events || data.recent_draws || data.recent_events || []);
                this.renderAILogs(this.currentPredictions);
                this.renderChart(this.currentPredictions);
                this.refreshProfitSimulation({
                    showLoading: previousPredictorId !== predictorId || !this.currentProfitSimulation,
                    defer: true,
                    loadingMessage: '正在更新收益模拟...'
                });
            } catch (error) {
                if (error.name === 'AbortError') {
                    return;
                }
                if (this.isDashboardLoadStale(sequence, predictorId)) {
                    return;
                }
                console.error('Failed to load predictor dashboard:', error);
            } finally {
                if (this.dashboardFetchController === controller) {
                    this.dashboardFetchController = null;
                }
                if (this.dashboardLoadingPredictorId === predictorId) {
                    this.dashboardLoadingPredictorId = null;
                }
                if (this.dashboardLoadPromise === requestPromise) {
                    this.dashboardLoadPromise = null;
                }
            }
        })();
        this.dashboardLoadPromise = requestPromise;
        return requestPromise;
    }

    renderStatsMetricOptions(lotteryType, availableMetrics, selectedMetric) {
        const select = document.getElementById('statsMetricView');
        const config = this.getLotteryConfig(lotteryType);
        const options = (config.primaryMetricOptions || []).filter((item) => !availableMetrics.length || availableMetrics.includes(item.key));
        select.innerHTML = options.map((item) => `
            <option value="${this.escapeHtml(item.key)}">查看玩法：${this.escapeHtml(this.metricMeta(item.key, lotteryType).label)}</option>
        `).join('');
        if (options.length) {
            const fallback = options[0].key;
            select.value = options.some((item) => item.key === selectedMetric) ? selectedMetric : fallback;
            this.selectedStatsMetric = select.value;
        }
    }

    renderPredictorGuardPanel(predictor) {
        const container = document.getElementById('predictorGuardPanel');
        if (!container) {
            return;
        }

        if (!predictor) {
            container.className = 'prediction-summary empty-panel';
            container.textContent = '请选择预测方案';
            return;
        }

        const runtimeStatus = predictor.runtime_status_label || '--';
        const errorCategory = this.guardErrorCategoryLabel(predictor.last_ai_error_category);
        const lastErrorMessage = predictor.last_ai_error_message || predictor.auto_pause_reason || '暂无';
        const recoveryText = this.guardRecoveryHint(predictor);
        const warning = predictor.auto_paused
            ? `<div class="warning-banner">方案已自动暂停，请先确认 API Key、模型余额或返回格式，再点击“解除自动暂停”。</div>`
            : '';

        container.className = 'prediction-summary';
        container.innerHTML = `
            ${warning}
            <div class="prediction-grid prediction-grid-compact">
                ${this.renderCurrentPredictionCard('运行状态', runtimeStatus)}
                ${this.renderCurrentPredictionCard('连续失败', String(predictor.consecutive_ai_failures || 0))}
                ${this.renderCurrentPredictionCard('错误类型', errorCategory)}
                ${this.renderCurrentPredictionCard('最近错误时间', predictor.last_ai_error_at || '--')}
                ${this.renderCurrentPredictionCard('自动暂停时间', predictor.auto_paused_at || '--')}
                ${this.renderCurrentPredictionCard('恢复方式', recoveryText)}
            </div>
            <div class="detail-list">
                <div class="detail-row">
                    <span class="detail-label">最近错误详情</span>
                    <strong>${this.escapeHtml(lastErrorMessage)}</strong>
                </div>
                <div class="detail-row">
                    <span class="detail-label">当前方案</span>
                    <strong>${this.escapeHtml(predictor.name || '--')} / ${this.escapeHtml(this.predictorExecutionLabel(predictor))}</strong>
                </div>
            </div>
        `;
    }

    renderStats(stats) {
        if (this.currentLotteryType === 'jingcai_football') {
            this.renderFootballStats(stats);
            return;
        }
        const container = document.getElementById('statsGrid');
        const currentMetricKey = this.selectedStatsMetric || stats.primary_metric || 'big_small';
        const currentMetric = (stats.metrics || {})[currentMetricKey] || {};
        const recent20 = currentMetric.recent_20 || {};
        const recent100 = currentMetric.recent_100 || {};
        const streaks = this.resolveMetricStreaks(stats, currentMetricKey);
        const metricLabel = this.targetLabel(currentMetricKey);

        container.innerHTML = `
            <article class="stat-card">
                <span class="stat-label">总预测次数</span>
                <strong class="stat-value">${stats.total_predictions || 0}</strong>
            </article>
            <article class="stat-card">
                <span class="stat-label">已结算期数</span>
                <strong class="stat-value">${stats.settled_predictions || 0}</strong>
            </article>
            <article class="stat-card">
                <span class="stat-label">过期未结算</span>
                <strong class="stat-value">${stats.expired_predictions || 0}</strong>
            </article>
            <article class="stat-card">
                <span class="stat-label">当前查看玩法</span>
                <strong class="stat-value">${this.escapeHtml(metricLabel || '--')}</strong>
            </article>
            <article class="stat-card">
                <span class="stat-label">20期命中率</span>
                <strong class="stat-value">${this.formatRatioRate(recent20)}</strong>
            </article>
            <article class="stat-card">
                <span class="stat-label">100期命中率</span>
                <strong class="stat-value">${this.formatRatioRate(recent100)}</strong>
            </article>
            <article class="stat-card">
                <span class="stat-label">当前连中 / 连挂</span>
                <strong class="stat-value">${streaks.current_hit_streak || 0} / ${streaks.current_miss_streak || 0}</strong>
            </article>
        `;

        this.renderStatsMetricHint(currentMetricKey);
        this.renderMetricStats(stats.metrics || {});
        this.renderStreakStats(stats, currentMetricKey);
    }

    renderFootballStats(stats) {
        const container = document.getElementById('statsGrid');
        const currentMetricKey = this.selectedStatsMetric || stats.primary_metric || 'spf';
        const currentMetric = (stats.metrics || {})[currentMetricKey] || {};
        const recent20 = currentMetric.recent_20 || {};
        const recent100 = currentMetric.recent_100 || {};
        const metricLabel = this.targetLabel(currentMetricKey);
        const streaks = stats.streaks || {};

        container.innerHTML = `
            <article class="stat-card">
                <span class="stat-label">预测批次数</span>
                <strong class="stat-value">${stats.total_predictions || 0}</strong>
            </article>
            <article class="stat-card">
                <span class="stat-label">已结算批次</span>
                <strong class="stat-value">${stats.settled_predictions || 0}</strong>
            </article>
            <article class="stat-card">
                <span class="stat-label">待结算批次</span>
                <strong class="stat-value">${stats.pending_predictions || 0}</strong>
            </article>
            <article class="stat-card">
                <span class="stat-label">当前查看玩法</span>
                <strong class="stat-value">${this.escapeHtml(metricLabel || '--')}</strong>
            </article>
            <article class="stat-card">
                <span class="stat-label">20 场命中率</span>
                <strong class="stat-value">${this.formatRatioRate(recent20)}</strong>
            </article>
            <article class="stat-card">
                <span class="stat-label">100 场命中率</span>
                <strong class="stat-value">${this.formatRatioRate(recent100)}</strong>
            </article>
            <article class="stat-card">
                <span class="stat-label">当前连中 / 连挂</span>
                <strong class="stat-value">${streaks.current_hit_streak || 0} / ${streaks.current_miss_streak || 0}</strong>
            </article>
        `;

        this.renderStatsMetricHint(currentMetricKey);
        this.renderMetricStats(stats.metrics || {});
        this.renderStreakStats(stats, currentMetricKey);
    }

    renderStatsMetricHint(metricKey) {
        const container = document.getElementById('statsMetricHint');
        if (!container) {
            return;
        }

        const meta = this.metricMeta(metricKey);
        const aliasText = meta.alias ? `<span class="metric-hint-alias">${this.escapeHtml(meta.alias)}</span>` : '';

        container.className = 'metric-hint';
        container.innerHTML = `
            <div class="metric-hint-head">
                <div>
                    <strong>${this.escapeHtml(meta.label)}</strong>
                    ${aliasText}
                </div>
                <span class="tag">${this.escapeHtml(meta.shortRule)}</span>
            </div>
            <p>${this.escapeHtml(meta.description)}</p>
            <p class="metric-hint-foot">统计规则：${this.escapeHtml(meta.formula)}</p>
        `;
    }

    renderMetricStats(metrics) {
        const tbody = document.getElementById('metricStatsBody');
        const metricOrder = (this.currentLotteryType || 'pc28') === 'jingcai_football'
            ? ['spf', 'rqspf']
            : ['number', 'big_small', 'odd_even', 'combo', 'double_group', 'kill_group'];
        const rows = metricOrder
            .map((key) => ({ key, metric: metrics[key] }))
            .filter((item) => item.metric);

        if (!rows.length) {
            tbody.innerHTML = '<tr><td colspan="4" class="empty-cell">暂无统计数据</td></tr>';
            return;
        }

        tbody.innerHTML = rows.map(({ key, metric }) => {
            const meta = this.metricMeta(key);
            const aliasText = meta.alias ? `<span class="metric-alias">${this.escapeHtml(meta.alias)}</span>` : '';

            return `
                <tr>
                    <td>
                        <div class="metric-label-stack">
                            <strong>${this.escapeHtml(meta.label)}</strong>
                            ${aliasText}
                        </div>
                    </td>
                    <td>${this.formatRatioRate(metric.recent_20)}</td>
                    <td>${this.formatRatioRate(metric.recent_100)}</td>
                    <td>${this.formatRatioRate(metric.overall)}</td>
                </tr>
            `;
        }).join('');
    }

    renderStreakStats(stats, metricKey) {
        const container = document.getElementById('streakStats');
        const streaks = this.resolveMetricStreaks(stats, metricKey);
        const metricLabel = this.targetLabel(metricKey);

        container.innerHTML = `
            <article class="streak-card">
                <span class="stat-label">当前查看玩法</span>
                <strong class="streak-value">${this.escapeHtml(metricLabel)}</strong>
            </article>
            <article class="streak-card">
                <span class="stat-label">当前连中</span>
                <strong class="streak-value">${streaks.current_hit_streak || 0}</strong>
            </article>
            <article class="streak-card">
                <span class="stat-label">当前连挂</span>
                <strong class="streak-value">${streaks.current_miss_streak || 0}</strong>
            </article>
            <article class="streak-card">
                <span class="stat-label">100期最大连中</span>
                <strong class="streak-value">${streaks.recent_100_max_hit_streak || 0}</strong>
            </article>
            <article class="streak-card">
                <span class="stat-label">100期最大连挂</span>
                <strong class="streak-value">${streaks.recent_100_max_miss_streak || 0}</strong>
            </article>
            <article class="streak-card">
                <span class="stat-label">历史最大连中</span>
                <strong class="streak-value">${streaks.historical_max_hit_streak || 0}</strong>
            </article>
            <article class="streak-card">
                <span class="stat-label">历史最大连挂</span>
                <strong class="streak-value">${streaks.historical_max_miss_streak || 0}</strong>
            </article>
        `;
    }

    resolveMetricStreaks(stats, metricKey) {
        const metricStreaks = ((stats.metric_streaks || {})[metricKey]) || null;
        return metricStreaks || stats.streaks || {};
    }

    renderCurrentPrediction(currentPrediction, latestPrediction, predictor) {
        if (this.currentLotteryType === 'jingcai_football') {
            this.renderFootballCurrentPrediction(currentPrediction, latestPrediction, predictor);
            return;
        }
        const container = document.getElementById('currentPrediction');

        if (!predictor) {
            container.className = 'prediction-summary empty-panel';
            container.textContent = '请选择预测方案';
            this.updatePredictorActionState(null);
            return;
        }

        if ((predictor.lottery_type || 'pc28') === 'jingcai_football') {
            this.renderFootballCurrentPrediction(currentPrediction, latestPrediction, predictor);
            return;
        }

        const targetTags = (predictor.prediction_targets || []).map((item) => this.renderBadge(this.targetLabel(item, predictor.lottery_type || 'pc28'))).join('');
        const prediction = currentPrediction || latestPrediction;

        if (!prediction) {
            container.className = 'prediction-summary empty-panel';
            container.innerHTML = `
                <div class="summary-head">
                    <div>
                        <h4>${this.escapeHtml(predictor.name)}</h4>
                        <p>${this.escapeHtml(this.predictorMetaLabel(predictor))}</p>
                    </div>
                    <div class="badge-row">${targetTags}</div>
                </div>
                ${this.renderPredictorGuardNotice(predictor)}
                ${this.renderPredictorExecutionPanel(predictor)}
                <p>当前尚无预测记录，点击“立即预测”或等待自动轮询。</p>
            `;
            return;
        }

        const statusText = this.predictionStatusLabel(prediction.status);
        const errorBlock = prediction.error_message
            ? `<div class="warning-banner">${this.escapeHtml(prediction.error_message)}</div>`
            : '';
        const predictionSnapshot = this.buildPlaySnapshot(
            prediction.prediction_number,
            prediction.prediction_big_small,
            prediction.prediction_odd_even,
            prediction.prediction_combo
        );
        const actualSnapshot = this.buildPlaySnapshot(
            prediction.actual_number,
            prediction.actual_big_small,
            prediction.actual_odd_even,
            prediction.actual_combo
        );
        const confidenceText = this.formatPercent(
            prediction.confidence !== null && prediction.confidence !== undefined ? prediction.confidence * 100 : null
        );

        container.className = 'prediction-summary';
        container.innerHTML = `
            <div class="summary-head">
                <div>
                    <h4>${this.escapeHtml(predictor.name)}</h4>
                    <p>${this.escapeHtml(this.predictorMetaLabel(predictor))}</p>
                </div>
                <div class="badge-row">
                    ${targetTags}
                    <span class="status-chip ${prediction.status}">${statusText}</span>
                </div>
            </div>
            ${this.renderPredictorGuardNotice(predictor)}
            ${errorBlock}
            ${this.renderPredictorExecutionPanel(predictor)}
            <div class="metric-hint prediction-note">
                <div class="metric-hint-head">
                    <div>
                        <strong>展示口径说明</strong>
                    </div>
                    <span class="tag">仅展示模型命中口径</span>
                </div>
                <p>组合结果由大/小和单/双拼装而成；组合投注与排除对角组为统计派生口径，不代表模型额外输出了新的独立下注项。</p>
                <p class="metric-hint-foot">当前页面不会直接折算赔率、回本或真实盘口收益。</p>
            </div>
            <div class="prediction-section">
                <div class="prediction-section-head">
                    <div>
                        <span class="mini-label">本期原始输出</span>
                        <p class="section-hint">先看模型直接输出的号码和基础标签。</p>
                    </div>
                </div>
                <div class="prediction-grid prediction-grid-compact">
                    ${this.renderCurrentPredictionCard('预测期号', prediction.issue_no || '--')}
                    ${this.renderCurrentPredictionCard('预测号码', predictionSnapshot.numberText || '--')}
                    ${this.renderCurrentPredictionCard('大/小', predictionSnapshot.bigSmall || '--')}
                    ${this.renderCurrentPredictionCard('单/双', predictionSnapshot.oddEven || '--')}
                    ${this.renderCurrentPredictionCard('组合结果', predictionSnapshot.combo || '--', predictionSnapshot.combo ? '组合结果固定为小双 / 小单 / 大双 / 大单' : '当前无可用组合标签')}
                    ${this.renderCurrentPredictionCard('置信度', confidenceText)}
                </div>
            </div>
            <div class="prediction-section">
                <div class="prediction-section-head">
                    <div>
                        <span class="mini-label">派生玩法映射</span>
                        <p class="section-hint">把组合结果翻译成真实可下注的组合投注分组。</p>
                    </div>
                </div>
                <div class="prediction-grid prediction-grid-compact">
                    ${this.renderCurrentPredictionCard('组合分组', predictionSnapshot.doubleGroup || '--', predictionSnapshot.doubleGroup ? '用于辅助解释组合投注的分组口径' : '依赖组合结果')}
                    ${this.renderCurrentPredictionCard('组合投注', predictionSnapshot.pairTicket || '--', predictionSnapshot.pairTicket ? '真实下注按这个组合票面结算' : '依赖组合结果')}
                    ${this.renderCurrentPredictionCard('排除对角组', predictionSnapshot.killGroup ? `杀${predictionSnapshot.killGroup}` : '--', predictionSnapshot.killGroup ? '实际未开出该组合即计为命中' : '依赖组合结果')}
                    ${this.renderCurrentPredictionCard('更新时间', prediction.updated_at || prediction.created_at || '--')}
                </div>
            </div>
            ${this.renderCurrentSettlement(prediction, predictionSnapshot, actualSnapshot)}
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

    buildFootballBatchFailureHint(prediction) {
        const label = prediction.batch_failure_label ? `子批次 ${prediction.batch_failure_label}` : '子批次失败';
        const matchText = prediction.batch_failure_match_text || prediction.issue_no || '--';
        const reason = prediction.batch_failure_reason || prediction.error_message || 'AI 子批次预测失败';
        return `${label} · ${matchText} · ${reason}`;
    }

    renderFootballFailedBatchSection(failedBatches) {
        if (!Array.isArray(failedBatches) || !failedBatches.length) {
            return '';
        }
        const cards = failedBatches.map((batch) => this.renderCurrentPredictionCard(
            `子批次 ${batch.batch_label || '--'}`,
            batch.match_text || '--',
            batch.reason || 'AI 子批次预测失败'
        )).join('');
        return `
            <div class="prediction-section">
                <div class="prediction-section-head">
                    <div>
                        <span class="mini-label">失败子批次</span>
                        <p class="section-hint">以下子批次未成功返回完整预测，系统已在其余子批次成功结果基础上保留可用场次。</p>
                    </div>
                </div>
                <div class="prediction-grid prediction-grid-compact">
                    ${cards}
                </div>
            </div>
        `;
    }

    renderFootballCurrentPrediction(currentPrediction, latestPrediction, predictor) {
        const container = document.getElementById('currentPrediction');
        if (!predictor) {
            container.className = 'prediction-summary empty-panel';
            container.textContent = '请选择预测方案';
            this.updatePredictorActionState(null);
            return;
        }

        const prediction = currentPrediction && Array.isArray(currentPrediction.items) ? currentPrediction : latestPrediction;
        const targetTags = (predictor.prediction_targets || []).map((item) => this.renderBadge(this.targetLabel(item, predictor.lottery_type || 'jingcai_football'))).join('');

        if (!prediction) {
            container.className = 'prediction-summary empty-panel';
            container.innerHTML = `
                <div class="summary-head">
                    <div>
                        <h4>${this.escapeHtml(predictor.name)}</h4>
                        <p>${this.escapeHtml(this.predictorMetaLabel(predictor))}</p>
                    </div>
                    <div class="badge-row">${targetTags}</div>
                </div>
                ${this.renderPredictorGuardNotice(predictor)}
                ${this.renderPredictorExecutionPanel(predictor)}
                <p>当前尚无竞彩足球预测记录，点击“立即预测”后会按当前批次生成多场比赛预测。</p>
            `;
            return;
        }

        if (Array.isArray(prediction.items)) {
            const items = prediction.items || [];
            const parlay = Array.isArray(prediction.recommended_parlay) ? prediction.recommended_parlay : [];
            const tickets = Array.isArray(prediction.recommended_tickets) ? prediction.recommended_tickets : [];
            const ticketWarnings = Array.isArray(prediction.recommended_ticket_warnings) ? prediction.recommended_ticket_warnings : [];
            const failedBatches = Array.isArray(prediction.failed_batches) ? prediction.failed_batches : [];
            const saleNotice = ticketWarnings.length
                ? ticketWarnings.map((item) => item.message || '--').join('；')
                : '若当前玩法停售、已开奖或缺少有效赔率快照，系统会自动跳过该玩法，不纳入推荐票面与收益模拟。';
            const saleNoticeBlock = ticketWarnings.length
                ? `<div class="warning-banner">${this.escapeHtml(saleNotice)}</div>`
                : `<div class="metric-hint prediction-note"><p>${this.escapeHtml(saleNotice)}</p></div>`;
            const previewRows = items.slice(0, 8).map((item) => {
                const marketSnapshot = item.market_snapshot || {};
                const spfOutcome = this.buildFootballOutcomeText('spf', (item.prediction_payload || {}).spf, marketSnapshot);
                const rqspfOutcome = this.buildFootballOutcomeText('rqspf', (item.prediction_payload || {}).rqspf, marketSnapshot);
                const spfOdds = this.buildFootballOddsText('spf', (item.prediction_payload || {}).spf, marketSnapshot);
                const rqspfOdds = this.buildFootballOddsText('rqspf', (item.prediction_payload || {}).rqspf, marketSnapshot);
                const spfHint = this.buildFootballAvailabilityHint('spf', marketSnapshot);
                const rqspfHint = this.buildFootballAvailabilityHint('rqspf', marketSnapshot);
                const snapshotSummary = this.buildFootballSnapshotSummary(marketSnapshot);
                const statusLabel = this.footballMatchStatusLabel(marketSnapshot);
                const failureHint = item.status === 'failed' ? this.buildFootballBatchFailureHint(item) : '';
                return `
                    <tr>
                        <td>${this.escapeHtml(item.issue_no || '--')}</td>
                        <td>${this.escapeHtml(item.title || '--')}</td>
                        <td>${this.escapeHtml(statusLabel)}${failureHint ? `<br><span class="hint-text">${this.escapeHtml(failureHint)}</span>` : ''}</td>
                        <td>${this.escapeHtml(spfOutcome || '--')}${spfOdds ? `<br><span class="hint-text">赔率 ${this.escapeHtml(spfOdds)}</span>` : ''}${spfHint ? `<br><span class="hint-text">${this.escapeHtml(spfHint)}</span>` : ''}${snapshotSummary ? `<br><span class="hint-text">${this.escapeHtml(snapshotSummary)}</span>` : ''}</td>
                        <td>${this.escapeHtml(rqspfOutcome || '--')}${rqspfOdds ? `<br><span class="hint-text">赔率 ${this.escapeHtml(rqspfOdds)}</span>` : ''}${rqspfHint ? `<br><span class="hint-text">${this.escapeHtml(rqspfHint)}</span>` : ''}</td>
                        <td>${this.formatPercent(item.confidence !== null && item.confidence !== undefined ? item.confidence * 100 : null)}</td>
                    </tr>
                `;
            }).join('');
            const previewCards = items.slice(0, 8).map((item) => {
                const marketSnapshot = item.market_snapshot || {};
                const spfOutcome = this.buildFootballOutcomeText('spf', (item.prediction_payload || {}).spf, marketSnapshot);
                const rqspfOutcome = this.buildFootballOutcomeText('rqspf', (item.prediction_payload || {}).rqspf, marketSnapshot);
                const spfOdds = this.buildFootballOddsText('spf', (item.prediction_payload || {}).spf, marketSnapshot);
                const rqspfOdds = this.buildFootballOddsText('rqspf', (item.prediction_payload || {}).rqspf, marketSnapshot);
                const spfHint = this.buildFootballAvailabilityHint('spf', marketSnapshot);
                const rqspfHint = this.buildFootballAvailabilityHint('rqspf', marketSnapshot);
                const snapshotSummary = this.buildFootballSnapshotSummary(marketSnapshot);
                const statusLabel = this.footballMatchStatusLabel(marketSnapshot);
                const failureHint = item.status === 'failed' ? this.buildFootballBatchFailureHint(item) : '';
                return this.renderMobileDataCard({
                    title: `${this.escapeHtml(item.issue_no || '--')} · ${this.escapeHtml(item.title || '--')}`,
                    badgeHtml: `<span class="tag">${this.escapeHtml(statusLabel)}</span>`,
                    sections: [
                        {
                            label: '胜平负',
                            content: `${this.escapeHtml(spfOutcome || '--')}${spfOdds ? `<br><span class="hint-text">赔率 ${this.escapeHtml(spfOdds)}</span>` : ''}${spfHint ? `<br><span class="hint-text">${this.escapeHtml(spfHint)}</span>` : ''}${snapshotSummary ? `<br><span class="hint-text">${this.escapeHtml(snapshotSummary)}</span>` : ''}`
                        },
                        {
                            label: '让球胜平负',
                            content: `${this.escapeHtml(rqspfOutcome || '--')}${rqspfOdds ? `<br><span class="hint-text">赔率 ${this.escapeHtml(rqspfOdds)}</span>` : ''}${rqspfHint ? `<br><span class="hint-text">${this.escapeHtml(rqspfHint)}</span>` : ''}`
                        },
                        {
                            label: '置信度',
                            content: this.formatPercent(item.confidence !== null && item.confidence !== undefined ? item.confidence * 100 : null)
                        }
                    ],
                    footer: failureHint ? this.escapeHtml(failureHint) : ''
                });
            }).join('');

            const candidateCards = parlay.map((item, index) => {
                const marketSnapshot = item.market_snapshot || {};
                const spfOutcome = this.buildFootballOutcomeText('spf', (item.prediction_payload || {}).spf, marketSnapshot);
                const rqspfOutcome = this.buildFootballOutcomeText('rqspf', (item.prediction_payload || {}).rqspf, marketSnapshot);
                const spfOdds = this.buildFootballOddsText('spf', (item.prediction_payload || {}).spf, marketSnapshot);
                const rqspfOdds = this.buildFootballOddsText('rqspf', (item.prediction_payload || {}).rqspf, marketSnapshot);
                const spfHint = this.buildFootballAvailabilityHint('spf', marketSnapshot);
                const rqspfHint = this.buildFootballAvailabilityHint('rqspf', marketSnapshot);
                const snapshotSummary = this.buildFootballSnapshotSummary(marketSnapshot);
                return this.renderCurrentPredictionCard(
                    `候选场次 ${index + 1}`,
                    `${item.issue_no || '--'} · ${item.title || '--'}`,
                    `SPF ${spfOutcome || '--'}${spfOdds ? ` @ ${spfOdds}` : ''}${spfHint ? ` · ${spfHint}` : ''} · RQSPF ${rqspfOutcome || '--'}${rqspfOdds ? ` @ ${rqspfOdds}` : ''}${rqspfHint ? ` · ${rqspfHint}` : ''} · 置信度 ${this.formatPercent(item.confidence !== null && item.confidence !== undefined ? item.confidence * 100 : null)}${snapshotSummary ? ` · ${snapshotSummary}` : ''}`
                );
            }).join('');

            const ticketCards = tickets.length
                ? tickets.map((ticket) => this.renderCurrentPredictionCard(
                    ticket.metric_label || '--',
                    ticket.ticket_text || '--',
                    `${ticket.odds_source_label || '预测批次赔率快照'} · 合计赔率 ${this.formatOdds(ticket.odds)}`
                )).join('')
                : this.renderCurrentPredictionCard('推荐逻辑', '按置信度挑两场', '若某个玩法缺少有效赔率快照，则不生成对应二串一票面。');

            container.className = 'prediction-summary';
            container.innerHTML = `
                <div class="summary-head">
                    <div>
                        <h4>${this.escapeHtml(predictor.name)}</h4>
                        <p>${this.escapeHtml(prediction.run_key || '--')} · 当前批次推荐票面</p>
                    </div>
                    <div class="badge-row">
                        ${targetTags}
                        <span class="status-chip ${prediction.status}">${this.predictionStatusLabel(prediction.status)}</span>
                    </div>
                </div>
                ${this.renderPredictorGuardNotice(predictor)}
                ${prediction.error_message ? `<div class="warning-banner">${this.escapeHtml(prediction.error_message)}</div>` : ''}
                ${this.renderPredictorExecutionPanel(predictor)}
                ${saleNoticeBlock}
                ${this.renderFootballFailedBatchSection(failedBatches)}
                <div class="prediction-grid prediction-grid-compact">
                    ${this.renderCurrentPredictionCard('预测批次', prediction.run_key || '--')}
                    ${this.renderCurrentPredictionCard('场次数量', String(items.length))}
                    ${this.renderCurrentPredictionCard('已结算', String(prediction.settled_items || 0))}
                    ${this.renderCurrentPredictionCard('平均置信度', this.formatPercent(prediction.confidence !== null && prediction.confidence !== undefined ? prediction.confidence * 100 : null))}
                    ${this.renderCurrentPredictionCard('更新时间', prediction.updated_at || prediction.created_at || '--')}
                </div>
                <div class="prediction-section">
                    <div class="prediction-section-head">
                        <div>
                            <span class="mini-label">默认推荐票面</span>
                            <p class="section-hint">默认先按置信度挑两场，再分别生成胜平负与让球胜平负二串一票面；赔率取预测批次快照。</p>
                        </div>
                    </div>
                    <div class="prediction-grid prediction-grid-compact">
                        ${ticketCards}
                        ${candidateCards}
                    </div>
                </div>
                <div class="prediction-section">
                    <div class="prediction-section-head">
                        <div>
                            <span class="mini-label">本批次前 8 场预测</span>
                            <p class="section-hint">每场同时展示 SPF / RQSPF 预测及对应赔率快照，避免把“胜 / 胜”误读成两场组合。</p>
                        </div>
                    </div>
                    <div class="table-wrap desktop-only">
                        <table class="data-table compact-table">
                            <thead>
                                <tr>
                                    <th>编号</th>
                                    <th>比赛</th>
                                    <th>状态</th>
                                    <th>胜平负</th>
                                    <th>让球胜平负</th>
                                    <th>置信度</th>
                                </tr>
                            </thead>
                            <tbody>
                                ${previewRows || '<tr><td colspan="6" class="empty-cell">暂无预测明细</td></tr>'}
                            </tbody>
                        </table>
                    </div>
                    <div id="footballPreviewCards" class="mobile-only mobile-card-list">
                        ${previewCards || '<div class="empty-panel">暂无预测明细</div>'}
                    </div>
                </div>
                <div class="summary-foot">
                    <div>
                        <span class="mini-label">批次说明</span>
                        <p>${this.escapeHtml(prediction.reasoning_summary || '当前批次已生成竞彩足球预测，可先参考默认单关与二串一票面。')}</p>
                    </div>
                </div>
            `;
            return;
        }

        const predictionPayload = prediction.prediction_payload || {};
        const actualPayload = prediction.actual_payload || {};
        const hitItems = this.buildHitItems(prediction);
        const hitCount = hitItems.filter((item) => item.value === 1).length;
        const attemptedCount = hitItems.filter((item) => item.value !== null && item.value !== undefined).length;

        container.className = 'prediction-summary';
        container.innerHTML = `
            <div class="summary-head">
                <div>
                    <h4>${this.escapeHtml(prediction.title || predictor.name || '--')}</h4>
                    <p>${this.escapeHtml(prediction.issue_no || '--')} · ${this.escapeHtml(prediction.actual_payload?.league || '')}</p>
                </div>
                <div class="badge-row">
                    ${targetTags}
                    <span class="status-chip ${prediction.status}">${this.predictionStatusLabel(prediction.status)}</span>
                </div>
            </div>
            ${prediction.error_message ? `<div class="warning-banner">${this.escapeHtml(prediction.error_message)}</div>` : ''}
            <div class="prediction-grid prediction-grid-compact">
                ${this.renderCurrentPredictionCard('场次', prediction.issue_no || '--')}
                ${this.renderCurrentPredictionCard('胜平负', this.buildFootballOutcomeText('spf', predictionPayload.spf, prediction.market_snapshot || {}), this.buildFootballOddsText('spf', predictionPayload.spf, prediction.market_snapshot || {}) ? `赔率 ${this.buildFootballOddsText('spf', predictionPayload.spf, prediction.market_snapshot || {})}` : '')}
                ${this.renderCurrentPredictionCard('让球胜平负', this.buildFootballOutcomeText('rqspf', predictionPayload.rqspf, prediction.market_snapshot || {}), this.buildFootballOddsText('rqspf', predictionPayload.rqspf, prediction.market_snapshot || {}) ? `赔率 ${this.buildFootballOddsText('rqspf', predictionPayload.rqspf, prediction.market_snapshot || {})}` : '')}
                ${this.renderCurrentPredictionCard('赔率快照', this.buildFootballSnapshotSummary(prediction.market_snapshot || {}) || '--')}
                ${this.renderCurrentPredictionCard('置信度', this.formatPercent(prediction.confidence !== null && prediction.confidence !== undefined ? prediction.confidence * 100 : null))}
                ${this.renderCurrentPredictionCard('赛果', actualPayload.score_text ? `${actualPayload.score_text} (${actualPayload.spf || '--'})` : '--')}
                ${this.renderCurrentPredictionCard('命中', attemptedCount ? `${hitCount}/${attemptedCount}` : '--')}
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

    buildFootballOutcomeText(metricKey, outcome, marketSnapshot = {}) {
        if (!outcome) {
            return '--';
        }
        if (metricKey !== 'rqspf') {
            return outcome;
        }
        const handicapText = ((marketSnapshot.rqspf || {}).handicap_text || '').trim();
        if (!handicapText) {
            return outcome;
        }
        return `${outcome}(让${handicapText})`;
    }

    buildFootballOddsText(metricKey, outcome, marketSnapshot = {}) {
        if (!outcome) {
            return '';
        }
        const rawValue = metricKey === 'rqspf'
            ? ((marketSnapshot.rqspf || {}).odds || {})[outcome]
            : (marketSnapshot.spf_odds || {})[outcome];
        if (rawValue === null || rawValue === undefined || rawValue === '') {
            return '';
        }
        return `${Number(rawValue).toFixed(2)} 倍`;
    }

    buildFootballAvailabilityHint(metricKey, marketSnapshot = {}) {
        const sellable = metricKey === 'rqspf'
            ? marketSnapshot.rqspf_sellable
            : marketSnapshot.spf_sellable;
        const label = metricKey === 'rqspf'
            ? marketSnapshot.rqspf_availability_label
            : marketSnapshot.spf_availability_label;
        return sellable ? '' : (label || '停售或无赔率快照');
    }

    buildFootballSnapshotSummary(marketSnapshot = {}) {
        const snapshots = marketSnapshot.odds_snapshots || {};
        const euro = snapshots.euro || {};
        const initial = euro.initial || {};
        const current = euro.current || {};
        const hasEuro = initial.win !== undefined || current.win !== undefined;
        if (!hasEuro) {
            return '';
        }
        return `欧赔 ${this.formatFootballSnapshotTriplet(initial)} -> ${this.formatFootballSnapshotTriplet(current)}`;
    }

    formatFootballSnapshotTriplet(payload = {}) {
        const values = ['win', 'draw', 'lose'].map((key) => {
            const value = payload[key];
            return value === null || value === undefined || value === '' ? '--' : Number(value).toFixed(2);
        });
        return values.join('/');
    }

    renderPublicSharePanel(predictor) {
        const container = document.getElementById('publicSharePanel');
        if (!container) {
            return;
        }

        if (!predictor) {
            container.className = 'prediction-summary empty-panel';
            container.textContent = '请选择预测方案';
            return;
        }

        const publicUrl = predictor.public_url || predictor.public_path || '--';
        const isAvailable = Boolean(predictor.public_page_available);
        const availabilityText = isAvailable ? '公开页可访问' : '方案已停用，公开页当前不可访问';
        const availabilityHint = isAvailable
            ? '当前地址可直接分享给访客；访客能看到的内容取决于公开层级，竞彩足球也会同步展示收益模拟结果。'
            : '启用自动预测后，公开页才会重新出现在首页榜单和公开详情中。';
        const disabledAttr = isAvailable ? '' : 'disabled';
        const openButton = isAvailable
            ? `<a class="btn ghost compact" href="${this.escapeHtml(publicUrl)}" target="_blank" rel="noopener noreferrer">打开公开页</a>`
            : '<button class="btn ghost compact" disabled>打开公开页</button>';

        container.className = 'prediction-summary';
        container.innerHTML = `
            <div class="summary-head">
                <div>
                    <h4>${this.escapeHtml(predictor.name || '--')}</h4>
                    <p>${this.escapeHtml(predictor.share_level_label || '--')} · ${this.escapeHtml(availabilityText)}</p>
                </div>
                <div class="badge-row">
                    <span class="tag">${this.escapeHtml(this.targetLabel(predictor.primary_metric || 'combo', predictor.lottery_type || 'pc28'))}</span>
                    <span class="tag">${this.escapeHtml(predictor.share_level_label || '--')}</span>
                </div>
            </div>
            <div class="prediction-card">
                <span class="mini-label">公开地址</span>
                <strong class="share-link-text">${this.escapeHtml(publicUrl)}</strong>
                <span class="card-hint">${this.escapeHtml(availabilityHint)}</span>
            </div>
            <div class="share-panel-actions">
                <button class="btn primary compact" data-action="copy-public-url" data-url="${this.escapeHtml(publicUrl)}" ${disabledAttr}>复制地址</button>
                ${openButton}
            </div>
        `;
    }

    renderProfitControls(predictor, resetMetric = false) {
        if (resetMetric) {
            this.currentProfitSimulation = null;
        }
        const ruleSelect = document.getElementById('profitRuleView');
        const metricSelect = document.getElementById('profitMetricView');
        const periodSelect = document.getElementById('profitPeriodView');
        const betProfileSelect = document.getElementById('profitBetProfileView');
        const betModeSelect = document.getElementById('profitBetModeView');
        const baseStakeInput = document.getElementById('profitBaseStakeView');
        const multiplierInput = document.getElementById('profitMultiplierView');
        const maxStepsInput = document.getElementById('profitMaxStepsView');
        const orderSelect = document.getElementById('profitOrderView');
        const oddsSelect = document.getElementById('profitOddsProfileView');
        const rules = predictor?.profit_rule_options || [];
        const metrics = predictor?.simulation_metrics || [];
        const periodOptions = predictor?.profit_period_options || [];
        const oddsProfiles = predictor?.odds_profiles || [];
        const profileOptions = (this.betProfiles || []).filter((item) => item.lottery_type === (predictor?.lottery_type || 'pc28'));
        const hasSavedBetProfiles = profileOptions.length > 0;

        if (!metrics.length || !rules.length) {
            ruleSelect.innerHTML = '<option value="">暂无规则</option>';
            ruleSelect.disabled = true;
            metricSelect.innerHTML = '<option value="">暂无玩法</option>';
            metricSelect.disabled = true;
            periodSelect.innerHTML = '<option value="">暂无区间</option>';
            periodSelect.disabled = true;
            betProfileSelect.hidden = true;
            betProfileSelect.innerHTML = '<option value="">手动输入参数</option>';
            betProfileSelect.disabled = true;
            betModeSelect.disabled = true;
            baseStakeInput.disabled = true;
            multiplierInput.disabled = true;
            maxStepsInput.disabled = true;
            orderSelect.disabled = true;
            oddsSelect.disabled = true;
            this.syncProfitOptionVisibility({
                ruleCount: 0,
                metricCount: 0,
                periodCount: 0,
                oddsCount: 0,
                hasSavedBetProfiles: false
            });
            return;
        }

        if (
            resetMetric ||
            !this.selectedProfitRuleId ||
            !rules.some((item) => item.key === this.selectedProfitRuleId)
        ) {
            this.selectedProfitRuleId = predictor.profit_rule_id || rules[0].key;
        }

        if (
            resetMetric ||
            !this.selectedProfitMetric ||
            !metrics.some((item) => item.key === this.selectedProfitMetric)
        ) {
            this.selectedProfitMetric = predictor.default_simulation_metric || metrics[0].key;
        }

        if (
            resetMetric ||
            !this.selectedProfitPeriodKey ||
            !periodOptions.some((item) => item.key === this.selectedProfitPeriodKey)
        ) {
            this.selectedProfitPeriodKey = predictor.default_profit_period_key || periodOptions[0]?.key || DEFAULT_PROFIT_PERIOD_KEY;
        }

        if (
            !this.selectedProfitOddsProfile ||
            !oddsProfiles.some((item) => item.key === this.selectedProfitOddsProfile)
        ) {
            this.selectedProfitOddsProfile = oddsProfiles[0]?.key || 'regular';
        }

        if (!['flat', 'martingale'].includes(this.selectedProfitBetMode)) {
            this.selectedProfitBetMode = DEFAULT_PROFIT_BET_MODE;
        }
        this.selectedProfitBaseStake = this.normalizePositiveNumber(this.selectedProfitBaseStake, DEFAULT_PROFIT_BASE_STAKE, 0.01);
        this.selectedProfitMultiplier = this.normalizePositiveNumber(this.selectedProfitMultiplier, DEFAULT_PROFIT_MULTIPLIER, 1.01);
        this.selectedProfitMaxSteps = this.normalizePositiveInt(this.selectedProfitMaxSteps, DEFAULT_PROFIT_MAX_STEPS, 1, 12);
        if (this.selectedProfitBetProfileId && !profileOptions.some((item) => String(item.id) === String(this.selectedProfitBetProfileId))) {
            this.selectedProfitBetProfileId = '';
        }

        ruleSelect.innerHTML = rules.map((item) => `
            <option value="${this.escapeHtml(item.key)}">${this.escapeHtml(item.label)}</option>
        `).join('');
        ruleSelect.value = this.selectedProfitRuleId;
        ruleSelect.disabled = false;

        metricSelect.innerHTML = metrics.map((item) => `
            <option value="${this.escapeHtml(item.key)}">${this.escapeHtml(item.label)}</option>
        `).join('');
        metricSelect.value = this.selectedProfitMetric;
        metricSelect.disabled = false;
        periodSelect.innerHTML = periodOptions.map((item) => `
            <option value="${this.escapeHtml(item.key)}">${this.escapeHtml(item.label)}</option>
        `).join('');
        periodSelect.value = this.selectedProfitPeriodKey;
        periodSelect.disabled = periodOptions.length <= 1;
        betProfileSelect.hidden = !hasSavedBetProfiles;
        betProfileSelect.innerHTML = ['<option value="">手动输入参数</option>'].concat(
            profileOptions.map((item) => `<option value="${item.id}">${this.escapeHtml(item.name)} · ${this.escapeHtml(item.strategy_label || '--')}</option>`)
        ).join('');
        betProfileSelect.value = this.selectedProfitBetProfileId || '';
        betProfileSelect.disabled = !hasSavedBetProfiles;
        betModeSelect.value = this.selectedProfitBetMode;
        betModeSelect.disabled = false;
        baseStakeInput.value = String(this.selectedProfitBaseStake);
        baseStakeInput.disabled = false;
        multiplierInput.value = String(this.selectedProfitMultiplier);
        maxStepsInput.value = String(this.selectedProfitMaxSteps);
        orderSelect.value = this.selectedProfitOrder;
        orderSelect.disabled = false;
        oddsSelect.innerHTML = oddsProfiles.map((item) => `
            <option value="${this.escapeHtml(item.key)}">${this.escapeHtml(item.label)}</option>
        `).join('');
        oddsSelect.disabled = false;
        oddsSelect.value = this.selectedProfitOddsProfile;
        this.syncProfitOptionVisibility({
            ruleCount: rules.length,
            metricCount: metrics.length,
            periodCount: periodOptions.length,
            oddsCount: oddsProfiles.length,
            hasSavedBetProfiles
        });
        this.syncProfitBetControlState();
    }

    cancelPendingProfitSimulation() {
        this.profitSimulationLoadSequence += 1;
        if (this.profitSimulationDeferredTimer) {
            window.clearTimeout(this.profitSimulationDeferredTimer);
            this.profitSimulationDeferredTimer = null;
        }
        if (this.profitSimulationFetchController) {
            this.profitSimulationFetchController.abort();
            this.profitSimulationFetchController = null;
        }
    }

    refreshProfitSimulation({ showLoading = true, defer = false, loadingMessage = '收益模拟计算中...' } = {}) {
        if (showLoading) {
            this.renderProfitSimulationLoading(loadingMessage);
        }
        if (defer) {
            this.scheduleProfitSimulationLoad();
            return Promise.resolve();
        }
        return this.loadProfitSimulation();
    }

    scheduleProfitSimulationLoad() {
        this.cancelPendingProfitSimulation();
        const sequence = this.profitSimulationLoadSequence;
        this.profitSimulationDeferredTimer = window.setTimeout(() => {
            this.profitSimulationDeferredTimer = null;
            this.loadProfitSimulation(sequence);
        }, 300);
    }

    async loadProfitSimulation(sequence = null) {
        const requestSequence = sequence ?? (this.profitSimulationLoadSequence + 1);
        if (sequence === null) {
            this.profitSimulationLoadSequence = requestSequence;
        }
        if (this.profitSimulationDeferredTimer) {
            window.clearTimeout(this.profitSimulationDeferredTimer);
            this.profitSimulationDeferredTimer = null;
        }
        const predictor = this.currentPredictor;
        const predictorId = this.currentPredictorId;
        if (requestSequence !== this.profitSimulationLoadSequence || !predictorId) {
            return;
        }
        if (!predictor || !this.currentPredictorId) {
            this.renderProfitSimulationEmpty('请选择预测方案');
            return;
        }

        const metrics = predictor.simulation_metrics || [];
        if (!metrics.length) {
            this.renderProfitSimulationEmpty('当前方案没有可用于收益模拟的玩法');
            return;
        }

        const rules = predictor.profit_rule_options || [];
        if (!this.selectedProfitRuleId || !rules.some((item) => item.key === this.selectedProfitRuleId)) {
            this.selectedProfitRuleId = predictor.profit_rule_id || rules[0]?.key || 'pc28_netdisk';
            document.getElementById('profitRuleView').value = this.selectedProfitRuleId;
        }

        if (!this.selectedProfitMetric || !metrics.some((item) => item.key === this.selectedProfitMetric)) {
            this.selectedProfitMetric = predictor.default_simulation_metric || metrics[0].key;
            document.getElementById('profitMetricView').value = this.selectedProfitMetric;
        }
        const periodOptions = predictor.profit_period_options || [];
        if (!this.selectedProfitPeriodKey || !periodOptions.some((item) => item.key === this.selectedProfitPeriodKey)) {
            this.selectedProfitPeriodKey = predictor.default_profit_period_key || periodOptions[0]?.key || DEFAULT_PROFIT_PERIOD_KEY;
            document.getElementById('profitPeriodView').value = this.selectedProfitPeriodKey;
        }

        if (this.profitSimulationFetchController) {
            this.profitSimulationFetchController.abort();
        }
        const controller = new AbortController();
        this.profitSimulationFetchController = controller;
        try {
            const response = await fetch(
                `/api/predictors/${predictorId}/simulation?profit_rule_id=${encodeURIComponent(this.selectedProfitRuleId)}&metric=${encodeURIComponent(this.selectedProfitMetric)}&period_key=${encodeURIComponent(this.selectedProfitPeriodKey)}&odds_profile=${encodeURIComponent(this.selectedProfitOddsProfile)}&bet_profile_id=${encodeURIComponent(this.selectedProfitBetProfileId || '')}&bet_mode=${encodeURIComponent(this.selectedProfitBetMode)}&base_stake=${encodeURIComponent(this.selectedProfitBaseStake)}&multiplier=${encodeURIComponent(this.selectedProfitMultiplier)}&max_steps=${encodeURIComponent(this.selectedProfitMaxSteps)}`,
                { credentials: 'include', signal: controller.signal }
            );
            if (response.status === 401) {
                window.location.href = '/login';
                return;
            }

            const data = await response.json();
            if (!response.ok) {
                throw new Error(data.error || '加载收益模拟失败');
            }
            if (requestSequence !== this.profitSimulationLoadSequence || predictorId !== this.currentPredictorId) {
                return;
            }

            this.currentProfitSimulation = data.simulation;
            this.renderProfitSimulation(data.simulation, predictor);
        } catch (error) {
            if (error.name === 'AbortError') {
                return;
            }
            if (requestSequence !== this.profitSimulationLoadSequence || predictorId !== this.currentPredictorId) {
                return;
            }
            console.error('Failed to load profit simulation:', error);
            this.renderProfitSimulationEmpty(error.message);
        } finally {
            if (this.profitSimulationFetchController === controller) {
                this.profitSimulationFetchController = null;
            }
        }
    }

    renderProfitSimulation(simulation, predictor) {
        this.currentProfitSimulation = simulation || null;
        const orderedRecords = this.orderProfitRecords(simulation.records || []);
        this.renderProfitSimulationHint(simulation, predictor);
        this.renderProfitSummary(simulation);
        this.renderProfitChart(orderedRecords);
        this.renderProfitTable(orderedRecords);
    }

    renderProfitSimulationHint(simulation, predictor) {
        const container = document.getElementById('profitSimulationHint');
        const period = simulation.period || {};
        const summary = simulation.summary || {};
        const selectedBetProfile = (this.betProfiles || []).find((item) => String(item.id) === String(this.selectedProfitBetProfileId));
        const primaryMetric = predictor?.primary_metric || '';
        const fallbackText = primaryMetric && primaryMetric !== simulation.metric
            ? `当前方案主玩法为 ${this.escapeHtml(this.targetLabel(primaryMetric, predictor?.lottery_type || this.currentLotteryType))}，收益模拟已回退为 ${this.escapeHtml(simulation.metric_label || '--')}。`
            : '当前默认按方案主玩法计算；切换其他玩法时才会额外发起计算。';
        const periodText = period.start_time && period.end_time
            ? `${period.label || '模拟区间'}：${period.start_time} 至 ${period.end_time}`
            : `${period.label || '模拟区间'}：--`;
        const oddsText = simulation.odds_source_label
            ? `赔率口径：${simulation.odds_source_label}`
            : `赔率盘：${simulation.odds_profile_label || '--'}`;

        container.className = 'metric-hint';
        container.innerHTML = `
            <div class="metric-hint-head">
                <div>
                    <strong>${this.escapeHtml(simulation.profit_rule_label || '--')} · ${this.escapeHtml(simulation.metric_label || '--')} · ${this.escapeHtml(simulation.odds_profile_label || '--')}</strong>
                    <span class="metric-hint-alias">${this.escapeHtml(periodText)}</span>
                </div>
                <span class="tag">${this.escapeHtml(simulation.bet_strategy_label || '--')}</span>
            </div>
            <p>${fallbackText}</p>
            ${selectedBetProfile ? `<p>当前已套用下注策略：<strong>${this.escapeHtml(selectedBetProfile.name || '--')}</strong>，系统会优先使用已保存的下注参数。</p>` : ''}
            <p class="metric-hint-foot">${this.escapeHtml(this.buildProfitHintFootText(simulation, summary, oddsText))}</p>
        `;
    }

    renderProfitSummary(simulation) {
        const container = document.getElementById('profitSummaryGrid');
        const summary = simulation.summary || {};
        const cards = [
            ['收益规则', simulation.profit_rule_label || '--'],
            ['当前玩法', simulation.metric_label || '--'],
            ['下注策略', simulation.bet_strategy_label || '--'],
            ['赔率口径', simulation.odds_source_label || simulation.odds_profile_label || '--'],
            ['总下注', this.formatUsd(summary.total_stake || 0)],
            ['净收益', this.formatSignedUsd(summary.net_profit || 0)],
            ['ROI', this.formatPercent(summary.roi_percentage || 0)],
            ['单注平均收益', this.formatSignedUsd(summary.average_profit || 0)],
            ['命中 / 回本 / 未中', `${summary.hit_count || 0} / ${summary.refund_count || 0} / ${summary.miss_count || 0}`]
        ];

        container.innerHTML = cards.map(([label, value]) => `
            <article class="stat-card">
                <span class="stat-label">${this.escapeHtml(label)}</span>
                <strong class="stat-value ${this.profitValueClass(label === 'ROI' ? summary.roi_percentage || 0 : summary.net_profit || 0, label)}">${this.escapeHtml(String(value))}</strong>
            </article>
        `).join('');
    }

    renderProfitChart(records) {
        const chartDom = document.getElementById('profitSimulationChart');
        if (!this.profitChart) {
            this.profitChart = echarts.init(chartDom, this.darkMode ? 'dark' : null);
        }

        if (!records.length) {
            this.profitChart.clear();
            this.profitChart.setOption({
                title: {
                    text: '当前区间暂无收益数据',
                    left: 'center',
                    top: 'middle',
                    textStyle: { color: this.darkMode ? '#94a3b8' : '#64748b', fontSize: 14 }
                }
            });
            return;
        }

        this.profitChart.setOption({
            animation: false,
            title: { show: false },
            tooltip: { trigger: 'axis' },
            grid: { top: 36, left: 36, right: 24, bottom: 36, containLabel: true },
            xAxis: {
                type: 'category',
                inverse: this.selectedProfitOrder === 'desc',
                data: records.map((item) => item.issue_no),
                axisLabel: { color: this.darkMode ? '#94a3b8' : '#64748b' },
                axisLine: { lineStyle: { color: this.darkMode ? '#334155' : '#cbd5e1' } }
            },
            yAxis: {
                type: 'value',
                axisLabel: {
                    formatter: (value) => `${value}U`,
                    color: this.darkMode ? '#94a3b8' : '#64748b'
                },
                splitLine: { lineStyle: { color: this.darkMode ? '#1e293b' : '#e2e8f0' } }
            },
            series: [
                {
                    name: '累计盈亏',
                    type: 'line',
                    smooth: true,
                    data: records.map((item) => item.cumulative_profit),
                    lineStyle: { width: 3, color: '#f59e0b' },
                    itemStyle: { color: '#f59e0b' }
                }
            ]
        });
    }

    renderProfitTable(records) {
        const tbody = document.getElementById('profitSimulationBody');
        const cardsContainer = document.getElementById('profitSimulationCards');
        if (!records.length) {
            tbody.innerHTML = '<tr><td colspan="10" class="empty-cell">当前区间暂无收益模拟记录</td></tr>';
            if (cardsContainer) {
                cardsContainer.innerHTML = '<div class="empty-panel">当前区间暂无收益模拟记录</div>';
            }
            return;
        }

        tbody.innerHTML = records.map((item) => `
            <tr>
                <td>${this.escapeHtml(item.issue_no || '--')}</td>
                <td>${this.escapeHtml(item.open_time || '--')}</td>
                <td>${this.escapeHtml(item.ticket_label || '--')}</td>
                <td>${this.escapeHtml(this.formatUsd(item.stake_amount || 0))}<br><span class="hint-text">${this.escapeHtml(item.bet_step_label || '--')}</span></td>
                <td>${this.escapeHtml(item.predicted_value || '--')}</td>
                <td>${this.escapeHtml(item.actual_value || '--')}</td>
                <td>${this.formatOdds(item.odds)}</td>
                <td>${this.renderProfitResult(item)}</td>
                <td><strong class="${this.profitValueClass(item.net_profit)}">${this.escapeHtml(this.formatSignedUsd(item.net_profit))}</strong></td>
                <td><strong class="${this.profitValueClass(item.cumulative_profit)}">${this.escapeHtml(this.formatSignedUsd(item.cumulative_profit))}</strong></td>
            </tr>
        `).join('');
        if (cardsContainer) {
            cardsContainer.innerHTML = this.renderProfitCards(records);
        }
    }

    renderProfitResult(item) {
        const reasonText = item.refund_reason ? `<span class="result-meta-text">${this.escapeHtml(item.refund_reason)}</span>` : '';
        return `
            <div class="hit-summary-block">
                <span class="hit-pill ${this.profitResultClass(item.result_type)}">${this.escapeHtml(item.result_label || '--')}</span>
                ${reasonText}
            </div>
        `;
    }

    renderProfitSimulationEmpty(message) {
        this.currentProfitSimulation = null;
        document.getElementById('profitSimulationHint').className = 'metric-hint empty-panel';
        document.getElementById('profitSimulationHint').textContent = message || '暂无收益模拟数据';
        document.getElementById('profitSummaryGrid').innerHTML = '';
        document.getElementById('profitSimulationBody').innerHTML = '<tr><td colspan="10" class="empty-cell">暂无收益模拟数据</td></tr>';
        const cardsContainer = document.getElementById('profitSimulationCards');
        if (cardsContainer) {
            cardsContainer.innerHTML = `<div class="empty-panel">${this.escapeHtml(message || '暂无收益模拟数据')}</div>`;
        }
        this.renderProfitChart([]);
    }

    renderProfitSimulationLoading(message) {
        const loadingMessage = message || '收益模拟计算中...';
        this.currentProfitSimulation = null;
        document.getElementById('profitSimulationHint').className = 'metric-hint empty-panel';
        document.getElementById('profitSimulationHint').textContent = loadingMessage;
        document.getElementById('profitSummaryGrid').innerHTML = '';
        document.getElementById('profitSimulationBody').innerHTML = '<tr><td colspan="10" class="empty-cell">收益模拟计算中...</td></tr>';
        const cardsContainer = document.getElementById('profitSimulationCards');
        if (cardsContainer) {
            cardsContainer.innerHTML = `<div class="empty-panel">${this.escapeHtml(loadingMessage)}</div>`;
        }
        this.renderProfitChart([]);
    }

    orderProfitRecords(records) {
        const normalized = [...(records || [])];
        if (this.selectedProfitOrder === 'asc') {
            return normalized;
        }
        return normalized.reverse();
    }

    syncProfitBetControlState() {
        const betModeSelect = document.getElementById('profitBetModeView');
        const baseStakeInput = document.getElementById('profitBaseStakeView');
        const multiplierInput = document.getElementById('profitMultiplierView');
        const maxStepsInput = document.getElementById('profitMaxStepsView');
        const hasProfileOverride = Boolean(this.selectedProfitBetProfileId);
        const isFlatMode = this.selectedProfitBetMode === DEFAULT_PROFIT_BET_MODE;
        betModeSelect.hidden = hasProfileOverride;
        baseStakeInput.hidden = hasProfileOverride;
        multiplierInput.hidden = hasProfileOverride || isFlatMode;
        maxStepsInput.hidden = hasProfileOverride || isFlatMode;
        betModeSelect.disabled = hasProfileOverride;
        baseStakeInput.disabled = hasProfileOverride;
        multiplierInput.disabled = hasProfileOverride || isFlatMode;
        maxStepsInput.disabled = hasProfileOverride || isFlatMode;
    }

    syncProfitOptionVisibility({
        ruleCount = 0,
        metricCount = 0,
        periodCount = 0,
        oddsCount = 0,
        hasSavedBetProfiles = false
    } = {}) {
        this.setProfitControlVisibility(document.getElementById('profitRuleView'), ruleCount > 1);
        this.setProfitControlVisibility(document.getElementById('profitMetricView'), metricCount > 1);
        this.setProfitControlVisibility(document.getElementById('profitPeriodView'), periodCount > 1);
        this.setProfitControlVisibility(document.getElementById('profitOddsProfileView'), oddsCount > 1);
        this.setProfitControlVisibility(document.getElementById('profitBetProfileView'), hasSavedBetProfiles);
    }

    setProfitControlVisibility(element, shouldShow) {
        if (!element) {
            return;
        }
        element.hidden = !shouldShow;
        if (!shouldShow) {
            element.disabled = true;
        }
    }

    buildProfitHintFootText(simulation, summary, oddsText) {
        const items = [
            `基础注 ${this.formatUsd(simulation.bet_config?.base_stake || 0)}`
        ];
        if (simulation.bet_mode === 'martingale') {
            items.push(
                `倍投 ${Number(simulation.bet_config?.multiplier || DEFAULT_PROFIT_MULTIPLIER).toFixed(2)} × ${Number.parseInt(simulation.bet_config?.max_steps || DEFAULT_PROFIT_MAX_STEPS, 10)} 手`
            );
        }
        items.push(oddsText);
        items.push(`当前共计下注 ${summary.bet_count || 0} 笔模拟票`);
        return items.join(' · ');
    }

    normalizePositiveNumber(value, fallback, min = 0.01, max = 1000000) {
        const parsed = Number(value);
        if (!Number.isFinite(parsed)) {
            return fallback;
        }
        return Math.min(max, Math.max(min, Number(parsed.toFixed(2))));
    }

    normalizePositiveInt(value, fallback, min = 1, max = 12) {
        const parsed = Number.parseInt(value, 10);
        if (!Number.isInteger(parsed)) {
            return fallback;
        }
        return Math.min(max, Math.max(min, parsed));
    }

    renderPredictionsTable(predictions) {
        const tbody = document.getElementById('predictionsBody');
        const cardsContainer = document.getElementById('predictionsCards');
        const filteredPredictions = this.getPreviewItems(this.filterPredictions(predictions));

        if (!filteredPredictions.length) {
            tbody.innerHTML = '<tr><td colspan="8" class="empty-cell">当前筛选条件下暂无记录</td></tr>';
            if (cardsContainer) {
                cardsContainer.innerHTML = '<div class="empty-panel">当前筛选条件下暂无记录</div>';
            }
            return;
        }

        tbody.innerHTML = filteredPredictions.map((prediction) => `
            <tr>
                <td>${this.escapeHtml(prediction.issue_no)}</td>
                <td><span class="status-chip ${prediction.status}">${this.predictionStatusLabel(prediction.status)}</span></td>
                <td>${this.renderPredictionResult(prediction)}</td>
                <td>${this.renderActualResult(prediction)}</td>
                <td>${this.renderHitSummary(prediction)}</td>
                <td>${this.formatPercent(prediction.confidence !== null && prediction.confidence !== undefined ? prediction.confidence * 100 : null)}</td>
                <td>${this.formatPercent(prediction.score_percentage)}</td>
                <td>${this.escapeHtml(prediction.settled_at || '--')}</td>
            </tr>
        `).join('');
        if (cardsContainer) {
            cardsContainer.innerHTML = this.renderPredictionCards(filteredPredictions);
        }
    }

    filterPredictions(predictions) {
        return (predictions || []).filter((prediction) => {
            if (this.predictionStatusFilter !== 'all' && prediction.status !== this.predictionStatusFilter) {
                return false;
            }

            if (this.predictionOutcomeFilter === 'all') {
                return true;
            }

            const hitValues = this.buildHitItems(prediction).map((item) => item.value);

            if (!hitValues.length) {
                return false;
            }

            if (this.predictionOutcomeFilter === 'hit') {
                return hitValues.some((value) => value === 1);
            }

            if (this.predictionOutcomeFilter === 'miss') {
                return hitValues.every((value) => value === 0);
            }

            return true;
        });
    }

    renderDrawsTable(draws) {
        const tbody = document.getElementById('drawsBody');
        const headerRow = document.getElementById('drawTableHeadRow');
        const cardsContainer = document.getElementById('drawsCards');
        const visibleDraws = this.getPreviewItems(draws || []);
        if ((this.currentLotteryType || 'pc28') === 'jingcai_football') {
            headerRow.innerHTML = `
                <th>场次</th>
                <th>联赛</th>
                <th>对阵</th>
                <th>胜平负</th>
                <th>让球胜平负</th>
                <th>状态 / 时间 / 比分</th>
            `;
        } else {
            headerRow.innerHTML = `
                <th>期号</th>
                <th>开奖号码</th>
                <th>大/小</th>
                <th>单/双</th>
                <th>组合结果</th>
                <th>开奖时间</th>
            `;
        }
        if (!visibleDraws.length) {
            tbody.innerHTML = '<tr><td colspan="6" class="empty-cell">暂无官方开奖数据</td></tr>';
            if (cardsContainer) {
                cardsContainer.innerHTML = '<div class="empty-panel">暂无官方开奖数据</div>';
            }
            return;
        }

        if ((this.currentLotteryType || 'pc28') === 'jingcai_football') {
            tbody.innerHTML = visibleDraws.map((draw) => {
                const meta = draw.meta_payload || {
                    match_no: draw.match_no,
                    spf_odds: draw.spf_odds,
                    rqspf: draw.rqspf,
                    settled: draw.settled
                };
                const result = draw.result_payload || {
                    score1: draw.score1,
                    score2: draw.score2
                };
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
            if (cardsContainer) {
                cardsContainer.innerHTML = this.renderDrawCards(visibleDraws);
            }
            return;
        }

        tbody.innerHTML = visibleDraws.map((draw) => `
            <tr>
                <td>${this.escapeHtml(draw.issue_no)}</td>
                <td><strong>${this.escapeHtml(draw.result_number_text)}</strong></td>
                <td>${draw.big_small}</td>
                <td>${draw.odd_even}</td>
                <td>${draw.combo}</td>
                <td>${this.escapeHtml(draw.open_time || '')}</td>
            </tr>
        `).join('');
        if (cardsContainer) {
            cardsContainer.innerHTML = this.renderDrawCards(visibleDraws);
        }
    }

    renderPredictionCards(predictions) {
        return (predictions || []).map((prediction) => {
            const statusText = this.predictionStatusLabel(prediction.status);
            const metaText = [
                `置信度 ${this.formatPercent(prediction.confidence !== null && prediction.confidence !== undefined ? prediction.confidence * 100 : null)}`,
                `得分 ${this.formatPercent(prediction.score_percentage)}`,
                `结算 ${this.escapeHtml(prediction.settled_at || '--')}`
            ].join(' · ');

            return this.renderMobileDataCard({
                title: `期号 ${this.escapeHtml(prediction.issue_no || '--')}`,
                badgeHtml: `<span class="status-chip ${prediction.status}">${statusText}</span>`,
                sections: [
                    { label: '预测明细', content: this.renderPredictionResult(prediction) },
                    { label: '开奖明细', content: this.renderActualResult(prediction) },
                    { label: '命中明细', content: this.renderHitSummary(prediction) }
                ],
                footer: metaText
            });
        }).join('');
    }

    renderDrawCards(draws) {
        if ((this.currentLotteryType || 'pc28') === 'jingcai_football') {
            return draws.map((draw) => {
                const meta = draw.meta_payload || {
                    match_no: draw.match_no,
                    spf_odds: draw.spf_odds,
                    rqspf: draw.rqspf,
                    settled: draw.settled
                };
                const result = draw.result_payload || {
                    score1: draw.score1,
                    score2: draw.score2
                };
                const spfOdds = meta.spf_odds || {};
                const rqspf = meta.rqspf || {};
                const spfText = ['胜', '平', '负'].map((key) => `${key}:${spfOdds[key] ?? '--'}`).join(' / ');
                const rqText = ['胜', '平', '负'].map((key) => `${key}:${(rqspf.odds || {})[key] ?? '--'}`).join(' / ');
                const scoreText = result.score1 !== null && result.score1 !== undefined && result.score2 !== null && result.score2 !== undefined
                    ? `${result.score1}:${result.score2}`
                    : '--';
                const statusLabel = this.footballMatchStatusLabel(draw);
                const teams = draw.home_team && draw.away_team
                    ? `${this.escapeHtml(draw.home_team)} vs ${this.escapeHtml(draw.away_team)}`
                    : this.escapeHtml(draw.event_name || '--');

                return this.renderMobileDataCard({
                    title: `${this.escapeHtml(meta.match_no || draw.event_key || '--')} · ${teams}`,
                    badgeHtml: `<span class="tag">${this.escapeHtml(statusLabel)}</span>`,
                    sections: [
                        { label: '联赛', content: this.escapeHtml(draw.league || '--') },
                        { label: '胜平负赔率', content: this.escapeHtml(spfText) },
                        { label: '让球胜平负赔率', content: this.escapeHtml((rqspf.handicap_text || '--') + ' [' + rqText + ']') },
                        { label: '比赛时间 / 比分', content: `${this.escapeHtml(draw.event_time || '--')}<br><span class="hint-text">${this.escapeHtml(scoreText)}</span>` }
                    ]
                });
            }).join('');
        }

        return draws.map((draw) => this.renderMobileDataCard({
            title: `期号 ${this.escapeHtml(draw.issue_no || '--')}`,
            sections: [
                { label: '开奖号码', content: `<strong>${this.escapeHtml(draw.result_number_text || '--')}</strong>` },
                { label: '大/小', content: this.escapeHtml(draw.big_small || '--') },
                { label: '单/双', content: this.escapeHtml(draw.odd_even || '--') },
                { label: '组合结果', content: this.escapeHtml(draw.combo || '--') },
                { label: '开奖时间', content: this.escapeHtml(draw.open_time || '--') }
            ]
        })).join('');
    }

    renderProfitCards(records) {
        return (records || []).map((item) => this.renderMobileDataCard({
            title: `期号/批次 ${this.escapeHtml(item.issue_no || '--')}`,
            sections: [
                { label: '开奖 / 开赛时间', content: this.escapeHtml(item.open_time || '--') },
                { label: '下注内容', content: this.escapeHtml(item.ticket_label || '--') },
                { label: '下注额 / 手数', content: `${this.escapeHtml(this.formatUsd(item.stake_amount || 0))}<br><span class="hint-text">${this.escapeHtml(item.bet_step_label || '--')}</span>` },
                { label: '预测值', content: this.escapeHtml(item.predicted_value || '--') },
                { label: '实际值', content: this.escapeHtml(item.actual_value || '--') },
                { label: '赔率', content: this.formatOdds(item.odds) },
                { label: '结果', content: this.renderProfitResult(item) },
                { label: '单期盈亏', content: `<strong class="${this.profitValueClass(item.net_profit)}">${this.escapeHtml(this.formatSignedUsd(item.net_profit))}</strong>` },
                { label: '累计盈亏', content: `<strong class="${this.profitValueClass(item.cumulative_profit)}">${this.escapeHtml(this.formatSignedUsd(item.cumulative_profit))}</strong>` }
            ]
        })).join('');
    }

    renderMobileDataCard({ title, badgeHtml = '', sections = [], footer = '' }) {
        return `
            <article class="prediction-card mobile-data-card">
                <div class="prediction-section-head">
                    <strong>${title || '--'}</strong>
                    ${badgeHtml || ''}
                </div>
                <div class="detail-list">
                    ${sections.map((item) => `
                        <div class="detail-row">
                            <span class="detail-label">${this.escapeHtml(item.label || '--')}</span>
                            <div>${item.content || '--'}</div>
                        </div>
                    `).join('')}
                </div>
                ${footer ? `<div class="card-hint">${footer}</div>` : ''}
            </article>
        `;
    }

    renderAILogs(predictions) {
        const container = document.getElementById('aiLogs');
        const visiblePredictions = this.getPreviewItems(predictions || []);
        if (!visiblePredictions.length) {
            container.innerHTML = '<div class="empty-panel">暂无 AI 输出记录</div>';
            return;
        }

        if ((this.currentLotteryType || 'pc28') === 'jingcai_football') {
            container.innerHTML = visiblePredictions.map((prediction) => `
                <article class="ai-log-card">
                    <div class="ai-log-head">
                        <div>
                            <strong>${this.escapeHtml(prediction.issue_no || prediction.title || '--')}</strong>
                            <span class="status-chip ${prediction.status}">${this.predictionStatusLabel(prediction.status)}</span>
                            ${prediction.batch_failure_label ? `<span class="tag">子批次 ${this.escapeHtml(prediction.batch_failure_label)}</span>` : ''}
                        </div>
                        <span>${this.escapeHtml(prediction.created_at || prediction.run_created_at || '--')}</span>
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
            return;
        }

        container.innerHTML = visiblePredictions.map((prediction) => `
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
        const xAxisData = ordered.map((item) => item.issue_no || item.event_key || '--');
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

    handleChartResize() {
        if (this.chart) {
            this.chart.resize();
        }
        if (this.profitChart) {
            this.profitChart.resize();
        }
    }

    renderEmptyPredictorState() {
        this.currentStats = null;
        this.currentLotteryType = 'pc28';
        this.selectedStatsMetric = null;
        this.selectedProfitRuleId = 'pc28_netdisk';
        this.selectedProfitMetric = null;
        this.selectedProfitPeriodKey = DEFAULT_PROFIT_PERIOD_KEY;
        this.selectedProfitBetProfileId = '';
        this.selectedProfitBetMode = DEFAULT_PROFIT_BET_MODE;
        this.selectedProfitBaseStake = DEFAULT_PROFIT_BASE_STAKE;
        this.selectedProfitMultiplier = DEFAULT_PROFIT_MULTIPLIER;
        this.selectedProfitMaxSteps = DEFAULT_PROFIT_MAX_STEPS;
        this.selectedProfitOrder = 'desc';
        this.currentProfitSimulation = null;
        this.currentPredictions = [];
        this.updatePredictorActionState(null);
        this.renderSubscriptionPredictorOptions();
        this.renderNotificationSenderOptions();
        this.syncSubscriptionBetProfileOptions();
        this.updateHistoryLinks();
        this.renderCurrentPredictorSubscriptionSummary([]);
        this.renderNotificationSubscriptions([]);
        this.renderPredictorGuardPanel(null);
        document.getElementById('currentPrediction').className = 'prediction-summary empty-panel';
        document.getElementById('currentPrediction').textContent = '暂无预测方案，请先新建方案';
        this.hideFootballActionResult();
        document.getElementById('publicSharePanel').className = 'prediction-summary empty-panel';
        document.getElementById('publicSharePanel').textContent = '暂无预测方案，请先新建方案';
        document.getElementById('profitPanel').style.display = '';
        document.getElementById('statsGrid').innerHTML = '';
        document.getElementById('statsMetricHint').className = 'metric-hint empty-panel';
        document.getElementById('statsMetricHint').textContent = '请选择玩法查看统计口径';
        document.getElementById('metricStatsBody').innerHTML = '<tr><td colspan="4" class="empty-cell">暂无统计数据</td></tr>';
        document.getElementById('streakStats').innerHTML = '<div class="empty-panel">暂无连中连挂数据</div>';
        document.getElementById('profitRuleView').innerHTML = '<option value="">暂无规则</option>';
        document.getElementById('profitRuleView').disabled = true;
        document.getElementById('profitMetricView').innerHTML = '<option value="">暂无玩法</option>';
        document.getElementById('profitMetricView').disabled = true;
        document.getElementById('profitPeriodView').innerHTML = '<option value="">暂无区间</option>';
        document.getElementById('profitPeriodView').disabled = true;
        document.getElementById('profitBetProfileView').hidden = true;
        document.getElementById('profitBetProfileView').innerHTML = '<option value="">手动输入参数</option>';
        document.getElementById('profitBetProfileView').disabled = true;
        document.getElementById('profitBetModeView').value = this.selectedProfitBetMode;
        document.getElementById('profitBetModeView').disabled = true;
        document.getElementById('profitBaseStakeView').value = String(this.selectedProfitBaseStake);
        document.getElementById('profitBaseStakeView').disabled = true;
        document.getElementById('profitMultiplierView').value = String(this.selectedProfitMultiplier);
        document.getElementById('profitMultiplierView').disabled = true;
        document.getElementById('profitMaxStepsView').value = String(this.selectedProfitMaxSteps);
        document.getElementById('profitMaxStepsView').disabled = true;
        document.getElementById('profitOrderView').value = this.selectedProfitOrder;
        document.getElementById('profitOrderView').disabled = true;
        document.getElementById('profitOddsProfileView').value = this.selectedProfitOddsProfile;
        document.getElementById('profitOddsProfileView').disabled = true;
        this.syncProfitOptionVisibility({
            ruleCount: 0,
            metricCount: 0,
            periodCount: 0,
            oddsCount: 0,
            hasSavedBetProfiles: false
        });
        this.syncProfitBetControlState();
        document.getElementById('profitSimulationHint').className = 'metric-hint empty-panel';
        document.getElementById('profitSimulationHint').textContent = '请选择预测方案';
        document.getElementById('profitSummaryGrid').innerHTML = '';
        document.getElementById('profitSimulationBody').innerHTML = '<tr><td colspan="10" class="empty-cell">暂无收益模拟数据</td></tr>';
        document.getElementById('profitSimulationCards').innerHTML = '<div class="empty-panel">暂无收益模拟数据</div>';
        document.getElementById('predictionsBody').innerHTML = '<tr><td colspan="8" class="empty-cell">暂无预测记录</td></tr>';
        document.getElementById('predictionsCards').innerHTML = '<div class="empty-panel">暂无预测记录</div>';
        document.getElementById('aiLogs').innerHTML = '<div class="empty-panel">暂无 AI 输出记录</div>';
        if (this.chart) {
            this.chart.clear();
        }
        if (this.profitChart) {
            this.profitChart.clear();
        }
    }

    switchTab(tabName) {
        this.activeContentTab = tabName;
        document.querySelectorAll('.tab-btn').forEach((button) => {
            button.classList.toggle('active', button.dataset.tab === tabName);
        });
        document.querySelectorAll('.tab-panel').forEach((panel) => {
            panel.classList.toggle('active', panel.id === `${tabName}Tab`);
        });
        if (this.isHistoryPage()) {
            this.loadPredictorHistoryTab(tabName);
        }
        this.handleChartResize();
    }

    openCreateModal() {
        document.getElementById('modalTitle').textContent = '新建预测方案';
        document.getElementById('predictorId').value = '';
        document.getElementById('lotteryType').disabled = false;
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
        document.getElementById('lotteryType').value = data.lottery_type || 'pc28';
        document.getElementById('lotteryType').disabled = true;
        this.formLotteryType = data.lottery_type || 'pc28';
        document.getElementById('predictorName').value = data.name || '';
        document.getElementById('engineType').value = data.engine_type || 'ai';
        document.getElementById('predictionMethod').value = data.prediction_method || '';
        document.getElementById('algorithmKey').value = data.algorithm_key || '';
        document.getElementById('apiUrl').value = data.api_url || '';
        document.getElementById('modelName').value = data.model_name || '';
        document.getElementById('apiMode').value = data.api_mode || 'auto';
        document.getElementById('apiKey').value = '';
        document.getElementById('temperature').value = data.temperature ?? 0.7;
        document.getElementById('dataInjectionMode').value = data.data_injection_mode || 'summary';
        const fallbackStrategy = document.getElementById('userAlgorithmFallbackStrategy');
        if (fallbackStrategy) {
            fallbackStrategy.value = data.user_algorithm_fallback_strategy || 'fail';
        }
        document.getElementById('systemPrompt').value = data.system_prompt || '';
        document.getElementById('predictorEnabled').checked = Boolean(data.enabled);
        document.getElementById('shareLevel').value = data.share_level || (data.share_predictions ? 'records' : 'stats_only');
        this.updateLotteryForm({
            selectedTargets: data.prediction_targets || [],
            selectedPrimaryMetric: data.primary_metric || null,
            selectedProfitRuleId: data.profit_rule_id || this.getLotteryConfig(this.formLotteryType).defaultProfitRuleId,
            selectedProfitDefaultMetric: data.profit_default_metric || data.default_simulation_metric || this.getLotteryConfig(this.formLotteryType).defaultProfitMetric,
            selectedHistoryWindow: data.history_window || 60,
            selectedAlgorithmKey: data.algorithm_key || ''
        });
        document.getElementById('historyWindow').value = data.history_window || 60;
        this.clearExternalPromptTemplate();
        this.presetExpanded = false;
        this.renderPresetCards();
        this.showModal();
    }

    showModal() {
        this.hideTestResult();
        this.hidePromptAssistantResult();
        document.getElementById('predictorModal').classList.add('show');
    }

    hideModal() {
        document.getElementById('predictorModal').classList.remove('show');
    }

    async openUserAlgorithmModal() {
        await this.loadUserAlgorithms();
        await this.loadUserAlgorithmTemplates();
        this.resetUserAlgorithmForm();
        document.getElementById('userAlgorithmModal').classList.add('show');
    }

    hideUserAlgorithmModal() {
        document.getElementById('userAlgorithmModal').classList.remove('show');
    }

    resetUserAlgorithmForm() {
        const lotteryType = document.getElementById('userAlgorithmLotteryFilter')?.value || 'jingcai_football';
        this.selectedUserAlgorithmId = null;
        this.lastUserAlgorithmBacktest = null;
        this.clearAlgorithmChat();
        document.getElementById('userAlgorithmId').value = '';
        document.getElementById('userAlgorithmName').value = '';
        document.getElementById('userAlgorithmDescription').value = '';
        document.getElementById('userAlgorithmLotteryType').value = lotteryType;
        this.resetUserAlgorithmBacktestFilters();
        this.loadUserAlgorithmSample();
        this.hideUserAlgorithmResult();
        this.renderUserAlgorithmOpsSummary(null);
        this.renderUserAlgorithmVersions([]);
        this.renderUserAlgorithmExecutionLogs([]);
    }

    resetUserAlgorithmBacktestFilters() {
        ['userAlgorithmBacktestStartDate', 'userAlgorithmBacktestEndDate', 'userAlgorithmBacktestRecentN', 'userAlgorithmBacktestLeagues'].forEach((id) => {
            const element = document.getElementById(id);
            if (element) {
                element.value = '';
            }
        });
        const marketType = document.getElementById('userAlgorithmBacktestMarketType');
        if (marketType) {
            marketType.value = '';
        }
    }

    async loadUserAlgorithmTemplates() {
        try {
            const response = await fetch('/api/user-algorithms/templates?lottery_type=jingcai_football', { credentials: 'include' });
            const data = await response.json();
            if (!response.ok) {
                throw new Error(data.error || '加载算法模板失败');
            }
            this.algorithmTemplates = data || [];
        } catch (error) {
            this.algorithmTemplates = [];
        }
        this.renderUserAlgorithmTemplateOptions();
    }

    renderUserAlgorithmTemplateOptions() {
        const select = document.getElementById('userAlgorithmTemplateSelect');
        if (!select) {
            return;
        }
        const options = (this.algorithmTemplates || []).map((item) => (
            `<option value="${this.escapeHtml(item.key)}">${this.escapeHtml(item.name)}</option>`
        ));
        select.innerHTML = `<option value="">选择模板</option>${options.join('')}`;
    }

    applyUserAlgorithmTemplate() {
        const key = document.getElementById('userAlgorithmTemplateSelect')?.value || '';
        const template = (this.algorithmTemplates || []).find((item) => item.key === key);
        if (!template) {
            this.showUserAlgorithmResult('error', '请先选择模板');
            return;
        }
        document.getElementById('userAlgorithmName').value = template.name || '';
        document.getElementById('userAlgorithmDescription').value = template.description || '';
        document.getElementById('userAlgorithmLotteryType').value = 'jingcai_football';
        document.getElementById('userAlgorithmDefinition').value = JSON.stringify(template.definition || {}, null, 2);
        this.showUserAlgorithmResult('success', `已填入模板：${template.name}`);
    }

    clearAlgorithmChat() {
        this.algorithmChatMessages = [];
        const input = document.getElementById('algorithmAiMessage');
        if (input) {
            input.value = '';
        }
        this.renderAlgorithmChat();
    }

    renderAlgorithmChat() {
        const container = document.getElementById('algorithmChatLog');
        if (!container) {
            return;
        }
        if (!this.algorithmChatMessages.length) {
            container.innerHTML = '<div class="empty-panel">暂无对话</div>';
            return;
        }
        container.innerHTML = this.algorithmChatMessages.map((message) => `
            <div class="algorithm-chat-message ${message.role === 'user' ? 'user' : 'assistant'}">
                <span>${message.role === 'user' ? '你' : 'AI'}</span>
                <p>${this.escapeHtml(message.content || '')}</p>
            </div>
        `).join('');
        container.scrollTop = container.scrollHeight;
    }

    loadUserAlgorithmSample() {
        const lotteryType = document.getElementById('userAlgorithmLotteryType')?.value || 'jingcai_football';
        const textarea = document.getElementById('userAlgorithmDefinition');
        if (!textarea) {
            return;
        }
        const sample = lotteryType === 'jingcai_football'
            ? this.buildFootballAlgorithmSample()
            : this.buildPc28AlgorithmSample();
        textarea.value = JSON.stringify(sample, null, 2);
    }

    buildFootballAlgorithmSample() {
        return {
            schema_version: 1,
            method_name: '进球率预测法',
            lottery_type: 'jingcai_football',
            targets: ['spf'],
            data_window: {
                recent_matches: 6,
                history_matches: 30
            },
            filters: [
                { field: 'spf_odds.win', op: 'gte', value: 1.35 },
                { field: 'home_goals_per_match_6', op: 'gte', value: 1.2 }
            ],
            score: [
                { feature: 'home_goals_per_match_6', transform: 'linear', weight: 0.34 },
                { feature: 'away_conceded_per_match_6', transform: 'linear', weight: 0.28 },
                { feature: 'implied_probability_spf_win', transform: 'linear', weight: 0.22 },
                { feature: 'injury_advantage', transform: 'linear', weight: 0.16 }
            ],
            decision: {
                target: 'spf',
                pick: '胜',
                min_confidence: 0.58,
                allow_skip: true
            },
            explain: {
                template: '主队近6场进球率 {home_goals_per_match_6}，客队失球率 {away_conceded_per_match_6}'
            }
        };
    }

    buildPc28AlgorithmSample() {
        return {
            schema_version: 1,
            method_name: 'PC28 草稿算法',
            lottery_type: 'pc28',
            targets: ['number', 'big_small', 'odd_even', 'combo'],
            data_window: {
                history_matches: 60
            },
            filters: [],
            score: [
                { feature: 'number_omission', transform: 'linear', weight: 0.45 },
                { feature: 'combo_frequency_n', transform: 'linear', weight: 0.35 },
                { feature: 'combo_transition_score', transform: 'linear', weight: 0.2 }
            ],
            decision: {
                target: 'combo',
                pick: 'max_score',
                min_confidence: 0.55,
                allow_skip: true
            }
        };
    }

    renderUserAlgorithmList(lotteryType = 'jingcai_football') {
        const container = document.getElementById('userAlgorithmList');
        if (!container) {
            return;
        }
        const algorithms = (this.userAlgorithms || []).filter((item) => item.lottery_type === lotteryType);
        if (!algorithms.length) {
            container.innerHTML = '<div class="empty-panel">暂无用户算法</div>';
            return;
        }
        container.innerHTML = algorithms.map((item) => `
            <article class="algorithm-list-card ${Number(item.id) === Number(this.selectedUserAlgorithmId) ? 'selected' : ''}" data-user-algorithm-id="${item.id}">
                <div>
                    <strong>${this.escapeHtml(item.name)}</strong>
                    <p>${this.escapeHtml(item.description || '未填写说明')}</p>
                </div>
                <span class="status-chip ${item.status === 'validated' ? 'settled' : item.status === 'disabled' ? 'failed' : 'pending'}">${this.userAlgorithmStatusLabel(item.status)}</span>
            </article>
        `).join('');
        container.querySelectorAll('[data-user-algorithm-id]').forEach((card) => {
            card.addEventListener('click', () => this.selectUserAlgorithm(Number(card.dataset.userAlgorithmId)));
        });
    }

    selectUserAlgorithm(algorithmId) {
        const algorithm = (this.userAlgorithms || []).find((item) => Number(item.id) === Number(algorithmId));
        if (!algorithm) {
            return;
        }
        this.selectedUserAlgorithmId = algorithm.id;
        document.getElementById('userAlgorithmId').value = algorithm.id;
        document.getElementById('userAlgorithmName').value = algorithm.name || '';
        document.getElementById('userAlgorithmDescription').value = algorithm.description || '';
        document.getElementById('userAlgorithmLotteryType').value = algorithm.lottery_type || 'jingcai_football';
        document.getElementById('userAlgorithmDefinition').value = JSON.stringify(algorithm.definition || {}, null, 2);
        document.getElementById('userAlgorithmLotteryFilter').value = algorithm.lottery_type || 'jingcai_football';
        this.lastUserAlgorithmBacktest = algorithm.active_backtest || null;
        this.clearAlgorithmChat();
        this.renderUserAlgorithmList(algorithm.lottery_type || 'jingcai_football');
        this.hideUserAlgorithmResult();
        this.loadUserAlgorithmOpsSummary(algorithm.id);
        this.loadUserAlgorithmVersions(algorithm.id);
        this.loadUserAlgorithmExecutionLogs(algorithm.id);
    }

    parseUserAlgorithmDefinition() {
        const raw = document.getElementById('userAlgorithmDefinition')?.value || '';
        try {
            return JSON.parse(raw);
        } catch (error) {
            throw new Error('算法 JSON 格式错误');
        }
    }

    async validateUserAlgorithmForm() {
        const algorithmId = document.getElementById('userAlgorithmId').value;
        const lotteryType = document.getElementById('userAlgorithmLotteryType').value || 'jingcai_football';
        let definition;
        try {
            definition = this.parseUserAlgorithmDefinition();
        } catch (error) {
            this.showUserAlgorithmResult('error', error.message);
            return;
        }
        const url = algorithmId ? `/api/user-algorithms/${algorithmId}/validate` : '/api/user-algorithms/0/validate';
        if (!algorithmId) {
            const localPayload = { definition, lottery_type: lotteryType };
            try {
                const result = await this.validateUserAlgorithmDraft(localPayload);
                this.renderUserAlgorithmValidation(result);
            } catch (error) {
                this.showUserAlgorithmResult('error', error.message);
            }
            return;
        }
        try {
            const response = await fetch(url, {
                method: 'POST',
                credentials: 'include',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ definition, lottery_type: lotteryType })
            });
            const data = await response.json();
            if (!response.ok) {
                throw new Error(data.error || (data.errors || []).join('；') || '算法校验失败');
            }
            this.renderUserAlgorithmValidation(data);
        } catch (error) {
            this.showUserAlgorithmResult('error', error.message);
        }
    }

    async generateUserAlgorithmDraft() {
        const button = document.getElementById('generateUserAlgorithmBtn');
        const lotteryType = document.getElementById('userAlgorithmLotteryType').value || 'jingcai_football';
        const messageText = document.getElementById('algorithmAiMessage').value.trim();
        const payload = {
            lottery_type: lotteryType,
            api_key: document.getElementById('algorithmAiApiKey').value.trim(),
            api_url: document.getElementById('algorithmAiApiUrl').value.trim(),
            model_name: document.getElementById('algorithmAiModelName').value.trim(),
            api_mode: 'auto',
            message: messageText,
            chat_history: this.algorithmChatMessages.slice(-8),
            backtest_summary: this.lastUserAlgorithmBacktest || null
        };
        try {
            payload.current_definition = this.parseUserAlgorithmDefinition();
        } catch (error) {
            payload.current_definition = null;
        }
        if (!payload.api_key || !payload.api_url || !payload.model_name || !payload.message) {
            this.showUserAlgorithmResult('error', '请填写 API Key、API 地址、模型名称和算法思路');
            return;
        }

        this.algorithmChatMessages.push({ role: 'user', content: messageText });
        this.renderAlgorithmChat();
        document.getElementById('algorithmAiMessage').value = '';
        button.disabled = true;
        button.textContent = '生成中...';
        this.showUserAlgorithmResult('info', '正在调用你的模型生成算法草稿...');
        try {
            const response = await fetch('/api/user-algorithms/ai-draft', {
                method: 'POST',
                credentials: 'include',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });
            const data = await response.json();
            if (!response.ok) {
                throw new Error(data.error || 'AI 生成算法失败');
            }
            if (data.reply_type === 'need_clarification') {
                const questions = (data.questions || []).map((item) => `- ${item}`).join('\n');
                const assistantText = `${data.message || 'AI 需要补充信息'}${questions ? `\n${questions}` : ''}`;
                this.algorithmChatMessages.push({ role: 'assistant', content: assistantText });
                this.renderAlgorithmChat();
                this.showUserAlgorithmResult('info', assistantText);
                return;
            }
            document.getElementById('userAlgorithmDefinition').value = JSON.stringify(data.algorithm || {}, null, 2);
            const currentName = document.getElementById('userAlgorithmName').value.trim();
            if (!currentName && data.algorithm?.method_name) {
                document.getElementById('userAlgorithmName').value = data.algorithm.method_name;
            }
            const notes = [
                data.message || 'AI 已生成算法草稿',
                data.change_summary ? `变更：${data.change_summary}` : '',
                ...(data.risk_notes || []).map((item) => `提醒：${item}`)
            ].filter(Boolean);
            this.algorithmChatMessages.push({ role: 'assistant', content: notes.join('\n') || '已生成算法草稿' });
            this.renderAlgorithmChat();
            this.renderUserAlgorithmValidation(data.validation || { valid: false, errors: [], warnings: [] });
            if (notes.length) {
                this.showUserAlgorithmResult(
                    data.validation?.valid ? 'success' : 'error',
                    `${notes.join('\n')}\n\n${document.getElementById('userAlgorithmResult').textContent}`
                );
            }
        } catch (error) {
            this.algorithmChatMessages.push({ role: 'assistant', content: `生成失败：${error.message}` });
            this.renderAlgorithmChat();
            this.showUserAlgorithmResult('error', error.message);
        } finally {
            button.disabled = false;
            button.textContent = '发送给 AI';
        }
    }

    async aiAdjustUserAlgorithm() {
        const algorithmId = document.getElementById('userAlgorithmId')?.value;
        const button = document.getElementById('aiAdjustUserAlgorithmBtn');
        if (!algorithmId) {
            this.showUserAlgorithmResult('error', '请先保存算法，再让 AI 根据回测生成新版本');
            return;
        }
        const messageText = document.getElementById('algorithmAiMessage')?.value.trim()
            || '请根据最近回测结果优化当前算法，优先处理跳过率、命中率、低赔率热门和置信度门槛。';
        const payload = {
            api_key: document.getElementById('algorithmAiApiKey')?.value.trim() || '',
            api_url: document.getElementById('algorithmAiApiUrl')?.value.trim() || '',
            model_name: document.getElementById('algorithmAiModelName')?.value.trim() || '',
            api_mode: 'auto',
            message: messageText,
            chat_history: this.algorithmChatMessages.slice(-8),
            backtest_summary: this.lastUserAlgorithmBacktest || null
        };
        if (!payload.api_key || !payload.api_url || !payload.model_name) {
            this.showUserAlgorithmResult('error', '请填写 API Key、API 地址和模型名称');
            return;
        }

        this.algorithmChatMessages.push({ role: 'user', content: messageText });
        this.renderAlgorithmChat();
        if (button) {
            button.disabled = true;
            button.textContent = '调参中...';
        }
        this.showUserAlgorithmResult('info', '正在调用你的模型根据回测生成新版本...');
        try {
            const response = await fetch(`/api/user-algorithms/${algorithmId}/ai-adjust`, {
                method: 'POST',
                credentials: 'include',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });
            const data = await response.json();
            if (!response.ok) {
                throw new Error(data.error || 'AI 调参失败');
            }
            if (data.reply_type === 'need_clarification') {
                const questions = (data.questions || []).map((item) => `- ${item}`).join('\n');
                const assistantText = `${data.message || 'AI 需要补充信息'}${questions ? `\n${questions}` : ''}`;
                this.algorithmChatMessages.push({ role: 'assistant', content: assistantText });
                this.renderAlgorithmChat();
                this.showUserAlgorithmResult('info', assistantText);
                return;
            }
            await this.loadUserAlgorithms();
            this.selectUserAlgorithm(data.algorithm.id);
            this.renderUserAlgorithmVersions(data.versions || []);
            this.renderUserAlgorithmValidation(data.validation || { valid: data.algorithm.status === 'validated', errors: [], warnings: [] });
            const notes = [
                data.message || 'AI 已根据回测生成新版本',
                data.change_summary ? `变更：${data.change_summary}` : '',
                ...(data.risk_notes || []).map((item) => `提醒：${item}`)
            ].filter(Boolean);
            this.algorithmChatMessages.push({ role: 'assistant', content: notes.join('\n') || '已生成调参版本' });
            this.renderAlgorithmChat();
            this.showUserAlgorithmResult('success', notes.join('\n') || 'AI 已根据回测生成新版本');
            this.updateLotteryForm();
        } catch (error) {
            this.algorithmChatMessages.push({ role: 'assistant', content: `调参失败：${error.message}` });
            this.renderAlgorithmChat();
            this.showUserAlgorithmResult('error', error.message);
        } finally {
            if (button) {
                button.disabled = false;
                button.textContent = 'AI 调参生成新版本';
            }
        }
    }

    async validateUserAlgorithmDraft(payload) {
        const response = await fetch('/api/user-algorithms/validate', {
            method: 'POST',
            credentials: 'include',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });
        const data = await response.json();
        if (!response.ok) {
            throw new Error(data.error || (data.errors || []).join('；') || '算法校验失败');
        }
        return data;
    }

    async dryRunUserAlgorithm() {
        const button = document.getElementById('dryRunUserAlgorithmBtn');
        const lotteryType = document.getElementById('userAlgorithmLotteryType').value || 'jingcai_football';
        let definition;
        try {
            definition = this.parseUserAlgorithmDefinition();
        } catch (error) {
            this.showUserAlgorithmResult('error', error.message);
            return;
        }

        button.disabled = true;
        button.textContent = '试跑中...';
        this.showUserAlgorithmResult('info', '正在使用样例比赛试跑当前算法...');
        try {
            const response = await fetch('/api/user-algorithms/dry-run', {
                method: 'POST',
                credentials: 'include',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ lottery_type: lotteryType, definition })
            });
            const data = await response.json();
            if (!response.ok) {
                throw new Error(data.error || '算法试跑失败');
            }
            const firstItem = (data.items || [])[0] || {};
            const firstDebug = ((data.debug || {}).rows || [])[0] || {};
            const lines = [
                '试跑完成。',
                `样例场次：${firstItem.match_no || '--'}`,
                `胜平负：${firstItem.predicted_spf || '跳过'}`,
                `让球胜平负：${firstItem.predicted_rqspf || '跳过'}`,
                `置信度：${firstItem.confidence === null || firstItem.confidence === undefined ? '--' : firstItem.confidence}`,
                `评分：${firstDebug.score === undefined ? '--' : firstDebug.score}`,
                `说明：${firstItem.reasoning_summary || '--'}`
            ];
            this.showUserAlgorithmResult('success', lines.join('\n'));
        } catch (error) {
            this.showUserAlgorithmResult('error', error.message);
        } finally {
            button.disabled = false;
            button.textContent = '试跑';
        }
    }

    async backtestUserAlgorithm() {
        const button = document.getElementById('backtestUserAlgorithmBtn');
        const algorithmId = document.getElementById('userAlgorithmId').value;
        const lotteryType = document.getElementById('userAlgorithmLotteryType').value || 'jingcai_football';
        let definition;
        try {
            definition = this.parseUserAlgorithmDefinition();
        } catch (error) {
            this.showUserAlgorithmResult('error', error.message);
            return;
        }

        button.disabled = true;
        button.textContent = '回测中...';
        this.showUserAlgorithmResult('info', '正在读取本地已开奖赛事回测当前算法...');
        try {
            const url = algorithmId ? `/api/user-algorithms/${algorithmId}/backtest` : '/api/user-algorithms/backtest';
            const backtestFilters = this.buildUserAlgorithmBacktestFilters();
            const requestBody = algorithmId
                ? { limit: backtestFilters.recent_n || 50, ...backtestFilters }
                : { lottery_type: lotteryType, definition, limit: backtestFilters.recent_n || 50, ...backtestFilters };
            const response = await fetch(url, {
                method: 'POST',
                credentials: 'include',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(requestBody)
            });
            const data = await response.json();
            if (!response.ok) {
                throw new Error(data.error || '算法回测失败');
            }
            const backtest = data.backtest || {};
            this.lastUserAlgorithmBacktest = backtest;
            if (data.algorithm) {
                await this.loadUserAlgorithms();
                this.selectUserAlgorithm(data.algorithm.id);
            }
            this.showUserAlgorithmResult('success', this.buildUserAlgorithmBacktestMessage(backtest, Boolean(algorithmId), data.adjustment_suggestions || []));
        } catch (error) {
            this.showUserAlgorithmResult('error', error.message);
        } finally {
            button.disabled = false;
            button.textContent = '回测';
        }
    }

    buildUserAlgorithmBacktestFilters() {
        const leaguesRaw = document.getElementById('userAlgorithmBacktestLeagues')?.value || '';
        const recentN = Number(document.getElementById('userAlgorithmBacktestRecentN')?.value || 0);
        const payload = {
            start_date: document.getElementById('userAlgorithmBacktestStartDate')?.value || '',
            end_date: document.getElementById('userAlgorithmBacktestEndDate')?.value || '',
            recent_n: recentN > 0 ? recentN : null,
            market_type: document.getElementById('userAlgorithmBacktestMarketType')?.value || '',
            leagues: leaguesRaw.split(/[,，]/).map((item) => item.trim()).filter(Boolean)
        };
        Object.keys(payload).forEach((key) => {
            if (payload[key] === '' || payload[key] === null || (Array.isArray(payload[key]) && !payload[key].length)) {
                delete payload[key];
            }
        });
        return payload;
    }

    buildUserAlgorithmBacktestMessage(backtest, savedBacktest = false, suggestions = []) {
        const hitRate = backtest.hit_rate || {};
        const profitSummary = backtest.profit_summary || {};
        const dataQuality = backtest.data_quality || {};
        const confidence = backtest.confidence_report || {};
        const spfStats = hitRate.spf || {};
        const rqspfStats = hitRate.rqspf || {};
        const spfProfit = profitSummary.spf || {};
        const rqspfProfit = profitSummary.rqspf || {};
        const spfStreak = (backtest.streaks || {}).spf || {};
        const spfDrawdown = (backtest.max_drawdown || {}).spf || {};
        const skipStats = Object.values(backtest.skip_reason_stats || {}).map((item) => `${item.label}:${item.count}`).join('，') || '--';
        const missingStats = Object.entries((dataQuality || {}).missing_field_stats || {}).map(([key, value]) => `${key}:${value}`).join('，') || '--';
        const suggestionText = (suggestions || []).map((item) => `${item.label}：${item.reason}`).join('；') || '--';
        const spfChart = ((backtest.chart_data || {}).spf) || {};
        const trendText = (spfChart.hit_rate_trend || []).slice(-3).map((item) => `${item.index}:${item.hit_rate}%`).join('，') || '--';
        const equityCurve = spfChart.equity_curve || [];
        const equityEnd = equityCurve.length ? equityCurve[equityCurve.length - 1] : '--';
        const recentRecords = (backtest.records || []).slice(0, 5).map((item) => {
            const spfText = item.predicted_spf
                ? `SPF ${item.predicted_spf}/${item.actual_spf || '--'}`
                : 'SPF 跳过';
            const rqspfText = item.predicted_rqspf
                ? `RQSPF ${item.predicted_rqspf}/${item.actual_rqspf || '--'}`
                : 'RQSPF 跳过';
            return `${item.match_no || '--'} ${spfText}，${rqspfText}`;
        });
        const lines = [
            savedBacktest ? '回测完成，结果已保存到当前版本。' : '回测完成。',
            `可信度：${confidence.label || '--'}${confidence.score === null || confidence.score === undefined ? '' : `（${confidence.score}分）`}`,
            `样本数：${backtest.sample_size || 0}`,
            `有效样本数：${backtest.effective_sample_count || 0}`,
            `字段完整率：${dataQuality.field_completeness_rate === null || dataQuality.field_completeness_rate === undefined ? '--' : `${dataQuality.field_completeness_rate}%`}`,
            `产生预测：${backtest.prediction_count || 0}`,
            `跳过预测：${backtest.skip_count || 0}`,
            `SPF 命中：${spfStats.ratio_text || '--'}${spfStats.hit_rate === null || spfStats.hit_rate === undefined ? '' : `（${spfStats.hit_rate}%）`}`,
            `SPF 模拟收益：${spfProfit.net_profit === undefined ? '--' : spfProfit.net_profit}${spfProfit.roi === null || spfProfit.roi === undefined ? '' : `（ROI ${spfProfit.roi}%）`}`,
            `SPF 连红/连黑：最长连红 ${spfStreak.max_hit_streak || 0}，最长连黑 ${spfStreak.max_miss_streak || 0}`,
            `SPF 最大回撤：${spfDrawdown.amount === undefined ? '--' : spfDrawdown.amount}`,
            `SPF 趋势点：${trendText}`,
            `SPF 净值曲线末值：${equityEnd}`,
            `RQSPF 命中：${rqspfStats.ratio_text || '--'}${rqspfStats.hit_rate === null || rqspfStats.hit_rate === undefined ? '' : `（${rqspfStats.hit_rate}%）`}`,
            `RQSPF 模拟收益：${rqspfProfit.net_profit === undefined ? '--' : rqspfProfit.net_profit}${rqspfProfit.roi === null || rqspfProfit.roi === undefined ? '' : `（ROI ${rqspfProfit.roi}%）`}`,
            `跳过原因：${skipStats}`,
            `缺字段统计：${missingStats}`,
            ...(backtest.sample_bias_flags || []).map((item) => `偏差提示：${item}`),
            `调参建议：${suggestionText}`,
            ...(backtest.risk_flags || []).map((item) => `提醒：${item}`),
            ...recentRecords.map((item) => `样本：${item}`)
        ];
        if (!backtest.sample_size) {
            lines.push('本地暂无可用于回测的已开奖竞彩足球样本。');
        }
        return lines.join('\n');
    }

    renderUserAlgorithmValidation(validation) {
        const errors = validation.errors || [];
        const warnings = validation.warnings || [];
        const lines = [
            validation.valid ? '校验通过，可以保存并用于竞彩足球预测方案。' : '校验未通过。',
            ...errors.map((item) => `错误：${item}`),
            ...warnings.map((item) => `提醒：${item}`)
        ];
        this.showUserAlgorithmResult(validation.valid ? 'success' : 'error', lines.join('\n'));
    }

    async loadUserAlgorithmOpsSummary(algorithmId) {
        const container = document.getElementById('userAlgorithmOpsSummary');
        if (!container || !algorithmId) {
            this.renderUserAlgorithmOpsSummary(null);
            return;
        }
        try {
            const response = await fetch(`/api/user-algorithms/${algorithmId}/ops-summary`, { credentials: 'include' });
            const data = await response.json();
            if (!response.ok) {
                throw new Error(data.error || '加载运营摘要失败');
            }
            this.renderUserAlgorithmOpsSummary(data);
        } catch (error) {
            this.renderUserAlgorithmOpsSummary(null);
        }
    }

    renderUserAlgorithmOpsSummary(summary) {
        const container = document.getElementById('userAlgorithmOpsSummary');
        if (!container) {
            return;
        }
        if (!summary) {
            container.style.display = 'none';
            container.innerHTML = '';
            return;
        }
        const backtest = (summary.recent_backtest || {}).summary || {};
        const risk = summary.risk_summary || {};
        const spf = ((backtest.targets || {}).spf) || {};
        const logs = summary.recent_execution_logs || [];
        const predictors = summary.bound_predictors || [];
        const latestLog = logs[0] || {};
        const riskFlags = (risk.flags || []).slice(0, 3);
        container.style.display = 'block';
        container.innerHTML = `
            <div class="algorithm-chat-message assistant">
                <span>运营摘要</span>
                <p>版本 ${summary.versions?.active_version || '--'} / 共 ${summary.versions?.total || 0} 版｜绑定方案 ${predictors.length} 个｜风险 ${this.escapeHtml(risk.label || '--')}</p>
                <p>最近回测 V${summary.recent_backtest?.version || '--'}｜样本 ${backtest.sample_size || 0}｜出手 ${backtest.prediction_count || 0}｜跳过 ${backtest.skip_rate === null || backtest.skip_rate === undefined ? '--' : `${backtest.skip_rate}%`}｜字段完整率 ${backtest.field_completeness_rate === null || backtest.field_completeness_rate === undefined ? '--' : `${backtest.field_completeness_rate}%`}</p>
                <p>SPF 命中 ${spf.ratio_text || '--'}${spf.hit_rate === null || spf.hit_rate === undefined ? '' : `（${spf.hit_rate}%）`}｜ROI ${spf.roi === null || spf.roi === undefined ? '--' : `${spf.roi}%`}</p>
                <p>最近执行 ${latestLog.run_key || '--'}｜${latestLog.status || '暂无'}｜预测 ${latestLog.prediction_count || 0}/${latestLog.match_count || 0}${latestLog.fallback_used ? '｜已降级' : ''}</p>
                ${riskFlags.map((item) => `<p>提醒：${this.escapeHtml(item)}</p>`).join('')}
            </div>
        `;
    }

    async loadUserAlgorithmVersions(algorithmId) {
        const container = document.getElementById('userAlgorithmVersionList');
        if (!container || !algorithmId) {
            this.renderUserAlgorithmVersions([]);
            return;
        }
        try {
            const response = await fetch(`/api/user-algorithms/${algorithmId}/versions`, { credentials: 'include' });
            const data = await response.json();
            if (!response.ok) {
                throw new Error(data.error || '加载算法版本失败');
            }
            this.renderUserAlgorithmVersions(data || []);
        } catch (error) {
            this.renderUserAlgorithmVersions([]);
        }
    }

    renderUserAlgorithmVersions(versions) {
        const container = document.getElementById('userAlgorithmVersionList');
        if (!container) {
            return;
        }
        if (!versions.length) {
            container.style.display = 'none';
            container.innerHTML = '';
            return;
        }
        container.style.display = 'block';
        container.innerHTML = versions.map((item) => {
            const backtest = item.backtest || {};
            const spfStats = (backtest.hit_rate || {}).spf || {};
            const confidence = backtest.confidence_report || {};
            const previous = versions.find((candidate) => Number(candidate.version) === Number(item.version) - 1);
            const previousSpfStats = ((previous || {}).backtest || {}).hit_rate?.spf || {};
            const hitDelta = this.formatNumberDelta(spfStats.hit_rate, previousSpfStats.hit_rate, '%');
            const summary = backtest.sample_size
                ? `样本 ${backtest.sample_size}，可信度 ${confidence.label || '--'}，SPF ${spfStats.ratio_text || '--'}${hitDelta ? `，较上版 ${hitDelta}` : ''}`
                : '暂无回测';
            return `
                <div class="algorithm-chat-message assistant">
                    <span>V${item.version}${item.is_active ? ' 当前' : ''}</span>
                    <p>${this.escapeHtml(item.change_summary || '版本更新')}｜${this.escapeHtml(summary)}</p>
                    ${item.is_active ? '' : `<button class="btn ghost compact" data-action="activate-user-algorithm-version" data-version="${item.version}">启用</button>`}
                </div>
            `;
        }).join('');
        container.querySelectorAll('[data-action="activate-user-algorithm-version"]').forEach((button) => {
            button.addEventListener('click', () => this.activateUserAlgorithmVersion(Number(button.dataset.version)));
        });
    }

    async loadUserAlgorithmExecutionLogs(algorithmId) {
        const container = document.getElementById('userAlgorithmExecutionLogList');
        if (!container || !algorithmId) {
            this.renderUserAlgorithmExecutionLogs([]);
            return;
        }
        try {
            const response = await fetch(`/api/user-algorithms/${algorithmId}/execution-logs?limit=10`, { credentials: 'include' });
            const data = await response.json();
            if (!response.ok) {
                throw new Error(data.error || '加载算法执行日志失败');
            }
            this.renderUserAlgorithmExecutionLogs(data || []);
        } catch (error) {
            this.renderUserAlgorithmExecutionLogs([]);
        }
    }

    renderUserAlgorithmExecutionLogs(logs) {
        const container = document.getElementById('userAlgorithmExecutionLogList');
        if (!container) {
            return;
        }
        if (!logs.length) {
            container.style.display = 'none';
            container.innerHTML = '';
            return;
        }
        const statusLabels = {
            succeeded: '成功',
            fallback_succeeded: '降级成功',
            skipped: '已跳过',
            failed: '失败'
        };
        const fallbackLabels = {
            fail: '失败即停止',
            builtin_baseline: '内置基线',
            skip: '跳过'
        };
        container.style.display = 'block';
        container.innerHTML = `
            <div class="algorithm-chat-message assistant">
                <span>最近执行日志</span>
                ${logs.map((item) => `
                    <p>V${item.algorithm_version || '--'}｜${this.escapeHtml(item.run_key || '--')}｜${statusLabels[item.status] || item.status || '--'}｜预测 ${item.prediction_count || 0}/${item.match_count || 0}｜跳过 ${item.skip_count || 0}｜${item.duration_ms || 0}ms｜策略 ${fallbackLabels[item.fallback_strategy] || item.fallback_strategy || '--'}${item.fallback_used ? '｜已降级' : ''}${item.error_message ? `｜${this.escapeHtml(item.error_message)}` : ''}</p>
                `).join('')}
            </div>
        `;
    }

    formatNumberDelta(current, previous, suffix = '') {
        if (current === null || current === undefined || previous === null || previous === undefined) {
            return '';
        }
        const delta = Number(current) - Number(previous);
        if (!Number.isFinite(delta)) {
            return '';
        }
        if (Math.abs(delta) < 0.005) {
            return `持平`;
        }
        return `${delta > 0 ? '+' : ''}${delta.toFixed(2)}${suffix}`;
    }

    async activateUserAlgorithmVersion(version) {
        const algorithmId = document.getElementById('userAlgorithmId').value;
        if (!algorithmId || !version) {
            return;
        }
        try {
            const response = await fetch(`/api/user-algorithms/${algorithmId}/activate-version`, {
                method: 'POST',
                credentials: 'include',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ version })
            });
            const data = await response.json();
            if (!response.ok) {
                throw new Error(data.error || '启用算法版本失败');
            }
            await this.loadUserAlgorithms();
            this.selectUserAlgorithm(data.algorithm.id);
            this.renderUserAlgorithmVersions(data.versions || []);
            this.showUserAlgorithmResult('success', data.message || '算法版本已启用');
            this.updateLotteryForm();
        } catch (error) {
            this.showUserAlgorithmResult('error', error.message);
        }
    }

    async adjustUserAlgorithm() {
        const algorithmId = document.getElementById('userAlgorithmId').value;
        const mode = document.getElementById('userAlgorithmAdjustMode')?.value || 'conservative';
        if (!algorithmId) {
            this.showUserAlgorithmResult('error', '请先保存算法，再生成调参版本');
            return;
        }
        try {
            const response = await fetch(`/api/user-algorithms/${algorithmId}/adjust`, {
                method: 'POST',
                credentials: 'include',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ mode })
            });
            const data = await response.json();
            if (!response.ok) {
                throw new Error(data.error || '调参失败');
            }
            await this.loadUserAlgorithms();
            this.selectUserAlgorithm(data.algorithm.id);
            this.renderUserAlgorithmVersions(data.versions || []);
            this.renderUserAlgorithmValidation(data.validation || { valid: data.algorithm.status === 'validated', errors: [], warnings: [] });
            this.showUserAlgorithmResult('success', data.change_summary || data.message || '算法已生成新版本');
            this.updateLotteryForm();
        } catch (error) {
            this.showUserAlgorithmResult('error', error.message);
        }
    }

    async saveUserAlgorithm() {
        const algorithmId = document.getElementById('userAlgorithmId').value;
        let definition;
        try {
            definition = this.parseUserAlgorithmDefinition();
        } catch (error) {
            this.showUserAlgorithmResult('error', error.message);
            return;
        }
        const payload = {
            name: document.getElementById('userAlgorithmName').value.trim(),
            description: document.getElementById('userAlgorithmDescription').value.trim(),
            lottery_type: document.getElementById('userAlgorithmLotteryType').value || 'jingcai_football',
            definition
        };
        const url = algorithmId ? `/api/user-algorithms/${algorithmId}` : '/api/user-algorithms';
        const method = algorithmId ? 'PUT' : 'POST';
        try {
            const response = await fetch(url, {
                method,
                credentials: 'include',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });
            const data = await response.json();
            if (!response.ok) {
                throw new Error(data.error || '保存用户算法失败');
            }
            await this.loadUserAlgorithms();
            this.selectUserAlgorithm(data.algorithm.id);
            this.renderUserAlgorithmValidation(data.validation || { valid: data.algorithm.status === 'validated', errors: [], warnings: [] });
            this.updateLotteryForm();
        } catch (error) {
            this.showUserAlgorithmResult('error', error.message);
        }
    }

    async disableUserAlgorithm() {
        const algorithmId = document.getElementById('userAlgorithmId').value;
        if (!algorithmId) {
            this.showUserAlgorithmResult('error', '请先选择要停用的算法');
            return;
        }
        if (!window.confirm('确定停用这个算法吗？已绑定的预测方案将不能继续使用它。')) {
            return;
        }
        try {
            const response = await fetch(`/api/user-algorithms/${algorithmId}`, {
                method: 'DELETE',
                credentials: 'include'
            });
            const data = await response.json();
            if (!response.ok) {
                throw new Error(data.error || '停用用户算法失败');
            }
            await this.loadUserAlgorithms();
            this.resetUserAlgorithmForm();
            const affected = (data.affected_predictors || []).map((item) => item.name).filter(Boolean).join('，');
            this.showUserAlgorithmResult('success', `${data.message || '用户算法已停用'}${affected ? `\n仍在使用的预测方案：${affected}` : ''}`);
            this.updateLotteryForm();
        } catch (error) {
            this.showUserAlgorithmResult('error', error.message);
        }
    }

    userAlgorithmStatusLabel(status) {
        if (status === 'validated') {
            return '已校验';
        }
        if (status === 'disabled') {
            return '已停用';
        }
        return '草稿';
    }

    showUserAlgorithmResult(type, message) {
        const element = document.getElementById('userAlgorithmResult');
        if (!element) {
            return;
        }
        element.style.display = 'block';
        element.className = `test-result ${type}`;
        element.textContent = message;
    }

    hideUserAlgorithmResult() {
        const element = document.getElementById('userAlgorithmResult');
        if (element) {
            element.style.display = 'none';
            element.textContent = '';
        }
    }

    resetForm() {
        document.getElementById('lotteryType').value = 'pc28';
        document.getElementById('engineType').value = 'ai';
        this.formLotteryType = 'pc28';
        this.formStateByLottery = this.buildInitialFormStateByLottery();
        document.getElementById('predictorName').value = '';
        document.getElementById('predictionMethod').value = '';
        document.getElementById('algorithmSource').value = 'builtin';
        document.getElementById('algorithmKey').value = 'pc28_frequency_v1';
        document.getElementById('apiUrl').value = '';
        document.getElementById('modelName').value = '';
        document.getElementById('apiMode').value = 'auto';
        document.getElementById('apiKey').value = '';
        document.getElementById('historyWindow').value = '60';
        document.getElementById('temperature').value = '0.7';
        document.getElementById('dataInjectionMode').value = 'summary';
        const fallbackStrategy = document.getElementById('userAlgorithmFallbackStrategy');
        if (fallbackStrategy) {
            fallbackStrategy.value = 'fail';
        }
        document.getElementById('systemPrompt').value = '';
        document.getElementById('predictorEnabled').checked = true;
        document.getElementById('shareLevel').value = 'stats_only';
        this.updateLotteryForm({
            selectedTargets: ['number', 'big_small', 'odd_even', 'combo'],
            selectedPrimaryMetric: 'big_small',
            selectedProfitRuleId: 'pc28_netdisk',
            selectedProfitDefaultMetric: 'big_small',
            selectedHistoryWindow: 60,
            selectedAlgorithmKey: 'pc28_frequency_v1'
        });
        this.hideTestResult();
        this.hidePromptAssistantResult();
        this.clearExternalPromptTemplate();
    }

    renderPresetCards() {
        const container = document.getElementById('presetCards');
        const toggleButton = document.getElementById('togglePresetListBtn');
        const visibleCount = 3;
        const lotteryType = this.getFormLotteryType();
        const presets = lotteryType === 'jingcai_football' ? FOOTBALL_PREDICTOR_PRESETS : PREDICTOR_PRESETS;

        container.innerHTML = presets.map((preset, index) => `
            <article class="preset-card ${index >= visibleCount && !this.presetExpanded ? 'hidden' : ''}" data-preset-id="${preset.id}">
                <div class="preset-card-head">
                    <div>
                        <h4>${this.escapeHtml(preset.title)}</h4>
                        <p>${this.escapeHtml(preset.description)}</p>
                    </div>
                </div>
                <div class="preset-meta">
                    ${preset.tags.map((tag) => `<span class="tag">${this.escapeHtml(tag)}</span>`).join('')}
                    <span class="tag">${lotteryType === 'jingcai_football' ? `历史 ${preset.historyWindow} 场` : `历史 ${preset.historyWindow} 期`}</span>
                    <span class="tag">${preset.injectionMode === 'raw' ? '原始模式' : '摘要模式'}</span>
                    <span class="tag">${preset.apiMode === 'responses' ? 'Responses' : preset.apiMode === 'chat_completions' ? 'Chat Completions' : '自动模式'}</span>
                </div>
                <div class="preset-actions">
                    <button type="button" class="btn ghost compact" data-apply-preset="${preset.id}">一键填充</button>
                </div>
            </article>
        `).join('');

        container.querySelectorAll('[data-apply-preset]').forEach((button) => {
            button.addEventListener('click', () => this.applyPreset(button.dataset.applyPreset));
        });

        if (presets.length <= visibleCount) {
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
        const lotteryType = this.getFormLotteryType();
        const config = this.getLotteryConfig(lotteryType);
        const presetSource = lotteryType === 'jingcai_football' ? FOOTBALL_PREDICTOR_PRESETS : PREDICTOR_PRESETS;
        const preset = presetSource.find((item) => item.id === presetId);
        if (!preset) {
            return;
        }

        if (!document.getElementById('predictorName').value.trim()) {
            document.getElementById('predictorName').value = preset.title;
        }
        document.getElementById('predictionMethod').value = preset.method;
        document.getElementById('apiMode').value = preset.apiMode || 'auto';
        document.getElementById('historyWindow').value = String(preset.historyWindow);
        document.getElementById('temperature').value = String(preset.temperature);
        document.getElementById('dataInjectionMode').value = preset.injectionMode;
        document.getElementById('systemPrompt').value = preset.prompt;
        this.updateLotteryForm({
            selectedTargets: lotteryType === 'pc28'
                ? ['number', ...preset.targets.filter((item) => item !== 'number')]
                : preset.targets,
            selectedPrimaryMetric: preset.primaryMetric || config.defaultPrimaryMetric,
            selectedProfitRuleId: preset.profitRuleId || config.defaultProfitRuleId,
            selectedProfitDefaultMetric: preset.profitDefaultMetric
                || (lotteryType === 'pc28'
                    ? (['big_small', 'odd_even', 'combo', 'number'].includes(preset.primaryMetric) ? preset.primaryMetric : 'combo')
                    : (['spf', 'rqspf'].includes(preset.primaryMetric) ? preset.primaryMetric : config.defaultProfitMetric)),
            selectedHistoryWindow: preset.historyWindow
        });
        document.getElementById('historyWindow').value = String(preset.historyWindow);
        this.clearExternalPromptTemplate();
    }

    collectFormData() {
        this.enforceNumberTarget();
        const predictionTargets = ['targetNumber', 'targetBigSmall', 'targetOddEven', 'targetCombo']
            .map((id) => document.getElementById(id))
            .filter((input) => input && input.checked && input.dataset.targetKey)
            .map((input) => input.dataset.targetKey);

        return {
            lottery_type: document.getElementById('lotteryType').value || 'pc28',
            engine_type: document.getElementById('engineType').value || 'ai',
            algorithm_key: document.getElementById('algorithmKey').value || '',
            name: document.getElementById('predictorName').value.trim(),
            prediction_method: document.getElementById('predictionMethod').value.trim(),
            api_url: document.getElementById('apiUrl').value.trim(),
            model_name: document.getElementById('modelName').value.trim(),
            api_mode: document.getElementById('apiMode').value,
            api_key: document.getElementById('apiKey').value.trim(),
            history_window: Number(document.getElementById('historyWindow').value || 60),
            temperature: Number(document.getElementById('temperature').value || 0.7),
            data_injection_mode: document.getElementById('dataInjectionMode').value,
            user_algorithm_fallback_strategy: document.getElementById('userAlgorithmFallbackStrategy')?.value || 'fail',
            primary_metric: document.getElementById('primaryMetric').value,
            profit_rule_id: document.getElementById('profitRuleId').value,
            profit_default_metric: document.getElementById('profitDefaultMetric').value,
            system_prompt: document.getElementById('systemPrompt').value.trim(),
            enabled: document.getElementById('predictorEnabled').checked,
            share_level: document.getElementById('shareLevel').value,
            prediction_targets: predictionTargets
        };
    }

    syncProfitMetricOptions(lotteryType = this.getFormLotteryType(), preferredValue = null) {
        const select = document.getElementById('profitDefaultMetric');
        if (!select) {
            return;
        }

        const config = this.getLotteryConfig(lotteryType);
        if (!config.supportsProfitSimulation) {
            select.innerHTML = '<option value="">当前彩种暂不支持收益模拟</option>';
            select.disabled = true;
            return;
        }

        this.enforceNumberTarget();
        const options = this.getFormProfitMetricOptions(lotteryType, this.getSelectedFormTargets());
        const primaryMetric = document.getElementById('primaryMetric')?.value || this.getFormState(lotteryType).primaryMetric;
        const previousValue = preferredValue || select.value || this.getFormState(lotteryType).profitDefaultMetric;
        if (!options.length) {
            select.innerHTML = '<option value="">暂无可用收益玩法</option>';
            select.disabled = true;
            this.saveCurrentFormState(lotteryType);
            return;
        }

        select.innerHTML = options.map((item) => `
            <option value="${this.escapeHtml(item.key)}">${this.escapeHtml(item.label)}</option>
        `).join('');
        select.disabled = false;
        select.value = this.resolveFormProfitMetric(lotteryType, options, primaryMetric, previousValue);
        this.saveCurrentFormState(lotteryType);
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

    async testPredictorConfig() {
        const predictorId = document.getElementById('predictorId').value;
        const payload = this.collectFormData();
        payload.predictor_id = predictorId ? Number(predictorId) : null;
        const isMachineEngine = payload.engine_type === 'machine';

        const button = document.getElementById('testPredictorBtn');
        button.disabled = true;
        button.textContent = isMachineEngine ? '检查中...' : '测试中...';
        this.showTestResult('info', isMachineEngine ? '正在检查机器算法配置，请稍候...' : '正在测试模型连通性，请稍候...');

        try {
            const response = await fetch('/api/predictors/test', {
                method: 'POST',
                credentials: 'include',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });
            const data = await response.json();
            if (!response.ok) {
                throw new Error(data.error || '模型测试失败');
            }

            const preview = data.response_preview || '模型已返回响应';
            this.showTestResult('success', this.buildTestResultMessage(data, preview));
        } catch (error) {
            this.showTestResult('error', error.message);
        } finally {
            button.disabled = false;
            button.textContent = isMachineEngine ? '检查算法' : '测试模型';
        }
    }

    async checkPromptAssistant() {
        const predictorId = document.getElementById('predictorId').value;
        const payload = this.collectFormData();
        payload.predictor_id = predictorId ? Number(predictorId) : null;

        try {
            const response = await fetch('/api/predictors/prompt-check', {
                method: 'POST',
                credentials: 'include',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });
            const data = await response.json();
            if (!response.ok) {
                throw new Error(data.error || '提示词检查失败');
            }
            this.renderPromptAssistantResult({
                mode: 'check',
                ...data
            });
        } catch (error) {
            this.renderPromptAssistantResult({
                mode: 'check',
                risk_level: 'high',
                summary: error.message,
                issues: []
            });
        }
    }

    async optimizePromptAssistant() {
        const predictorId = document.getElementById('predictorId').value;
        const payload = this.collectFormData();
        payload.predictor_id = predictorId ? Number(predictorId) : null;

        const button = document.getElementById('optimizePromptBtn');
        button.disabled = true;
        button.textContent = '优化中...';
        this.renderPromptAssistantResult({
            mode: 'optimize',
            risk_level: 'low',
            summary: '正在调用 AI 生成优化建议，请稍候...',
            issues: []
        });

        try {
            const response = await fetch('/api/predictors/prompt-optimize', {
                method: 'POST',
                credentials: 'include',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });
            const data = await response.json();
            if (!response.ok) {
                throw new Error(data.error || 'AI 优化失败');
            }
            this.renderPromptAssistantResult({
                mode: 'optimize',
                ...data
            });
        } catch (error) {
            this.renderPromptAssistantResult({
                mode: 'optimize',
                risk_level: 'high',
                summary: error.message,
                issues: []
            });
        } finally {
            button.disabled = false;
            button.textContent = 'AI 优化';
        }
    }

    applyOptimizedPrompt() {
        const optimizedPrompt = document.getElementById('promptAssistantResult').dataset.optimizedPrompt || '';
        if (!optimizedPrompt) {
            alert('当前没有可应用的优化结果');
            return;
        }
        document.getElementById('systemPrompt').value = optimizedPrompt;
    }

    async buildExternalPromptTemplate() {
        const predictorId = document.getElementById('predictorId').value;
        const payload = this.collectFormData();
        payload.predictor_id = predictorId ? Number(predictorId) : null;

        const button = document.getElementById('buildExternalPromptBtn');
        const textarea = document.getElementById('externalPromptTemplate');
        if (!button || !textarea) {
            return;
        }

        button.disabled = true;
        button.textContent = '生成中...';

        try {
            const response = await fetch('/api/predictors/prompt-template', {
                method: 'POST',
                credentials: 'include',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });
            const data = await response.json();
            if (!response.ok) {
                throw new Error(data.error || '生成模板失败');
            }

            textarea.value = data.prompt_template || '';
            textarea.focus();
        } catch (error) {
            alert(error.message);
        } finally {
            button.disabled = false;
            button.textContent = '生成模板';
        }
    }

    async copyExternalPromptTemplate() {
        const button = document.getElementById('copyExternalPromptBtn');
        const textarea = document.getElementById('externalPromptTemplate');
        const promptTemplate = textarea?.value?.trim();
        if (!promptTemplate) {
            alert('请先生成网页 AI 帮写模板');
            return;
        }

        await this.copyTextWithFeedback(button, promptTemplate);
    }

    clearExternalPromptTemplate() {
        const textarea = document.getElementById('externalPromptTemplate');
        if (!textarea) {
            return;
        }
        textarea.value = '';
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

        if (this.currentPredictor.auto_paused) {
            await this.resumeAutoPausedPredictor(this.currentPredictor.id);
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

    async resumeAutoPausedPredictor(predictorId) {
        try {
            const response = await fetch(`/api/predictors/${predictorId}/resume-auto-pause`, {
                method: 'POST',
                credentials: 'include'
            });
            const data = await response.json();
            if (!response.ok) {
                throw new Error(data.error || '解除自动暂停失败');
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

    async manualSettleFootball() {
        if (!this.currentPredictorId || !this.currentPredictor || (this.currentPredictor.lottery_type || 'pc28') !== 'jingcai_football') {
            alert('当前仅竞彩足球方案支持手动结算');
            return;
        }

        const button = document.getElementById('footballManualSettleBtn');
        if (!button) {
            return;
        }

        const originalButtonHtml = button.innerHTML;
        button.disabled = true;
        button.textContent = '结算中...';
        this.showFootballActionResult('info', '正在执行手动结算，请稍候...');

        try {
            const response = await fetch(`/api/predictors/${this.currentPredictorId}/jingcai/settle`, {
                method: 'POST',
                credentials: 'include',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    run_key: this.currentPredictions?.[0]?.run_key || this.currentPredictor?.latest_issue_no || ''
                })
            });
            const data = await this.parseJsonSafely(response);
            if (!response.ok) {
                throw new Error(data.error || '手动结算失败');
            }
            this.showFootballActionResult('success', data.message || '手动结算执行完成');
        } catch (error) {
            this.showFootballActionResult('error', error.message);
        } finally {
            button.disabled = false;
            button.innerHTML = originalButtonHtml;
            if (this.currentPredictorId) {
                await this.loadPredictorDashboard(this.currentPredictorId);
            }
        }
    }

    async replayFootballScheduleByDate() {
        if (!this.currentPredictorId || !this.currentPredictor || (this.currentPredictor.lottery_type || 'pc28') !== 'jingcai_football') {
            alert('当前仅竞彩足球方案支持按日期回放/刷新赛程');
            return;
        }

        const button = document.getElementById('footballReplayBtn');
        const dateInput = document.getElementById('footballReplayDate');
        if (!button || !dateInput) {
            return;
        }

        const selectedDate = String(dateInput.value || '').trim() || this.getTodayDateValue();
        if (!/^\d{4}-\d{2}-\d{2}$/.test(selectedDate)) {
            this.showFootballActionResult('error', '请输入有效日期（YYYY-MM-DD）');
            return;
        }

        const originalButtonHtml = button.innerHTML;
        button.disabled = true;
        button.textContent = '刷新中...';
        this.showFootballActionResult('info', `正在回放/刷新 ${selectedDate} 赛程，请稍候...`);

        try {
            const response = await fetch(`/api/predictors/${this.currentPredictorId}/jingcai/replay`, {
                method: 'POST',
                credentials: 'include',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ date: selectedDate })
            });
            const data = await this.parseJsonSafely(response);
            if (!response.ok) {
                throw new Error(data.error || '按日期回放/刷新赛程失败');
            }
            const successMessage = data.warning
                ? `${data.message || `${selectedDate} 赛程回放/刷新完成`}；${data.warning}`
                : (data.message || `${selectedDate} 赛程回放/刷新完成`);
            this.showFootballActionResult('success', successMessage);
        } catch (error) {
            this.showFootballActionResult('error', error.message);
        } finally {
            button.disabled = false;
            button.innerHTML = originalButtonHtml;
            if (this.currentPredictorId) {
                await this.loadPredictorDashboard(this.currentPredictorId);
            }
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
        const footballManualSettleButton = document.getElementById('footballManualSettleBtn');
        const footballReplayButton = document.getElementById('footballReplayBtn');
        const footballReplayDate = document.getElementById('footballReplayDate');

        if (!toggleButton || !predictNowButton) {
            return;
        }

        const isFootballPredictor = Boolean(predictor) && (predictor.lottery_type || 'pc28') === 'jingcai_football';
        if (footballManualSettleButton) {
            footballManualSettleButton.style.display = isFootballPredictor ? 'inline-flex' : 'none';
            footballManualSettleButton.disabled = !isFootballPredictor;
        }
        if (footballReplayButton) {
            footballReplayButton.style.display = isFootballPredictor ? 'inline-flex' : 'none';
            footballReplayButton.disabled = !isFootballPredictor;
        }
        if (footballReplayDate) {
            footballReplayDate.style.display = isFootballPredictor ? '' : 'none';
            footballReplayDate.disabled = !isFootballPredictor;
            if (isFootballPredictor && !footballReplayDate.value) {
                footballReplayDate.value = this.getTodayDateValue();
            }
        }
        if (!isFootballPredictor) {
            this.hideFootballActionResult();
        }

        if (!predictor) {
            toggleButton.disabled = true;
            toggleButton.innerHTML = '<i class="bi bi-pause-circle"></i> 暂停方案';
            predictNowButton.disabled = true;
            return;
        }

        toggleButton.disabled = false;
        if (predictor.auto_paused) {
            toggleButton.innerHTML = '<i class="bi bi-play-circle"></i> 解除自动暂停';
        } else {
            toggleButton.innerHTML = predictor.enabled
                ? '<i class="bi bi-pause-circle"></i> 暂停方案'
                : '<i class="bi bi-play-circle"></i> 恢复方案';
        }
        predictNowButton.disabled = false;
    }

    predictorStatusMeta(predictor) {
        if (!predictor) {
            return {
                className: 'disabled',
                label: '停用',
                actionTitle: '恢复方案',
                iconClass: 'bi-play-circle'
            };
        }
        if (!predictor.enabled) {
            return {
                className: 'disabled',
                label: '停用',
                actionTitle: '恢复方案',
                iconClass: 'bi-play-circle'
            };
        }
        if (predictor.auto_paused) {
            return {
                className: 'paused',
                label: '自动暂停',
                actionTitle: '解除自动暂停',
                iconClass: 'bi-play-circle'
            };
        }
        return {
            className: 'enabled',
            label: '启用',
            actionTitle: '暂停方案',
            iconClass: 'bi-pause-circle'
        };
    }

    guardErrorCategoryLabel(category) {
        const mapping = {
            quota: '额度不足',
            auth: '鉴权失败',
            rate_limit: '限流',
            parse: '响应解析失败',
            transport: '网络连接失败',
            ai_error: 'AI 调用失败'
        };
        return mapping[String(category || '').trim()] || '--';
    }

    guardRecoveryHint(predictor) {
        if (!predictor) {
            return '--';
        }
        if (predictor.auto_paused) {
            return '修复后点击上方“解除自动暂停”';
        }
        if (!predictor.enabled) {
            return '先启用方案，再执行预测';
        }
        return '当前无需处理';
    }

    renderPredictorGuardNotice(predictor) {
        if (!predictor || !predictor.auto_paused) {
            return '';
        }
        const errorType = this.guardErrorCategoryLabel(predictor.last_ai_error_category);
        const errorTime = predictor.auto_paused_at || predictor.last_ai_error_at || '--';
        const errorText = predictor.last_ai_error_message || predictor.auto_pause_reason || '最近一次 AI 调用失败';
        return `<div class="warning-banner">方案已自动暂停：连续失败 ${this.escapeHtml(String(predictor.consecutive_ai_failures || 0))} 次，错误类型 ${this.escapeHtml(errorType)}，时间 ${this.escapeHtml(errorTime)}。${this.escapeHtml(errorText)}</div>`;
    }

    getTodayDateValue() {
        const now = new Date();
        const localDate = new Date(now.getTime() - now.getTimezoneOffset() * 60000);
        return localDate.toISOString().slice(0, 10);
    }

    async parseJsonSafely(response) {
        try {
            return await response.json();
        } catch (error) {
            return {};
        }
    }

    showFootballActionResult(type, message, endpoint = '') {
        const container = document.getElementById('footballActionResult');
        if (!container) {
            return;
        }

        const timestamp = new Date().toLocaleString('zh-CN', { hour12: false });
        if (type === 'error') {
            container.className = 'warning-banner';
            container.textContent = `操作失败：${message}`;
            container.style.display = 'block';
            return;
        }

        container.className = 'metric-hint';
        container.style.display = 'block';
        const title = type === 'success' ? '操作成功' : '执行中';
        container.innerHTML = `
            <div class="metric-hint-head">
                <div><strong>${this.escapeHtml(title)}</strong></div>
                <span class="tag">${this.escapeHtml(timestamp)}</span>
            </div>
            <p>${this.escapeHtml(message)}</p>
            ${endpoint ? `<p class="metric-hint-foot">接口：${this.escapeHtml(endpoint)}</p>` : ''}
        `;
    }

    hideFootballActionResult() {
        const container = document.getElementById('footballActionResult');
        if (!container) {
            return;
        }
        container.className = 'metric-hint empty-panel';
        container.textContent = '';
        container.style.display = 'none';
    }

    buildTestResultMessage(data, preview) {
        const details = [];
        if (data.api_mode) {
            details.push(`模式：${data.api_mode}`);
        }
        if (data.response_model) {
            details.push(`模型：${data.response_model}`);
        }
        if (data.finish_reason) {
            details.push(`finish_reason：${data.finish_reason}`);
        }
        if (data.latency_ms !== null && data.latency_ms !== undefined) {
            details.push(`耗时：${data.latency_ms}ms`);
        }

        return `测试成功\n${details.join(' | ')}\n响应预览：${preview}`;
    }

    showTestResult(type, message) {
        const result = document.getElementById('testPredictorResult');
        if (!result) {
            return;
        }
        result.className = `test-result ${type}`;
        result.textContent = message;
        result.style.display = 'block';
    }

    hideTestResult() {
        const result = document.getElementById('testPredictorResult');
        if (!result) {
            return;
        }
        result.textContent = '';
        result.className = 'test-result';
        result.style.display = 'none';
    }

    renderPromptAssistantResult(data) {
        const container = document.getElementById('promptAssistantResult');
        const applyButton = document.getElementById('applyOptimizedPromptBtn');
        if (!container || !applyButton) {
            return;
        }

        const riskLevel = data.risk_level || 'low';
        const issues = data.issues || [];
        const why = data.why || [];
        const optimizedPrompt = data.optimized_prompt || '';
        const staticAnalysis = data.static_analysis || null;
        const summary = data.summary || '暂无结果';
        const recommendedVariables = data.recommended_variables || [];
        const recommendedSnippets = data.recommended_snippets || [];

        const issueHtml = issues.length
            ? issues.map((item) => {
                if (typeof item === 'string') {
                    return `<li>${this.escapeHtml(item)}</li>`;
                }
                const suggestion = item.suggestion ? ` 建议：${item.suggestion}` : '';
                return `<li><strong>${this.escapeHtml(item.title || item.level || '提示')}</strong>：${this.escapeHtml((item.detail || item) + suggestion)}</li>`;
            }).join('')
            : '<li>暂无明显问题</li>';

        const whyHtml = why.length
            ? `<div class="assistant-block"><span class="mini-label">优化思路</span><ul>${why.map((item) => `<li>${this.escapeHtml(item)}</li>`).join('')}</ul></div>`
            : '';

        const staticHtml = staticAnalysis
            ? `<div class="assistant-block"><span class="mini-label">静态检查摘要</span><p>${this.escapeHtml(staticAnalysis.summary || '')}</p></div>`
            : '';

        const variableHtml = recommendedVariables.length
            ? `<div class="assistant-block"><span class="mini-label">推荐变量</span><ul>${recommendedVariables.map((item) => `<li><strong>${this.escapeHtml(`{{${item.name}}}`)}</strong>：${this.escapeHtml(item.reason || '')}</li>`).join('')}</ul></div>`
            : '';

        const snippetHtml = recommendedSnippets.length
            ? `<div class="assistant-block"><span class="mini-label">建议插入片段</span><pre>${this.escapeHtml(recommendedSnippets.map((item) => item.snippet).join('\n\n'))}</pre></div>`
            : '';

        const metaBits = [];
        if (data.api_mode) {
            metaBits.push(`模式：${data.api_mode}`);
        }
        if (data.response_model) {
            metaBits.push(`模型：${data.response_model}`);
        }
        if (data.finish_reason) {
            metaBits.push(`finish_reason：${data.finish_reason}`);
        }
        if (data.latency_ms !== null && data.latency_ms !== undefined) {
            metaBits.push(`耗时：${data.latency_ms}ms`);
        }

        container.dataset.optimizedPrompt = optimizedPrompt;
        container.className = `prompt-assistant-result ${riskLevel}`;
        container.style.display = 'block';
        container.innerHTML = `
            <div class="assistant-block">
                <span class="mini-label">检查结论</span>
                <p>${this.escapeHtml(summary)}</p>
                ${metaBits.length ? `<p class="field-hint">${this.escapeHtml(metaBits.join(' | '))}</p>` : ''}
            </div>
            <div class="assistant-block">
                <span class="mini-label">问题列表</span>
                <ul>${issueHtml}</ul>
            </div>
            ${whyHtml}
            ${variableHtml}
            ${snippetHtml}
            ${optimizedPrompt ? `<div class="assistant-block"><span class="mini-label">优化后提示词</span><pre>${this.escapeHtml(optimizedPrompt)}</pre></div>` : ''}
            ${staticHtml}
        `;

        applyButton.style.display = optimizedPrompt ? 'inline-flex' : 'none';
    }

    hidePromptAssistantResult() {
        const container = document.getElementById('promptAssistantResult');
        const applyButton = document.getElementById('applyOptimizedPromptBtn');
        if (container) {
            container.innerHTML = '';
            container.dataset.optimizedPrompt = '';
            container.className = 'prompt-assistant-result';
            container.style.display = 'none';
        }
        if (applyButton) {
            applyButton.style.display = 'none';
        }
    }

    targetLabel(target, lotteryType = this.currentLotteryType) {
        return this.metricMeta(target, lotteryType).label;
    }

    predictorExecutionLabel(predictor) {
        if (!predictor) {
            return '--';
        }
        return predictor.execution_label || predictor.algorithm_label || predictor.model_name || '--';
    }

    predictorEngineLabel(predictor) {
        if (!predictor) {
            return '--';
        }
        return predictor.engine_type_label || (predictor.engine_type === 'machine' ? '机器算法' : 'AI 模型');
    }

    predictorStyleDescription(predictor) {
        if (!predictor) {
            return '';
        }
        return predictor.execution_description || predictor.algorithm_description || '';
    }

    predictorMetaLabel(predictor) {
        if (!predictor) {
            return '--';
        }
        const engineLabel = this.predictorExecutionLabel(predictor);
        const methodLabel = predictor.prediction_method || (predictor.engine_type === 'machine' ? '内置机器算法' : '自定义策略');
        return `${engineLabel} · ${methodLabel}`;
    }

    renderPredictorExecutionPanel(predictor) {
        if (!predictor) {
            return '';
        }
        return `
            <div class="prediction-section">
                <div class="prediction-section-head">
                    <div>
                        <span class="mini-label">执行方式</span>
                        <p class="section-hint">这里说明当前方案到底是模型驱动，还是内置机器算法驱动。</p>
                    </div>
                </div>
                <div class="prediction-grid prediction-grid-compact">
                    ${this.renderCurrentPredictionCard('执行引擎', this.predictorEngineLabel(predictor))}
                    ${this.renderCurrentPredictionCard('核心方案', this.predictorExecutionLabel(predictor))}
                    ${this.renderCurrentPredictionCard('风格说明', this.predictorStyleDescription(predictor) || '--')}
                    ${this.renderCurrentPredictionCard('策略名', predictor.prediction_method || '--')}
                </div>
            </div>
        `;
    }

    predictionStatusLabel(status) {
        const mapping = {
            pending: '待开奖',
            settled: '已结算',
            expired: '过期未结算',
            failed: '执行失败'
        };
        return mapping[status] || status;
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

    renderPredictionResult(prediction) {
        if ((prediction.lottery_type || this.currentLotteryType || 'pc28') === 'jingcai_football') {
            return this.renderFootballPredictionResult(prediction);
        }
        const snapshot = this.buildPlaySnapshot(
            prediction.prediction_number,
            prediction.prediction_big_small,
            prediction.prediction_odd_even,
            prediction.prediction_combo
        );
        return `
            <div class="result-stack">
                <strong>${this.escapeHtml(snapshot.numberText || '--')}</strong>
                <span class="result-meta-text">大/小：${this.escapeHtml(snapshot.bigSmall || '--')} · 单/双：${this.escapeHtml(snapshot.oddEven || '--')} · 组合结果：${this.escapeHtml(snapshot.combo || '--')}</span>
                <span class="result-meta-text">组合投注：${this.escapeHtml(snapshot.pairTicket || '--')} · 排除对角组：${this.escapeHtml(snapshot.killGroup ? `杀${snapshot.killGroup}` : '--')}</span>
            </div>
        `;
    }

    renderActualResult(prediction) {
        if ((prediction.lottery_type || this.currentLotteryType || 'pc28') === 'jingcai_football') {
            return this.renderFootballActualResult(prediction);
        }
        if (prediction.status === 'pending') {
            return '<span class="hint-text">等待开奖</span>';
        }
        if (prediction.status === 'expired') {
            return '<span class="hint-text">超出可追溯窗口</span>';
        }
        if (prediction.actual_number === null || prediction.actual_number === undefined) {
            return '<span class="hint-text">无</span>';
        }

        const snapshot = this.buildPlaySnapshot(
            prediction.actual_number,
            prediction.actual_big_small,
            prediction.actual_odd_even,
            prediction.actual_combo
        );
        return `
            <div class="result-stack">
                <strong>${this.escapeHtml(snapshot.numberText || '--')}</strong>
                <span class="result-meta-text">大/小：${this.escapeHtml(snapshot.bigSmall || '--')} · 单/双：${this.escapeHtml(snapshot.oddEven || '--')} · 组合结果：${this.escapeHtml(snapshot.combo || '--')}</span>
                <span class="result-meta-text">组合投注：${this.escapeHtml(snapshot.pairTicket || '--')} · 排除对角组：${this.escapeHtml(snapshot.killGroup ? `杀${snapshot.killGroup}` : '--')}</span>
            </div>
        `;
    }

    renderHitSummary(prediction, options = {}) {
        if ((prediction.lottery_type || this.currentLotteryType || 'pc28') === 'jingcai_football') {
            return this.renderFootballHitSummary(prediction, options);
        }
        if (prediction.status === 'pending') {
            return '<span class="hint-text">未结算</span>';
        }
        if (prediction.status === 'expired') {
            return '<span class="hint-text">未补结算</span>';
        }
        if (prediction.status === 'failed') {
            return '<span class="hint-text">无结果</span>';
        }

        const items = this.buildHitItems(prediction);
        if (!items.length) {
            return '<span class="hint-text">无可统计结果</span>';
        }

        const hitCount = items.filter((item) => item.value === 1).length;
        const summaryText = `本期纳入统计 ${items.length} 项，命中 ${hitCount} 项`;

        return `
            <div class="hit-summary-block">
                <div class="hit-list ${options.detailed ? 'detailed' : ''}">
                    ${items.map((item) => `<span class="hit-pill ${this.hitClass(item.value)}">${this.escapeHtml(item.label)} ${this.hitMark(item.value)}</span>`).join('')}
                </div>
                ${options.detailed ? `<span class="result-meta-text">${this.escapeHtml(summaryText)}</span>` : ''}
            </div>
        `;
    }

    renderCurrentPredictionCard(label, value, hint = '') {
        return `
            <div class="prediction-card">
                <span class="mini-label">${this.escapeHtml(label)}</span>
                <strong>${this.escapeHtml(value)}</strong>
                ${hint ? `<span class="card-hint">${this.escapeHtml(hint)}</span>` : ''}
            </div>
        `;
    }

    renderCurrentSettlement(prediction, predictionSnapshot, actualSnapshot) {
        if (prediction.status === 'pending') {
            return `
                <div class="prediction-section">
                    <div class="prediction-section-head">
                        <div>
                            <span class="mini-label">结算状态</span>
                            <p class="section-hint">等待官方开奖后，会自动补出开奖快照和派生玩法命中情况。</p>
                        </div>
                    </div>
                    <div class="prediction-card">
                        <span class="mini-label">当前状态</span>
                        <strong>待开奖</strong>
                        <span class="card-hint">当前只能查看预测侧映射，不能判断命中。</span>
                    </div>
                </div>
            `;
        }

        if (prediction.status === 'expired') {
            return `
                <div class="prediction-section">
                    <div class="prediction-card">
                        <span class="mini-label">结算状态</span>
                        <strong>过期未结算</strong>
                        <span class="card-hint">当前记录已超出补结算窗口，无法生成完整开奖对照。</span>
                    </div>
                </div>
            `;
        }

        if (prediction.status === 'failed') {
            return `
                <div class="prediction-section">
                    <div class="prediction-card">
                        <span class="mini-label">结算状态</span>
                        <strong>执行失败</strong>
                        <span class="card-hint">本期没有有效预测结果，因此不存在开奖对照和命中统计。</span>
                    </div>
                </div>
            `;
        }

        return `
            <div class="prediction-section">
                <div class="prediction-section-head">
                    <div>
                        <span class="mini-label">最新结算对照</span>
                        <p class="section-hint">同一期开奖结果会同时展示基础标签和派生玩法，便于直接核对。</p>
                    </div>
                </div>
                <div class="prediction-compare-grid">
                    <div class="prediction-card">
                        <span class="mini-label">预测快照</span>
                        ${this.renderSnapshotDetails(predictionSnapshot)}
                    </div>
                    <div class="prediction-card">
                        <span class="mini-label">开奖快照</span>
                        ${this.renderSnapshotDetails(actualSnapshot)}
                    </div>
                    <div class="prediction-card">
                        <span class="mini-label">命中明细</span>
                        ${this.renderHitSummary(prediction, { detailed: true })}
                    </div>
                </div>
            </div>
        `;
    }

    renderSnapshotDetails(snapshot) {
        const rows = [
            { label: '单点', value: snapshot.numberText || '--' },
            { label: '大/小', value: snapshot.bigSmall || '--' },
            { label: '单/双', value: snapshot.oddEven || '--' },
            { label: '组合结果', value: snapshot.combo || '--' },
            { label: '组合投注', value: snapshot.pairTicket || '--' },
            { label: '排除对角组', value: snapshot.killGroup ? `杀${snapshot.killGroup}` : '--' }
        ];

        return `
            <div class="detail-list">
                ${rows.map((row) => `
                    <div class="detail-row">
                        <span class="detail-label">${this.escapeHtml(row.label)}</span>
                        <strong>${this.escapeHtml(row.value)}</strong>
                    </div>
                `).join('')}
            </div>
        `;
    }

    buildHitItems(prediction) {
        if ((prediction.lottery_type || this.currentLotteryType || 'pc28') === 'jingcai_football') {
            return this.buildFootballHitItems(prediction);
        }
        const items = [];

        if (prediction.hit_number !== null && prediction.hit_number !== undefined) {
            items.push({ label: '单点', value: prediction.hit_number });
        }
        if (prediction.hit_big_small !== null && prediction.hit_big_small !== undefined) {
            items.push({ label: '大/小', value: prediction.hit_big_small });
        }
        if (prediction.hit_odd_even !== null && prediction.hit_odd_even !== undefined) {
            items.push({ label: '单/双', value: prediction.hit_odd_even });
        }
        if (prediction.hit_combo !== null && prediction.hit_combo !== undefined) {
            items.push({ label: '组合结果', value: prediction.hit_combo });
        }

        const predictedGroup = this.deriveDoubleGroup(prediction.prediction_combo);
        const actualGroup = this.deriveDoubleGroup(prediction.actual_combo);
        if (predictedGroup && actualGroup) {
            items.push({ label: '组合分组', value: predictedGroup === actualGroup ? 1 : 0 });
        }

        const killGroup = this.deriveKillGroup(prediction.prediction_combo);
        if (killGroup && prediction.actual_combo) {
            items.push({ label: '排除统计', value: prediction.actual_combo !== killGroup ? 1 : 0 });
        }

        return items;
    }

    metricMeta(metricKey, lotteryType = this.currentLotteryType) {
        if (lotteryType === 'jingcai_football') {
            const footballMapping = {
                spf: {
                    label: '胜平负',
                    alias: '常规盘',
                    shortRule: '三分类',
                    description: '按 90 分钟含伤停补时赛果统计主胜、平局、客胜。',
                    formula: '预测胜平负 = 实际胜平负'
                },
                rqspf: {
                    label: '让球胜平负',
                    alias: '让球盘',
                    shortRule: '三分类',
                    description: '按官方让球数修正主队比分后统计胜平负。',
                    formula: '预测让球胜平负 = 实际让球胜平负'
                },
                spf_parlay: {
                    label: '胜平负二串一',
                    alias: '默认两场组合',
                    shortRule: '两场全中',
                    description: '先按置信度选出两场，再把两场胜平负票面组合成二串一。',
                    formula: '两场胜平负都命中才算命中'
                },
                rqspf_parlay: {
                    label: '让球胜平负二串一',
                    alias: '默认两场组合',
                    shortRule: '两场全中',
                    description: '先按置信度选出两场，再把两场让球胜平负票面组合成二串一。',
                    formula: '两场让球胜平负都命中才算命中'
                }
            };
            return footballMapping[metricKey] || {
                label: metricKey || '--',
                alias: null,
                shortRule: '未定义',
                description: '当前玩法说明暂未定义。',
                formula: '--'
            };
        }
        const mapping = {
            number: {
                label: '单点',
                alias: '和值号码',
                shortRule: '精确匹配',
                description: '按精确和值投注，直接对应 00-27 的单点玩法。',
                formula: '预测单点 = 开奖和值'
            },
            big_small: {
                label: '大/小',
                alias: null,
                shortRule: '二分类',
                description: '把和值分成小(00-13)与大(14-27)，按单独的大/小玩法结算。',
                formula: '预测大/小 = 开奖大/小'
            },
            odd_even: {
                label: '单/双',
                alias: null,
                shortRule: '二分类',
                description: '按和值奇偶结算，单独对应单/双玩法。',
                formula: '预测单/双 = 开奖单/双'
            },
            combo: {
                label: '组合投注',
                alias: '组合结果为：小双 / 小单 / 大双 / 大单',
                shortRule: '二组选一',
                description: '模型先给出组合结果，再映射到真实可下注的组合投注：小单/大双 或 大单/小双。',
                formula: '预测组合结果映射到对应组合投注票面'
            },
            double_group: {
                label: '组合分组统计',
                alias: '辅助统计口径',
                shortRule: '组别匹配',
                description: '把组合结果合并成两组：大双 / 小单 为“双组”，大单 / 小双 为“单组”。',
                formula: '预测组别 = 开奖组别'
            },
            kill_group: {
                label: '排除统计',
                alias: '旧显示名：杀组',
                shortRule: '排除统计',
                description: '按预测组合结果的对角组合做排除，只要实际没开出被杀组合，就算命中。',
                formula: '开奖结果 ≠ 被杀组合'
            }
        };

        return mapping[metricKey] || {
            label: metricKey || '--',
            alias: null,
            shortRule: '未定义',
            description: '当前玩法说明暂未定义。',
            formula: '--'
        };
    }

    renderFootballPredictionResult(prediction) {
        if (prediction.status === 'failed') {
            return `
                <div class="result-stack">
                    <strong>${this.escapeHtml(prediction.title || prediction.issue_no || '--')}</strong>
                    <span class="result-meta-text">${this.escapeHtml(this.buildFootballBatchFailureHint(prediction))}</span>
                </div>
            `;
        }
        const payload = prediction.prediction_payload || {};
        const marketSnapshot = prediction.market_snapshot || {};
        const spfText = this.buildFootballOutcomeText('spf', payload.spf, marketSnapshot);
        const rqspfText = this.buildFootballOutcomeText('rqspf', payload.rqspf, marketSnapshot);
        const spfOdds = this.buildFootballOddsText('spf', payload.spf, marketSnapshot);
        const rqspfOdds = this.buildFootballOddsText('rqspf', payload.rqspf, marketSnapshot);
        return `
            <div class="result-stack">
                <strong>${this.escapeHtml(prediction.title || prediction.issue_no || '--')}</strong>
                <span class="result-meta-text">胜平负：${this.escapeHtml(spfText || '--')} · 让球胜平负：${this.escapeHtml(rqspfText || '--')}</span>
                <span class="result-meta-text">赔率快照：SPF ${this.escapeHtml(spfOdds || '--')} · RQSPF ${this.escapeHtml(rqspfOdds || '--')}</span>
            </div>
        `;
    }

    renderFootballActualResult(prediction) {
        if (prediction.status === 'failed') {
            return `<span class="hint-text">${this.escapeHtml(prediction.batch_failure_reason || prediction.error_message || 'AI 子批次预测失败')}</span>`;
        }
        if (prediction.status === 'pending') {
            return '<span class="hint-text">等待赛果</span>';
        }
        const payload = prediction.actual_payload || {};
        return `
            <div class="result-stack">
                <strong>${this.escapeHtml(payload.score_text || '--')}</strong>
                <span class="result-meta-text">胜平负：${this.escapeHtml(payload.spf || '--')} · 让球胜平负：${this.escapeHtml(payload.rqspf || '--')}</span>
            </div>
        `;
    }

    renderFootballHitSummary(prediction, options = {}) {
        if (prediction.status === 'failed') {
            return `<span class="hint-text">${this.escapeHtml(prediction.batch_failure_label ? `子批次 ${prediction.batch_failure_label} 失败` : '执行失败')}</span>`;
        }
        if (prediction.status === 'pending') {
            return '<span class="hint-text">未结算</span>';
        }
        const items = this.buildFootballHitItems(prediction);
        if (!items.length) {
            return '<span class="hint-text">无可统计结果</span>';
        }
        const hitCount = items.filter((item) => item.value === 1).length;
        const summaryText = `本场纳入统计 ${items.length} 项，命中 ${hitCount} 项`;
        return `
            <div class="hit-summary-block">
                <div class="hit-list ${options.detailed ? 'detailed' : ''}">
                    ${items.map((item) => `<span class="hit-pill ${this.hitClass(item.value)}">${this.escapeHtml(item.label)} ${this.hitMark(item.value)}</span>`).join('')}
                </div>
                ${options.detailed ? `<span class="result-meta-text">${this.escapeHtml(summaryText)}</span>` : ''}
            </div>
        `;
    }

    buildFootballHitItems(prediction) {
        const hitPayload = prediction.hit_payload || {};
        const items = [];
        if (hitPayload.spf !== null && hitPayload.spf !== undefined) {
            items.push({ label: '胜平负', value: hitPayload.spf });
        }
        if (hitPayload.rqspf !== null && hitPayload.rqspf !== undefined) {
            items.push({ label: '让球胜平负', value: hitPayload.rqspf });
        }
        return items;
    }

    buildPlaySnapshot(number, bigSmall, oddEven, combo) {
        const resolvedCombo = combo || this.buildCombo(bigSmall, oddEven);
        const numberText = number !== null && number !== undefined
            ? String(number).padStart(2, '0')
            : null;
        const doubleGroup = this.deriveDoubleGroup(resolvedCombo);
        const pairTicket = this.derivePairTicket(resolvedCombo);
        const killGroup = this.deriveKillGroup(resolvedCombo);

        return {
            numberText,
            bigSmall: bigSmall || null,
            oddEven: oddEven || null,
            combo: resolvedCombo || null,
            doubleGroup,
            pairTicket,
            killGroup
        };
    }

    buildCombo(bigSmall, oddEven) {
        if (!bigSmall || !oddEven) {
            return null;
        }
        if (!['大', '小'].includes(bigSmall) || !['单', '双'].includes(oddEven)) {
            return null;
        }
        return `${bigSmall}${oddEven}`;
    }

    deriveDoubleGroup(combo) {
        if (!combo) {
            return null;
        }
        if (combo === '大单' || combo === '小双') {
            return '单组';
        }
        if (combo === '大双' || combo === '小单') {
            return '双组';
        }
        return null;
    }

    derivePairTicket(combo) {
        const group = this.deriveDoubleGroup(combo);
        if (!group) {
            return null;
        }
        return group === '单组' ? '大单 / 小双' : '大双 / 小单';
    }

    deriveKillGroup(combo) {
        const mapping = {
            大单: '小双',
            小双: '大单',
            大双: '小单',
            小单: '大双'
        };
        return mapping[combo] || null;
    }

    formatDoubleGroupText(snapshot) {
        if (!snapshot.doubleGroup) {
            return '--';
        }
        if (!snapshot.pairTicket) {
            return snapshot.doubleGroup;
        }
        return `${snapshot.doubleGroup}（${snapshot.pairTicket}）`;
    }

    async copyPublicUrl(button) {
        const url = button?.dataset?.url;
        if (!url || button.disabled) {
            return;
        }
        await this.copyTextWithFeedback(button, url);
    }

    async copyTextWithFeedback(button, text) {
        if (!button || !text || button.disabled) {
            return false;
        }

        const originalText = button.textContent;

        try {
            if (navigator.clipboard && navigator.clipboard.writeText) {
                await navigator.clipboard.writeText(text);
            } else {
                const input = document.createElement('input');
                input.value = text;
                document.body.appendChild(input);
                input.select();
                document.execCommand('copy');
                input.remove();
            }
            button.textContent = '已复制';
            return true;
        } catch (error) {
            console.error('Failed to copy text:', error);
            button.textContent = '复制失败';
            return false;
        } finally {
            window.setTimeout(() => {
                button.textContent = originalText;
            }, 1500);
        }
    }

    hitClass(value) {
        if (value === 1) return 'hit';
        if (value === 0) return 'miss';
        return 'unknown';
    }

    hitMark(value) {
        if (value === 1) return '✓';
        if (value === 0) return '✗';
        return '-';
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

    formatUsd(value) {
        const amount = Number(value || 0);
        return `${amount.toFixed(2)} USDT`;
    }

    formatSignedUsd(value) {
        const amount = Number(value || 0);
        const prefix = amount > 0 ? '+' : '';
        return `${prefix}${amount.toFixed(2)} USDT`;
    }

    formatOdds(value) {
        if (value === null || value === undefined || value === '') {
            return '--';
        }
        return `${Number(value).toFixed(2)} 倍`;
    }

    profitResultClass(resultType) {
        if (resultType === 'hit') return 'hit';
        if (resultType === 'refund') return 'refund';
        if (resultType === 'miss') return 'miss';
        return 'unknown';
    }

    profitValueClass(value, label = '') {
        const amount = Number(value || 0);
        if (label === '当前玩法' || label === '赔率盘' || label === '总下注' || label === '命中 / 回本 / 未中') {
            return '';
        }
        if (amount > 0) return 'profit-positive';
        if (amount < 0) return 'profit-negative';
        return 'profit-neutral';
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
